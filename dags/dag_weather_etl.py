"""Clima (OpenWeather day_summary), incremental por data.

Piloto do padrão my_ingestion com load: none — extract e transform vêm do
pipelines.clima.openweather.openweather_etl (high-water mark, datas faltantes e
all_dfs.csv); a carga fica aqui: CSV -> staging -> upsert em
raw_openweather.openweather_daily -> JSONs movidos para bronze/weather_project.
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor

from core import build_etl
from core.incremental import read_dates_csv
from include.utils.db_interactors import (
    execute_query,
    move_files_after_loading,
    send_csv_df_to_db,
)
from pipelines.clima.openweather.openweather_etl import CONFIG_FILE, ETLS

ENTIDADE = "daily"
STG_TABLE = "raw_openweather.stg_openweather_daily"
RAW_TABLE = "raw_openweather.openweather_daily"

_COLS = """
    date,
    cloud_cover_afternoon,
    humidity_afternoon,
    precipitation_total,
    temperature_min,
    temperature_max,
    temperature_afternoon,
    temperature_night,
    temperature_evening,
    temperature_morning,
    pressure_afternoon,
    wind_max_speed,
    wind_max_direction
"""

query_upsert = f"""
    INSERT INTO {RAW_TABLE} ({_COLS})
    SELECT {_COLS} FROM {STG_TABLE}
    ON CONFLICT (date) DO UPDATE SET
        cloud_cover_afternoon = EXCLUDED.cloud_cover_afternoon,
        humidity_afternoon = EXCLUDED.humidity_afternoon,
        precipitation_total = EXCLUDED.precipitation_total,
        temperature_min = EXCLUDED.temperature_min,
        temperature_max = EXCLUDED.temperature_max,
        temperature_afternoon = EXCLUDED.temperature_afternoon,
        temperature_night = EXCLUDED.temperature_night,
        temperature_evening = EXCLUDED.temperature_evening,
        temperature_morning = EXCLUDED.temperature_morning,
        pressure_afternoon = EXCLUDED.pressure_afternoon,
        wind_max_speed = EXCLUDED.wind_max_speed,
        wind_max_direction = EXCLUDED.wind_max_direction;
"""
# Pré-requisito no banco do ambiente:
# ALTER TABLE raw_openweather.openweather_daily
#     ADD CONSTRAINT openweather_date_pk PRIMARY KEY (date);

query_drop_stg = f"DROP TABLE IF EXISTS {STG_TABLE};"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _etl():
    return build_etl(CONFIG_FILE, ENTIDADE, ETLS[ENTIDADE])


@dag(
    dag_id="weather_etl",
    default_args=default_args,
    description="ETL for Weather Data",
    start_date=datetime(2025, 9, 21),
    schedule=None,  # em validação, ver README (seção Validação dos pilotos); original "0 1 * * *"
    catchup=False,
    tags=["atibaia"],
)
def weather_etl():
    check_api_availability = HttpSensor(
        task_id="check_api",
        http_conn_id="openweather_conn",
        endpoint="data/3.0/onecall/day_summary",
        request_params={
            "lat": -23.137,
            "lon": -46.5547861,
            "date": "{{ ds }}",
            # do .env (settings.openweather_api_key usa a mesma variável)
            "appid": os.environ.get("OPENWEATHER_API_KEY", ""),
        },
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    @task
    def extract():
        # high-water mark em RAW_TABLE -> missing_dates.csv -> um JSON por dia
        _etl().extract()

    @task.short_circuit
    def has_new_dates() -> bool:
        cfg = _etl().cfg
        dates = read_dates_csv(cfg.landing_dir / cfg.options["control_file"])
        return bool(dates)

    @task
    def transform():
        _etl().transform()

    @task
    def load_staging():
        schema, table = STG_TABLE.split(".")
        send_csv_df_to_db(_etl().cfg.bronze_filepath, table, schema)

    @task
    def upsert_raw():
        execute_query(query_upsert)

    @task
    def clear_staging():
        from settings import settings

        cfg = _etl().cfg
        bronze_dir = settings.lake_root / "bronze" / "weather_project"
        move_files_after_loading(cfg.landing_dir, bronze_dir)

    @task
    def drop_staging():
        execute_query(query_drop_stg)

    (
        check_api_availability
        >> extract()
        >> has_new_dates()
        >> transform()
        >> load_staging()
        >> upsert_raw()
        >> clear_staging()
        >> drop_staging()
    )


dag = weather_etl()
