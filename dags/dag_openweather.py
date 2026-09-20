"""Clima (OpenWeather day_summary), incremental por data.

As três etapas vêm do pipelines.clima.openweather.openweather_etl: high-water
mark em raw_openweather.openweather_daily -> datas faltantes -> um JSON por dia
no landing -> all_dfs.csv -> full refresh da tabela. O landing acumula os JSONs —
nada é movido depois da carga, porque é dele que o transform reconstrói a tabela.

Não usa include/utils/etl_dag.py porque tem sensor da API e short-circuit quando
não há data nova.
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from core import build_etl
from core.incremental import read_dates_csv
from pipelines.clima.openweather.openweather_etl import CONFIG_FILE, ETLS

ENTIDADE = "daily"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _etl():
    return build_etl(CONFIG_FILE, ENTIDADE, ETLS[ENTIDADE])


@dag(
    dag_id="openweather__weather__ingestion",
    default_args=default_args,
    description="ETL for Weather Data",
    start_date=datetime(2025, 9, 21),
    schedule="0 1 * * *",
    catchup=False,
    tags=["atibaia"],
)
def weather_etl():
    check_api_availability = HttpSensor(
        task_id="check_api",
        http_conn_id="openweather_conn",
        endpoint="data/3.0/onecall/day_summary",
        request_params={
            # do .env (o settings do my_ingestion usa as mesmas variáveis)
            "lat": os.environ.get("OPENWEATHER_LAT", ""),
            "lon": os.environ.get("OPENWEATHER_LON", ""),
            "date": "{{ ds }}",
            "appid": os.environ.get("OPENWEATHER_API_KEY", ""),
        },
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    @task
    def extract():
        # high-water mark na tabela -> missing_dates.csv -> um JSON por dia
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
    def load():
        # Full refresh: TRUNCATE + COPY do all_dfs.csv, numa transação.
        _etl().load()

    (
        check_api_availability
        >> extract()
        >> has_new_dates()
        >> transform()
        >> load()
    )


dag = weather_etl()
