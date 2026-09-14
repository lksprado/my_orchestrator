"""Energia solar (portal APsystems), incremental por data.

extract e transform vêm do my_ingestion (load: none): high-water mark em
raw_apsystem, JSON por dia via Selenium remoto e os CSVs diário e horário. A carga
fica aqui, como na dag_weather_etl: CSV -> staging -> upsert -> JSONs movidos
para bronze/solar_project -> drop da staging.
"""

from airflow.sdk import task
from core import PipelineConfig
from pipelines.energia.solar.solar_etl import CONFIG_FILE, ETLS

from include.utils.db_interactors import execute_query, move_files_after_loading, send_csv_df_to_db
from include.utils.etl_dag import etl_group, source_dag

# Schema vem do solar_config.yml (raw_apsystem, o que o my_analytics lê).
SCHEMA = PipelineConfig.from_yaml(CONFIG_FILE, "daily_energy", criar_dirs=False).db_schema

UPSERT = {
    "daily_energy": f"""
        INSERT INTO {SCHEMA}.solar_daily_energy (date, duration, total, co2, max)
        SELECT date, duration, total, co2, max FROM {SCHEMA}.stg_solar_daily_energy
        ON CONFLICT (date) DO UPDATE SET
            duration = EXCLUDED.duration,
            total = EXCLUDED.total,
            co2 = EXCLUDED.co2,
            max = EXCLUDED.max;
    """,
    "hourly_energy": f"""
        INSERT INTO {SCHEMA}.solar_hourly_energy (datetime, energy)
        SELECT datetime, energy FROM {SCHEMA}.stg_solar_hourly_energy
        ON CONFLICT (datetime) DO UPDATE SET energy = EXCLUDED.energy;
    """,
}
# Pré-requisito: PKs em raw_apsystem.solar_daily_energy(date) e solar_hourly_energy(datetime).


def _cfg(entidade: str) -> PipelineConfig:
    return PipelineConfig.from_yaml(CONFIG_FILE, entidade)


with source_dag(
    "solar",
    schedule="0 0 * * *",
    tags=["atibaia"],
    description="Energia solar: geração diária e horária",
) as dag:
    daily = etl_group(CONFIG_FILE, ETLS, "daily_energy")
    hourly = etl_group(CONFIG_FILE, ETLS, "hourly_energy")

    @task.short_circuit
    def has_bronze() -> bool:
        # Sem datas novas o transform não grava CSV (e o anterior foi removido
        # na última carga): nada a carregar.
        return all(_cfg(e).bronze_filepath.is_file() for e in UPSERT)

    @task
    def load_staging():
        for cfg in (_cfg(e) for e in UPSERT):
            send_csv_df_to_db(cfg.bronze_filepath, f"stg_{cfg.db_table}", SCHEMA)

    @task
    def upsert_raw():
        for sql in UPSERT.values():
            execute_query(sql)

    @task
    def clear_staging():
        from settings import settings

        move_files_after_loading(
            _cfg("daily_energy").landing_dir, settings.lake_root / "bronze" / "solar_project"
        )

    @task
    def drop_staging():
        for entidade in UPSERT:
            execute_query(f"DROP TABLE IF EXISTS {SCHEMA}.stg_{_cfg(entidade).db_table};")

    (
        daily
        >> hourly
        >> has_bronze()
        >> load_staging()
        >> upsert_raw()
        >> clear_staging()
        >> drop_staging()
    )
