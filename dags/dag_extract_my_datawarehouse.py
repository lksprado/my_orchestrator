"""Exporta marts de energia e inflação do the_dw para CSV em gold/.

Os models foram renomeados no the_dw; os nomes dos CSVs continuam os antigos
para não quebrar quem consome os arquivos. As colunas seguem os models atuais.
"""

import logging
import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

GOLD_DIR = "/usr/local/airflow/mylake/gold/"

# arquivo em gold/ -> tabela no the_dw
EXPORTS = {
    "mrt_energia_clima": "marts_energy.solar_energy_daily_weather_conditions",
    "mrt_energia_hora": "marts_energy.solar_energy_hourly_generation",
    "mrt_inflation": "presentation_inflation.inflation",
}

logger = logging.getLogger(__name__)


@dag(
    dag_id="extract_postgres_mydatawarehouse",
    start_date=datetime(2025, 11, 17),
    schedule="30 3 * * *",
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
    tags=["gold"],
)
def extract_pipeline():
    @task
    def export(filename: str, table: str) -> str:
        engine = PostgresHook(postgres_conn_id="postgres_dw").get_sqlalchemy_engine()
        df = pd.read_sql(f"SELECT * FROM {table}", con=engine)
        filepath = os.path.join(GOLD_DIR, f"{filename}.csv")
        df.to_csv(filepath, sep=";", index=False)
        logger.info(f"{table} -> {filepath} ({len(df)} linhas)")
        return filepath

    for filename, table in EXPORTS.items():
        export.override(task_id=f"export_{filename}")(filename, table)


dag = extract_pipeline()
