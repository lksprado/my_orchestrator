"""Exporta as tabelas de apresentação do domínio demodados para CSV em gold/.

Lê do the_dw (schema presentation_demodados) pela connection postgres_dw.
"""

import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

SCHEMA = "presentation_demodados"
GOLD_DIR = "/usr/local/airflow/mylake/gold/"


@dag(
    dag_id="extract_postgres_demodados",
    start_date=datetime(2026, 6, 17),
    schedule="30 4 * * *",
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
    tags=["demodados"],
)
def extract_pipeline():
    @task
    def get_tables() -> list[str]:
        hook = PostgresHook(postgres_conn_id="postgres_dw")
        rows = hook.get_records(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %(schema)s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            parameters={"schema": SCHEMA},
        )
        return [row[0] for row in rows]

    @task
    def export_table(table: str) -> str:
        engine = PostgresHook(postgres_conn_id="postgres_dw").get_sqlalchemy_engine()
        df = pd.read_sql(f"SELECT * FROM {SCHEMA}.{table}", con=engine)
        filepath = os.path.join(GOLD_DIR, f"{table}.csv")
        df.to_csv(filepath, sep=";", index=False)
        return filepath

    export_table.expand(table=get_tables())


dag = extract_pipeline()
