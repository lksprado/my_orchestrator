"""
Smoke test do wiring com o my_ingestion e o my_analytics.
"""

import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DBT_PROJECT = Path("/usr/local/airflow/dbt/my_analytics/dbt_project.yml")


@dag(
    dag_id="smoke_my_ingestion",
    schedule="@hourly",
    start_date=datetime(2026, 9, 13),
    catchup=False,
    default_args={"retries": 2},
    tags=["infra"],
)
def smoke_my_ingestion():
    @task
    def check_imports_and_settings() -> dict:
        import core
        import pandas
        import pipelines
        import sqlalchemy
        from settings import settings

        info = {
            "core": core.__file__,
            "pipelines": pipelines.__file__,
            "env": settings.env,
            "lake_root": str(settings.lake_root),
            "seeds_root": str(settings.seeds_root),
            "db_host": settings.db_target.host,
            "db_name": settings.db_target.name,
            "pandas": pandas.__version__,
            "sqlalchemy": sqlalchemy.__version__,
        }
        for k, v in info.items():
            logger.info("%s = %s", k, v)
        if not settings.lake_root.is_dir():
            raise FileNotFoundError(f"LAKE_ROOT não montado: {settings.lake_root}")
        if not settings.seeds_root.is_dir():
            raise FileNotFoundError(f"SEEDS_ROOT não montado: {settings.seeds_root}")
        return info

    @task
    def check_database():
        from core import PostgresClient

        df = PostgresClient(log=logger).read_sql("SELECT current_database() AS db")
        logger.info("Conectado em %s", df["db"].iloc[0])

    @task
    def check_credentials():
        """Credenciais que são arquivo: o .env aponta, mas o bind pode faltar.

        settings.google_credentials_file é None quando a chave não está no .env,
        e gspread só reclama na hora do extract, com "No such file or directory:
        'None'". Aqui isso vira uma smoke vermelha logo depois do deploy.
        """
        import json

        from settings import settings

        caminho = settings.google_credentials_file
        if caminho is None:
            raise ValueError("GOOGLE_CREDENTIALS_FILE não está no .env")
        if not caminho.is_file():
            raise FileNotFoundError(f"service account não montada: {caminho}")
        conta = json.loads(caminho.read_text(encoding="utf-8"))["client_email"]
        logger.info("service account %s em %s", conta, caminho)

    @task
    def check_dbt_project():
        if not DBT_PROJECT.is_file():
            raise FileNotFoundError(f"my_analytics não montado: {DBT_PROJECT}")
        logger.info("my_analytics em %s", DBT_PROJECT.parent)

    check_imports_and_settings() >> [
        check_database(),
        check_dbt_project(),
        check_credentials(),
    ]


dag = smoke_my_ingestion()
