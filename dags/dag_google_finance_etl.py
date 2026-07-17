from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable
from pendulum import datetime

# -----------------------------
# Imports do projeto (submódulo include/finance)
# -----------------------------
from include.finance.src.google_finance_etl import run_google_finance_etl
from include.finance.src.utils.tools import load_csvs_to_raw

# -----------------------------
# PATHS / CONFIG
# -----------------------------
FINANCE_DIR = Path("/usr/local/airflow/include/finance")
CREDENTIALS = FINANCE_DIR / "secrets" / "finances-py-a58b0d2a733f.json"
CONFIG_PATH = FINANCE_DIR / "config.yml"
OUTPUT_DIR = Path("/usr/local/airflow/mylake/bronze/investments/google")

# -----------------------------
# DAG CONFIG
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 17),
    "retries": 2,
}


@dag(
    dag_id="google_finance_etl",
    default_args=default_args,
    description="Ingesta abas do Google Sheets de investimentos para o schema raw",
    schedule="0 3 * * *",
    catchup=False,
    tags=["finance"],
)
def google_finance_etl():

    # -----------------------------
    # TASKS
    # -----------------------------
    @task
    def extract() -> str:
        # Variable.get em runtime (não no corpo da @dag) para não bater no
        # metadata DB a cada parse do dag-processor.
        run_google_finance_etl(
            output_dir=OUTPUT_DIR,
            credentials=str(CREDENTIALS),
            sheet_url=Variable.get("google_finance_sheet_url"),
            config_path=CONFIG_PATH,
        )
        return str(OUTPUT_DIR)

    @task
    def load(output_dir: str):
        db_url = PostgresHook(postgres_conn_id="postgres_dw").get_uri()
        load_csvs_to_raw(
            input_dir=output_dir,
            db_url=db_url,
            pattern="google_*.csv",
            strip_prefix="google_",
        )

    # -----------------------------
    # FLUXO
    # -----------------------------
    load(extract())


# Instancia a DAG
dag = google_finance_etl()
