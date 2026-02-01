from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

# -----------------------------
# Imports do projeto
# -----------------------------
from include.inflation.src.main import search_products
from include.utils.db_interactors import send_single_batch_df_to_db

# -----------------------------
# DAG CONFIG
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 31),
    "retries": 0,
}


@dag(
    dag_id="inflation_etl",
    default_args=default_args,
    description="EL for Atacadao",
    schedule="0 0 28 * *",
    catchup=False,
    tags=["inflation"],
)
def inflation_etl():
    output_folder = Path("/usr/local/airflow/mylake/raw/inflation/atacadao")
    store_file = Path("/usr/local/airflow/mylake/inflation/src/store_config.yml")
    products_file = Path("/usr/local/airflow/mylake/inflation/src/products_config.yml")

    @task
    def extraction():
        search_products(
            store_config_file=store_file,
            output_dir=output_folder,
            products_config_file=products_file,
        )

    @task
    def loading():
        send_single_batch_df_to_db(
            dir=output_folder,
            file_extension="csv",
            schema="raw",
            table_name="atacadao_raw",
            how="replace",
        )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dag_dbt_inflation",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    # -----------------------------
    # FLUXO
    # -----------------------------

    extraction_task = extraction()
    loading_task = loading()

    extraction_task >> loading_task >> trigger_dbt


# Instancia a DAG
dag = inflation_etl()
