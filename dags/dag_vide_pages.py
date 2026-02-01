from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from include.utils.db_interactors import send_single_batch_df_to_db
from include.vide.src.main import extract_link_content

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="vide_pages_etl",
    default_args=default_args,
    description="ETL for VIde",
    start_date=datetime(2026, 1, 23),
    schedule="5 20 * * FRI",
    catchup=False,
    tags=["livros"],
)
def vide_pages_etl():
    raw_folder = "/usr/local/airflow/mylake/raw/vide/paginas"
    config_file = "/usr/local/airflow/include/vide/src/config.yml"

    @task
    def make_request():
        return extract_link_content(output_dir=raw_folder, config_file=config_file)

    @task
    def load_raw():
        send_single_batch_df_to_db(
            dir=raw_folder,
            file_extension="json",
            schema="raw",
            table_name="vide_raw_category_pages",
        )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dag_dbt_vide",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    extraction = make_request()
    load = load_raw()

    extraction >> load >> trigger_dbt


dag = vide_pages_etl()
