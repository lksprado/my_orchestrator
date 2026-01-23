from datetime import datetime, timedelta

from airflow.decorators import dag, task

from include.utils.db_interactors import send_single_batch_df_to_db
from include.vide.src.main import extraction_featured_books

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="vide_homepage_etl",
    default_args=default_args,
    description="ETL for VIde",
    start_date=datetime(2026, 1, 21),
    schedule="00 20 * * *",
    catchup=False,
    tags=["livros"],
)
def vide_homepage_etl():
    raw_folder = "/usr/local/airflow/mylake/raw/vide/destaques"

    @task
    def make_request():
        return extraction_featured_books(output_dir=raw_folder)

    @task
    def load_raw():
        send_single_batch_df_to_db(
            dir=raw_folder,
            file_extension="json",
            schema="raw",
            table_name="vide_raw_home_featured",
        )

    extraction = make_request()
    load = load_raw()

    extraction >> load


dag = vide_homepage_etl()
