"""Google Books: dados dos livros da seed_biblioteca (a cada 60 dias).

DAG do Airflow montada por include/utils/etl_dag.py. O extract lê a seed
seeds.seed_biblioteca (dbt) e só busca na API os livros sem JSON no landing;
os demais vêm do landing no transform.

Agenda por intervalo, não cron: ``timedelta`` vira ``DeltaTriggerTimetable`` no
Airflow 3. Com ``catchup=False`` a primeira run sai ao despausar (start_date já
passado) e as seguintes a cada 60 dias a partir dela.
"""

from datetime import datetime, timedelta

from pipelines.livros.google_books.google_books_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "google_books__livros__ingestion",
    schedule=timedelta(days=60),
    start_date=datetime(2026, 9, 30),
    tags=["books"],
    description="Google Books: dados dos livros da seed_biblioteca",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "livros")
