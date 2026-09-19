"""Vide Editorial: livros em destaque na home (diário).

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.livros.vide_editorial.vide_editorial_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "vide__featured__ingestion",
    schedule="30 6 * * *",
    tags=["books"],
    description="Vide Editorial: livros em destaque",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "livros_em_destaque")
