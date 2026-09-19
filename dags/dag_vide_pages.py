"""Vide Editorial: páginas das categorias (semanal, só extração).

O my_ingestion não carrega esta entidade (load: none); os JSONs ficam no lake.

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.livros.vide_editorial.vide_editorial_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "vide__pages__ingestion",
    schedule="0 7 * * 5",
    tags=["books"],
    description="Vide Editorial: páginas de categorias",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "categorias")
