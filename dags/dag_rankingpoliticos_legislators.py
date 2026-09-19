"""Ranking dos Políticos: deputados e senadores (semanal).

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.legislativo.ranking_politicos.ranking_politicos_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "ranking_politicos__legislators__ingestion",
    schedule="0 7 * * 1",
    tags=["politics"],
    description="Ranking dos Políticos: deputados e senadores",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "deputados")
    etl_group(CONFIG_FILE, ETLS, "senadores")
