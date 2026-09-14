"""Ranking dos Políticos: deputados e senadores (semanal)."""

from include.utils.etl_dag import etl_group, source_dag
from pipelines.legislativo.ranking_politicos.ranking_politicos_etl import CONFIG_FILE, ETLS

with source_dag(
    "ranking_politicos",
    schedule=None,  # em validação, ver README (Validação das DAGs); original "0 7 * * 1"
    tags=["demodados"],
    description="Ranking dos Políticos: deputados e senadores",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "deputados")
    etl_group(CONFIG_FILE, ETLS, "senadores")
