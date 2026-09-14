"""Vide Editorial: livros em destaque na home (diário)."""

from include.utils.etl_dag import etl_group, source_dag
from pipelines.livros.vide_editorial.vide_editorial_etl import CONFIG_FILE, ETLS

with source_dag(
    "vide_editorial",
    schedule=None,  # em validação, ver README (Validação das DAGs); original "30 6 * * *"
    tags=["livros"],
    description="Vide Editorial: livros em destaque",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "livros_em_destaque")
