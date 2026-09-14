"""Atacadão: coleta mensal de preços da cesta pessoal.

Exceção ao GenericETL: grava CSVs por palavra-chave em raw/inflation/atacadao e
não carrega em banco (o dbt consome via seed; ver atacadao_historico).
"""

from airflow.sdk import task
from include.utils.etl_dag import source_dag
from pipelines.precos.atacadao.run import _PRODUCTS_CONFIG, _STORE_CONFIG, search_products

with source_dag(
    "atacadao",
    schedule=None,  # em validação, ver README (Validação das DAGs); original "0 7 28 * *"
    tags=["inflation"],
    description="Atacadão: preços da cesta pessoal",
) as dag:

    @task
    def coletar_precos():
        from settings import settings

        search_products(
            store_config_file=_STORE_CONFIG,
            products_config_file=_PRODUCTS_CONFIG,
            output_dir=settings.lake_root / "raw" / "inflation" / "atacadao",
        )

    coletar_precos()
