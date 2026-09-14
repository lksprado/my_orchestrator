"""Atacadão: consolida os CSVs mensais no seed minha_inflacao (manual).

Lê bronze/inflation/months (alimentado fora do my_ingestion) e grava em
SEEDS_ROOT (em dev, o working tree do the_dw).
"""

from airflow.sdk import task
from include.utils.etl_dag import source_dag
from pipelines.precos.atacadao.historic import make_file

with source_dag(
    "atacadao_historico",
    schedule=None,
    tags=["inflation"],
    description="Atacadão: seed minha_inflacao a partir dos CSVs mensais",
) as dag:

    @task
    def consolidar_historico():
        from settings import settings

        make_file(
            input_dir=settings.lake_root / "bronze" / "inflation" / "months",
            output_dir=settings.seeds_root,
            filename="minha_inflacao",
        )

    consolidar_historico()
