"""Investimentos: de-para das instituições do FGC (manual).

Exceção ao GenericETL: lê a lista do conglomerado prudencial no lake e grava o
seed em SEEDS_ROOT (em dev, o working tree do the_dw). Depende da camada
intermediate já materializada.
"""

from airflow.sdk import task
from core import PipelineConfig
from pipelines.financas.investimentos.investimentos_etl import CONFIG_FILE
from pipelines.financas.investimentos.investimentos_fgc import build_depara

from include.utils.etl_dag import source_dag

with source_dag(
    "investimentos_fgc",
    schedule=None,
    tags=["financas"],
    description="Investimentos: seed de-para das instituições do FGC",
) as dag:

    @task
    def gerar_depara():
        from settings import settings

        cfg = PipelineConfig.from_yaml(CONFIG_FILE, "fgc")
        build_depara(cfg.landing_filepath, settings.seeds_root)

    gerar_depara()
