"""NHL games_summary: 1 request -> JSON no landing -> raw_nhl.nhl_raw_all_games_summary (JSONB).

Piloto do padrão my_ingestion com load: jsonb (JsonbLoader, overwrite: true,
controle em raw_nhl.nhl_ingestion_control). É a base de IDs dos pipelines
dinâmicos: o dag_nhl_master roda o dbt depois deste.
"""

from datetime import datetime

from airflow.decorators import dag, task

from core import build_etl
from pipelines.esportes.nhl.nhl_etl import CONFIG_FILE, ETLS

ENTIDADE = "games_summary"

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 2}


def _etl():
    return build_etl(CONFIG_FILE, ENTIDADE, ETLS[ENTIDADE])


@dag(
    dag_id="nhl_games_summary",
    default_args=default_args,
    description="NHL: resumo de todos os jogos (base dos IDs)",
    schedule=None,  # disparada pelo nhl_master_pipeline
    start_date=datetime(2026, 9, 13),
    catchup=False,
    tags=["nhl"],
)
def nhl_games_summary():
    @task
    def extract():
        _etl().extract()

    @task
    def load():
        _etl().load()

    extract() >> load()


dag = nhl_games_summary()
