"""NHL (my_ingestion: esportes/nhl): nove entidades JSONB em raw_nhl.

Cadeia única, na ordem em que uma entidade alimenta a seguinte: games_summary é a
base dos game_id, teams traduz id -> triCode para club_stats, e club_stats alimenta
players e player_game_log. Os IDs pendentes saem de consultas na própria raw
(params_* do nhl_etl.py), então não há dbt no meio: o dbt__build roda depois.

Em série de propósito — games_details e play_by_play fazem milhares de requests na
api-web.nhle.com. A concorrência dentro de cada entidade é o options.workers do YAML.

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from datetime import datetime

from pipelines.esportes.nhl.nhl_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "nhl__stats__ingestion",
    schedule="0 8 * * *",  # antes do dbt__build (30 9)
    start_date=datetime(2026, 9, 30),  # 1a execução em 30/09 08:00
    tags=["nhl"],
    description="NHL: temporadas, times, jogos, eventos e jogadores",
) as dag:
    # A ordem de ETLS no nhl_etl.py já é a ordem de execução.
    anterior = None
    for entidade in ETLS:
        grupo = etl_group(CONFIG_FILE, ETLS, entidade)
        if anterior is not None:
            anterior >> grupo
        anterior = grupo
