"""Câmara dos Deputados (my_ingestion: legislativo/camara).

votacoes gera id_votacoes.csv e id_proposicao.csv, que parametrizam as quatro
entidades seguintes. arquivo_proposicoes e arquivo_proposicoes_temas (arquivos
anuais com todas as proposições, ~1,5 GB rebaixados a cada run) não dependem de
nada. Cadastro (legislaturas e deputados) fica em camara_cadastro.

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.legislativo.camara.camara_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "camara__bills__ingestion",
    schedule="30 2 * * 1",
    tags=["politics"],
    description="Câmara: votações, votos, orientações e proposições (votadas e todas)",
    max_active_tasks=2,
) as dag:
    votacoes = etl_group(CONFIG_FILE, ETLS, "votacoes")
    votacoes >> [
        etl_group(CONFIG_FILE, ETLS, entidade)
        for entidade in ("votos_deputados", "votos_orientacao", "proposicao", "proposicao_tema")
    ]
    etl_group(CONFIG_FILE, ETLS, "arquivo_proposicoes")
    etl_group(CONFIG_FILE, ETLS, "arquivo_proposicoes_temas")
