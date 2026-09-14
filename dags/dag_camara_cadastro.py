"""Câmara: cadastro de deputados e legislaturas (manual, troca de legislatura).

Atualiza id_deputados.csv a partir da API antes de extrair os perfis.
"""

from airflow.sdk import task
from pipelines.legislativo._params.atualizar_deputados import obter_ids_deputados_atuais
from pipelines.legislativo.camara.camara_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "camara_cadastro",
    schedule=None,
    tags=["demodados"],
    description="Câmara: ids dos deputados atuais, perfis e legislaturas",
) as dag:

    @task
    def atualizar_ids_deputados():
        obter_ids_deputados_atuais()

    atualizar_ids_deputados() >> etl_group(CONFIG_FILE, ETLS, "deputados")
    etl_group(CONFIG_FILE, ETLS, "legislaturas")
