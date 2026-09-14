"""Investimentos: posições da B3 e extratos da Avenue (manual).

Os Excel da B3 e os PDFs da Avenue são colocados à mão no landing
(raw/investments/b3|avenue/<pessoa>/); dispare depois de copiar os arquivos.

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.financas.investimentos.investimentos_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "investimentos_arquivos",
    schedule=None,
    tags=["financas"],
    description="Investimentos: arquivos da B3 e da Avenue para raw_b3 e raw_avenue",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "b3")
    etl_group(CONFIG_FILE, ETLS, "avenue")
