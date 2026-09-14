"""Fundos imobiliários: coleta mensal e consolidação (manual).

Exceção ao GenericETL: a lógica mora no __main__ do run.py, então a task chama o
módulo pela linha de comando, no mesmo ambiente. Selenium via SELENIUM_REMOTE_URL.
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param

from include.utils.etl_dag import source_dag

COMMAND = (
    "python -m pipelines.financas.fundos_imobiliarios.run"
    "{% if params.month %} --month {{ params.month }}{% endif %}"
    "{% if params.force %} --force{% endif %}"
    "{% if params.consolidate_only %} --consolidate-only{% endif %}"
)

with source_dag(
    "fundos_imobiliarios",
    schedule=None,
    tags=["financas"],
    description="Fundos imobiliários: lista, indicadores e histórico do mês",
    params={
        "month": Param(None, type=["null", "string"], description="YYYY-MM; vazio = mês corrente"),
        "force": Param(False, type="boolean", description="Reextrai e sobrescreve o mês"),
        "consolidate_only": Param(False, type="boolean", description="Só consolida"),
    },
) as dag:
    BashOperator(task_id="run", bash_command=COMMAND, cwd="/usr/local/airflow")
