import os
from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="demodados",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="demodadosdw",
        profile_args={"schema": "raw"},
    ),
)

# dbt_env = Variable.get("dbt_env", default_var="dev").lower()
# if dbt_env not in ("dev", "prod"):
#     raise ValueError(f"dbt_env inválido: {dbt_env!r}, use 'dev' ou 'prod'")

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/demodadosdw",  ### caminho dentro da maquina docker
        project_name="demodadosdw",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        "target": profile_config.target_name,
        "threads": 1,
    },
    schedule="0 3 * * *",
    start_date=datetime(2026, 6, 25, 0, 20),
    catchup=False,
    dag_id="dag_dbt_demodados",
    default_args={"retries": 2},
    tags=["demodados"],
    max_active_tasks=2,
)
