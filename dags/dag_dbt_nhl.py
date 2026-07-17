import os

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config_dev = ProfileConfig(
    profile_name="my_datawarehouse",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dw",
        profile_args={"schema": "raw"},
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/my_datawarehouse",  ### caminho dentro da maquina docker
        project_name="my_datawarehouse",
    ),
    profile_config=profile_config_dev,
    # Sem load_method explícito → Cosmos usa o cache do `dbt ls` (chave por hash
    # do projeto), igual à dag_dbt_demodados. dbt_deps removido: as deps já estão
    # vendoradas em dbt_packages/, não precisam ser reinstaladas a cada parse.
    render_config=RenderConfig(
        selector="nhl",
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        "target": profile_config_dev.target_name,
        "threads": 1,
    },
    # schedule="@weekly",
    # start_date=datetime(2025, 10, 25, 21, 5),
    catchup=False,
    dag_id="dag_dbt_nhl",
    default_args={"retries": 2},
    tags=["nhl"],
)
