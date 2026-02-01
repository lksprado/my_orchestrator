import os

from cosmos import DbtDag, LoadMode, ProfileConfig, ProjectConfig, RenderConfig
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
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        selector="inflation",
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        dbt_deps=True,
    ),
    operator_args={
        "install_deps": True,
        "target": profile_config_dev.target_name,
        "threads": 1,
    },
    # schedule="@weekly",
    # start_date=datetime(2025, 10, 25, 21, 5),
    catchup=False,
    dag_id="dag_dbt_inflation",
    default_args={"retries": 2},
    tags=["nhl"],
)
