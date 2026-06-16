import os
from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="my_datawarehouse",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dw",
        profile_args={"schema": "raw"},
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/my_datawarehouse",
        project_name="my_datawarehouse",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        "target": profile_config.target_name,
        "threads": 1,
    },
    schedule="30 9 * * *",
    start_date=datetime(2026, 6, 15),
    catchup=False,
    dag_id="dag_dbt_my_datawarehouse",
    default_args={"retries": 2},
    tags=["nhl", "atibaia", "inflation", "livros"],
    max_active_tasks=2,
)
