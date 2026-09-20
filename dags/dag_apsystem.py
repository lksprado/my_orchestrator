"""Energia solar (portal APsystems), incremental por data.

Tudo vem do my_ingestion: high-water mark em raw_apsystem, um JSON por dia via
Selenium remoto, os CSVs diário e horário e a carga (full refresh em
raw_apsystem.solar_daily_energy / solar_hourly_energy). O landing acumula os
JSONs — nada é movido depois da carga, porque é dele que o transform reconstrói
as tabelas.

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.energia.solar.solar_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "apsystem__energy__ingestion",
    schedule="0 0 * * *",
    tags=["atibaia"],
    description="Energia solar: geração diária e horária",
) as dag:
    # hourly depois de daily: só daily extrai, e os dois leem o mesmo landing.
    etl_group(CONFIG_FILE, ETLS, "daily_energy") >> etl_group(
        CONFIG_FILE, ETLS, "hourly_energy"
    )
