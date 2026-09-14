"""Senado: status das matérias do e-Cidadania.

O status lê ecidadania_paginas_consolidado.csv no parameter_dir do Senado, mas o
arquivo nasce no bronze do e-Cidadania (DAG ecidadania_paginas). O YAML não
declara esse vínculo; a primeira task copia o arquivo.
"""

import shutil

from airflow.sdk import task
from core import PipelineConfig
from pipelines.legislativo.ecidadania.ecidadania_etl import CONFIG_FILE as ECIDADANIA_CONFIG
from pipelines.legislativo.senado.senado_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "senado_status",
    schedule="0 6 * * *",
    tags=["demodados"],
    description="Senado: status das matérias consultadas no e-Cidadania",
) as dag:

    @task
    def copiar_paginas_ecidadania():
        origem = PipelineConfig.from_yaml(ECIDADANIA_CONFIG, "paginas").bronze_filepath
        destino = PipelineConfig.from_yaml(CONFIG_FILE, "status").parameter_filepath
        if not origem.is_file():
            raise FileNotFoundError(f"Rode ecidadania_paginas antes: {origem} não existe")
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origem, destino)

    copiar_paginas_ecidadania() >> etl_group(CONFIG_FILE, ETLS, "status")
