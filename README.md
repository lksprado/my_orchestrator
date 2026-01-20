# Projeto: Airflow3

Este repositório contém um conjunto de DAGs (Directed Acyclic Graphs) para orquestração de workflows utilizando o Apache Airflow. O objetivo principal deste projeto é demonstrar a capacidade de automação e integração de diferentes fontes de dados e processos de ETL (Extract, Transform, Load).

## Estrutura do Projeto

- **dags/**: Contém os arquivos Python que definem as DAGs do Airflow. Cada arquivo representa um workflow específico.
- **dbt/**: Diretório para projetos DBT (Data Build Tool), que inclui modelos, análises e pacotes necessários para a transformação de dados.
- **include/**: Contém módulos e scripts auxiliares utilizados nas DAGs e processos de ETL.
- **local_setup/**: Configurações locais, incluindo arquivos de docker-compose e requisitos de ambiente.
- **nhl_extraction/**: Scripts específicos para extração de dados relacionados à NHL (National Hockey League).
- **openweather/**: Scripts para integração com a API OpenWeather para coleta de dados meteorológicos.
- **Solar/**: Scripts e configurações para projetos relacionados à energia solar.
- **tests/**: Contém testes automatizados para garantir a qualidade e funcionalidade das DAGs e scripts.

## Objetivos do Projeto

- Demonstrar a automação de processos de ETL utilizando o Apache Airflow.
- Integrar diferentes fontes de dados, como APIs e bancos de dados, em um fluxo de trabalho coeso.
- Facilitar a visualização e monitoramento de workflows através da interface do Airflow.

## Projetos nas DAGs

### 1. DAG: `dag_dbt_demodados_full.py`
Esta DAG é responsável por executar um fluxo completo de ETL utilizando o DBT para transformar dados de demodados. Ela integra dados de várias fontes e aplica transformações necessárias para análise.

### 2. DAG: `dag_dbt_mydw_full.py`
Foca na construção de um data warehouse, utilizando o DBT para modelar e transformar dados. Esta DAG garante que os dados estejam prontos para relatórios e análises.

### 3. DAG: `dag_deputados.py`
Coleta e processa dados sobre deputados, integrando informações de diferentes fontes para análise política e legislativa.

### 4. DAG: `dag_ecidadania_bignumbers.py`
Esta DAG analisa grandes volumes de dados relacionados à cidadania, extraindo insights e métricas relevantes.

### 5. DAG: `dag_ecidadania_maisvotados.py`
Foca na coleta e análise de dados sobre os candidatos mais votados, permitindo uma visão clara do cenário eleitoral.

### 6. DAG: `dag_ecidadania_paginas.py`
Responsável por coletar dados de páginas relacionadas à cidadania, integrando informações de diferentes fontes.

### 7. DAG: `dag_ecidadania_status.py`
Monitora e analisa o status de processos relacionados à cidadania, garantindo que as informações estejam sempre atualizadas.

### 8. DAG: `dag_extract_demodados.py`
Extrai dados de demodados para posterior transformação e análise, garantindo que as informações estejam disponíveis para uso.

### 9. DAG: `dag_extract_marts.py`
Foca na extração de dados de marts, preparando-os para análises mais profundas.

### 10. DAG: `dag_governismo_deputados.py`
Analisa dados relacionados ao governismo e deputados, permitindo uma visão clara das interações políticas.

### 11. DAG: `dag_governismo_senadores.py`
Semelhante à DAG anterior, mais focada em senadores e suas interações políticas.

### 12. DAG: `dag_nhl_*`
Um conjunto de DAGs que coleta e analisa dados da NHL, incluindo estatísticas de jogos, detalhes de partidas e informações sobre equipes e jogadores.

### 13. DAG: `dag_radar_parlamentares.py`
Monitora e analisa a atividade parlamentar, fornecendo insights sobre o desempenho dos parlamentares.

### 14. DAG: `dag_ranking_parlamentares.py`
Classifica parlamentares com base em diferentes métricas, permitindo uma análise comparativa.

### 15. DAG: `dag_senadores.py`
Coleta e processa dados sobre senadores, integrando informações relevantes para análise legislativa.

### 16. DAG: `dag_solar_etl.py` e `dag_solar_full_etl.py`
Estas DAGs são responsáveis por processos de ETL relacionados à energia solar, integrando dados de diferentes fontes para análise e relatórios.

### 17. DAG: `dag_weather_etl.py` e `dag_weather_full.py`
Coletam e processam dados meteorológicos, permitindo análises sobre padrões climáticos e suas implicações.

## Licença

Este projeto está licenciado sob a MIT License. Veja o arquivo LICENSE para mais detalhes.