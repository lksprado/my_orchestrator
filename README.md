# Airflow3

Orquestração Apache Airflow (Astronomer 3.0 / Airflow 2.10.x) com pipelines de ETL em PostgreSQL seguindo arquitetura medallion: raw → dbt (bronze/silver/gold) → exportação CSV.

**UI:** http://localhost:8080 | **API:** http://localhost:8090

## Estrutura

| Diretório | Descrição |
|-----------|-----------|
| `dags/` | DAG files — um por pipeline |
| `include/` | Módulos ETL por domínio (git submodules) |
| `dbt/` | `my_datawarehouse` (NHL, solar, inflação, livros) e `demodadosdw` (dados políticos) |
| `tests/` | Validação de importação e conexões |

## DAGs

### Dados Políticos

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_camara_votacoes` | Votações da Câmara, dispara sub-pipelines | Seg 02:30 |
| `dag_camara_votos_deputados` | Votos por deputado | *triggered* |
| `dag_camara_votos_orientacao` | Orientação de votos por partido | *triggered* |
| `dag_senado_votacoes` | Votações do Senado | dia 1 02:30 |
| `dag_senado_votos_orientacao` | Orientação de votos no Senado | dia 1 03:30 |
| `dag_senado_votos_senadores` | Votos individuais de senadores | dia 1 04:30 |
| `dag_ecidadania_bignumbers` | Big numbers do e-Cidadania | diário 05:00 |
| `dag_ecidadania_maisvotados` | Matérias mais votadas | diário 05:30 |
| `dag_ecidadania_status` | Status de matérias | diário 06:00 |
| `dag_ecidadania_paginas` | Paginação do portal | dia 20 06:30 |
| `dag_deputados` | Dados cadastrais de deputados | manual |
| `dag_senadores` | Dados cadastrais de senadores | manual |
| `dag_ranking_deputados` | Ranking de deputados (Politicos.org.br) | Seg 07:00 |
| `dag_ranking_senadores` | Ranking de senadores (Politicos.org.br) | Seg 07:30 |
| `dag_dbt_demodados` | DBT projeto `demodadosdw` | diário 03:00 |
| `dag_extract_demodados` | Exporta gold layer do demodados para CSV | diário 04:30 |

### Solar & Clima

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_solar_etl` | Energia solar (APSYSTEM) → raw + S3 | diário 00:00 |
| `dag_solar_full_etl` | Reprocessamento histórico solar | manual |
| `dag_weather_etl` | OpenWeather → raw + S3 | diário 01:00 |
| `dag_weather_full` | Reprocessamento histórico clima | manual |

### NHL

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_nhl_master` | Orquestra todo o pipeline NHL | diário 08:00 |
| `dag_nhl_games_summary` | Resumo de partidas | *triggered* |
| `dag_nhl_games_summary_details` | Detalhes do resumo | *triggered* |
| `dag_nhl_games_details` | Detalhes completos de partidas | *triggered* |
| `dag_nhl_games_play_by_play` | Play-by-play | *triggered* |
| `dag_nhl_game_log` | Logs por jogador | *triggered* |
| `dag_nhl_club_stats` | Estatísticas de clubes | *triggered* |
| `dag_nhl_players` | Dados de jogadores | *triggered* |
| `dag_nhl_seasons` | Temporadas | 1 out 02:00 |
| `dag_nhl_teams` | Times | 1 out 03:00 |
| `dag_dbt_nhl` | DBT selector `nhl` | *triggered* |

### Inflação

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_inflation` | Preços Atacadão → raw | dia 28 07:00 |

### Livros (Vide Editorial)

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_vide_home` | Livros em destaque (homepage) | diário 06:30 |
| `dag_vide_pages` | Páginas e categorias | Sex 07:00 |

### DBT & Exportação (my_datawarehouse)

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_dbt_my_datawarehouse` | DBT projeto completo: NHL, solar, inflação, livros | diário 09:30 |
| `dag_extract_my_datawarehouse` | Exporta marts para CSV (energia, inflação) | diário 03:30 |

## Licença

MIT
