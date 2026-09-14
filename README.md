# Airflow3

Orquestração Apache Airflow (Astro Runtime 3.0-10 / Airflow 3.0.6 / Python 3.12) com pipelines de ETL em PostgreSQL seguindo arquitetura medallion: raw → dbt (staging/intermediate/marts) → exportação CSV.

O código de ingestão vive no monorepo [`my_ingestion`](https://github.com/lksprado/my_ingestion) e a transformação no [`the_dw`](https://github.com/lksprado/my_datawarehouse); este repo só orquestra. Os dois **não** são submódulos: em dev entram por volume do `~/workspace` (edita lá, o Airflow vê na hora) e em prod pelos commits fixados em `deploy/versions.txt`.

**UI:** http://localhost:8080 | **API:** http://localhost:8090

## Estrutura

| Diretório | Descrição |
|-----------|-----------|
| `dags/` | DAG files — um por pipeline |
| `include/my_ingestion/` | Ponto de montagem do monorepo de ingestão, fora do git (`src/` entra no `PYTHONPATH`: `core`, `pipelines`, `settings`) |
| `include/utils/` | Helpers do lado Airflow (`db_interactors.py`) — carga via connection `postgres_dw` |
| `include/{local_setup,Solar,openweather,nhl_extraction,vide,inflation,finance}/` | Submódulos antigos, **em extinção**: só as DAGs ainda não migradas dependem deles |
| `dbt/the_dw/` | Ponto de montagem do projeto dbt único (todos os domínios, inclusive demodados), fora do git |
| `deploy/prod-dags.txt` | Allowlist de DAGs promovidas ao `atb` |
| `deploy/versions.txt` | Commit exato do `my_ingestion` e do `the_dw` que prod executa (`deploy/checkout_versions.sh` aplica) |
| `tests/` | Validação de importação e conexões |

## Configuração (dev × prod por variáveis de ambiente)

Tudo vem do `.env` da raiz (gitignored e dockerignored; o Astro injeta em todos os containers). Copie de `.env.example` e preencha.

| Variável | dev (esta máquina) | prod (`atb`) |
|---|---|---|
| `ENV` | `dev` | `prod` |
| `LAKE_ROOT` | `/usr/local/airflow/mylake` (bind mount do lake) | idem, com o lake do servidor montado |
| `SEEDS_ROOT` | `/usr/local/airflow/dbt/the_dw/seeds` | idem |
| `DB__DEV__*` | Postgres local, **obrigatoriamente** `analytics_dev` (`settings.py` valida) | vazio |
| `DB__PROD__*` | vazio | Postgres de produção |
| `APSYSTEMS_*`, `OPENWEATHER_API_KEY`, `GOOGLE_CREDENTIALS_FILE`, `URL_FINANCE__*` | credenciais | só as dos pipelines promovidos |

`ENV` escolhe o bloco `environments` dos YAMLs do `my_ingestion` e o perfil `DB__<ENV>__*`; dentro do container os dois blocos resolvem para os mesmos caminhos (`/usr/local/airflow/mylake`). A connection `postgres_dw` (Cosmos e `include/utils`) vem do mesmo `.env`, na variável `AIRFLOW_CONN_POSTGRES_DW`, e tem que apontar para o **mesmo banco** do perfil ativo. A variável tem precedência sobre a connection gravada no banco do Airflow.

> Os bancos antigos `postgres` e `demodados` não existem mais no Postgres local (só `analytics_dev` e `metabase`). As DAGs legadas que dependiam deles já estão quebradas até migrarem.

### Padrão das DAGs migradas

```python
from core import build_etl
from pipelines.legislativo.camara.camara_etl import CONFIG_FILE, ETLS

@task
def extract():
    build_etl(CONFIG_FILE, "votacoes", ETLS["votacoes"]).extract()   # só dentro da task
```

Uma `@task` por etapa (`extract`/`transform`/`load`), credenciais via `settings` (nada de `Variable.get`), `retries >= 2`, tag por domínio. `load: none` (solar, weather): o `include/utils` carrega e faz o upsert. Migradas até agora: `dag_dbt_the_dw`, `dag_camara_votacoes`, `dag_weather_etl`, `dag_nhl_games_summary`, `dag_smoke_my_ingestion`.

**pandas fica em 2.1.4** (Airflow 3.0.6 → SQLAlchemy 1.4); o `my_ingestion` roda com pandas 3.x no venv dele. A DAG `smoke_my_ingestion` valida o wiring (imports, `.env`, banco, the_dw) em dev e em prod.

### Prod (`atb`)

O Airflow de produção é o `homelab/stacks/airflow` (Runtime 3.3-2). Quando uma DAG entrar em `deploy/prod-dags.txt`, o servidor precisa de: `.env` com `ENV=prod` + `DB__PROD__*` + segredos; `PYTHONPATH` igual ao do `Dockerfile` daqui; `deploy/checkout_versions.sh <destino>` para colocar `my_ingestion` e `the_dw` nos commits de `deploy/versions.txt`, montados nos mesmos caminhos do container de dev; lake montado em `/usr/local/airflow/mylake`; imagem com **Python 3.12** (o default do 3.3-2 é 3.14 e o `my_ingestion` pina `<3.13`). Nada disso é automatizado ainda.

## DAGs

### Dados Políticos

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_camara_votacoes` | Votações → votos por deputado e orientação de bancada (`my_ingestion`) | Seg 02:30 |
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
| `dag_extract_demodados` | Exporta gold layer do demodados para CSV | diário 04:30 |

### Solar & Clima

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_solar_etl` | Energia solar (APSYSTEM) → raw | diário 00:00 |
| `dag_solar_full_etl` | Reprocessamento histórico solar | manual |
| `dag_weather_etl` | OpenWeather → `raw_openweather` (`my_ingestion`) | diário 01:00 |
| `dag_weather_full` | Reprocessamento histórico clima | manual |

### NHL

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_nhl_master` | Orquestra todo o pipeline NHL | diário 08:00 |
| `dag_nhl_games_summary` | Resumo de partidas (`my_ingestion`, JSONB) | *triggered* |
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

### DBT, exportação e infra

| DAG | Descrição | Schedule (UTC) |
|-----|-----------|----------------|
| `dag_dbt_the_dw` | dbt do `the_dw` completo (todos os domínios) via Cosmos | diário 09:30 |
| `dag_smoke_my_ingestion` | Smoke test do wiring (imports, `.env`, banco, the_dw) | manual |
| `dag_extract_my_datawarehouse` | Exporta marts para CSV (energia, inflação) | diário 03:30 |

## Licença

MIT
