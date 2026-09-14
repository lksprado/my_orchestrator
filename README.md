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

> Os bancos antigos `postgres` e `demodados` não existem mais no Postgres local (só `analytics_dev` e `metabase`). Os submódulos legados de `include/` foram removidos em 2026-09-14; as DAGs que os importavam ficam em `dags/.airflowignore` como referência até migrarem. O repo não tem mais nenhum submódulo.

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

## Validação dos pilotos

As DAGs migradas para o `my_ingestion` ficam com `schedule=None` até rodarem e baterem com os dados migrados em `analytics_dev` (os bancos antigos não existem mais, então a base de comparação são as cópias migradas). Rodadas com `airflow dags test <dag_id>` dentro do scheduler.

| DAG | Data | Antes | Depois | Resultado |
|---|---|---|---|---|
| `weather_etl` | 2026-09-13 | 1794 linhas, até 2026-08-14 | 1824 linhas, até 2026-09-13; 30 dias novos sem buraco e sem nulos; checksum das 1794 linhas antigas idêntico; staging limpo e JSONs em `bronze/weather_project` | ✅ passou |
| `nhl_games_summary` | 2026-09-13 | 74289 jogos | 75698 jogos (inclui 2026-27); nenhum id anterior ausente; mesmo formato de payload; controle registrado com overwrite | ✅ passou |
| `camara_votacoes_pipeline` | 2026-09-13 | cópias migradas: votações 189607, votos por deputado 1896426, orientações 100105 | votações 190431 (até 2026-09-03), votos por deputado 1899613, orientações 100230; nenhum voto nem orientação do legado ausente; `aprovacao`, `tipovoto` e `orientacaovoto` iguais em todas as chaves comuns; uma votação do legado (`2265737-40`) sumiu porque a Câmara a renumerou para `2265737-46` (mesma data e órgão, id antigo dá 404) | ✅ passou |
| `dag_dbt_the_dw` | 2026-09-13 | — | 238 de 244 tasks com sucesso (Cosmos, `postgres_dw` via `.env` e volume do `the_dw` funcionando); `staging_openweather.stg_weather_daily` com as datas novas do weather (1824 linhas, até 2026-09-13). Três erros no `the_dw`, reproduzidos em 2026-09-14 no código atual (`94f1b44`) direto no banco, fora do Airflow: `stg_proventos` faz `quantidade::INT` e a `raw_b3.proventos` recarregada como texto em 2026-09-13 tem `"75.0"`; o teste `not_null_fct_products_sku` procura `sku`, mas a view `marts_inflation.fct_products` só tem `created_date, product_sk, high_price, low_price`; o teste `not_null_fct_games_is_regulation_loss` acha 3 nulos. Não rodaram por dependência: `int_dividendos`, `dividendos`, `inflation` | ❌ não passou (conteúdo do the_dw) |

A câmara leva cerca de 12 minutos (rebuild do bronze de votos). Os 404 em `votos_orientacao` são esperados: essa entidade não guarda as votações sem orientação e tenta de novo a cada execução. O dbt ainda lê as cópias `raw_camara.raw_camara_*`, não estas tabelas novas.

Contagem do dbt: o `the_dw` tem 459 nós (162 models, 223 testes, 58 sources, 16 seeds) e o Cosmos gera 244 tasks, sem perder nada. Cada model vira uma task de run (162); os 66 models com testes ganham uma task `.test` que roda todos os testes dele; as 16 seeds viram tasks; sources não viram task.

Como nem todos passaram, os schedules continuam `None` (os originais estão anotados em cada DAG).

Testes de DAG: rode dentro do scheduler, que tem os volumes. O `astro dev pytest` sobe um container sem eles e as DAGs migradas falham com `No module named 'core'`:

```bash
docker exec $(docker ps -qf name=scheduler) bash -c 'cd /usr/local/airflow && pytest -q tests/dags/'
```

`nhl_games_summary` está no `.airflowignore` (fora de temporada); para testar use `--dagfile-path /usr/local/airflow/dags/dag_nhl_games_summary.py`.

## DAGs

As tabelas abaixo descrevem o catálogo completo. Só rodam hoje as DAGs migradas (ver "Validação dos pilotos"), as de exportação e o smoke test; as demais estão em `dags/.airflowignore` aguardando migração.

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
