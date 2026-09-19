# Airflow3

Orquestração Apache Airflow (Astro Runtime 3.0-10 / Airflow 3.0.6 / Python 3.12) com pipelines de ETL em PostgreSQL seguindo arquitetura medallion: raw → dbt (staging/intermediate/marts) → exportação CSV.

O código de ingestão vive no monorepo [`my_ingestion`](https://github.com/lksprado/my_ingestion) e a transformação no [`my_analytics`](https://github.com/lksprado/my_analytics); este repo só orquestra. Os dois **não** são submódulos: em dev entram por volume do `~/workspace` (edita lá, o Airflow vê na hora) e em prod pelos commits fixados em `deploy/versions.txt`, embutidos na imagem.

**UI:** http://localhost:8080 | **API:** http://localhost:8090

## Estrutura

| Diretório | Descrição |
|-----------|-----------|
| `dags/` | DAG files — um por pipeline |
| `include/my_ingestion/` | Ponto de montagem do monorepo de ingestão, fora do git (`src/` entra no `PYTHONPATH`: `core`, `pipelines`, `settings`) |
| `include/utils/` | Helpers do lado Airflow (`db_interactors.py`) — carga via connection `postgres_dw` |
| `dbt/my_analytics/` | Ponto de montagem do projeto dbt único (todos os domínios, inclusive demodados), fora do git |
| `deploy/prod-dags.txt` | Allowlist de DAGs promovidas ao `atb` |
| `deploy/versions.txt` | Commit exato do `my_ingestion` e do `my_analytics` que prod executa |
| `deploy/deploy_prod.sh` + `deploy/prod/` | Monta o Airflow de prod nesta máquina e envia para `atb:/srv/airflow` |
| `tests/` | Validação de importação e conexões |

## Configuração (dev × prod por variáveis de ambiente)

Tudo vem do `.env` da raiz (gitignored e dockerignored; o Astro injeta em todos os containers). Copie de `.env.example` e preencha.

| Variável | dev (esta máquina) | prod (`atb`) |
|---|---|---|
| `ENV` | `dev` | `prod` |
| `LAKE_ROOT` | `/usr/local/airflow/mylake` (bind mount do lake) | idem, com o lake do servidor montado |
| `SEEDS_ROOT` | `/usr/local/airflow/dbt/my_analytics/seeds` | idem |
| `DB__DEV__*` | Postgres local, **obrigatoriamente** `analytics_dev` (`settings.py` valida) | vazio |
| `DB__PROD__*` | vazio | Postgres de produção |
| `APSYSTEMS_*`, `OPENWEATHER_API_KEY`, `GOOGLE_CREDENTIALS_FILE`, `URL_FINANCE__*` | credenciais | só as dos pipelines promovidos |
| `SELENIUM_REMOTE_URL` | `http://host.docker.internal:4444/wd/hub` | `http://selenium:4444/wd/hub` |

`ENV` escolhe o bloco `environments` dos YAMLs do `my_ingestion` e o perfil `DB__<ENV>__*`; dentro do container os dois blocos resolvem para os mesmos caminhos (`/usr/local/airflow/mylake`). A connection `postgres_dw` (Cosmos e `include/utils`) vem do mesmo `.env`, na variável `AIRFLOW_CONN_POSTGRES_DW`, e tem que apontar para o **mesmo banco** do perfil ativo. A variável tem precedência sobre a connection gravada no banco do Airflow.

> Os bancos antigos `postgres` e `demodados` não existem mais no Postgres local (só `analytics_dev` e `metabase`). Os submódulos legados de `include/` foram removidos em 2026-09-14; as DAGs que os importavam ficam em `dags/.airflowignore` como referência até migrarem. O repo não tem mais nenhum submódulo.

### Padrão das DAGs: uma por fonte do my_ingestion

```python
"""Ranking dos Políticos: deputados e senadores (semanal).

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from include.utils.etl_dag import etl_group, source_dag
from pipelines.legislativo.ranking_politicos.ranking_politicos_etl import CONFIG_FILE, ETLS

with source_dag("ranking_politicos", schedule="0 7 * * 1", tags=["demodados"]) as dag:
    etl_group(CONFIG_FILE, ETLS, "deputados")
    etl_group(CONFIG_FILE, ETLS, "senadores")
```

- `etl_group` cria um grupo por entidade só com as etapas que ela tem (`extract`, `transform`, `check_bronze`, `load`), na ordem certa. `check_bronze` impede que um bronze vazio zere a raw.
- Todo arquivo de DAG precisa ter as palavras "airflow" e "dag"; sem elas o Airflow pula o arquivo em silêncio. O teste `tests/dags/test_etl_dag.py` pega esse caso.
- Disparo manual com o parâmetro `steps=["transform","load"]` reprocessa o landing sem consultar a fonte. Substitui as antigas DAGs `*_full`.
- Credenciais vêm do `.env` via `settings`, nunca de `Variable.get`.
- DAG nova nasce com `schedule=None` e só recebe o agendamento depois de validada (seção abaixo). No Airflow local as DAGs nascem **pausadas**: despause na UI para começarem a rodar.

**pandas fica em 2.1.4** (Airflow 3.0.6 → SQLAlchemy 1.4); o `my_ingestion` roda com pandas 3.x no venv dele. A DAG `smoke_my_ingestion` valida o wiring (imports, `.env`, banco, my_analytics) em dev e em prod.

**Requisitos de execução:** `poppler-utils` na imagem (PDFs da Avenue); `SELENIUM_REMOTE_URL` no `.env` para Solar e fundos imobiliários (a imagem não tem Chrome; em dev é o `selenium_container`); tabelas `raw_apsystem.solar_*` com PK para o upsert.

**Nomes de tabela alinhados com o my_analytics** desde o `my_ingestion` `b3c79f3` (2026-09-14): as fontes que o dbt já lia carregam nas mesmas tabelas (`raw_camara.raw_camara_votacoes`, `raw_senado.raw_senado_votacoes`, `raw_apsystem.solar_*`, `raw_vide_editora.vide_raw_home_featured`, `raw_google_sheets.*`...). `raw_<fonte>.<entidade>` vale só para fonte nova. Única exceção que resta: o Atacadão não carrega em banco, e o my_analytics ainda lê `raw_atacadao.atacadao_raw`.

### Prod (`atb`)

O Airflow de produção é `/srv/airflow` no `atb`, com a mesma imagem do dev (Runtime 3.0-10, Python 3.12), UI em `http://100.82.7.107:8080`. Ele é **gerado** por `deploy/deploy_prod.sh`, que roda nesta máquina (o `my_ingestion` é privado e o `atb` não guarda credencial do GitHub):

A `main` é protegida: promover DAG (`deploy/prod-dags.txt`), trocar SHA (`deploy/versions.txt`) ou mudar código entra por PR com aprovação manual. Depois do merge, com a `main` local atualizada:

```bash
deploy/deploy_prod.sh --dry-run            # monta e mostra o que mudaria no atb
deploy/deploy_prod.sh                      # monta e envia (rsync --delete)
ssh -t atb /srv/airflow/start.sh restart   # rebuild e restart (pede o sudo)
```

O script exige árvore limpa e `HEAD == origin/main`, leva só as DAGs de `deploy/prod-dags.txt`, troca o override/`.astro/config.yaml`/`start.sh` pelos de `deploy/prod/`, põe `my_ingestion` (`src/`) e `my_analytics` nos SHAs de `deploy/versions.txt` (com `dbt deps` do `package-lock.yml`) e grava `DEPLOYED.txt`. O código vai na imagem e, como o Astro também monta `dags/` e `include/` do projeto nos containers, é lido direto de `/srv/airflow`; o `rsync` ignora os `__pycache__` que o scheduler (root) grava ali. O `.env` de prod vive só no `atb` (modelo em `deploy/prod/.env.example`): banco `analytics_prod` no `postgres-dwh` da `homelab-net`, Selenium próprio do projeto. O lake são os buckets do SeaweedFS montados por FUSE em `/srv/lake/buckets` (serviço `seaweedfs-mount` do homelab).

## Validação das DAGs

Cada DAG rodou com `airflow dags test <dag_id> --dagfile-path ...` dentro do scheduler e foi comparada com as cópias migradas em `analytics_dev` (os bancos antigos não existem mais). A validação usou os nomes novos; com o alinhamento, as tabelas que o my_analytics lê foram recarregadas do mesmo bronze e têm as mesmas contagens da coluna "Depois". Critério: execução com sucesso e nenhuma chave da cópia ausente, com a diferença explicada quando houver. Quem passou recebeu o schedule alvo em commit próprio.

| DAG | Data | Antes | Depois | Resultado |
|---|---|---|---|---|
| `weather_etl` | 2026-09-13 | 1794 linhas, até 2026-08-14 | 1824 linhas, até 2026-09-13; 30 dias novos sem buraco; checksum das linhas antigas idêntico | ✅ passou |
| `nhl_games_summary` | 2026-09-13 | 74289 jogos | 75698 jogos; nenhum id anterior ausente | ✅ passou (fora de temporada, segue no `.airflowignore`) |
| `ranking_politicos` | 2026-09-14 | cópias: deputados 462, senadores 71 | deputados 1033 (todos os anos do landing), nenhum ausente; senadores 77, 2 ausentes (Eduardo Girão e Ana Paula Lobato não estão mais na API) ; steps=transform,load pulou o extract | ✅ passou |
| `radar_congresso` | 2026-09-14 | cópia de parlamentares 593 (07/06); governismo sem cópia útil (0 linhas / inexistente); CLI: 5988, 936, 594 | governismo 5988 e 936 (iguais à CLI, até 2026 T1); parlamentares 594 = API atual (17 saíram, 18 entraram desde 07/06) | ✅ passou |
| `ecidadania` | 2026-09-14 | cópias: bignumbers 260, mais_votados 771 (até 15/08) | bignumbers 261, mais_votados 774 (até 14/09); nenhuma extração do legado ausente | ✅ passou |
| `vide_editorial` | 2026-09-14 | cópia vide_raw_home_featured 6510 (217 arquivos, até 12/09) | 6540 (218 arquivos, até 14/09); nenhum livro/data do legado ausente | ✅ passou |
| `investimentos_google` | 2026-09-14 | cópia raw_google_sheets: 9 abas | raw_google: as mesmas 9 abas com as mesmas contagens; colunas iguais + arquivo_origem e data_carga | ✅ passou |
| `investimentos_arquivos` | 2026-09-14 | raw_b3 (8 tabelas) e raw_avenue (2), carregadas pela CLI em 13/09 | mesmas contagens; raw_b3 com checksum idêntico descontando o prefixo do caminho em source_path; raw_avenue: bronze do container idêntico ao gerado pela CLI no host (pdftotext 22.12 x 24.02 sem diferença), checksum da tabela difere da carga anterior da CLI | ✅ passou |
| `camara_cadastro` | 2026-09-14 | cópias: deputados 2192, legislaturas 6238 | ids de deputados atualizados; deputados 2207, legislaturas 6246; nenhum id do legado ausente | ✅ passou |
| `senado_cadastro` | 2026-09-14 | cópias: legislaturas 1137 (502 parlamentares), senadores 162 (85) | legislaturas 1138 (503), nenhum ausente; senadores 162 (86), 1 ausente (Hermes Klann saiu da lista atual; entraram Lourdinha Pereira e Sargento Reginauro) | ✅ passou |
| `ecidadania_paginas` | 2026-09-14 | cópia 3915 linhas, 9 extrações (até 20/06) | 4350 linhas, 10 extrações (até 14/09); nenhuma linha do legado ausente | ✅ passou |
| `senado_status` | 2026-09-14 | cópia 491 matérias | bronze de páginas copiado do e-Cidadania; 491 matérias, nenhuma ausente | ✅ passou |
| `vide_editorial_categorias` | 2026-09-14 | extrações semanais do legado: ~211-214 páginas e ~7500 produtos (11/09: 214 e 7536) | 211 páginas JSON e 7478 produtos em 8 categorias (só extract; sem carga, como no my_ingestion) | ✅ passou |
| `atacadao` | 2026-09-14 | coletas do legado: 44 palavras-chave, ~3300-4700 linhas | 1ª execução: 43 arquivos (a busca por "Sal" falhou de forma passageira e o código pula sem avisar); 2ª execução: 44 arquivos. Sem carga em banco, como no my_ingestion | ⚠️ passou com ressalva |
| `camara` | 2026-09-14 | cópias: proposicao 64364, proposicao_tema 1885 (votações e votos já validados como piloto em 13/09) | proposicao 64677 e proposicao_tema 1925, nenhum id válido ausente (os 2 "ausentes" são linhas da cópia com a ementa quebrando a coluna id); votações 190431, votos 1899613, orientações 100230 | ✅ passou |
| `solar` | 2026-09-14 | raw_solar (cópia de raw_apsystem): 1790 dias e 42960 horas até 14/08 | 1819 dias e 43656 horas até 13/09; checksum das linhas antigas idêntico; 29 dias novos com 24 horas cada; falta 15/08, que o portal não devolve em JSON e que o watermark não tenta de novo. Exigiu corrigir a navegação no my_ingestion (c3ab58d) e a senha do portal no .env | ⚠️ passou com ressalva |
| `senado` | 2026-09-14 | cópias: votações 3166, votos 255045, orientações 6717, processos 2575 | votações 3182, votos 256340, orientações 6753, processos 2583; nenhuma chave do legado ausente | ✅ passou |
| `investimentos_fgc` | 2026-09-14 | seed_de_para_instituicoes_fgc.csv no my_analytics | falhou: o SQL do my_ingestion lê intermediate.int_renda_fixa, e o my_analytics gera intermediate_financas.int_renda_fixa. Nenhum seed gravado. Entrada instituicoes_conglomerado_prudencial.csv copiada de ~/workspace/investments para o lake | ❌ não passou (desalinhamento com o my_analytics) |
| `atacadao_historico` | 2026-09-14 | seed_minha_inflacao.csv no my_analytics: 1271 linhas, colunas ... Categoria, Mes | execução ok e 1271 linhas, mas o arquivo gerado (minha_inflacao.csv) tem Mês passado e Var no lugar de Categoria e Mes; arquivo removido do my_analytics. Entradas copiadas de ~/workspace/webscraping-inflation/data/months para bronze/inflation/months | ❌ não passou (formato diverge do seed do my_analytics) |
| `fundos_imobiliarios` | 2026-09-14 | 2026-05 (repositório antigo): 287 fundos, 270 com indicadores, 265748 linhas de histórico | 2026-09: 272 fundos, 243 com indicadores, 264971 linhas de histórico, consolidados com o mês novo. Nenhum dos 29 sem indicadores tinha indicadores em maio. A 1ª execução derrubava a aba do Chrome remoto (/dev/shm de 64 MB); corrigido no my_ingestion (8bcde24) | ✅ passou |
| `dag_dbt_my_analytics` | 2026-09-13, revalidado 2026-09-14 | — | 238 de 244 tasks, igual nas duas execuções. Na de 14/09 (my_analytics `b7f8248`, raw alinhada e recarregada) todos os models de staging de Câmara, Senado, e-Cidadania, Radar, Ranking, Vide, Google Sheets e APsystems rodaram. Três erros no `my_analytics`, reproduzidos fora do Airflow em `94f1b44`: `stg_proventos` faz `quantidade::INT` com valores `"75.0"`; o teste `not_null_fct_products_sku` procura uma coluna que a view não tem; o teste de `fct_games.is_regulation_loss` acha 3 nulos | ❌ não passou (conteúdo do my_analytics) |
| exportações | 2026-09-14 | — | `extract_postgres_demodados` e `extract_my_analytics` gravaram os CSVs em `gold/` | ✅ passou |

Diferenças que vêm da fonte, não da DAG: a Câmara renumerou uma votação; o Ranking e o Radar tiraram parlamentares da lista atual; o portal da APsystems não devolve 15/08/2026.

Entradas manuais copiadas para o lake durante a validação, conforme o README do `my_ingestion`: `raw/investments/instituicoes/` (de `~/workspace/investments`), `bronze/inflation/months/` (de `~/workspace/webscraping-inflation`) e `raw/fii/` (de `~/workspace/fundos-imobiliarios`).

Contagem do dbt: o `my_analytics` tem 459 nós (162 models, 223 testes, 58 sources, 16 seeds) e o Cosmos gera 244 tasks, sem perder nada. Cada model vira uma task de run; os 66 models com testes ganham uma task `.test` que roda todos os testes dele; as seeds viram tasks; sources não viram task.

Testes de DAG: rode dentro do scheduler, que tem os volumes. O `astro dev pytest` sobe um container sem eles e as DAGs migradas falham com `No module named 'core'`:

```bash
docker exec $(docker ps -qf name=scheduler) bash -c 'cd /usr/local/airflow && pytest -q tests/dags/'
```

DAG listada no `.airflowignore` (NHL) precisa de `--dagfile-path` no `airflow dags test`.

## DAGs

Horários em UTC. "manual" = sem agendamento, disparo pela UI.

### Legislativo (tag `demodados`)

| DAG | O que faz | Schedule |
|-----|-----------|----------|
| `camara` | Votações → votos por deputado, orientações, proposições e temas | seg 02:30 |
| `camara_cadastro` | Atualiza os ids dos deputados atuais → perfis; legislaturas | manual |
| `senado` | Votações (2001 em diante) → votos por senador e processos; orientações | dia 1 02:30 |
| `senado_cadastro` | Senadores atuais e legislaturas | manual |
| `senado_status` | Copia o bronze de páginas do e-Cidadania → status das matérias | diário 06:00 |
| `ecidadania` | Big numbers e matérias mais votadas | diário 05:00 |
| `ecidadania_paginas` | Todas as páginas de consultas públicas (alimenta `senado_status`) | dia 20 06:30 |
| `ranking_politicos` | Ranking dos Políticos: deputados e senadores | seg 07:00 |
| `radar_congresso` | Radar do Congresso: governismo e parlamentares | semanal |
| `extract_postgres_demodados` | Exporta `presentation_demodados` para CSV em `gold/` | diário 04:30 |

### Finanças (tag `financas`)

| DAG | O que faz | Schedule |
|-----|-----------|----------|
| `investimentos_google` | Abas das planilhas do Google Sheets → `raw_google_sheets` | diário 03:00 |
| `investimentos_arquivos` | Excel da B3 e PDFs da Avenue colocados no landing → `raw_b3`, `raw_avenue` | manual |
| `investimentos_fgc` | Seed de-para das instituições do FGC no `my_analytics` | manual |
| `fundos_imobiliarios` | Lista, indicadores e histórico dos FII do mês (params `month`, `force`, `consolidate_only`) | manual |

### Solar & Clima (tag `atibaia`)

| DAG | O que faz | Schedule |
|-----|-----------|----------|
| `solar` | Geração diária e horária (Selenium) → upsert em `raw_apsystem` | diário 00:00 |
| `weather_etl` | OpenWeather → upsert em `raw_openweather` | diário 01:00 |

### Inflação (tag `inflation`)

| DAG | O que faz | Schedule |
|-----|-----------|----------|
| `atacadao` | Preços da cesta pessoal (CSVs no lake, sem carga em banco) | dia 28 07:00 |
| `atacadao_historico` | Consolida os CSVs mensais no seed `minha_inflacao` | manual |

### Livros (tag `livros`)

| DAG | O que faz | Schedule |
|-----|-----------|----------|
| `vide_editorial` | Livros em destaque na home | diário 06:30 |
| `vide_editorial_categorias` | Páginas das categorias (só extração) | sex 07:00 |

### dbt, exportação e infra

| DAG | O que faz | Schedule |
|-----|-----------|----------|
| `dag_dbt_my_analytics` | dbt do `my_analytics` completo via Cosmos | manual (09:30 depois de validado) |
| `extract_my_analytics` | Exporta marts de energia e inflação para CSV em `gold/` | diário 03:30 |
| `smoke_my_ingestion` | Smoke test do wiring (imports, `.env`, banco, my_analytics) | manual |

### NHL (legado, em `.airflowignore` até a temporada)

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

## Licença

MIT
