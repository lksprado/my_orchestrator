# Esteira de deploy: do código ao Airflow de produção

Guia de consulta para quando você for criar ou mudar alguma coisa. Explica o que vive em cada
repositório, como uma mudança chega ao `atb` e como conferir o que está rodando lá.

---

## 1. As peças

São três repositórios de aplicação. **A branch principal de cada um é produção:** o merge de um PR
aprovado já é a publicação. Não existe SHA fixado, comando de deploy nem restart manual.

| Repositório | O que tem | Branch que vai para prod |
|---|---|---|
| `my_ingestion` | extração e transformação das fontes (`src/pipelines/<domínio>/<fonte>/`) e a carga nas tabelas `raw_*` | `main` |
| `my_analytics` | projeto dbt (staging → intermediate → marts) | `main` |
| `my_orchestrator` | DAGs, `Dockerfile`, `requirements.txt`, scripts e o workflow de deploy | `main` |
| `homelab` | infra do atb: Postgres, SeaweedFS (lake), observabilidade | `main` (runner do homelab) |

Quem faz o deploy é o workflow **Deploy prod** (`.github/workflows/deploy.yml` do `my_orchestrator`),
que roda no self-hosted runner `atb-airflow`, dentro do próprio atb. Um merge no `my_orchestrator`
dispara o workflow direto; um merge no `my_ingestion` ou no `my_analytics` dispara um workflow
pequeno lá (`deploy-prod.yml`) que só avisa o `my_orchestrator` (`repository_dispatch`).
Qualquer que seja o gatilho, o deploy leva **a ponta dos três repos**.

Os dois ambientes:

| | **dev** (sua máquina) | **prod** (`atb`) |
|---|---|---|
| Airflow | `~/workspace/my_orchestrator`, `astro dev start`, http://localhost:8090 | `/srv/airflow`, http://100.82.7.107:8080 |
| Código do `my_ingestion` / `my_analytics` | **working tree** de `~/workspace/...`, montado ao vivo | **ponta da branch principal**, embutida na imagem a cada merge |
| DAGs | todas as de `dags/` (menos o `.airflowignore`) | as mesmas de dev |
| Banco das cargas raw (`DB__<ENV>__*`) | `ingestion_sandbox` (localhost:5435) | `analytics_prod` (`postgres-dwh`, 100.82.7.107:5432) |
| Banco do dbt (`postgres_dw`) | `analytics_dev` (localhost:5435) | `analytics_prod` (o mesmo) |
| Lake | `/media/lucas/Files/2.Projetos/0.mylake` | buckets do SeaweedFS em `/srv/lake/buckets` |
| Configuração | `~/workspace/my_orchestrator/.env` | `/srv/airflow/.env` (só no servidor) |

Duas consequências que explicam quase tudo:

1. **Dev vê o que está no seu disco agora; prod vê o que está mergeado.** Uma mudança no
   `my_ingestion` funciona em dev na hora e chega ao prod quando o PR dela for mergeado.
2. **Prod só roda o que passou por PR.** Ninguém publica da própria máquina: o deploy faz
   checkout no GitHub, não copia do seu disco.

---

## 2. O ciclo básico (vale para qualquer mudança)

```
desenvolver e validar em dev
        │
        ▼
PR no repo (my_ingestion / my_analytics / my_orchestrator) → você aprova → merge
        │
        ▼  automático
Deploy prod no runner do atb:
  checkout das 3 pontas → build_prod.sh (rsync para /srv/airflow)
  → restart só se precisar → dbt deps só se precisar → verify_prod.sh
        │
        ▼
Actions verde = prod atualizado; conferir na UI e despausar a DAG nova
```

As branches principais são protegidas: nunca dê push direto nelas. Sempre branch → PR.

**Ordem entre repos:** quando a DAG depende de código novo do `my_ingestion`, faça o merge do PR
do `my_ingestion` **primeiro** e o da DAG depois. Cada merge publica o que está nas pontas naquele
momento; se a DAG chegar antes do código, ela dá erro de importação e o deploy fica vermelho.
Pelo mesmo motivo, uma mudança no `my_ingestion` tem que continuar funcionando com as DAGs que já
estão em prod.

**Quase todo deploy é só rsync.** Em prod, como em dev, o Airflow lê do disco as DAGs, o
`my_ingestion` e o projeto dbt: o deploy copia os arquivos e o Airflow vê a mudança em até
1 minuto, sem reiniciar. O `deploy/estado.sh` decide o resto:

| Mudança | O que o deploy faz |
|---|---|
| só `.md` / `docs/` | nem dispara |
| DAG nova, alterada, renomeada ou apagada | rsync + verificação (sem queda) |
| código do `my_ingestion` (`src/`) | rsync + verificação (sem queda) |
| model, macro, seed do `my_analytics` | rsync + verificação (sem queda) |
| pacote dbt novo (`package-lock.yml`) | rsync + `dbt deps` no scheduler + verificação (sem queda) |
| `requirements.txt`, `Dockerfile`, `packages.txt`, `deploy/prod/*`, `plugins/` | **restart** (rebuild da imagem, 1-2 min fora do ar) |
| variável nova no `/srv/airflow/.env` | **restart**, no próximo deploy ou em *Run workflow* |

O resumo de cada run no *Actions* mostra se houve restart e `dbt deps`. Para forçar um restart:
*Run workflow* com a caixa **restart** marcada.

**O restart derruba tarefas em execução** (elas tentam de novo pelos `retries`). Como ele só
acontece com mudança de imagem, config ou `.env`, faça esses merges fora do horário das DAGs longas.

---

## 3. Cenário completo: fonte nova do zero

Exemplo: uma fonte `exemplo` no domínio `legislativo`, com uma entidade `itens`, que carrega em
`raw_exemplo.itens`, vira um model no dbt e ganha uma DAG.

### 3.1 `my_ingestion`: o pipeline

1. Crie `src/pipelines/legislativo/exemplo/` copiando uma fonte parecida
   (`ranking_politicos` é um bom modelo simples):
   - `exemplo_config.yml`: `db_schema`, os caminhos do lake **nos dois blocos**
     `environments.dev` (`${LAKE_ROOT}/...`) e `environments.prod`
     (`/usr/local/airflow/mylake/...`), e uma entrada em `sources:` por entidade
     (`base_url`, `landing_file`, `bronze_file`, `db_table`).
   - `exemplo_etl.py`: funções `extract` / `transform` e o dicionário `ETLS = {"itens": Etl(...)}`,
     mais o `CONFIG_FILE`.
   - `__init__.py`.
2. Rode pela CLI em dev:
   ```bash
   cd ~/workspace/my_ingestion
   uv run python -m pipelines.legislativo.exemplo.exemplo_etl
   uv run task test && uv run task lint
   ```
3. Confira `raw_exemplo.itens` no `ingestion_sandbox`. Validado, leve para o banco do dbt:
   `scripts/raw_copy.sh promote raw_exemplo` (no `my_ingestion`).
4. Credencial nova? Coloque no `settings.py` e no `.env.example` do `my_ingestion`, e anote: ela vai
   precisar existir também no `.env` do `my_orchestrator` (dev) e no `/srv/airflow/.env` (prod).
5. Biblioteca Python nova? Além do `pyproject.toml` do `my_ingestion`, ela precisa entrar no
   `requirements.txt` do `my_orchestrator` (bloco "deps do include/my_ingestion", mesma versão do
   `uv.lock`). O Airflow não instala o `my_ingestion` como pacote, só as dependências listadas ali.
   Nesse caso o PR do `requirements.txt` vai **antes** do merge do `my_ingestion`.
6. Commit, PR e merge no `my_ingestion`. O merge já publica em prod (não quebra nada: nenhuma DAG
   de prod usa o pipeline novo ainda).

### 3.2 `my_analytics`: o model

1. Declare a tabela nova em `models/staging/_sources.yml`:
   ```yaml
   - name: exemplo
     schema: raw_exemplo
     tables:
       - name: itens
   ```
2. Crie o model. **O schema sai da pasta** (macro `generate_schema_name`), não do config:
   - `models/staging/exemplo/stg_exemplo_itens.sql` → schema `staging_exemplo`
   - `models/intermediate/<dominio>/int_x.sql` → `intermediate_<dominio>`
   - `models/marts/<dominio>/mrt_x.sql` → `marts_<dominio>`
3. Rode em dev: `cd ~/workspace/my_analytics && uv run dbt build --select stg_exemplo_itens+` e confira no `analytics_dev`.
4. Commit, PR e merge na `main`. O merge já publica em prod.

Não é preciso mexer em DAG para o dbt: o `dag_dbt_my_analytics` (Cosmos) lê o projeto inteiro e
cria as tasks dos models novos sozinho. Mas ele está com `schedule=None` em prod: models novos só
rodam quando alguém dispara essa DAG, ou quando ela ganhar um schedule.

### 3.3 `my_orchestrator`: a DAG

1. Crie `dags/dag_exemplo.py` no padrão das outras:
   ```python
   """Exemplo: itens.

   DAG do Airflow montada por include/utils/etl_dag.py.
   """

   from pipelines.legislativo.exemplo.exemplo_etl import CONFIG_FILE, ETLS

   from include.utils.etl_dag import etl_group, source_dag

   with source_dag("exemplo", schedule=None, tags=["legislativo"]) as dag:  # em validação
       etl_group(CONFIG_FILE, ETLS, "itens")
   ```
   O arquivo precisa conter as palavras "airflow" e "dag" (o docstring cobre isso); sem elas o
   Airflow ignora o arquivo em silêncio.
2. Valide em dev (o `my_ingestion` do seu disco já está montado no container):
   ```bash
   astro dev restart    # só se mudou requirements.txt ou Dockerfile
   docker exec $(docker ps -qf name=scheduler) bash -c \
     'cd /usr/local/airflow && airflow dags test exemplo --dagfile-path /usr/local/airflow/dags/dag_exemplo.py'
   docker exec $(docker ps -qf name=scheduler) bash -c 'cd /usr/local/airflow && pytest -q tests/dags/'
   ```
3. Passou? Troque `schedule=None` pelo cron de verdade.
4. Um PR no `my_orchestrator` com `dags/dag_exemplo.py`.

   **Só faça o merge depois que o PR do `my_ingestion` estiver mergeado** (seção 2, "Ordem entre repos").
5. Credencial nova? Coloque no servidor **antes** do merge (o deploy nunca toca no `.env`):
   ```bash
   ssh atb
   nano /srv/airflow/.env      # adicionar a variável
   exit
   ```
6. Aprove e faça o merge.

### 3.4 Deploy

Não há nada a rodar. Acompanhe:

```bash
cd ~/workspace/my_orchestrator
gh run list --workflow deploy.yml --limit 3   # o run do merge aparece em segundos
gh run watch                                   # segue até terminar (~30 s sem restart; ~3 min com)
```

Ou pelo navegador: *GitHub → my_orchestrator → Actions → Deploy prod*. O último passo
(`verify_prod.sh`) só fica verde com 0 import errors e o número de DAGs igual ao de arquivos em `dags/` fora do `.airflowignore`.

### 3.5 Conferir em prod

1. UI em http://100.82.7.107:8080 (usuário `admin`): a DAG aparece, sem erro de importação.
2. Toda DAG nova nasce **pausada**. Despause no toggle; se quiser rodar já, use o ▶ (Trigger).
3. Depois da primeira execução, confira `raw_exemplo.itens` no `analytics_prod`.

---

## 4. Como garantir que o prod está na versão atual do `my_ingestion`

Com CI/CD, "atual" é o normal: depois de cada merge o prod recebe a ponta. Dá para conferir
com dois comandos:

**O que está rodando no atb agora?**
```bash
ssh atb head -2 /srv/airflow/DEPLOYED.txt
# my_orchestrator 7a4deac my_ingestion 514ce4b my_analytics dcd5d68
# montado em 2026-09-19T15:02:11-03:00 por repository_dispatch (run 123456)
```

**Qual é a ponta do `my_ingestion`?**
```bash
git -C ~/workspace/my_ingestion fetch -q
git -C ~/workspace/my_ingestion rev-parse --short origin/main
```

Leitura:
- Iguais → prod está na versão atual.
- Diferentes → o último deploy falhou ou ainda está rodando. Veja em *Actions → Deploy prod*
  (`gh run list --workflow deploy.yml` no `my_orchestrator`). Depois de corrigir, ou se o run
  nem apareceu, republique sem commit: *Run workflow* na mesma tela, ou
  `gh workflow run deploy.yml` no `my_orchestrator`.

A DAG `smoke_my_ingestion` (manual) confirma o ambiente pelo lado de dentro: o log de
`check_imports_and_settings` mostra `env = prod`, o banco e de onde o `core` foi importado.

---

## 5. Mudanças menores

| Quero... | Onde mexer | Publica? |
|---|---|---|
| Corrigir um pipeline que já existe | PR no `my_ingestion` | sim, no merge |
| Mudar ou criar model dbt | PR no `my_analytics` | sim, no merge |
| Mudar schedule ou lógica de uma DAG | PR no `my_orchestrator` | sim, no merge |
| Tirar uma DAG de dev e prod sem apagar o arquivo | linha em `dags/.airflowignore` (PR) | sim, no merge |
| Mudar senha ou credencial | `/srv/airflow/.env` no atb | *Run workflow* no Deploy prod (detecta o `.env` alterado e reinicia) |
| Pausar ou despausar | toggle na UI | não |
| Voltar uma versão | `git revert` do commit via PR no repo que quebrou | sim, no merge |

---

## 6. Regras e armadilhas

- **Prod = dev.** Toda DAG em `dags/` fora do `.airflowignore` vai para o prod no merge, inclusive as
  financeiras (`investments_*`). Sem as credenciais no `.env` do atb, elas falham na execução.
- **Credencial nova sem `.env` no atb** = a DAG quebra na importação (`settings` valida no import) ou
  na execução. Crie a variável antes do merge.
- **Deploy vermelho** = prod ficou com o código novo, mas com problema (import error, DAG faltando).
  Corrija com um PR novo ou `git revert` + PR; o merge publica de novo.
- **Dependência nova só no `my_ingestion`** = `ModuleNotFoundError` no Airflow. Espelhe no
  `requirements.txt` do `my_orchestrator`.
- **`pandas` fica em 2.1.4** no Airflow (SQLAlchemy 1.4). Não suba sem o Airflow migrar.
- **YAML sem o bloco `prod`** = o pipeline não acha os caminhos do lake em prod.
- **Rodando em dev e prod ao mesmo tempo**, cada fonte é consultada duas vezes. Os dados não se
  misturam (bancos e lakes separados), mas convém deixar agendada só uma das duas.
- **Se o container `seaweedfs-mount` reiniciar**, reinicie o Airflow também (*Run workflow* no Deploy
  prod, com a caixa **restart** marcada): ele guarda o mount antigo do lake.
- **`start.sh` falha com "/srv/lake/buckets não está montado"**: o serviço de mount do homelab está
  fora. Suba com `cd /srv/homelab && sudo docker compose --env-file stacks/seaweedfs/.env -f stacks/seaweedfs/docker-compose.yml up -d`.

---

## 7. Onde está cada coisa

| Coisa | Onde |
|---|---|
| Workflow de deploy | `my_orchestrator/.github/workflows/deploy.yml` (runner `atb-airflow`) |
| Gatilhos dos outros repos | `.github/workflows/deploy-prod.yml` no `my_ingestion` e no `my_analytics` |
| Scripts chamados pelo workflow | `my_orchestrator/deploy/build_prod.sh` e `deploy/verify_prod.sh` |
| Arquivos só de prod (override, `start.sh`, modelo de `.env`) | `my_orchestrator/deploy/prod/` |
| O que está no ar | `atb:/srv/airflow/DEPLOYED.txt` |
| `.env` de prod | `atb:/srv/airflow/.env` |
| Senha do `admin` da UI | `atb:/srv/airflow/simple_auth_manager_passwords.json.generated` |
| Lake de prod | SeaweedFS (buckets `raw`, `bronze`, `staging`, `gold`), no host em `/srv/lake/buckets` |
| Secrets do deploy | `INGESTION_READ_TOKEN` (my_orchestrator), `DEPLOY_DISPATCH_TOKEN` (my_ingestion e my_analytics) |
| Infra (Postgres, SeaweedFS) | repo `homelab`, `/srv/homelab` no atb; runbook em `docs/runbook.md` |
