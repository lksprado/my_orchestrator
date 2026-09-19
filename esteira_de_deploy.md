# Esteira de deploy: do código ao Airflow de produção

Guia de consulta para quando você for criar ou mudar alguma coisa. Explica o que vive em cada
repositório, como uma mudança chega ao `atb` e como conferir o que está rodando lá.

---

## 1. As peças

São três repositórios. Só o `my_orchestrator` é deployado; os outros dois entram nele por SHA.

| Repositório | O que tem | Como chega ao prod |
|---|---|---|
| `my_ingestion` | extração e transformação das fontes (`src/pipelines/<domínio>/<fonte>/`) e a carga nas tabelas `raw_*` | pelo SHA em `my_orchestrator/deploy/versions.txt` |
| `my_analytics` | projeto dbt (staging → intermediate → marts) | pelo SHA em `my_orchestrator/deploy/versions.txt` |
| `my_orchestrator` | DAGs, `Dockerfile`, `requirements.txt`, allowlist e scripts de deploy | `deploy/deploy_prod.sh` (rsync da sua máquina para o atb) |
| `homelab` | infra do atb: Postgres, SeaweedFS (lake), observabilidade | merge na `main` → o runner do atb aplica sozinho |

Os dois ambientes:

| | **dev** (sua máquina) | **prod** (`atb`) |
|---|---|---|
| Airflow | `~/workspace/my_orchestrator`, `astro dev start`, http://localhost:8090 | `/srv/airflow`, http://100.82.7.107:8080 |
| Código do `my_ingestion` / `my_analytics` | **working tree** de `~/workspace/...`, montado ao vivo | **SHA fixado** em `deploy/versions.txt` |
| DAGs | todas as de `dags/` (menos o `.airflowignore`) | só as de `deploy/prod-dags.txt` |
| Banco | `analytics_dev` (localhost:5435) | `analytics_prod` (`postgres-dwh`, 100.82.7.107:5432) |
| Lake | `/media/lucas/Files/2.Projetos/0.mylake` | buckets do SeaweedFS em `/srv/lake/buckets` |
| Configuração | `~/workspace/my_orchestrator/.env` | `/srv/airflow/.env` (só no servidor) |

Duas consequências que explicam quase tudo:

1. **Dev vê o que está no seu disco agora; prod vê só o que tem SHA no `versions.txt`.** Uma mudança
   no `my_ingestion` funciona em dev na hora e não chega ao prod até você trocar o SHA.
2. **Prod só roda o que passou por PR.** O `deploy_prod.sh` recusa rodar com alteração local ou com
   a `main` diferente da `origin/main`.

---

## 2. O ciclo básico (vale para qualquer mudança)

```
desenvolver e validar em dev
        │
        ▼
PR no repo de origem (my_ingestion / my_analytics)  → merge
        │
        ▼
PR no my_orchestrator: DAG, allowlist e/ou SHA novo  → você aprova → merge
        │
        ▼
git checkout main && git pull           (no my_orchestrator)
deploy/deploy_prod.sh                   (monta e envia para o atb)
ssh -t atb /srv/airflow/start.sh restart   (rebuild + restart, pede o sudo do atb)
        │
        ▼
conferir na UI do prod e despausar a DAG
```

A `main` do `my_orchestrator` é protegida: nunca dê push direto nela. Sempre branch → PR.

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
3. Confira `raw_exemplo.itens` no `analytics_dev`.
4. Credencial nova? Coloque no `settings.py` e no `.env.example` do `my_ingestion`, e anote: ela vai
   precisar existir também no `.env` do `my_orchestrator` (dev) e no `/srv/airflow/.env` (prod).
5. Biblioteca Python nova? Além do `pyproject.toml` do `my_ingestion`, ela precisa entrar no
   `requirements.txt` do `my_orchestrator` (bloco "deps do include/my_ingestion", mesma versão do
   `uv.lock`). O Airflow não instala o `my_ingestion` como pacote, só as dependências listadas ali.
6. Commit, PR e merge no `my_ingestion`. Anote o SHA:
   ```bash
   git -C ~/workspace/my_ingestion fetch
   git -C ~/workspace/my_ingestion rev-parse origin/main
   ```

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
4. Commit, PR e merge. Anote o SHA (`git -C ~/workspace/my_analytics rev-parse origin/main`).

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
4. Um PR só no `my_orchestrator`, com três mudanças:
   - `dags/dag_exemplo.py`;
   - a linha `dag_exemplo.py` em `deploy/prod-dags.txt`;
   - os SHAs novos em `deploy/versions.txt` (`my_ingestion` e, se mudou, `my_analytics`).

   **A DAG e o SHA têm que ir juntos (ou o SHA antes).** A DAG faz `import pipelines.legislativo.exemplo`;
   se ela chegar ao prod com o `my_ingestion` antigo, aparece como erro de importação na UI.
5. Aprove e faça o merge.

### 3.4 Deploy

```bash
cd ~/workspace/my_orchestrator
git checkout main && git pull
deploy/deploy_prod.sh --dry-run     # opcional: lista o que muda no atb
deploy/deploy_prod.sh
```

Se houver credencial nova, coloque no servidor **antes** do restart (o deploy nunca toca no `.env`):

```bash
ssh atb
nano /srv/airflow/.env      # adicionar a variável
exit
```

E reinicie (rebuild da imagem, pede o sudo do atb):

```bash
ssh -t atb /srv/airflow/start.sh restart
```

### 3.5 Conferir em prod

1. UI em http://100.82.7.107:8080 (usuário `admin`): a DAG aparece, sem erro de importação.
2. Toda DAG nova nasce **pausada**. Despause no toggle; se quiser rodar já, use o ▶ (Trigger).
3. Depois da primeira execução, confira `raw_exemplo.itens` no `analytics_prod`.

---

## 4. Como garantir que o prod está na versão atual do `my_ingestion`

O prod roda **exatamente** o SHA do `deploy/versions.txt` que estava na `main` no último deploy.
Três perguntas, três comandos:

**O que está rodando no atb agora?**
```bash
ssh atb head -1 /srv/airflow/DEPLOYED.txt
# my_orchestrator 7a4deac my_ingestion 514ce4b my_analytics dcd5d68
```

**O que o `versions.txt` pede?**
```bash
grep -v '^#' ~/workspace/my_orchestrator/deploy/versions.txt
```

**Qual é o último commit publicado do `my_ingestion`?**
```bash
git -C ~/workspace/my_ingestion fetch -q
git -C ~/workspace/my_ingestion rev-parse --short origin/main
```

Leitura:
- `DEPLOYED.txt` ≠ `versions.txt` → o merge aconteceu mas o deploy não. Rode `deploy_prod.sh` + restart.
- `versions.txt` ≠ `origin/main` do `my_ingestion` → existe código novo que não foi promovido.
  Isso pode ser de propósito (prod só anda quando você decide). Para promover:
  ```bash
  cd ~/workspace/my_orchestrator
  git checkout main && git pull
  git checkout -b chore/promove-my-ingestion-<sha curto>
  # trocar o SHA do my_ingestion em deploy/versions.txt pelo de origin/main
  git commit -m "chore(deploy): promove my_ingestion para <sha curto>" deploy/versions.txt
  git push -u origin HEAD
  gh pr create --base main
  # aprovar/merge → checkout main, pull, deploy_prod.sh, restart
  ```
- Os três iguais → prod está na versão atual.

Antes de promover, veja o que entra: `git -C ~/workspace/my_ingestion log --oneline <sha antigo>..origin/main`.

A DAG `smoke_my_ingestion` (manual) confirma o ambiente pelo lado de dentro: o log de
`check_imports_and_settings` mostra `env = prod`, o banco e de onde o `core` foi importado.

---

## 5. Mudanças menores

| Quero... | Onde mexer | Precisa de deploy? |
|---|---|---|
| Corrigir um pipeline que já existe | PR no `my_ingestion`; depois PR no `my_orchestrator` trocando o SHA | sim |
| Mudar ou criar model dbt | PR no `my_analytics`; depois PR trocando o SHA | sim |
| Mudar schedule ou lógica de uma DAG | PR no `my_orchestrator` | sim |
| Levar uma DAG que já existe ao prod | linha nova em `deploy/prod-dags.txt` (PR) | sim |
| Tirar uma DAG do prod | apagar a linha em `deploy/prod-dags.txt` (PR); o deploy remove o arquivo do atb | sim |
| Mudar senha ou credencial | `/srv/airflow/.env` no atb | só `start.sh restart` |
| Pausar ou despausar | toggle na UI | não |
| Voltar uma versão | `git revert` do commit (ex.: o bump de SHA) via PR | sim |

---

## 6. Regras e armadilhas

- **Dados sensíveis ficam só em dev.** As DAGs financeiras (`investimentos_*`) não estão na allowlist,
  e o prod não tem as credenciais delas. Não promova sem decidir isso conscientemente.
- **Credencial nova sem `.env` no atb** = a DAG quebra na importação (`settings` valida no import) ou
  na execução. Crie a variável antes do restart.
- **Dependência nova só no `my_ingestion`** = `ModuleNotFoundError` no Airflow. Espelhe no
  `requirements.txt` do `my_orchestrator`.
- **`pandas` fica em 2.1.4** no Airflow (SQLAlchemy 1.4). Não suba sem o Airflow migrar.
- **YAML sem o bloco `prod`** = o pipeline não acha os caminhos do lake em prod.
- **Rodando em dev e prod ao mesmo tempo**, cada fonte é consultada duas vezes. Os dados não se
  misturam (bancos e lakes separados), mas convém deixar agendada só uma das duas.
- **Se o container `seaweedfs-mount` reiniciar**, rode `start.sh restart` no Airflow também: ele guarda
  o mount antigo do lake.
- **`start.sh` falha com "/srv/lake/buckets não está montado"**: o serviço de mount do homelab está
  fora. Suba com `cd /srv/homelab && sudo docker compose --env-file stacks/seaweedfs/.env -f stacks/seaweedfs/docker-compose.yml up -d`.

---

## 7. Onde está cada coisa

| Coisa | Onde |
|---|---|
| Script de deploy | `my_orchestrator/deploy/deploy_prod.sh` |
| Allowlist de DAGs de prod | `my_orchestrator/deploy/prod-dags.txt` |
| Versões de prod | `my_orchestrator/deploy/versions.txt` |
| Arquivos só de prod (override, `start.sh`, modelo de `.env`) | `my_orchestrator/deploy/prod/` |
| O que está no ar | `atb:/srv/airflow/DEPLOYED.txt` |
| `.env` de prod | `atb:/srv/airflow/.env` |
| Senha do `admin` da UI | `atb:/srv/airflow/simple_auth_manager_passwords.json.generated` |
| Lake de prod | SeaweedFS (buckets `raw`, `bronze`, `staging`, `gold`), no host em `/srv/lake/buckets` |
| Infra (Postgres, SeaweedFS) | repo `homelab`, `/srv/homelab` no atb; runbook em `docs/runbook.md` |
