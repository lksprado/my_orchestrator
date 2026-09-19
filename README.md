# my_orchestrator

Airflow (Astro Runtime 3.0-10, Airflow 3.0.6, Python 3.12) que orquestra os outros dois repos:

- o **extrair e carregar** vem do [`my_ingestion`](https://github.com/lksprado/my_ingestion);
- o **transformar** (dbt) vem do [`my_analytics`](https://github.com/lksprado/my_analytics).

Este repo tem só as DAGs, a imagem e o deploy.

> Visão geral do deploy dos quatro repos (runners, tokens, troubleshooting):
> [`homelab/docs/como_funciona_o_deploy.md`](https://github.com/lksprado/homelab/blob/main/docs/como_funciona_o_deploy.md).

## Uso em dev

```bash
cp .env.example .env      # preencha; ENV=dev e banco analytics_dev
pre-commit install        # bloqueia commit direto na main
astro dev start           # UI em http://localhost:8090
```

O `my_ingestion` e o `my_analytics` entram por volume de `~/workspace/`. O Airflow local lê o
que está no seu disco agora, sem commit. As DAGs nascem **pausadas**: despause na UI.

Testes das DAGs (dentro do scheduler, que tem os volumes; o `astro dev pytest` não tem):

```bash
docker exec $(docker ps -qf name=scheduler) bash -c 'cd /usr/local/airflow && pytest -q tests/dags/'
```

## DAG nova

Uma DAG por fonte do `my_ingestion`:

```python
"""Ranking dos Políticos: deputados e senadores (semanal)."""

from include.utils.etl_dag import etl_group, source_dag
from pipelines.legislativo.ranking_politicos.ranking_politicos_etl import CONFIG_FILE, ETLS

with source_dag("ranking_politicos", schedule="0 7 * * 1", tags=["demodados"]) as dag:
    etl_group(CONFIG_FILE, ETLS, "deputados")
    etl_group(CONFIG_FILE, ETLS, "senadores")
```

- Nasce com `schedule=None` e só ganha agendamento depois de validada em dev.
- O arquivo precisa conter as palavras "airflow" e "dag"; sem elas o Airflow o ignora em silêncio.
- Credenciais vêm do `.env` via `settings`, nunca de `Variable.get`.
- Para ir a produção, o arquivo entra em `deploy/prod-dags.txt`. O que não está lá fica só em dev.

Passo a passo completo (fonte, model e DAG nova até prod): [`esteira_de_deploy.md`](esteira_de_deploy.md).

## Deploy

**A `main` é produção.** Não existe comando de deploy: o merge publica.

### Ao abrir o PR

O workflow `PR` (`.github/workflows/pr.yml`) confere três coisas:

- toda DAG listada em `deploy/prod-dags.txt` existe;
- as DAGs e o `include/utils` compilam;
- os scripts de `deploy/` têm sintaxe válida.

Import error de verdade só aparece com o Airflow completo. Ele é pego depois do merge, no passo *Verificar*.

### Ao fazer o merge

O workflow **Deploy prod** roda no runner `atb-airflow`, dentro do atb:

1. Pega a ponta da `main` dos três repos.
2. Sincroniza o resultado em `/srv/airflow`.
3. Decide sozinho o que mais precisa:

| O que mudou | O que acontece |
|---|---|
| só `.md` ou `docs/` | nada, nem dispara |
| DAG, `prod-dags.txt`, `include/` | só rsync; entra em até 1 min, sem queda |
| `requirements.txt`, `Dockerfile`, `packages.txt`, `plugins/`, `deploy/prod/*` | **restart** (rebuild da imagem, ~3 min) |

4. Confere o resultado:
   - 0 import errors;
   - número de DAGs igual ao da allowlist;
   - nenhuma porta aberta em `0.0.0.0`.

Verde no Actions significa que o prod está atualizado. Vermelho manda e-mail.

> O restart derruba tarefas em execução, que voltam pelos `retries`. Se uma DAG longa estiver
> rodando, deixe o merge de `requirements.txt` ou `Dockerfile` para depois.

**Acompanhar:**
- `gh run watch -R lksprado/my_orchestrator`;
- para ver qual versão está no ar: `ssh atb head -2 /srv/airflow/DEPLOYED.txt`.

**Variável nova ou senha trocada em prod:**
1. `ssh atb`, edite `/srv/airflow/.env` (o modelo é `deploy/prod/.env.example`).
2. Rode *Actions → Deploy prod → Run workflow*. Ele percebe que o `.env` mudou e reinicia.

**Desfazer:** `git revert` numa branch, PR e merge.

## Licença

MIT
