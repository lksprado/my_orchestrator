# Como Rodar

Primeira vez (ou máquina nova):
```shell
git clone https://github.com/lksprado/my_ingestion.git ~/workspace/my_ingestion
git clone https://github.com/lksprado/my_analytics.git ~/workspace/my_analytics
cp .env.example .env        # preencher credenciais; dev exige DB__DEV__NAME=analytics_dev
astro dev start
```

UI em http://localhost:8090 (a 8080 é do Airflow do homelab). Depois de subir, rode a DAG
`smoke_my_ingestion` para conferir o wiring.

## Dev: código ao vivo por volume

O `my_ingestion` e o `my_analytics` **não são submódulos**. O `docker-compose.override.yml` monta
`~/workspace/my_ingestion/src` e `~/workspace/my_analytics` dentro do container. Editar lá reflete
no Airflow na hora, sem rebuild e sem ponteiro para atualizar.

Só `src/` do my_ingestion é montado, de propósito: o `.env` dele (localhost, `/media/...`)
não pode ser lido dentro do container. A configuração vem apenas do `.env` daqui.

Mudou dependência no `my_ingestion`? Espelhe em `requirements.txt` (bloco "deps do
include/my_ingestion") e rode `astro dev restart` para rebuildar.

## DAGs no dia a dia

- **DAG nova nasce pausada** no Airflow local: despause na UI para o agendamento valer.
- **Reprocessar sem consultar a fonte:** dispare a DAG com a configuração abaixo. O extract é pulado e o transform relê todo o landing.
  ```json
  {"steps": ["transform", "load"]}
  ```
  Para Solar e weather, que movem os JSONs para o bronze depois da carga, devolva os arquivos ao staging antes.
- **DAGs manuais** e o que precisam antes:
  - `camara_cadastro`, `senado_cadastro`: nada; rode quando mudar a legislatura.
  - `investimentos_arquivos`: copie os Excel da B3 e os PDFs da Avenue para `raw/investments/b3|avenue/<pessoa>/`.
  - `investimentos_fgc`: `raw/investments/instituicoes/instituicoes_conglomerado_prudencial.csv` no lake e a camada intermediate do my_analytics construída. Hoje falha: o SQL lê `intermediate.int_renda_fixa`, e o my_analytics gera `intermediate_financas`.
  - `atacadao_historico`: CSVs mensais em `bronze/inflation/months/`. Grava `minha_inflacao.csv` nos seeds do my_analytics, com colunas diferentes do seed atual.
  - `fundos_imobiliarios`: params `month` (YYYY-MM, vazio = mês corrente), `force` e `consolidate_only`. Usa o Selenium remoto e leva uns 40 minutos.
- **Validar uma DAG** sem esperar o scheduler (a DAG precisa estar em arquivo; a do NHL, ignorada, também funciona assim):
  ```shell
  docker exec $(docker ps -qf name=scheduler) bash -c \
    'cd /usr/local/airflow && airflow dags test <dag_id> --dagfile-path /usr/local/airflow/dags/<arquivo>.py'
  ```

## Prod: versões fixadas

Passo a passo completo (fonte nova no my_ingestion, model no my_analytics, DAG nova, como
conferir a versão em prod): [`esteira_de_deploy.md`](esteira_de_deploy.md).


Prod executa exatamente os commits de `deploy/versions.txt` e só as DAGs de
`deploy/prod-dags.txt`. A `main` é protegida: toda mudança entra por PR, com aprovação manual.

1. Branch e commit (um assunto por PR):
   ```shell
   git checkout -b chore/promove-my-ingestion-<sha curto>
   git -C ~/workspace/my_ingestion rev-parse origin/main   # SHA a promover
   # editar deploy/versions.txt (ou adicionar a DAG em deploy/prod-dags.txt)
   git commit -m "chore(deploy): promove my_ingestion para <sha curto>" deploy/versions.txt
   git push -u origin HEAD
   gh pr create --base main
   ```
2. Aprovar e fazer o merge no GitHub.
3. Com a `main` local atualizada, enviar para o `atb` e reiniciar (o segundo comando pede o sudo do servidor):
   ```shell
   git checkout main && git pull
   deploy/deploy_prod.sh              # --dry-run para ver antes o que muda
   ssh -t atb /srv/airflow/start.sh restart
   ```

O `deploy_prod.sh` recusa rodar com alteração local ou com `HEAD` diferente de `origin/main`:
o que vai para o `atb` é sempre código que passou por PR. Variável nova de ambiente vai à mão em
`/srv/airflow/.env` (o deploy nunca toca nesse arquivo) e vale depois do `restart`.

## Sem submódulos

O repo não tem mais submódulos. Os antigos de `include/` foram removidos em 2026-09-14 e o código deles vive no `my_ingestion`. As DAGs legadas estão em `dags/.airflowignore` até serem migradas.

# Troubleshooting
Problema:
Erro ao criar tabela via dataframe com pandas `to_sql`: "Engine object has no attribute 'cursor' "
Solução:
pandas fica em 2.1.4 enquanto o Airflow usar SQLAlchemy 1.4 (Airflow 3.0.6). Não suba
o pandas para a versão do my_ingestion (3.x) sem o Airflow ter migrado para SQLAlchemy 2.

Problema:
DAG falha na importação com `ModuleNotFoundError: No module named 'core'`.
Solução:
O `~/workspace/my_ingestion` não existe ou não está montado. Clone o repo e rode `astro dev restart`.

Problema:
DAG falha na importação com `ValidationError` do `settings` (faltam `DB__DEV__*`, `LAKE_ROOT`...).
Solução:
O `.env` da raiz está incompleto; compare com `.env.example` e `astro dev restart`.
