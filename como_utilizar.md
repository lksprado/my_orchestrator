# Como Rodar

Primeira vez (ou máquina nova):
```shell
git clone https://github.com/lksprado/my_ingestion.git ~/workspace/my_ingestion
git clone https://github.com/lksprado/my_datawarehouse.git ~/workspace/the_dw
cp .env.example .env        # preencher credenciais; dev exige DB__DEV__NAME=analytics_dev
astro dev start
```

UI em http://localhost:8090 (a 8080 é do Airflow do homelab). Depois de subir, rode a DAG
`smoke_my_ingestion` para conferir o wiring.

## Dev: código ao vivo por volume

O `my_ingestion` e o `the_dw` **não são submódulos**. O `docker-compose.override.yml` monta
`~/workspace/my_ingestion/src` e `~/workspace/the_dw` dentro do container. Editar lá reflete
no Airflow na hora, sem rebuild e sem ponteiro para atualizar.

Só `src/` do my_ingestion é montado, de propósito: o `.env` dele (localhost, `/media/...`)
não pode ser lido dentro do container. A configuração vem apenas do `.env` daqui.

Mudou dependência no `my_ingestion`? Espelhe em `requirements.txt` (bloco "deps do
include/my_ingestion") e rode `astro dev restart` para rebuildar.

## Prod: versões fixadas

Prod executa exatamente os commits de `deploy/versions.txt`. Para promover uma versão nova,
troque o SHA em commit próprio:
```shell
git -C ~/workspace/my_ingestion rev-parse origin/main   # SHA a promover
# editar deploy/versions.txt
git commit -m "chore(deploy): promove my_ingestion para <sha curto>" deploy/versions.txt
```
No servidor, `deploy/checkout_versions.sh <destino>` coloca os repos nesses commits.

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
