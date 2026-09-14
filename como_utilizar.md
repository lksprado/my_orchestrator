# Como Rodar

Primeira vez (ou máquina nova):
```shell
git submodule update --init --recursive
cp .env.example .env        # preencher credenciais; dev exige DB__DEV__NAME=analytics_dev
astro dev start
```

UI em http://localhost:8090 (a 8080 é do Airflow do homelab). Depois de subir, rode a DAG
`smoke_my_ingestion` para conferir o wiring.

## Dev: código ao vivo

Em dev o `docker-compose.override.yml` monta o working tree de `~/workspace/my_ingestion/src`
e `~/workspace/the_dw` por cima dos submódulos `include/my_ingestion` e `dbt/the_dw`.
Editar lá reflete no Airflow sem rebuild nem bump de ponteiro. O bump só importa para
o build da imagem (deps do `requirements.txt`) e para prod.

Só `src/` do my_ingestion é montado, de propósito: o `.env` dele (localhost, `/media/...`)
não pode ser lido dentro do container. A configuração vem apenas do `.env` daqui.

## Submódulos

`include/my_ingestion`, `dbt/the_dw` e os submódulos antigos de `include/` (em extinção).
Após commit e push nos repos originais, para atualizar o ponteiro:
```shell
git pull origin main
git submodule update --remote include/my_ingestion dbt/the_dw
git add include/my_ingestion dbt/the_dw
git commit -m "chore: bump submódulos para última versão"
git push origin main
```

Mudou dependência no `my_ingestion`? Espelhe em `requirements.txt` (bloco "deps do
include/my_ingestion") e rode `astro dev restart` para rebuildar.

# Troubleshooting
Problema:
Erro ao criar tabela via dataframe com pandas `to_sql`: "Engine object has no attribute 'cursor' "
Solução:
pandas fica em 2.1.4 enquanto o Airflow usar SQLAlchemy 1.4 (Airflow 3.0.6). Não suba
o pandas para a versão do my_ingestion (3.x) sem o Airflow ter migrado para SQLAlchemy 2.

Problema:
DAG falha na importação com `ValidationError` do `settings` (faltam `DB__DEV__*`, `LAKE_ROOT`...).
Solução:
O `.env` da raiz está incompleto; compare com `.env.example` e `astro dev restart`.
