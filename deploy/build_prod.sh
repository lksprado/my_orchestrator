#!/usr/bin/env bash
#
# Monta o Airflow de prod a partir de três checkouts e sincroniza com o destino.
# Quem chama é o workflow .github/workflows/deploy.yml, no runner do atb, depois
# de fazer checkout da ponta de cada repo (main é produção).
#
#   deploy/build_prod.sh <my_ingestion> <my_analytics> [destino]
#
# O repo my_orchestrator é o que contém este script. O destino padrão é
# /srv/airflow/. O que vai para lá:
#   - este repo no HEAD (só o que está versionado);
#   - dags/ inteiro, como em dev (o .airflowignore vale nos dois);
#   - deploy/prod/* no lugar do override de dev, do .astro/config.yaml e do start.sh;
#   - my_ingestion (só src/) em include/my_ingestion/src e my_analytics em
#     dbt/my_analytics (os pacotes dbt são instalados pelo passo "dbt deps" do
#     workflow, no próprio servidor).
# O .env e o arquivo de senhas do Airflow no destino nunca são tocados.

set -euo pipefail

[[ $# -ge 2 ]] || { echo "uso: $0 <my_ingestion> <my_analytics> [destino]" >&2; exit 2; }
ingestion=$(cd "$1" && pwd)
analytics=$(cd "$2" && pwd)
destino=${3:-/srv/airflow/}

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"

build=$(mktemp -d "${TMPDIR:-/tmp}/airflow-prod.XXXXXX")
chmod 755 "$build"  # o rsync -a levaria o 700 do mktemp para o destino
trap 'rm -rf "$build"' EXIT

# 1. Este repo, só o que está versionado.
git archive HEAD | tar -x -C "$build"

# 2. Arquivos de prod.
cp deploy/prod/docker-compose.override.yml "$build/docker-compose.override.yml"
cp deploy/prod/config.yaml "$build/.astro/config.yaml"
cp deploy/prod/start.sh "$build/start.sh"
rm -f "$build/airflow_settings.yaml"

# 3. my_ingestion e my_analytics, no commit que está em checkout.
mkdir -p "$build/include/my_ingestion" "$build/dbt/my_analytics"
git -C "$ingestion" archive HEAD src | tar -x -C "$build/include/my_ingestion"
git -C "$analytics" archive HEAD | tar -x -C "$build/dbt/my_analytics"

# 4. O que está no ar.
curto() { git -C "$1" rev-parse --short HEAD; }
cat > "$build/DEPLOYED.txt" <<EOF
my_orchestrator $(curto "$repo") my_ingestion $(curto "$ingestion") my_analytics $(curto "$analytics")
montado em $(date -Iseconds) por ${DEPLOY_ORIGEM:-execução manual}
EOF

# 5. Sincroniza. --delete remove o que saiu do repo (DAG apagada ou renomeada).
# Ficam de fora o que é gerado no servidor: __pycache__ e dbt_packages/target/logs
# (o scheduler, root, grava nos binds e o usuário do runner não consegue apagar) e
# os hashes do deploy/estado.sh.
rsync -a --delete --exclude=/.env --exclude=/simple_auth_manager_passwords.json.generated \
      --exclude=__pycache__/ --exclude='/.deploy-*.sha256' \
      --exclude=/dbt/my_analytics/dbt_packages/ --exclude=/dbt/my_analytics/target/ \
      --exclude=/dbt/my_analytics/logs/ "$build/" "$destino"

echo "publicado em $destino: $(head -1 "$build/DEPLOYED.txt")"
