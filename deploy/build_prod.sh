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
#   - de dags/, só os arquivos de deploy/prod-dags.txt;
#   - deploy/prod/* no lugar do override de dev, do .astro/config.yaml e do start.sh;
#   - my_ingestion (só src/) em include/my_ingestion/src e my_analytics em
#     dbt/my_analytics. Os pacotes dbt são instalados no build da imagem (Dockerfile).
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

# 2. DAGs da allowlist e arquivos de prod.
mapfile -t permitidas < <(grep -vE '^\s*(#|$)' deploy/prod-dags.txt)
for f in "${permitidas[@]}"; do
    [[ -f "$build/dags/$f" ]] || { echo "erro: $f está em prod-dags.txt mas não existe em dags/" >&2; exit 1; }
done
for f in "$build"/dags/*.py; do
    nome=$(basename "$f")
    printf '%s\n' "${permitidas[@]}" | grep -qxF "$nome" || rm "$f"
done
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
DAGs: ${permitidas[*]}
EOF

# 5. Sincroniza. --delete remove o que saiu da allowlist. __pycache__ fica de
# fora: o Astro monta dags/ e include/ nos containers e o scheduler (root) grava
# ali arquivos que o usuário do runner não consegue apagar.
rsync -a --delete --exclude=/.env --exclude=/simple_auth_manager_passwords.json.generated \
      --exclude=__pycache__/ "$build/" "$destino"

echo "publicado em $destino: $(head -1 "$build/DEPLOYED.txt")"
