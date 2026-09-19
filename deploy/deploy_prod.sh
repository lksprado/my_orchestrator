#!/usr/bin/env bash
#
# Monta o Airflow de prod nesta máquina e envia para o atb.
#
#   deploy/deploy_prod.sh              monta e envia (rsync para atb:/srv/airflow)
#   deploy/deploy_prod.sh --dry-run    só monta, mostra o diretório e o que o rsync mudaria
#
# Roda aqui, não no servidor: o my_ingestion é privado e o atb não guarda
# credencial do GitHub. O que vai para o atb:
#   - este repo no HEAD (que tem que ser igual a origin/main, sem alterações);
#   - de dags/, só os arquivos de deploy/prod-dags.txt;
#   - deploy/prod/* no lugar do override de dev, do .astro/config.yaml e do start.sh;
#   - my_ingestion (só src/) e my_analytics nos SHAs de deploy/versions.txt, dentro
#     da imagem (include/my_ingestion/src e dbt/my_analytics), com os pacotes dbt
#     do package-lock.yml.
# O .env e o arquivo de senhas do Airflow no atb nunca são tocados.
# Depois: ssh -t atb /srv/airflow/start.sh restart

set -euo pipefail

DESTINO=atb:/srv/airflow/
WORKSPACE=${WORKSPACE:-$HOME/workspace}
dry_run=false
[[ "${1:-}" == "--dry-run" ]] && dry_run=true

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"

# 1. Só código publicado: sem alterações locais e HEAD == origin/main.
[[ -z "$(git status --porcelain)" ]] || { echo "erro: $repo tem alterações locais" >&2; exit 1; }
git fetch --quiet origin main
[[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/main)" ]] \
    || { echo "erro: HEAD difere de origin/main (dê push ou atualize)" >&2; exit 1; }

build=$(mktemp -d "${TMPDIR:-/tmp}/airflow-prod.XXXXXX")
chmod 755 "$build"  # o rsync -a leva o 700 do mktemp para /srv/airflow
$dry_run || trap 'rm -rf "$build"' EXIT

# 2. Este repo, só o que está versionado.
git archive HEAD | tar -x -C "$build"

# 3. DAGs da allowlist e arquivos de prod.
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

# 4. Repos nos commits fixados.
declare -A shas
while read -r nome sha url; do
    [[ -z "${nome:-}" || "$nome" == \#* ]] && continue
    src="$WORKSPACE/$nome"
    [[ -d "$src/.git" ]] || { echo "erro: falta o clone $src" >&2; exit 1; }
    git -C "$src" cat-file -e "$sha^{commit}" 2>/dev/null || git -C "$src" fetch --quiet origin
    git -C "$src" cat-file -e "$sha^{commit}" || { echo "erro: $nome não tem o commit $sha" >&2; exit 1; }
    case "$nome" in
        my_ingestion)
            mkdir -p "$build/include/my_ingestion"
            git -C "$src" archive "$sha" src | tar -x -C "$build/include/my_ingestion" ;;
        my_analytics)
            mkdir -p "$build/dbt/my_analytics"
            git -C "$src" archive "$sha" | tar -x -C "$build/dbt/my_analytics" ;;
        *) echo "erro: repo desconhecido em versions.txt: $nome" >&2; exit 1 ;;
    esac
    shas[$nome]=$sha
done < deploy/versions.txt

# dbt_packages/ não é versionado no my_analytics: instala as versões do
# package-lock.yml com o dbt do venv dele.
dbt="$WORKSPACE/my_analytics/.venv/bin/dbt"
[[ -x "$dbt" ]] || { echo "erro: falta $dbt (uv sync no my_analytics)" >&2; exit 1; }
(cd "$build/dbt/my_analytics" && "$dbt" deps --quiet --project-dir . --profiles-dir .)
rm -rf "$build/dbt/my_analytics/logs"

# 5. O que está no ar.
cat > "$build/DEPLOYED.txt" <<EOF
my_orchestrator $(git rev-parse --short HEAD) my_ingestion ${shas[my_ingestion]:0:7} my_analytics ${shas[my_analytics]:0:7}
montado em $(date -Iseconds)
DAGs: ${permitidas[*]}
EOF

# 6. Envio. --delete remove do atb o que saiu da allowlist.
rsync_args=(-a --delete --exclude=/.env --exclude=/simple_auth_manager_passwords.json.generated)
if $dry_run; then
    echo "montado em $build"
    rsync "${rsync_args[@]}" --dry-run --itemize-changes "$build/" "$DESTINO" | grep -v '^\.' || true
    exit 0
fi
rsync "${rsync_args[@]}" "$build/" "$DESTINO"

echo "enviado: $(head -1 "$build/DEPLOYED.txt")"
echo "suba com:  ssh -t atb /srv/airflow/start.sh restart"
