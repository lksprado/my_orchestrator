#!/usr/bin/env bash
#
# Confere o Airflow de prod depois de um deploy. Roda no atb (workflow deploy.yml).
#
#   deploy/verify_prod.sh [diretório do projeto]     padrão: /srv/airflow
#
# Falha se: o scheduler não está de pé, há import error, o número de DAGs difere
# da allowlist, ou alguma porta está publicada em 0.0.0.0 (regra de ouro do homelab).

set -euo pipefail

projeto=${1:-/srv/airflow}
repo=$(cd "$(dirname "$0")/.." && pwd)
esperadas=$(grep -cvE '^\s*(#|$)' "$repo/deploy/prod-dags.txt")

scheduler=$(sudo docker ps -q \
    --filter "label=com.docker.compose.project.working_dir=$projeto" \
    --filter "label=com.docker.compose.service=scheduler")
[[ -n "$scheduler" ]] || { echo "erro: scheduler de $projeto não está rodando" >&2; exit 1; }

airflow() { sudo docker exec "$scheduler" airflow "$@"; }

# O dag-processor leva alguns segundos para parsear tudo depois do restart, e
# import errors antigos só somem quando o arquivo é reparseado: espera até 3 min.
for tentativa in $(seq 1 18); do
    erros=$(airflow dags list-import-errors -o json 2>/dev/null | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo "?")
    total=$(airflow dags list -o json 2>/dev/null | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo "?")
    echo "tentativa $tentativa: $total DAGs (esperadas $esperadas), $erros import errors"
    [[ "$erros" == "0" && "$total" == "$esperadas" ]] && break
    sleep 10
done

if [[ "$erros" != "0" ]]; then
    echo "erro: import errors no Airflow de prod" >&2
    airflow dags list-import-errors >&2 || true
    exit 1
fi
[[ "$total" == "$esperadas" ]] || { echo "erro: $total DAGs carregadas, esperadas $esperadas" >&2; exit 1; }

# Regra de ouro: nada publicado em 0.0.0.0 (a 22 é protegida pelo UFW).
fora=$(ss -ltn | awk '{print $4}' | grep -E '^0\.0\.0\.0:' | grep -v ':22$' || true)
[[ -z "$fora" ]] || { echo "erro: porta publicada em 0.0.0.0: $fora" >&2; exit 1; }

echo "ok: $total DAGs, 0 import errors, nenhuma porta em 0.0.0.0"
