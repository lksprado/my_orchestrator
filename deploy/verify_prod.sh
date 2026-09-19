#!/usr/bin/env bash
#
# Confere o Airflow de prod depois de um deploy. Roda no atb (workflow deploy.yml).
#
#   deploy/verify_prod.sh [diretório do projeto]     padrão: /srv/airflow
#
# Falha se: o scheduler não está de pé, há import error, o número de DAGs difere
# do de arquivos em dags/ fora do .airflowignore (pega arquivo pulado em silêncio)
# ou alguma porta está publicada em 0.0.0.0 (regra de ouro do homelab).

set -euo pipefail

projeto=${1:-/srv/airflow}
esperadas=$(find "$projeto/dags" -maxdepth 1 -name '*.py' -printf '%f\n' \
    | grep -cvxFf <(grep -vE '^\s*(#|$)' "$projeto/dags/.airflowignore") || true)

scheduler=$(sudo docker ps -q \
    --filter "label=com.docker.compose.project.working_dir=$projeto" \
    --filter "label=com.docker.compose.service=scheduler")
[[ -n "$scheduler" ]] || { echo "erro: scheduler de $projeto não está rodando" >&2; exit 1; }

# Consulta direto no banco de metadados. O CLI (airflow dags list -o json) não
# serve: ele escreve avisos de log no stdout antes do JSON. Só a última linha,
# prefixada com RESULTADO, é lida.
consulta='
from airflow.models.dag import DagModel
from airflow.models.errors import ParseImportError
from airflow.utils.session import create_session
with create_session() as s:
    dags = s.query(DagModel).filter(DagModel.is_stale.is_(False)).count()
    erros = s.query(ParseImportError).all()
    print("RESULTADO", dags, len(erros))
    for e in erros:
        print("IMPORT_ERROR", e.filename, (e.stacktrace or "").strip().splitlines()[-1:])
'
consultar() { sudo docker exec "$scheduler" python -c "$consulta" 2>/dev/null || true; }

# O dag-processor leva alguns segundos para parsear tudo depois do restart, e
# import errors antigos só somem quando o arquivo é reparseado: espera até 5 min.
total="?"; erros="?"
for tentativa in $(seq 1 30); do
    saida=$(consultar)
    read -r _ total erros < <(grep '^RESULTADO ' <<<"$saida" | tail -1) || true
    total=${total:-?}; erros=${erros:-?}
    echo "tentativa $tentativa: $total DAGs (esperadas $esperadas), $erros import errors"
    [[ "$erros" == "0" && "$total" == "$esperadas" ]] && break
    sleep 10
done

if [[ "$erros" != "0" ]]; then
    echo "erro: import errors no Airflow de prod" >&2
    grep '^IMPORT_ERROR ' <<<"$saida" >&2 || true
    exit 1
fi
[[ "$total" == "$esperadas" ]] || { echo "erro: $total DAGs carregadas, esperadas $esperadas" >&2; exit 1; }

# Regra de ouro: nada publicado em 0.0.0.0 (a 22 é protegida pelo UFW).
fora=$(ss -ltn | awk '{print $4}' | grep -E '^0\.0\.0\.0:' | grep -v ':22$' || true)
[[ -z "$fora" ]] || { echo "erro: porta publicada em 0.0.0.0: $fora" >&2; exit 1; }

echo "ok: $total DAGs, 0 import errors, nenhuma porta em 0.0.0.0"
