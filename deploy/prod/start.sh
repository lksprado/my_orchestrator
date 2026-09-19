#!/usr/bin/env bash
#
# Sobe o Airflow de prod (atb) via Astro CLI com as variáveis de interpolação.
#
#   ./start.sh                 astro dev start (timeout de 120s)
#   ./start.sh restart         astro dev restart: rebuild depois de um deploy
#   ./start.sh --wait 5m       sobrescreve o timeout
#
# Por que este script existe:
#
# 1. O Astro lê o .env para as variáveis DO CONTAINER, mas a interpolação de
#    ${BIND_IP} / ${AIRFLOW_PORT} no docker-compose.override.yml usa o ambiente
#    do processo. Sem elas exportadas, o Docker recebe um bind vazio e NÃO
#    publica a porta.
#
# 2. `sudo` limpa o ambiente (env_reset é o padrão no Ubuntu), então as
#    variáveis vão inline no próprio sudo.
#
# 3. Não se faz `source .env`: connections em JSON sem aspas quebram o shell.
#    Lemos só as duas chaves de interpolação, com grep.

set -euo pipefail
cd "$(dirname "$0")"

[[ -f .env ]] || { echo "erro: falta o .env (copie de deploy/prod/.env.example)" >&2; exit 1; }
mountpoint -q /srv/lake || { echo "erro: /srv/lake não está montado (serviço seaweedfs-mount do homelab)" >&2; exit 1; }

ler() {
    local valor
    valor=$(grep -E "^${1}=" .env | head -1 | cut -d= -f2- | tr -d '"'"'"'')
    [[ -n "$valor" ]] || { echo "erro: ${1} não definida no .env" >&2; exit 1; }
    printf '%s' "$valor"
}

BIND_IP=$(ler BIND_IP)
AIRFLOW_PORT=$(ler AIRFLOW_PORT)

comando=start
if [[ "${1:-}" == "restart" ]]; then
    comando=restart
    shift
fi

# Sem --wait explícito, usa 120s: o padrão de 1m estoura com imagem reconstruída.
args=("$@")
[[ " ${args[*]} " == *" --wait "* ]] || args+=(--wait 120s)

echo "astro dev ${comando} em ${BIND_IP}:${AIRFLOW_PORT} ($(head -1 DEPLOYED.txt 2>/dev/null || echo 'sem DEPLOYED.txt'))"

exec sudo "BIND_IP=${BIND_IP}" "AIRFLOW_PORT=${AIRFLOW_PORT}" \
    astro dev "${comando}" "${args[@]}"
