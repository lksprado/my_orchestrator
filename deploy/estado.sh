#!/usr/bin/env bash
#
# Decide o que o deploy precisa além do rsync, comparando o que está em
# /srv/airflow com o hash gravado no último deploy que deu certo.
#
#   deploy/estado.sh precisa <restart|dbt-deps> [projeto]   imprime true ou false
#   deploy/estado.sh gravar  <restart|dbt-deps> [projeto]   grava o hash atual
#
# restart:  imagem (Dockerfile, requirements.txt, packages.txt, .dockerignore),
#           config dos containers (override, .astro/config.yaml), plugins/ (só
#           carregam no start) e o .env. Código de DAG, my_ingestion e dbt não
#           entram: são lidos do disco pelos binds.
# dbt-deps: package-lock.yml do my_analytics, e se o dbt_packages/ existe.
#
# O hash só é gravado depois do passo correspondente dar certo, então um passo
# que falhou é repetido no deploy seguinte.

set -euo pipefail

acao=${1:?uso: $0 <precisa|gravar> <restart|dbt-deps> [projeto]}
nome=${2:?uso: $0 <precisa|gravar> <restart|dbt-deps> [projeto]}
projeto=${3:-/srv/airflow}
cd "$projeto"

hash_atual() {
    case "$nome" in
        restart)
            {
                for f in Dockerfile requirements.txt packages.txt .dockerignore \
                         docker-compose.override.yml .astro/config.yaml .env; do
                    # Arquivo ausente também conta (entra como "ausente").
                    if [[ -f "$f" ]]; then sha256sum "$f"; else echo "ausente $f"; fi
                done
                find plugins -type f ! -path '*/__pycache__/*' -print0 2>/dev/null \
                    | sort -z | xargs -0 -r sha256sum
            } | sha256sum | cut -d' ' -f1 ;;
        dbt-deps)
            {
                sha256sum dbt/my_analytics/package-lock.yml 2>/dev/null || echo "sem package-lock"
                [[ -d dbt/my_analytics/dbt_packages ]] && echo "com dbt_packages" || echo "sem dbt_packages"
            } | sha256sum | cut -d' ' -f1 ;;
        *) echo "erro: nome desconhecido: $nome" >&2; exit 2 ;;
    esac
}

arquivo=".deploy-${nome}.sha256"
case "$acao" in
    precisa)
        if [[ -f "$arquivo" && "$(cat "$arquivo")" == "$(hash_atual)" ]]; then
            echo false
        else
            echo true
        fi ;;
    gravar)
        hash_atual > "$arquivo" ;;
    *) echo "erro: ação desconhecida: $acao" >&2; exit 2 ;;
esac
