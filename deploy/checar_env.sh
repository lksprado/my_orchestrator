#!/usr/bin/env bash
#
# Confere chaves de configuração. NUNCA lê, imprime ou compara valores: só nomes
# de chave e se o valor é vazio ou não.
#
#   deploy/checar_env.sh templates                 .env.example (dev) x deploy/prod/.env.example
#   deploy/checar_env.sh prod [/srv/airflow/.env]  template de prod x .env real do atb
#
# Por que existe: o .env não é versionado nem copiado de dev para prod (o build é
# um git archive e o rsync tem --exclude=/.env), então cada chave nova precisa ser
# escrita à mão nos dois lugares. Esquecer o lado de prod não quebra nada no parse
# — quebra a task em produção, horas depois. Foi o que aconteceu com
# GOOGLE_CREDENTIALS_FILE em investments__googlesheets__ingestion.
#
# Roda no .github/workflows/pr.yml: "templates" no runner do GitHub, "prod" no
# runner do atb, que enxerga /srv/airflow/.env. Falhando no PR, o segredo entra em
# prod ANTES do merge, que é a ordem certa.

set -euo pipefail
cd "$(dirname "$0")/.."

# Chaves que existem de propósito em um lado só. GOOGLE_DRIVE_* é da DAG
# presentation_export_prod, que é só de dev (fica no .gitignore, nunca vai ao atb).
SO_DEV='^(DB__DEV__|GOOGLE_DRIVE_)'
SO_PROD='^(BIND_IP|AIRFLOW_PORT|AIRFLOW__API__SECRET_KEY|AIRFLOW_CONN_OPENWEATHER_CONN)$'

# Nomes de todas as chaves do arquivo.
chaves() { grep -oE '^[A-Za-z_][A-Za-z0-9_]*=' "$1" | tr -d '=' | sort -u; }

# Nomes das chaves com valor não-vazio. O valor em si não sai daqui.
preenchidas() { grep -oE '^[A-Za-z_][A-Za-z0-9_]*=.+' "$1" | cut -d= -f1 | sort -u; }

falhou=0
erro() { echo "  ✗ $1" >&2; falhou=1; }

case "${1:-}" in
templates)
    dev=.env.example
    prod=deploy/prod/.env.example
    echo "Comparando $dev com $prod"

    while read -r chave; do
        [[ -z "$chave" ]] && continue
        erro "$chave está em $dev e falta em $prod"
    done < <(comm -23 <(chaves "$dev") <(chaves "$prod") | grep -Ev "$SO_DEV" || true)

    while read -r chave; do
        [[ -z "$chave" ]] && continue
        erro "$chave está em $prod e falta em $dev"
    done < <(comm -13 <(chaves "$dev") <(chaves "$prod") | grep -Ev "$SO_PROD" || true)

    [[ $falhou -eq 0 ]] && echo "  ✓ os dois templates têm as mesmas chaves"
    ;;

prod)
    template=deploy/prod/.env.example
    alvo=${2:-/srv/airflow/.env}
    [[ -r "$alvo" ]] || { echo "erro: não consigo ler $alvo" >&2; exit 1; }
    echo "Conferindo as chaves de $template em $alvo"

    # O template tem placeholder vazio de propósito; o .env real, não.
    while read -r chave; do
        [[ -z "$chave" ]] && continue
        erro "$chave: ausente ou vazia em $alvo (declarada em $template)"
    done < <(comm -23 <(chaves "$template") <(preenchidas "$alvo"))

    [[ $falhou -eq 0 ]] && echo "  ✓ todas as chaves do template estão preenchidas"
    ;;

*)
    echo "uso: $0 <templates|prod> [caminho do .env]" >&2
    exit 2
    ;;
esac

if [[ $falhou -ne 0 ]]; then
    echo >&2
    echo "Acrescente as chaves que faltam e rode de novo. Em prod:" >&2
    echo "  ssh atb  # e edite /srv/airflow/.env (github-runner:lcs, rw de grupo)" >&2
    exit 1
fi
