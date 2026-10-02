#!/usr/bin/env bash
#
# Confere se toda chave do .env de dev existe em prod. NUNCA lê, imprime ou compara
# valores: só nomes de chave.
#
#   SEGREDOS='${{ toJSON(secrets) }}' deploy/checar_env.sh
#
# Compara o .env.example (dev) com as chaves que o deploy/gerar_env.py põe no .env
# de prod: deploy/prod/config.env + secrets do environment `prod` do GitHub.
#
# Por que existe: o .env de dev é local e o de prod é gerado no deploy, então
# credencial nova precisa existir nos dois lados. Esquecer o lado de prod não
# quebra nada no parse — quebra a task em produção, horas depois. Foi o que
# aconteceu com GOOGLE_CREDENTIALS_FILE em investments__googlesheets__ingestion.
#
# Roda no .github/workflows/pr.yml (PR vermelho = o secret ainda não existe) e no
# deploy.yml, antes de gravar o .env.
#
# Só chave FALTANDO em prod falha. Secret sobrando (sem par no .env.example) é
# aviso: entre o `gh secret set` e o merge do PR que declara a chave, e entre o
# merge que a remove e o `gh secret delete`, ele sobra de propósito — e um deploy
# disparado por outro repo nessa janela não pode quebrar por isso.

set -euo pipefail
cd "$(dirname "$0")/.."

# Chaves que existem de propósito em um lado só. GOOGLE_DRIVE_* é da DAG
# presentation_export_prod, que é só de dev (fica no .gitignore, nunca vai ao atb).
SO_DEV='^(DB__DEV__|GOOGLE_DRIVE_)'
SO_PROD='^(BIND_IP|AIRFLOW_PORT|AIRFLOW__API__SECRET_KEY|AIRFLOW_CONN_OPENWEATHER_CONN)$'

dev=.env.example
chaves_dev=$(grep -oE '^[A-Za-z_][A-Za-z0-9_]*=' "$dev" | tr -d '=' | sort -u)
chaves_prod=$(python3 deploy/gerar_env.py chaves | sort -u)
echo "Comparando $dev com deploy/prod/config.env + secrets do environment prod"

falhou=0
while read -r chave; do
    [[ -z "$chave" ]] && continue
    echo "  ✗ $chave está em $dev e falta em prod" >&2
    echo "      gh secret set $chave --env prod -R lksprado/my_orchestrator" >&2
    echo "      (ou, se não for sensível, em deploy/prod/config.env)" >&2
    falhou=1
done < <(comm -23 <(echo "$chaves_dev") <(echo "$chaves_prod") | grep -Ev "$SO_DEV" || true)

while read -r chave; do
    [[ -z "$chave" ]] && continue
    echo "  ! aviso: $chave está em prod e falta em $dev (PR pendente ou secret a apagar)" >&2
done < <(comm -13 <(echo "$chaves_dev") <(echo "$chaves_prod") | grep -Ev "$SO_PROD" || true)

[[ $falhou -eq 0 ]] || exit 1
echo "  ✓ toda chave de dev tem par em prod"
