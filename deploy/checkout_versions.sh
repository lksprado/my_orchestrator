#!/usr/bin/env bash
#
# Coloca cada repositório de deploy/versions.txt no commit fixado.
#
#   deploy/checkout_versions.sh <destino>
#
# Para cada linha "nome sha url": clona em <destino>/<nome> se faltar, busca o
# commit e faz checkout destacado nele. Idempotente. Recusa sobrescrever um
# working tree com alterações locais.

set -euo pipefail

[[ $# -eq 1 ]] || { echo "uso: $0 <destino>" >&2; exit 1; }
destino=$1
arquivo="$(dirname "$0")/versions.txt"
mkdir -p "$destino"

while read -r nome sha url; do
    [[ -z "${nome:-}" || "$nome" == \#* ]] && continue
    dir="$destino/$nome"
    if [[ ! -d "$dir/.git" ]]; then
        echo "clonando $nome"
        git clone --quiet "$url" "$dir"
    fi
    if [[ -n "$(git -C "$dir" status --porcelain)" ]]; then
        echo "erro: $dir tem alterações locais" >&2
        exit 1
    fi
    git -C "$dir" fetch --quiet origin "$sha"
    git -C "$dir" -c advice.detachedHead=false checkout --quiet --detach "$sha"
    echo "$nome @ $(git -C "$dir" rev-parse --short HEAD)"
done < "$arquivo"
