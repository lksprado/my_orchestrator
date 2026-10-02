#!/usr/bin/env python3
"""Gera o .env de prod: deploy/prod/config.env + secrets do GitHub.

    SEGREDOS='${{ toJSON(secrets) }}' deploy/gerar_env.py chaves
    SEGREDOS='${{ toJSON(secrets) }}' deploy/gerar_env.py escrever [env] [dir_secrets]

``chaves``   imprime só os NOMES das variáveis do .env que seria gerado (para o
             deploy/checar_env.sh). Nenhum valor sai daqui.
``escrever`` grava o .env (padrão /srv/airflow/.env) e os arquivos de credencial
             (padrão /srv/secrets). Só reescreve o que mudou, então um deploy sem
             secret alterado não muda o hash do deploy/estado.sh nem reinicia.

Não existe lista de chaves para manter: todo secret visível ao job vira
``NOME=valor`` no .env, menos os de infraestrutura do workflow (IGNORADOS). Secret
``ARQUIVO__<NOME>_<EXT>`` vira o arquivo ``<nome>.<ext>`` no dir_secrets
(``ARQUIVO__FINANCES_PY_JSON`` → ``finances-py.json``). Criar ou apagar uma
credencial é ``gh secret set|delete NOME --env prod -R lksprado/my_orchestrator``
e o próximo deploy aplica.

O valor vai cru (``NOME=valor``, sem aspas), como no .env escrito à mão que este
arquivo substitui. Valor com quebra de linha só como ARQUIVO__. Os erros citam o
nome, nunca o valor.
"""

import json
import os
import re
import sys
import tempfile
from pathlib import Path

CONFIG = Path(__file__).resolve().parent / "prod" / "config.env"
# Secrets do próprio workflow, não da aplicação.
IGNORADOS = {"github_token", "INGESTION_READ_TOKEN"}
PREFIXO_ARQUIVO = "ARQUIVO__"
NOME = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def falhar(msg: str) -> None:
    print(f"erro: {msg}", file=sys.stderr)
    sys.exit(1)


def ler_config() -> list[tuple[str, str]]:
    pares = []
    for linha in CONFIG.read_text(encoding="utf-8").splitlines():
        if not linha.strip() or linha.lstrip().startswith("#"):
            continue
        nome, sep, valor = linha.partition("=")
        if not sep or not NOME.match(nome):
            falhar(f"linha inválida em {CONFIG.name}: {nome!r}")
        if not valor:
            falhar(f"{nome} vazia em {CONFIG.name}")
        pares.append((nome, valor))
    return pares


def ler_segredos() -> tuple[dict[str, str], dict[str, str]]:
    """Separa os secrets em variáveis do .env e arquivos (nome → conteúdo)."""
    bruto = os.environ.get("SEGREDOS")
    if not bruto:
        falhar("SEGREDOS vazio: passe ${{ toJSON(secrets) }} no env do passo")
    variaveis, arquivos = {}, {}
    for nome, valor in json.loads(bruto).items():
        if nome in IGNORADOS:
            continue
        if not NOME.match(nome):
            falhar(f"secret com nome inválido para variável de ambiente: {nome}")
        if not valor:
            falhar(f"secret {nome} vazio")
        if nome.startswith(PREFIXO_ARQUIVO):
            base, _, ext = nome.removeprefix(PREFIXO_ARQUIVO).rpartition("_")
            if not base:
                falhar(f"{nome}: use {PREFIXO_ARQUIVO}<NOME>_<EXTENSÃO>")
            arquivos[f"{base.lower().replace('_', '-')}.{ext.lower()}"] = valor
        elif "\n" in valor or "\r" in valor:
            falhar(
                f"secret {nome} tem quebra de linha; arquivo vai como {PREFIXO_ARQUIVO}"
            )
        else:
            variaveis[nome] = valor
    if not variaveis:
        # Environment errado ou sem secrets: falhar em vez de publicar .env sem senhas.
        falhar("nenhum secret de aplicação visível ao job (falta environment: prod?)")
    return variaveis, arquivos


def montar() -> tuple[list[tuple[str, str]], dict[str, str]]:
    config = ler_config()
    variaveis, arquivos = ler_segredos()
    if repetidas := sorted({n for n, _ in config} & variaveis.keys()):
        falhar(f"definidas em {CONFIG.name} e como secret: {', '.join(repetidas)}")
    return config + sorted(variaveis.items()), arquivos


def gravar_se_mudou(destino: Path, conteudo: str) -> bool:
    if destino.exists() and destino.read_text(encoding="utf-8") == conteudo:
        return False
    # Temporário no mesmo diretório: o replace é atômico e nasce com 600.
    fd, tmp = tempfile.mkstemp(dir=destino.parent, prefix=f".{destino.name}.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(conteudo)
        os.replace(tmp, destino)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return True


def main() -> None:
    acao = sys.argv[1] if len(sys.argv) > 1 else ""
    if acao == "chaves":
        pares, _ = montar()
        print("\n".join(sorted(n for n, _ in pares)))
    elif acao == "escrever":
        env = Path(sys.argv[2] if len(sys.argv) > 2 else "/srv/airflow/.env")
        dir_secrets = Path(sys.argv[3] if len(sys.argv) > 3 else "/srv/secrets")
        pares, arquivos = montar()
        for nome, conteudo in sorted(arquivos.items()):
            mudou = gravar_se_mudou(dir_secrets / nome, conteudo)
            print(f"{dir_secrets / nome}: {'atualizado' if mudou else 'sem mudança'}")
        conteudo = "".join(f"{n}={v}\n" for n, v in pares)
        mudou = gravar_se_mudou(env, conteudo)
        print(f"{env}: {len(pares)} chaves, {'atualizado' if mudou else 'sem mudança'}")
    else:
        print(
            f"uso: {sys.argv[0]} <chaves|escrever> [env] [dir_secrets]", file=sys.stderr
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
