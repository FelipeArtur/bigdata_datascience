#!/bin/bash
set -e

echo "======================================================================"
echo "Configuração de Ambiente Virtual Python (venv) - Loja LoL Big Data"
echo "======================================================================"

VENV_DIR=".venv"

# 1. Verificar Python 3
if ! command -v python3 &> /dev/null; then
    echo "Erro: python3 não encontrado no sistema operacional."
    exit 1
fi

echo "- Python detectado: $(python3 --version)"

# 2. Criar .venv se não existir
if [ ! -d "$VENV_DIR" ]; then
    echo "==> Criando ambiente virtual em $VENV_DIR ..."
    python3 -m venv "$VENV_DIR"
else
    echo "- Ambiente virtual já existe em $VENV_DIR."
fi

# 3. Atualizar pip e instalar dependências
echo "==> Instalando dependências de requirements.txt..."
"$VENV_DIR/bin/pip" install --upgrade pip --quiet
"$VENV_DIR/bin/pip" install -r requirements.txt --quiet

echo ""
echo "======================================================================"
echo "Ambiente Virtual configurado com sucesso!"
echo "Para ativar seu ambiente, execute no terminal:"
echo ""
echo "    source .venv/bin/activate"
echo ""
echo "Para abrir o JupyterLab:"
echo "    jupyter lab"
echo "======================================================================"
