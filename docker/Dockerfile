FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/default-java

# Instalação de dependências do sistema e Java (JRE) para execução do Apache Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia e instala as dependências Python
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

# Expõe a porta do JupyterLab
EXPOSE 8888

# Comando padrão: Inicia o JupyterLab na porta 8888 acessível pelo navegador
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=", "--NotebookApp.password="]
