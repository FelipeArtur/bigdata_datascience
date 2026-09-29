FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/default-java SPARK_LOCAL_IP=127.0.0.1
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless libpango-1.0-0 libpangoft2-1.0-0 \
    fonts-liberation fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN useradd --create-home --uid 1000 estudante && chown estudante:estudante /app
USER estudante
# Dependências JVM disponíveis também sem rede durante a execução.
RUN python -c 'from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; configure_spark_with_delta_pip(SparkSession.builder.master("local[1]").config("spark.jars.repositories", "https://repo.maven.apache.org/maven2"), extra_packages=["org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"]).getOrCreate().stop()'
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser"]
