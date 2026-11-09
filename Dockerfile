FROM python:3.12-slim-trixie

# Java pour Spark (Spark 4 supporte Java 17 et 21 ; Debian trixie ne fournit plus que 21)
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Connecteur Spark-Kafka téléchargé une fois pour toutes à la construction de l'image,
# pour que rien ne soit téléchargé pendant la séance
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1').getOrCreate().stop()" \
 && JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
 && find /root -path '*ivy2*' -name '*.jar' \( -name 'spark-sql-kafka*' -o -name 'spark-token-provider-kafka*' \
      -o -name 'kafka-clients*' -o -name 'commons-pool2*' \) -exec cp {} "$JARS"/ \; \
 && ls "$JARS" | grep -E 'kafka|commons-pool2' \
 && rm -rf /root/.ivy2*

ENV PYTHONPATH=/td PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /td
