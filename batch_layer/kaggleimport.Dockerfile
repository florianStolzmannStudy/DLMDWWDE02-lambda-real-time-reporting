FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Europe/Berlin \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update && apt-get install -y --no-install-recommends \
      default-jre-headless netcat-openbsd ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt || true \
 && pip install --no-cache-dir pyspark

COPY job_kaggleimport.py /app/job_kaggleimport.py

RUN printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  ': "${HBASE_HOST:=hbase}"' \
  ': "${HBASE_PORT:=9090}"' \
  'echo "[kaggleimport-entrypoint] warte auf HBase Thrift @ ${HBASE_HOST}:${HBASE_PORT} ..."' \
  'for i in $(seq 1 120); do' \
  '  if nc -z "${HBASE_HOST}" "${HBASE_PORT}" >/dev/null 2>&1; then' \
  '    echo "[kaggleimport-entrypoint] HBase erreichbar."' \
  '    break' \
  '  fi' \
  '  sleep 1' \
  'done' \
  'echo "[kaggleimport-entrypoint] starte Kaggle-Import (single-shot) ..."' \
  'exec python3 /app/job_kaggleimport.py' \
  > /usr/local/bin/entrypoint_kaggleimport.sh \
 && chmod +x /usr/local/bin/entrypoint_kaggleimport.sh

ENTRYPOINT ["/usr/local/bin/entrypoint_kaggleimport.sh"]
