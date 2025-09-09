FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Europe/Berlin

RUN apt-get update && apt-get install -y --no-install-recommends \
      cron netcat-openbsd ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
 || pip install --no-cache-dir happybase thriftpy2 kafka-python prometheus-client

COPY job_scheduler.py /app/job_scheduler.py

RUN printf '0 0 * * * root python3 /app/job_scheduler.py >> /proc/1/fd/1 2>> /proc/1/fd/2\n' > /etc/cron.d/batch-cron \
 && chmod 0644 /etc/cron.d/batch-cron

RUN printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  ': "${HBASE_HOST:=hbase}"' \
  ': "${HBASE_PORT:=9090}"' \
  'echo "[scheduler-entrypoint] warte auf HBase Thrift @ ${HBASE_HOST}:${HBASE_PORT} ..."' \
  'for i in $(seq 1 120); do' \
  '  if nc -z "${HBASE_HOST}" "${HBASE_PORT}" >/dev/null 2>&1; then' \
  '    echo "[scheduler-entrypoint] HBase erreichbar."' \
  '    break' \
  '  fi' \
  '  sleep 1' \
  'done' \
  'echo "[scheduler-entrypoint] starte Scheduler einmalig ..."' \
  'python3 /app/job_scheduler.py || echo "[scheduler-entrypoint] Scheduler exit code $?";' \
  'echo "[scheduler-entrypoint] starte Cron (täglicher Lauf) ..."' \
  'exec cron -f' \
  > /usr/local/bin/entrypoint_scheduler.sh \
 && chmod +x /usr/local/bin/entrypoint_scheduler.sh

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=5 \
  CMD pgrep -x cron >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint_scheduler.sh"]
