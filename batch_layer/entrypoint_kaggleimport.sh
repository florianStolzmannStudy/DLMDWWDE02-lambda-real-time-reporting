#!/usr/bin/env bash
set -euo pipefail

: "${HBASE_HOST:=hbase}"
: "${HBASE_PORT:=9090}"

echo "[kaggleimport-entrypoint] warte auf HBase Thrift @ ${HBASE_HOST}:${HBASE_PORT} ..."
for i in $(seq 1 120); do
  if nc -z "${HBASE_HOST}" "${HBASE_PORT}" >/dev/null 2>&1; then
    echo "[kaggleimport-entrypoint] HBase erreichbar."
    break
  fi
  sleep 1
done

echo "[kaggleimport-entrypoint] starte Kaggle-Import (single-shot) ..."
exec python3 /app/job_kaggleimport.py
