#!/usr/bin/env bash

set -Euo pipefail

PREFIX="[scheduler-entrypoint]"
PY_CMD="${PY_CMD:-python3}"
APP_FILE="${APP_FILE:-/app/job_scheduler.py}"

HBASE_HOST="${HBASE_HOST:-hbase}"
HBASE_PORT="${HBASE_PORT:-9090}"
WAIT_FOR_HBASE="${WAIT_FOR_HBASE:-true}"
HBASE_WAIT_MAX_SEC="${HBASE_WAIT_MAX_SEC:-180}"

RUN_ON_START="${RUN_ON_START:-true}"

log() { echo "${PREFIX} $*"; }

if [ ! -f "${APP_FILE}" ]; then
  log "FEHLER: ${APP_FILE} nicht gefunden. Inhalt von /app:"
  ls -la /app || true
  exit 1
fi

if [ "${WAIT_FOR_HBASE}" = "true" ] || [ "${WAIT_FOR_HBASE}" = "1" ]; then
  if command -v nc >/dev/null 2>&1; then
    log "warte auf HBase Thrift @ ${HBASE_HOST}:${HBASE_PORT} (max ${HBASE_WAIT_MAX_SEC}s) ..."
    SECS=0
    until nc -z "${HBASE_HOST}" "${HBASE_PORT}" >/dev/null 2>&1; do
      sleep 1
      SECS=$((SECS+1))
      if [ "${SECS}" -ge "${HBASE_WAIT_MAX_SEC}" ]; then
        log "WARN: HBase Thrift nach ${HBASE_WAIT_MAX_SEC}s nicht erreichbar – fahre trotzdem fort."
        break
      fi
    done
    if [ "${SECS}" -lt "${HBASE_WAIT_MAX_SEC}" ]; then
      log "HBase erreichbar."
    fi
  else
    log "WARN: 'nc' (netcat) nicht installiert – überspringe HBase-Wartecheck."
  fi
fi

if [ "${RUN_ON_START}" = "true" ] || [ "${RUN_ON_START}" = "1" ]; then
  log "starte Scheduler einmalig ..."
  set -o pipefail
  ${PY_CMD} "${APP_FILE}" || log "WARN: Scheduler einmaliger Lauf exit code $?"
else
  log "RUN_ON_START=false – überspringe einmaligen Scheduler-Lauf."
fi

log "prüfe Cron-Installation ..."
CRON_BIN=""
if command -v cron >/dev/null 2>&1; then
  CRON_BIN="cron -f"
elif command -v crond >/dev/null 2>&1; then
  CRON_BIN="crond -f -l 8"
fi

if [ -d /etc/cron.d ]; then
  log "Cron-Dateien in /etc/cron.d:"
  ls -la /etc/cron.d || true
fi

if [ -z "${CRON_BIN}" ]; then
  log "FEHLER: Weder 'cron' noch 'crond' gefunden. Bitte im Image installieren."
  exit 127
fi

log "starte Cron (Foreground) ..."
exec ${CRON_BIN}