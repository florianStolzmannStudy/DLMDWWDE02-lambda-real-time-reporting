import os
import time
import json
import logging
import threading
from typing import List, Any, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from fastapi import FastAPI, Response
import uvicorn
from prometheus_client import Counter, CONTENT_TYPE_LATEST, generate_latest

API_BASE = os.getenv("API_BASE", "http://api_service:8000").rstrip("/")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

TOPIC_STREAM = os.getenv("TOPIC_STREAM", "taxi_stream")
ENABLE_PREV_DAY_TOPIC = os.getenv("ENABLE_PREV_DAY_TOPIC", "true").lower() == "true"
PREVIOUS_DAY_TOPIC = os.getenv("PREVIOUS_DAY_TOPIC", "taxi_data_previousDay")

STREAM_BATCH_SIZE = int(os.getenv("STREAM_BATCH_SIZE", "1"))
STREAM_INTERVAL_SEC = float(os.getenv("STREAM_INTERVAL_SEC", "1.0"))
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "10.0"))
STARTUP_RETRY_SEC = float(os.getenv("STARTUP_RETRY_SEC", "2.0"))
MAX_STARTUP_RETRIES = int(os.getenv("MAX_STARTUP_RETRIES", "60"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Europe/Berlin"))

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ingestor")

EVENTS_PROCESSED = Counter("events_processed_total", "Anzahl verarbeiteter Events")
PUBLISH_RECORDS = Counter("publish_records_total", "Anzahl veröffentlichter Records")

app = FastAPI(title="Ingestor Metrics")

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

def start_metrics_server():
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="warning")

def create_producer_with_retry() -> KafkaProducer:
    attempt = 0
    last_err: Optional[Exception] = None
    while attempt < MAX_STARTUP_RETRIES:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                linger_ms=50,
                acks="all",
                retries=5,
                max_in_flight_requests_per_connection=5,
            )
            return producer
        except Exception as e:
            last_err = e
            attempt += 1
            log.exception("Kafka nicht erreichbar (%s). Versuch %d/%d …",
                        repr(e), attempt, MAX_STARTUP_RETRIES)
            time.sleep(STARTUP_RETRY_SEC)
    raise RuntimeError(f"Kafka-Verbindung fehlgeschlagen: {last_err}")

def fetch_stream_once() -> Optional[Any]:
    url = f"{API_BASE}/stream"
    try:
        resp = requests.get(url, timeout=HTTP_TIMEOUT_SEC)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.exception("HTTP-Fehler bei GET %s: %s", url, e)
        return None

    try:
        data = resp.json()
    except ValueError:
        for line in resp.text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                log.exception("Konnte Zeile nicht als JSON parsen: %s", line[:200])
        return None

    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data:
        return data[0]
    return None

def fetch_stream_batch() -> List[Any]:
    records: List[Any] = []
    for _ in range(max(1, STREAM_BATCH_SIZE)):
        rec = fetch_stream_once()
        if rec is not None:
            records.append(rec)
    return records

def extract_event_date(rec: Any) -> Optional[datetime]:
    ts = None
    if isinstance(rec, dict):
        ts = rec.get("tpep_pickup_datetime") or rec.get("pickup_datetime") or rec.get("event_time")
    if not ts:
        return None
    s = str(ts)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.fromisoformat(s[:19])
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(TZ)

def publish_records(producer: KafkaProducer, records: List[Any]) -> int:
    sent = 0
    yesterday = (datetime.now(TZ) - timedelta(days=1)).date()

    for rec in records:
        try:
            producer.send(TOPIC_STREAM, value=rec).get(timeout=10)
            sent += 1
            PUBLISH_RECORDS.inc()
        except KafkaError as e:
            log.exception("Kafka send-Fehler an Topic '%s': %s", TOPIC_STREAM, repr(e))
            continue

        if ENABLE_PREV_DAY_TOPIC:
            try:
                dt = extract_event_date(rec)
                if dt and dt.date() == yesterday:
                    producer.send(PREVIOUS_DAY_TOPIC, value=rec).get(timeout=10)
            except KafkaError as e:
                log.exception("Kafka send-Fehler (PreviousDay) ignoriert: %s", repr(e))

    try:
        producer.flush(timeout=10)
    except Exception as e:
        log.exception("Flush-Fehler ignoriert: %s", e)

    return sent

def api_ready() -> bool:
    try:
        r = requests.get(f"{API_BASE}/health", timeout=HTTP_TIMEOUT_SEC)
        if 200 <= r.status_code < 500:
            return True
    except requests.RequestException:
        pass
    try:
        r2 = requests.get(f"{API_BASE}/stream", timeout=HTTP_TIMEOUT_SEC)
        return r2.ok
    except requests.RequestException:
        return False

def main() -> None:
    if not TOPIC_STREAM:
        raise RuntimeError("TOPIC_STREAM darf nicht leer sein.")

    threading.Thread(target=start_metrics_server, daemon=True).start()
    for attempt in range(MAX_STARTUP_RETRIES):
        if api_ready():
            break
        time.sleep(STARTUP_RETRY_SEC)
    else:
        raise RuntimeError(f"API nicht erreichbar unter {API_BASE}")

    producer = create_producer_with_retry()

    while True:
        loop_start = time.time()
        fetched = 0
        sent = 0
        try:
            records = fetch_stream_batch()
            fetched = len(records)
            if fetched:
                EVENTS_PROCESSED.inc(fetched)
                sent = publish_records(producer, records)
        except Exception as e:
            log.exception("Loop-Fehler: %s", e)

        elapsed = time.time() - loop_start
        sleep_for = max(0.0, STREAM_INTERVAL_SEC - elapsed)
        if sleep_for:
            time.sleep(sleep_for)

if __name__ == "__main__":
    main()
