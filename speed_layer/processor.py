import os
import json
import time
import socket
import logging
import uuid
from datetime import datetime
from typing import Any, Dict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import happybase
from kafka import KafkaConsumer
from prometheus_client import Counter, CONTENT_TYPE_LATEST, generate_latest

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("processor_speedlayer_tumbling")

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "taxi_stream"
READ_FROM_BEGINNING = True

DRAIN_MAX_SECONDS = 0
DRAIN_MAX_RECORDS = 0
POLL_TIMEOUT_MS = 2000
MAX_POLL_RECORDS = 5000

HBASE_HOST = "hbase"
HBASE_PORT = 9090
HBASE_CONNECT_RETRIES = 40
HBASE_CONNECT_DELAY = 1.5
HBASE_CONNECT_BACKOFF = 1.2

HBASE_TABLE_TUMBLING = "taxi_realtime_tumbling"
HBASE_CF = "rt"
HBASE_QUAL = "data"
HBASE_BATCH_SIZE = 500

WINDOW_SECONDS = 10
WINDOW_MAX_RECORDS = 1000
WINDOW_ALIGN_TO_EPOCH = True

FMT = "%Y-%m-%d %H:%M:%S"

TUMBLING_WINDOWS_TOTAL = Counter("tumbling_windows_total", "Anzahl ausgeführter Tumbling-Fenster")
TUMBLING_EVENTS_PROCESSED_TOTAL = Counter("tumbling_events_processed_total", "Anzahl verarbeiteter Events")

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")
            return
        output = generate_latest()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.send_header("Content-Length", str(len(output)))
        self.end_headers()
        self.wfile.write(output)

def start_metrics_server(host: str = "0.0.0.0", port: int = 7000):
    server = ThreadingHTTPServer((host, port), MetricsHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server

def get_hbase_connection() -> happybase.Connection:
    delay = HBASE_CONNECT_DELAY
    last_err = None
    for attempt in range(1, HBASE_CONNECT_RETRIES + 1):
        try:
            conn = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, autoconnect=True)
            _ = conn.tables()
            return conn
        except Exception as e:
            last_err = e
            if attempt < HBASE_CONNECT_RETRIES:
                log.exception("HBase noch nicht bereit (Versuch %d/%d): %s → retry in %.1fs", attempt, HBASE_CONNECT_RETRIES, e, delay)
                time.sleep(delay)
                delay *= HBASE_CONNECT_BACKOFF
            else:
                break
    raise RuntimeError(f"HBase nach mehreren Versuchen nicht erreichbar: {last_err}")

def ensure_table_exists(conn: happybase.Connection, name: str, family: str) -> None:
    try:
        tables = conn.tables()
    except Exception as e:
        log.exception("tables() schlug fehl, retry in 2s: %s", e)
        time.sleep(2)
        tables = conn.tables()
    if tables and isinstance(tables[0], (bytes, bytearray)):
        exists = name.encode("utf-8") in tables
    else:
        exists = name in tables
    if not exists:
        conn.create_table(name, {family: dict(max_versions=1)})

def parse_json(s: Any) -> bool:
    try:
        json.loads(s if isinstance(s, str) else s.decode("utf-8") if isinstance(s, (bytes, bytearray)) else s)
        return True
    except Exception:
        return False

def epoch_floor(ts_sec: float, window_size: int) -> int:
    return int(ts_sec) - (int(ts_sec) % window_size)

class TumblingWindow:
    def __init__(self, window_seconds: int, align_to_epoch: bool):
        now = time.time()
        self.window_seconds = window_seconds
        self.align_to_epoch = align_to_epoch
        self.window_start_epoch = epoch_floor(now, window_seconds) if align_to_epoch else int(now)
        self.count = 0
    def window_end_epoch(self) -> int:
        return self.window_start_epoch + self.window_seconds
    def within_current(self, ts_sec: float) -> bool:
        return int(ts_sec) < self.window_end_epoch()
    def add(self):
        self.count += 1
        TUMBLING_EVENTS_PROCESSED_TOTAL.inc()
    def roll_to(self, ts_sec: float):
        self.window_start_epoch = epoch_floor(ts_sec, self.window_seconds) if self.align_to_epoch else int(ts_sec)
        self.count = 0
    def to_json(self) -> Dict[str, Any]:
        return {
            "window_start": datetime.utcfromtimestamp(self.window_start_epoch).strftime(FMT),
            "window_end": datetime.utcfromtimestamp(self.window_end_epoch()).strftime(FMT),
            "count": self.count,
        }

def make_consumer(topic: str, broker: str) -> KafkaConsumer:
    try:
        host, _ = broker.split(":")
        addrs = {ai[4][0] for ai in socket.getaddrinfo(host, None)}
    except Exception as e:
        log.exception("DNS-Auflösung Broker '%s' fehlgeschlagen: %s", broker, e)
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[broker],
        group_id=None,
        auto_offset_reset=("earliest" if READ_FROM_BEGINNING else "latest"),
        enable_auto_commit=False,
        value_deserializer=lambda b: b.decode("utf-8"),
        request_timeout_ms=7000,
        metadata_max_age_ms=20000,
        reconnect_backoff_ms=500,
        reconnect_backoff_max_ms=4000,
        fetch_max_wait_ms=250,
        fetch_min_bytes=1,
        max_partition_fetch_bytes=1048576,
        connections_max_idle_ms=600000,
    )
    return consumer

def flush_window(batch, window: TumblingWindow) -> int:
    if window.count <= 0:
        return 0
    data_json = window.to_json()
    rowkey = f"{window.window_start_epoch}-{uuid.uuid4().hex[:8]}".encode("utf-8")
    col = f"{HBASE_CF}:{HBASE_QUAL}".encode("utf-8")
    batch.put(rowkey, {col: json.dumps(data_json, ensure_ascii=False).encode("utf-8")})
    TUMBLING_WINDOWS_TOTAL.inc()
    return window.count

def limits_hit(start_ts: float, total_in: int) -> bool:
    if DRAIN_MAX_SECONDS and (time.time() - start_ts) >= DRAIN_MAX_SECONDS:
        return True
    if DRAIN_MAX_RECORDS and total_in >= DRAIN_MAX_RECORDS:
        return True
    return False

def main() -> None:
    start_metrics_server("0.0.0.0", 7000)
    conn = get_hbase_connection()
    ensure_table_exists(conn, HBASE_TABLE_TUMBLING, HBASE_CF)
    table = conn.table(HBASE_TABLE_TUMBLING)
    consumer = make_consumer(KAFKA_TOPIC, KAFKA_BROKER)
    run_start = time.time()
    total_in = total_ok = total_skip = 0
    window = TumblingWindow(WINDOW_SECONDS, WINDOW_ALIGN_TO_EPOCH)
    try:
        with table.batch(batch_size=HBASE_BATCH_SIZE) as batch:
            while True:
                if limits_hit(run_start, total_in):
                    total_ok += flush_window(batch, window)
                    run_start = time.time()
                try:
                    polled = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_POLL_RECORDS)
                except Exception as e:
                    log.exception("Kafka.poll() Fehler: %s", e)
                    time.sleep(1.0)
                    continue
                if not polled:
                    now = time.time()
                    if not window.within_current(now):
                        total_ok += flush_window(batch, window)
                        window.roll_to(now)
                    continue
                for _, msgs in polled.items():
                    for msg in msgs:
                        total_in += 1
                        try:
                            raw = msg.value
                            if not parse_json(raw):
                                total_skip += 1
                                continue
                            now = time.time()
                            if not window.within_current(now):
                                total_ok += flush_window(batch, window)
                                window.roll_to(now)
                            window.add()
                            if window.count >= WINDOW_MAX_RECORDS:
                                total_ok += flush_window(batch, window)
                                next_start = window.window_end_epoch() + 0.001
                                window.roll_to(next_start)
                        except Exception:
                            total_skip += 1
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
