import os
import json
import logging
import time
import happybase

from collections import Counter as ColCounter
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, Callable, Type
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

ENABLE_KAFKA_ENRICH = os.getenv("ENABLE_KAFKA_ENRICH", "true").lower() == "true"
KafkaConsumer = None
KafkaError = Exception
NoBrokersAvailable = Exception
TopicAlreadyExistsError = Exception
UnknownTopicOrPartitionError = Exception
KafkaAdminClient = None
NewTopic = None
if ENABLE_KAFKA_ENRICH:
    try:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError, UnknownTopicOrPartitionError
        from kafka.admin import KafkaAdminClient, NewTopic
    except ImportError:
        raise RuntimeError("ENABLE_KAFKA_ENRICH=true, aber 'kafka-python' ist nicht installiert.")

HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))

RAW_TABLE = os.getenv("RAW_TABLE", "taxi_raw")
AGG_TABLE = os.getenv("AGG_TABLE", "taxi_agg")

RAW_FAMILY = os.getenv("RAW_FAMILY", "raw")
RAW_QUAL = os.getenv("RAW_QUALIFIER", "data")

BATCH_SIZE = int(os.getenv("HBASE_BATCH_SIZE", "500"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("job_scheduler")

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Europe/Berlin"))
YDAY = (datetime.now(LOCAL_TZ) - timedelta(days=1)).date()
YDAY_STR = YDAY.strftime("%Y-%m-%d")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKER", "kafka:9092")
PREVIOUS_DAY_TOPIC = os.getenv("PREVIOUS_DAY_TOPIC", "taxi_data_previousDay")
CONSUMER_TIMEOUT_MS = int(os.getenv("CONSUMER_TIMEOUT_MS", "5000"))

KAFKA_CONNECT_RETRIES = int(os.getenv("KAFKA_CONNECT_RETRIES", "30"))
KAFKA_CONNECT_DELAY_SEC = float(os.getenv("KAFKA_CONNECT_DELAY_SEC", "2"))
KAFKA_TOPIC_WAIT_RETRIES = int(os.getenv("KAFKA_TOPIC_WAIT_RETRIES", "30"))
KAFKA_TOPIC_WAIT_DELAY_SEC = float(os.getenv("KAFKA_TOPIC_WAIT_DELAY_SEC", "2"))
KAFKA_POLL_ERROR_RETRIES = int(os.getenv("KAFKA_POLL_ERROR_RETRIES", "5"))
KAFKA_POLL_ERROR_BACKOFF = float(os.getenv("KAFKA_POLL_ERROR_BACKOFF", "1.5"))

KAFKA_CREATE_TOPIC = os.getenv("KAFKA_CREATE_TOPIC", "true").lower() == "true"
KAFKA_TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1"))
KAFKA_TOPIC_REPLICATION = int(os.getenv("KAFKA_TOPIC_REPLICATION", "1"))

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "pushgateway:9091")
JOB_NAME = os.getenv("PROM_JOB_NAME", "job_scheduler")
ENV_LABEL = os.getenv("ENV", "local")
METRICS_STATE_FILE = os.getenv("METRICS_STATE_FILE", "/data/metrics_state.json")

HBASE_CONNECT_RETRIES = int(os.getenv("HBASE_CONNECT_RETRIES", "30"))
HBASE_CONNECT_DELAY_SEC = float(os.getenv("HBASE_CONNECT_DELAY_SEC", "2"))
HBASE_OP_RETRIES = int(os.getenv("HBASE_OP_RETRIES", "5"))
HBASE_OP_DELAY_SEC = float(os.getenv("HBASE_OP_DELAY_SEC", "1"))
HBASE_OP_BACKOFF = float(os.getenv("HBASE_OP_BACKOFF", "1.5"))

RAW_TABLE_WAIT_RETRIES = int(os.getenv("RAW_TABLE_WAIT_RETRIES", "30"))
RAW_TABLE_WAIT_DELAY_SEC = float(os.getenv("RAW_TABLE_WAIT_DELAY_SEC", "2"))

ENFORCE_RAW_MIN_ROWS = os.getenv("ENFORCE_RAW_MIN_ROWS", "true").lower() == "true"
RAW_MIN_ROWS = int(os.getenv("RAW_MIN_ROWS", "2964624"))
RAW_MIN_ROWS_TIMEOUT_SEC = int(os.getenv("RAW_MIN_ROWS_TIMEOUT_SEC", "900"))
RAW_MIN_ROWS_RETRY_DELAY_SEC = float(os.getenv("RAW_MIN_ROWS_RETRY_DELAY_SEC", "15"))
RAW_COUNT_BATCH_SIZE = int(os.getenv("RAW_COUNT_BATCH_SIZE", "5000"))

def retry_call(
        fn: Callable,
        *,
        retries: int,
        delay: float,
        backoff: float = 1.0,
        retry_on: tuple[Type[BaseException], ...] = (Exception,),
        args: tuple = (),
        kwargs: Optional[dict] = None,
        on_retry: Optional[Callable[[int, BaseException], None]] = None,
):
    if kwargs is None:
        kwargs = {}
    _delay = delay
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except retry_on as e:
            last_exc = e
            if on_retry:
                try:
                    on_retry(attempt, e)
                except Exception:
                    pass
            if attempt == retries:
                break
            time.sleep(_delay)
            _delay *= max(1.0, backoff)
    if last_exc:
        raise last_exc

def connect_hbase() -> happybase.Connection:
    return happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, autoconnect=True)

def connect_hbase_with_retry() -> happybase.Connection:
    def _on_retry(i, e):
        log.exception("HBase nicht erreichbar (Versuch %d/%d): %s", i, HBASE_CONNECT_RETRIES, e)
    return retry_call(
        connect_hbase,
        retries=HBASE_CONNECT_RETRIES,
        delay=HBASE_CONNECT_DELAY_SEC,
        backoff=1.2,
        retry_on=(Exception,),
        on_retry=_on_retry,
    )

def ensure_table_exists(conn: happybase.Connection, name: str, family: str) -> None:
    def _tables():
        return conn.tables()
    def _create():
        conn.create_table(name, {family: dict()})
    tbls = retry_call(_tables, retries=HBASE_OP_RETRIES, delay=HBASE_OP_DELAY_SEC, backoff=HBASE_OP_BACKOFF)
    if tbls and isinstance(tbls[0], (bytes, bytearray)):
        exists = name.encode("utf-8") in tbls
    else:
        exists = name in tbls
    if not exists:
        retry_call(_create, retries=HBASE_OP_RETRIES, delay=HBASE_OP_DELAY_SEC, backoff=HBASE_OP_BACKOFF)

def wait_for_table(conn: happybase.Connection, name: str, retries: int, delay: float) -> bool:
    for i in range(1, retries + 1):
        try:
            tables = conn.tables()
            if tables and isinstance(tables[0], (bytes, bytearray)):
                exists = name.encode("utf-8") in tables
            else:
                exists = name in tables
            if exists:
                return True
            else:
                log.error("Tabelle '%s' existiert nicht (Versuch %d/%d)", name, i, retries)
        except Exception as e:
            log.exception("Fehler beim Prüfen von Tabelle '%s' (Versuch %d/%d): %s", name, i, retries, e)
        time.sleep(delay)
    return False

def parse_json(b: bytes) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return None

def extract_day(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    s = str(ts)
    if len(s) < 10:
        return None
    return s[:10]

def _count_rows_up_to(conn: happybase.Connection, table_name: str, limit_rows: int) -> int:
    """Zählt Zeilen bis maximal limit_rows und bricht dann ab (schnell)."""
    table = conn.table(table_name)
    n = 0
    for _rowkey, _ in table.scan(batch_size=RAW_COUNT_BATCH_SIZE, scan_batching=RAW_COUNT_BATCH_SIZE):
        n += 1
        if n >= limit_rows:
            break
    return n

def wait_for_raw_min_rows(conn: happybase.Connection) -> bool:
    if not ENFORCE_RAW_MIN_ROWS:
        return True
    start = time.time()
    attempt = 0
    while True:
        attempt += 1
        try:
            cnt = _count_rows_up_to(conn, RAW_TABLE, RAW_MIN_ROWS)
            if cnt >= RAW_MIN_ROWS:
                log.info("RAW-Min-Row erreicht: %d ≥ %d", cnt, RAW_MIN_ROWS)
                return True
            elapsed = int(time.time() - start)
            log.warning("RAW hat erst %d/%d Zeilen (elapsed=%ss) – warte weiter ...", cnt, RAW_MIN_ROWS, elapsed)
        except Exception as e:
            log.exception("RAW-Min-Row-Zählung fehlgeschlagen (Versuch %d): %s", attempt, e)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect_hbase_with_retry()
        if time.time() - start >= RAW_MIN_ROWS_TIMEOUT_SEC:
            log.error("Timeout: RAW-Min-Row %d nicht erreicht nach %ss. Keine Aggregation.", RAW_MIN_ROWS, RAW_MIN_ROWS_TIMEOUT_SEC)
            return False
        time.sleep(RAW_MIN_ROWS_RETRY_DELAY_SEC)

def _load_state(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_state(path: str, state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    except Exception:
        pass
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, path)

def push_metrics(runs_delta: int, wrote_value: int, duration_sec: float) -> None:
    state = _load_state(METRICS_STATE_FILE)
    prev_runs = int(state.get("scheduler_runs_total", 0))
    new_runs = prev_runs + int(runs_delta)

    registry = CollectorRegistry()
    labels = {"job": JOB_NAME, "table": AGG_TABLE, "env": ENV_LABEL}

    runs_total = Counter("scheduler_runs_total", "Gesamtzahl der Scheduler-Durchläufe", labelnames=list(labels.keys()), registry=registry)
    wrote_total = Gauge("scheduler_wrote_total", "Anzahl geschriebener Tages-JSON im letzten Lauf (ersetzt pro Ausführung)", labelnames=list(labels.keys()), registry=registry)
    last_dur = Gauge("scheduler_last_duration_seconds", "Laufzeit des letzten Durchlaufs (Sekunden)", labelnames=list(labels.keys()), registry=registry)

    runs_total.labels(**labels).inc(new_runs)
    wrote_total.labels(**labels).set(wrote_value)
    last_dur.labels(**labels).set(duration_sec)

    try:
        push_to_gateway(PUSHGATEWAY_URL, job=JOB_NAME, registry=registry)
    except Exception as e:
        log.exception("Konnte Metriken nicht pushen: %s", e)

    state["scheduler_runs_total"] = new_runs
    _save_state(METRICS_STATE_FILE, state)

def _new_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        PREVIOUS_DAY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        request_timeout_ms=7000,
        metadata_max_age_ms=20000,
        reconnect_backoff_ms=500,
        reconnect_backoff_max_ms=4000,
        fetch_max_wait_ms=250,
        fetch_min_bytes=1,
        max_partition_fetch_bytes=1048576,
        connections_max_idle_ms=600000,
    )

def _wait_topic_ready(consumer: KafkaConsumer, topic: str) -> bool:
    for i in range(1, KAFKA_TOPIC_WAIT_RETRIES + 1):
        try:
            parts = consumer.partitions_for_topic(topic)
            if parts:
                return True
            log.error("Kafka-Topic '%s' noch nicht verfügbar (Versuch %d/%d)", topic, i, KAFKA_TOPIC_WAIT_RETRIES)
        except (KafkaError, OSError) as e:
            log.exception("Kafka-Topic-Check Fehler (Versuch %d/%d): %s", i, KAFKA_TOPIC_WAIT_RETRIES, e)
        time.sleep(KAFKA_TOPIC_WAIT_DELAY_SEC)
    return False

def ensure_kafka_topic_exists(topic: str) -> bool:
    if not ENABLE_KAFKA_ENRICH or KafkaAdminClient is None or not KAFKA_CREATE_TOPIC:
        return True
    last = None
    for i in range(1, KAFKA_CONNECT_RETRIES + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="scheduler-admin")
            try:
                admin.create_topics([NewTopic(name=topic, num_partitions=KAFKA_TOPIC_PARTITIONS, replication_factor=KAFKA_TOPIC_REPLICATION)])
                log.info("Kafka-Topic '%s' erstellt (Partitions=%d, RF=%d).", topic, KAFKA_TOPIC_PARTITIONS, KAFKA_TOPIC_REPLICATION)
            except TopicAlreadyExistsError:
                pass
            finally:
                try:
                    admin.close()
                except Exception:
                    pass

            c = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
            parts = c.partitions_for_topic(topic)
            try:
                c.close()
            except Exception:
                pass
            if parts:
                return True
            log.error("Kafka-Topic '%s' hat noch keine Partitionen (Versuch %d/%d).", topic, i, KAFKA_CONNECT_RETRIES)
        except (NoBrokersAvailable, KafkaError, OSError, ConnectionError) as e:
            last = e
            log.exception("Kafka-Admin/Topic-Check fehlgeschlagen (Versuch %d/%d): %s", i, KAFKA_CONNECT_RETRIES, e)
        time.sleep(KAFKA_CONNECT_DELAY_SEC)
    if last:
        log.error("Kafka-Topic '%s' nicht bereit: %s", topic, last)
    return False

def make_consumer_with_retry() -> Optional[KafkaConsumer]:
    last = None
    for i in range(1, KAFKA_CONNECT_RETRIES + 1):
        try:
            c = _new_consumer()
            if _wait_topic_ready(c, PREVIOUS_DAY_TOPIC):
                return c
            try:
                c.close()
            except Exception:
                pass
        except (NoBrokersAvailable, KafkaError, OSError, ConnectionError) as e:
            last = e
            log.exception("Kafka nicht erreichbar/bereit (Versuch %d/%d): %s", i, KAFKA_CONNECT_RETRIES, e)
        time.sleep(KAFKA_CONNECT_DELAY_SEC)
    if last:
        log.exception("Kafka-Verbindung endgültig fehlgeschlagen: %s", last)
    return None

def main() -> None:
    start_ts = time.time()
    conn = None
    wrote = 0
    try:
        conn = connect_hbase_with_retry()
        ensure_table_exists(conn, AGG_TABLE, "agg")

        if not wait_for_table(conn, RAW_TABLE, retries=RAW_TABLE_WAIT_RETRIES, delay=RAW_TABLE_WAIT_DELAY_SEC):
            log.error("RAW-Tabelle '%s' existiert nach Wartezeit nicht. Breche ab.", RAW_TABLE)
            duration = time.time() - start_ts
            push_metrics(runs_delta=1, wrote_value=wrote, duration_sec=duration)
            return

        if not wait_for_raw_min_rows(conn):
            duration = time.time() - start_ts
            push_metrics(runs_delta=1, wrote_value=wrote, duration_sec=duration)
            return

        counts = ColCounter()
        col_str = f"{RAW_FAMILY}:{RAW_QUAL}"
        col_key = col_str.encode("utf-8")

        def _scan_generator():
            table = conn.table(RAW_TABLE)
            return table.scan(columns=[col_str], batch_size=BATCH_SIZE, scan_batching=BATCH_SIZE)

        raw_records = 0
        attempt = 0
        while True:
            attempt += 1
            try:
                for _, data in _scan_generator():
                    raw_records += 1
                    payload = data.get(col_key)
                    if not payload:
                        continue
                    rec = parse_json(payload)
                    if not rec:
                        continue
                    day = extract_day(rec.get("tpep_pickup_datetime"))
                    if not day:
                        continue
                    counts[day] += 1
                break
            except Exception as e:
                if attempt >= HBASE_OP_RETRIES:
                    log.exception("RAW-Scan fehlgeschlagen (Versuch %d/%d): %s", attempt, HBASE_OP_RETRIES, e)
                    raise
                log.exception("RAW-Scan Fehler (Versuch %d/%d): %s – versuche Reconnect ...", attempt, HBASE_OP_RETRIES, e)
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                conn = connect_hbase_with_retry()
                time.sleep(HBASE_OP_DELAY_SEC * (HBASE_OP_BACKOFF ** (attempt - 1)))

        log.info("RAW-Records gescannt: %d; unterschiedliche Tage gefunden: %d", raw_records, len(counts))

        if ENABLE_KAFKA_ENRICH and KafkaConsumer is not None:
            if not ensure_kafka_topic_exists(PREVIOUS_DAY_TOPIC):
                log.error("Kafka-Topic '%s' nicht verfügbar. Überspringe Enrichment.", PREVIOUS_DAY_TOPIC)
                consumer = None
            else:
                consumer = make_consumer_with_retry()

            if consumer is not None:
                MAX_KAFKA_SECONDS = int(os.getenv("MAX_KAFKA_SECONDS", "15"))
                MAX_KAFKA_RECORDS = int(os.getenv("MAX_KAFKA_RECORDS", "200000"))
                POLL_TIMEOUT_MS = int(os.getenv("CONSUMER_TIMEOUT_MS", "2000"))
                MAX_POLL_RECORDS = int(os.getenv("MAX_POLL_RECORDS", "5000"))

                start_poll = time.time()
                poll_errors = 0
                backoff = 0.5
                k_total = 0
                try:
                    while True:
                        if time.time() - start_poll >= MAX_KAFKA_SECONDS or k_total >= MAX_KAFKA_RECORDS:
                            break
                        try:
                            batch = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_POLL_RECORDS)
                        except (KafkaError, OSError, ConnectionError) as e:
                            poll_errors += 1
                            log.exception("Kafka poll()-Fehler (Fehler #%d): %s", poll_errors, e)
                            try:
                                consumer.close()
                            except Exception:
                                pass
                            if poll_errors >= KAFKA_POLL_ERROR_RETRIES:
                                log.error("Kafka poll()-Fehlerlimit erreicht, breche Enrichment ab.")
                                break
                            time.sleep(backoff)
                            backoff *= KAFKA_POLL_ERROR_BACKOFF
                            consumer = make_consumer_with_retry()
                            if consumer is None:
                                log.error("Kafka-Consumer nach Fehler nicht wiederherstellbar.")
                                break
                            continue

                        if not batch:
                            time.sleep(0.1)
                            continue

                        for _, msgs in batch.items():
                            for msg in msgs:
                                k_total += 1
                                rec = msg.value if isinstance(msg.value, dict) else None
                                if not rec:
                                    continue
                                if extract_day(rec.get("tpep_pickup_datetime")) == YDAY_STR:
                                    counts[YDAY_STR] += 1
                finally:
                    try:
                        consumer.close()
                    except Exception:
                        pass
                log.info("Kafka-Enrichment verarbeitet: %d Nachrichten", k_total)

        if not counts:
            log.warning("Keine Tagesaggregationen ermittelt – schreibe nichts in '%s'.", AGG_TABLE)
            duration = time.time() - start_ts
            push_metrics(runs_delta=1, wrote_value=0, duration_sec=duration)
            return

        def _write_agg():
            table = conn.table(AGG_TABLE)
            wrote_local = 0
            with table.batch(batch_size=BATCH_SIZE) as b:
                for day, n in sorted(counts.items()):
                    doc = {"date": day, "rides": int(n)}
                    b.put(day.encode("utf-8"), {b"agg:data": json.dumps(doc).encode("utf-8")})
                    wrote_local += 1
            return wrote_local

        wrote_attempts = 0
        while True:
            try:
                wrote = retry_call(_write_agg, retries=HBASE_OP_RETRIES, delay=HBASE_OP_DELAY_SEC, backoff=HBASE_OP_BACKOFF)
                break
            except Exception as e:
                wrote_attempts += 1
                if wrote_attempts >= HBASE_OP_RETRIES:
                    log.exception("Schreiben in '%s' fehlgeschlagen (Versuch %d/%d): %s", AGG_TABLE, wrote_attempts, HBASE_OP_RETRIES, e)
                    raise
                log.exception("Fehler beim Schreiben in '%s' (Versuch %d/%d): %s – Reconnect ...", AGG_TABLE, wrote_attempts, e)
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                conn = connect_hbase_with_retry()

        log.info("Geschriebene Tages-Docs in '%s': %d", AGG_TABLE, wrote)

        try:
            duration = time.time() - start_ts
            push_metrics(runs_delta=1, wrote_value=wrote, duration_sec=duration)
        except Exception as e:
            log.exception("Konnte Metriken nicht pushen: %s", e)

    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
