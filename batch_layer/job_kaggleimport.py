import os
import json
import zipfile
import time
import logging
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
)
import happybase

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

DATASET_ZIP  = os.getenv("DATASET_ZIP", "/data/nyc_taxi.zip")
DATASET_PATH = os.getenv("DATASET_PATH", "/data/nyc_taxi.csv")
HBASE_HOST   = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT   = int(os.getenv("HBASE_PORT", "9090"))
TABLE_NAME   = os.getenv("TABLE_NAME", "taxi_raw")
BATCH_SIZE   = int(os.getenv("HBASE_BATCH_SIZE", "500"))

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "pushgateway:9091")
JOB_NAME        = os.getenv("PROM_JOB_NAME", "job_kaggleimport")
ENV_LABEL       = os.getenv("ENV", "local")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("job_kaggleimport")

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
])

def ensure_csv_extracted():
    if not os.path.exists(DATASET_PATH):
        try:
            with zipfile.ZipFile(DATASET_ZIP, "r") as zf:
                zf.extractall(os.path.dirname(DATASET_PATH))
        except Exception as e:
            logging.exception("Konnte ZIP nicht entpacken: %s", e)
            raise

def _tables_contains_name(tbls, name_str: str) -> bool:
    if not tbls:
        return False
    first = tbls[0]
    if isinstance(first, (bytes, bytearray)):
        return name_str.encode() in tbls
    return name_str in tbls

def ensure_table_exists_driver():
    conn = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, autoconnect=True)
    try:
        families = {"raw": dict()}
        tables = conn.tables()
        if not _tables_contains_name(tables, TABLE_NAME):
            conn.create_table(TABLE_NAME, families)
    finally:
        conn.close()

def write_partition_gen(index: int, rows_iter) -> Iterator[int]:
    connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, autoconnect=True)
    table = connection.table(TABLE_NAME)

    key_prefix = f"{index:04d}_"
    written = 0

    with table.batch(batch_size=BATCH_SIZE) as b:
        for i, row in enumerate(rows_iter):
            as_dict = {}
            for field in row.__fields__:
                val = getattr(row, field)
                if hasattr(val, "isoformat"):
                    as_dict[field] = val.isoformat()
                else:
                    as_dict[field] = None if val is None else val

            row_key = f"{key_prefix}{i:010d}".encode("utf-8")
            b.put(row_key, {b"raw:data": json.dumps(as_dict).encode("utf-8")})
            written += 1

    connection.close()
    yield written

def push_metrics(rows_written: int, duration_sec: float):
    registry = CollectorRegistry()

    labels = {"job": JOB_NAME, "table": TABLE_NAME, "env": ENV_LABEL}

    g_rows = Gauge(
        "kaggleimport_rows_count",
        "Anzahl aller Taxi-Imports",
        labelnames=list(labels.keys()),
        registry=registry,
    )
    g_dur = Gauge(
        "kaggleimport_duration_seconds",
        "Verarbeitungsdauer in Sekunden",
        labelnames=list(labels.keys()),
        registry=registry,
    )

    g_rows.labels(**labels).set(rows_written)
    g_dur.labels(**labels).set(duration_sec)

    push_to_gateway(PUSHGATEWAY_URL, job=JOB_NAME, registry=registry)

def main():
    start = time.time()
    total_written = 0

    spark = (
        SparkSession.builder
        .appName("job_kaggleimport")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    try:
        ensure_table_exists_driver()
        ensure_csv_extracted()

        df = (
            spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv(DATASET_PATH)
        )

        total_written = df.rdd.mapPartitionsWithIndex(write_partition_gen).sum()

    except Exception as e:
        log.exception("Batch-Job fehlgeschlagen: %s", e)

    finally:
        duration = time.time() - start
        try:
            push_metrics(rows_written=total_written, duration_sec=duration)
        except Exception as e:
            log.exception("Konnte Metriken nicht pushen: %s", e)

        spark.stop()

if __name__ == "__main__":
    main()