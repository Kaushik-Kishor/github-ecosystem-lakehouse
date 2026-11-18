"""
Nexus CDC Streaming Pipeline — repositories

Consumes Debezium CDC events from Redpanda, deduplicates changes by
repository, and applies them to the Iceberg silver table with foreachBatch.

Safeguards:
1. Acquire a host-wide singleton lock before creating Spark. The lock lives
   under ~/.locks rather than beside the checkpoint, so launching the script
   from another working directory cannot bypass it.
2. Keep the lock file permanently. flock is released by the OS when the
   process exits; unlinking the path can let another process lock a new inode
   while an older process still holds the unlinked inode.
3. Persist and fully materialize each Kafka micro-batch before doing any
   additional actions or starting the Iceberg MERGE. A full count() forces
   every partition into the cache; isEmpty() alone may only materialize a
   subset of the batch.
"""

import fcntl
import os
import sys
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.iceberg_session import get_spark


# ── Configuration ────────────────────────────────────────────────────────────

TOPIC = "nexus.public.repos"
BOOTSTRAP_SERVERS = "localhost:19092"
TARGET_TABLE = "nexus.silver.repos"

CHECKPOINT_PATH = str(Path("checkpoints/repos_cdc_v6").resolve())
MAX_OFFSETS_PER_TRIGGER = 10_000

# One lock per user on this WSL/Linux host, independent of the current directory.
LOCK_FILE = Path.home() / ".locks" / "nexus-repos-cdc.lock"


def acquire_pipeline_lock():
    """Acquire a non-blocking process-wide lock and retain its open handle."""
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)

    # a+ avoids truncating the diagnostic PID while another process owns the lock.
    lock_handle = open(LOCK_FILE, "a+", encoding="utf-8")

    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_handle.seek(0)
        existing_owner = lock_handle.read().strip() or "owner details unavailable"
        lock_handle.close()
        raise RuntimeError(
            "Another Nexus repositories CDC process already holds the lock "
            f"({LOCK_FILE}). Existing lock metadata: {existing_owner}"
        )

    lock_handle.seek(0)
    lock_handle.truncate()
    lock_handle.write(
        f"pid={os.getpid()} started_epoch={time.time():.3f}\n"
        f"cwd={Path.cwd()}\n"
    )
    lock_handle.flush()

    print(
        f"CDC singleton lock acquired: {LOCK_FILE} (PID {os.getpid()})",
        flush=True,
    )
    return lock_handle


# ── Target Iceberg table ─────────────────────────────────────────────────────

def create_target_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nexus.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS nexus.silver.repos (
            repo_id BIGINT,
            name STRING,
            owner STRING,
            stars BIGINT,
            is_archived BOOLEAN,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (bucket(32, repo_id))
        LOCATION 's3://lake/warehouse/silver/repos_clean_20261118'
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    print(f"Target table ready: {TARGET_TABLE}", flush=True)


# ── Micro-batch processor ────────────────────────────────────────────────────

def process_batch(batch_df, batch_id):
    spark = batch_df.sparkSession

    # Persist, then force a complete materialization before doing other work.
    # isEmpty() is deliberately not used here: it can stop after finding one row
    # and therefore need not cache every Kafka partition.
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    latest_changes = None
    temp_view_created = False

    try:
        raw_count = batch_df.count()
        if raw_count == 0:
            print(f"Batch {batch_id}: empty, skipping", flush=True)
            return

        print(
            f"Batch {batch_id}: materialized {raw_count} Kafka records",
            flush=True,
        )

        parsed = batch_df.select(
            F.coalesce(
                F.get_json_object("value", "$.payload.after.repo_id"),
                F.get_json_object("value", "$.payload.before.repo_id"),
            ).cast("long").alias("repo_id"),
            F.get_json_object("value", "$.payload.after.name").alias("name"),
            F.get_json_object("value", "$.payload.after.owner").alias("owner"),
            F.get_json_object("value", "$.payload.after.stars").cast("long").alias("stars"),
            F.get_json_object("value", "$.payload.after.is_archived")
                .cast("boolean").alias("is_archived"),
            F.to_timestamp(
                F.get_json_object("value", "$.payload.after.updated_at")
            ).alias("updated_at"),
            F.get_json_object("value", "$.payload.op").alias("op"),
            F.col("partition"),
            F.col("offset"),
        ).filter(
            F.col("repo_id").isNotNull()
            & F.col("op").isin("r", "c", "u", "d")
        )

        # The topic currently has one partition, so offset provides a total
        # ordering for events belonging to a repository.
        window = Window.partitionBy("repo_id").orderBy(F.col("offset").desc())

        latest_changes = (
            parsed
            .withColumn("row_num", F.row_number().over(window))
            .filter(F.col("row_num") == 1)
            .drop("row_num", "partition", "offset")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        processed_count = latest_changes.count()
        if processed_count == 0:
            print(f"Batch {batch_id}: no valid repository changes", flush=True)
            return

        print(
            f"Batch {batch_id}: applying {processed_count} repository changes",
            flush=True,
        )

        latest_changes.createOrReplaceTempView("repos_cdc_batch")
        temp_view_created = True

        # Do not attempt to repair or mutate Iceberg metadata inside this
        # callback. If the commit reports invalid snapshot history, fail the
        # batch so Spark does not advance its checkpoint past an unapplied batch.
        spark.sql(
            """
            MERGE INTO nexus.silver.repos AS target
            USING repos_cdc_batch AS source
            ON target.repo_id = source.repo_id

            WHEN MATCHED AND source.op = 'd'
                THEN DELETE

            WHEN MATCHED
                THEN UPDATE SET
                    target.name = source.name,
                    target.owner = source.owner,
                    target.stars = source.stars,
                    target.is_archived = source.is_archived,
                    target.updated_at = source.updated_at

            WHEN NOT MATCHED AND source.op <> 'd'
                THEN INSERT (
                    repo_id, name, owner, stars, is_archived, updated_at
                )
                VALUES (
                    source.repo_id, source.name, source.owner,
                    source.stars, source.is_archived, source.updated_at
                )
            """
        )

        print(f"Batch {batch_id}: Iceberg merge completed", flush=True)

    finally:
        if temp_view_created:
            spark.catalog.dropTempView("repos_cdc_batch")
        if latest_changes is not None:
            latest_changes.unpersist()
        batch_df.unpersist()


# ── Streaming entry point ────────────────────────────────────────────────────

def run_stream():
    # Acquire before creating Spark or touching the target table.
    lock_handle = acquire_pipeline_lock()
    spark = None
    query = None

    try:
        spark = get_spark("Nexus-Repos-CDC-Stream")
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.shuffle.partitions", "8")

        create_target_table(spark)

        print(f"Topic:                   {TOPIC}", flush=True)
        print(f"Target table:            {TARGET_TABLE}", flush=True)
        print(f"Max offsets per trigger: {MAX_OFFSETS_PER_TRIGGER}", flush=True)
        print(f"Checkpoint:              {CHECKPOINT_PATH}", flush=True)
        print(f"Lock file:               {LOCK_FILE}", flush=True)

        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
            .option("failOnDataLoss", "false")
            .option("kafka.max.poll.interval.ms", "600000")
            .option("kafka.session.timeout.ms", "60000")
            .option("kafka.heartbeat.interval.ms", "20000")
            .option("kafka.request.timeout.ms", "120000")
            .option("kafka.default.api.timeout.ms", "120000")
            .load()
        )

        valid_events = (
            kafka_stream
            .filter(F.col("value").isNotNull())
            .select(
                F.col("value").cast("string").alias("value"),
                F.col("partition"),
                F.col("offset"),
            )
        )

        query = (
            valid_events.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime="10 seconds")
            .start()
        )

        print("CDC pipeline is running.", flush=True)
        query.awaitTermination()

    except KeyboardInterrupt:
        print("Stopping CDC pipeline...", flush=True)
        if query is not None:
            query.stop()
    finally:
        if spark is not None:
            spark.stop()

        # Keep the lock file on disk. Unlocking and closing releases flock;
        # the stable path prevents inode races with another launcher.
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()
        print("CDC singleton lock released.", flush=True)


if __name__ == "__main__":
    try:
        run_stream()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
