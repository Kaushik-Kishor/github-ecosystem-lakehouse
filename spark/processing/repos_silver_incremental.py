import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.iceberg_session import get_spark
from spark.processing.cdc_transformations import build_changes


BRONZE_TABLE = "nexus.bronze.repo_cdc"
SILVER_TABLE = "nexus.silver.repos_v2"

PIPELINE_NAME = "nexus-repos-silver"
TOPIC = "nexus.public.repos"
PARTITION_ID = 0

CONTROL_SCHEMA = "pipeline_control"
CONTROL_TABLE = "cdc_offsets"

LOCK_NAME = "nexus-repos-silver-incremental"


def connect_control_db():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="iceberg",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def get_latest_bronze_snapshot(spark):
    row = spark.sql(f"""
        SELECT snapshot_id, operation
        FROM {BRONZE_TABLE}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """).first()

    if row is None:
        raise RuntimeError("No snapshots found in the Bronze CDC table.")

    return int(row["snapshot_id"]), row["operation"]


def read_checkpoint(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT last_applied_offset, last_bronze_snapshot_id
            FROM pipeline_control.cdc_offsets
            WHERE pipeline_name = %s
              AND topic = %s
              AND partition_id = %s
        """, (PIPELINE_NAME, TOPIC, PARTITION_ID))

        row = cur.fetchone()

    if row is None:
        raise RuntimeError("Silver CDC checkpoint row does not exist.")

    if row[1] is None:
        raise RuntimeError("Bronze snapshot checkpoint has not been initialized.")

    return int(row[0]), int(row[1])


def advance_checkpoint(conn, old_offset, old_snapshot, new_offset, new_snapshot):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_control.cdc_offsets
            SET last_applied_offset = %s,
                last_bronze_snapshot_id = %s,
                last_successful_run = NOW(),
                updated_at = NOW()
            WHERE pipeline_name = %s
              AND topic = %s
              AND partition_id = %s
              AND last_applied_offset = %s
              AND last_bronze_snapshot_id = %s
        """, (
            new_offset,
            new_snapshot,
            PIPELINE_NAME,
            TOPIC,
            PARTITION_ID,
            old_offset,
            old_snapshot,
        ))

        if cur.rowcount != 1:
            raise RuntimeError(
                "Checkpoint compare-and-set failed. "
                "The saved checkpoint changed unexpectedly."
            )

    conn.commit()


def merge_changes(spark, changes):
    changes.createOrReplaceTempView("repos_silver_incremental_changes")

    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING repos_silver_incremental_changes AS source
        ON target.repo_id = source.repo_id

        WHEN MATCHED
          AND source.last_kafka_offset > target.last_kafka_offset
        THEN UPDATE SET
            target.name = source.name,
            target.owner = source.owner,
            target.stars = source.stars,
            target.is_archived = source.is_archived,
            target.updated_at = source.updated_at,
            target.is_deleted = source.is_deleted,
            target.last_kafka_offset = source.last_kafka_offset,
            target.source_op = source.source_op,
            target.source_ts_ms = source.source_ts_ms,
            target.applied_at = source.applied_at

        WHEN NOT MATCHED THEN INSERT (
            repo_id,
            name,
            owner,
            stars,
            is_archived,
            updated_at,
            is_deleted,
            last_kafka_offset,
            source_op,
            source_ts_ms,
            applied_at
        )
        VALUES (
            source.repo_id,
            source.name,
            source.owner,
            source.stars,
            source.is_archived,
            source.updated_at,
            source.is_deleted,
            source.last_kafka_offset,
            source.source_op,
            source.source_ts_ms,
            source.applied_at
        )
    """)


def main():
    load_dotenv(Path.cwd() / ".env")

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    required = ["POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {missing}")

    spark = get_spark("Nexus-Repos-Silver-Incremental")
    conn = None

    try:
        spark.sparkContext.setLogLevel("WARN")

        conn = connect_control_db()
        conn.autocommit = True

        # Session-level lock prevents concurrent Silver writers.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                (LOCK_NAME,)
            )

        old_offset, old_snapshot = read_checkpoint(conn)

        latest_snapshot, latest_operation = get_latest_bronze_snapshot(spark)

        print("\n========== INCREMENTAL SILVER RUN ==========")
        print(f"Saved Kafka offset:       {old_offset}")
        print(f"Saved Bronze snapshot:    {old_snapshot}")
        print(f"Latest Bronze snapshot:   {latest_snapshot}")
        print(f"Latest snapshot operation:{latest_operation}")

        if latest_snapshot == old_snapshot:
            print("No new Bronze snapshots. Silver is already up to date.")
            return


        incremental_df = (
            spark.read.format("iceberg")
            .option("start-snapshot-id", str(old_snapshot))
            .option("end-snapshot-id", str(latest_snapshot))
            .load(BRONZE_TABLE)
        )

        # Validate the complete incremental offset range, including
        # null-valued Kafka tombstones.
        stats = incremental_df.agg(
            F.count("*").alias("record_count"),
            F.min("offset").alias("min_offset"),
            F.max("offset").alias("max_offset"),
            F.countDistinct("offset").alias("distinct_offsets"),
            F.countDistinct("partition").alias("partition_count"),
        ).first()

        record_count = int(stats["record_count"] or 0)

        if record_count == 0:
            print(
                "New Bronze snapshot contains no records. "
                "Advancing the snapshot checkpoint only."
            )
            advance_checkpoint(
                conn,
                old_offset,
                old_snapshot,
                old_offset,
                latest_snapshot,
            )
            return

        min_offset = int(stats["min_offset"])
        max_offset = int(stats["max_offset"])
        distinct_offsets = int(stats["distinct_offsets"])
        partition_count = int(stats["partition_count"])

        print(f"New Bronze records:       {record_count:,}")
        print(f"Offset range:             {min_offset}–{max_offset}")
        print(f"Distinct offsets:          {distinct_offsets:,}")
        print(f"Partitions in batch:       {partition_count}")

        if partition_count != 1:
            raise RuntimeError(
                "This processor currently expects one Kafka partition. "
                "Partition-aware multi-partition ordering must be implemented "
                "before expanding the topic."
            )

        if min_offset != old_offset + 1:
            raise RuntimeError(
                f"Offset gap detected: expected {old_offset + 1}, "
                f"received {min_offset}. Check Bronze ingestion."
            )

        if distinct_offsets != record_count:
            raise RuntimeError(
                "Duplicate Kafka offsets detected in the incremental Bronze read."
            )

        if record_count != max_offset - old_offset:
            raise RuntimeError(
                "Offset range is not contiguous. Refusing to advance checkpoint."
            )

        changes = build_changes(incremental_df)
        change_count = changes.count()

        print(f"Repository changes:       {change_count:,}")

        # Commit Iceberg first. If the following PostgreSQL update fails,
        # the same batch will be replayed and the offset-guarded MERGE
        # will make that replay idempotent.
        if change_count:
            print("Applying incremental Iceberg MERGE...", flush=True)
            merge_changes(spark, changes)
            print("Iceberg MERGE committed.", flush=True)
        else:
            print("No state-changing events in this batch.")

        # Advance both progress markers only after the Iceberg operation.
        advance_checkpoint(
            conn,
            old_offset,
            old_snapshot,
            max_offset,
            latest_snapshot,
        )

        print("\nCheckpoint advanced successfully.")
        print(f"New Kafka offset:         {max_offset}")
        print(f"New Bronze snapshot:      {latest_snapshot}")

        print("\n========== SILVER SUMMARY ==========")
        spark.sql(f"""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN is_deleted THEN 1 ELSE 0 END) AS deleted_rows,
                SUM(CASE WHEN NOT is_deleted THEN 1 ELSE 0 END) AS active_rows,
                MAX(last_kafka_offset) AS max_applied_offset
            FROM {SILVER_TABLE}
        """).show(truncate=False)

    finally:
        if conn is not None:
            conn.close()

        spark.stop()


if __name__ == "__main__":
    main()
