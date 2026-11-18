from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.iceberg_session import get_spark


BRONZE_TABLE = "nexus.bronze.repo_cdc"
SILVER_TABLE = "nexus.silver.repos_v2"


def main():
    spark = get_spark("Nexus-Repos-Silver-Batch")

    try:
        spark.sparkContext.setLogLevel("WARN")

        print("Reading Bronze CDC history...", flush=True)

        bronze = spark.table(BRONZE_TABLE)

        # Extract Debezium envelope fields.
        events = bronze.select(
            F.col("offset").alias("kafka_offset"),
            F.get_json_object(
                "message_key", "$.payload.repo_id"
            ).cast("long").alias("key_repo_id"),
            F.get_json_object(
                "message_value", "$.payload.op"
            ).alias("operation"),
            F.get_json_object(
                "message_value", "$.payload.after.repo_id"
            ).cast("long").alias("after_repo_id"),
            F.get_json_object(
                "message_value", "$.payload.before.repo_id"
            ).cast("long").alias("before_repo_id"),
            F.get_json_object(
                "message_value", "$.payload.after.name"
            ).alias("after_name"),
            F.get_json_object(
                "message_value", "$.payload.before.name"
            ).alias("before_name"),
            F.get_json_object(
                "message_value", "$.payload.after.owner"
            ).alias("after_owner"),
            F.get_json_object(
                "message_value", "$.payload.before.owner"
            ).alias("before_owner"),
            F.get_json_object(
                "message_value", "$.payload.after.stars"
            ).cast("int").alias("after_stars"),
            F.get_json_object(
                "message_value", "$.payload.before.stars"
            ).cast("int").alias("before_stars"),
            F.get_json_object(
                "message_value", "$.payload.after.is_archived"
            ).cast("boolean").alias("after_is_archived"),
            F.get_json_object(
                "message_value", "$.payload.before.is_archived"
            ).cast("boolean").alias("before_is_archived"),
            F.coalesce(
                F.get_json_object(
                    "message_value", "$.payload.after.updated_at"
                ),
                F.get_json_object(
                    "message_value", "$.payload.before.updated_at"
                )
            ).alias("event_updated_at"),
            F.get_json_object(
                "message_value", "$.payload.ts_ms"
            ).cast("long").alias("source_ts_ms"),
        )

        # Exclude null-valued Kafka tombstones. The preceding Debezium
        # delete event is the actual state transition.
        events = events.filter(
            F.col("operation").isin("r", "c", "u", "d")
        )

        events = events.withColumn(
            "repo_id",
            F.coalesce(
                "after_repo_id",
                "before_repo_id",
                "key_repo_id"
            )
        )

        events = events.filter(F.col("repo_id").isNotNull())

        # One latest event per repository, ordered by Kafka offset.
        window = Window.partitionBy("repo_id").orderBy(
            F.col("kafka_offset").desc()
        )

        latest = (
            events
            .withColumn("_row_number", F.row_number().over(window))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )

        # For deletes, preserve the last known fields from payload.before.
        changes = latest.select(
            "repo_id",
            F.coalesce("after_name", "before_name").alias("name"),
            F.coalesce("after_owner", "before_owner").alias("owner"),
            F.coalesce("after_stars", "before_stars").alias("stars"),
            F.coalesce(
                "after_is_archived", "before_is_archived"
            ).alias("is_archived"),
            F.to_timestamp("event_updated_at").alias("updated_at"),
            (F.col("operation") == "d").alias("is_deleted"),
            F.col("kafka_offset").alias("last_kafka_offset"),
            F.col("operation").alias("source_op"),
            "source_ts_ms",
            F.current_timestamp().alias("applied_at"),
        )

        changes.createOrReplaceTempView("repos_silver_changes")

        print(
            f"Latest repository states prepared: {changes.count():,}",
            flush=True
        )

        print("Applying Iceberg MERGE...", flush=True)

        spark.sql(f"""
            MERGE INTO {SILVER_TABLE} AS target
            USING repos_silver_changes AS source
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

        print("\nSilver batch completed.", flush=True)

        print("=== SILVER SUMMARY ===")
        spark.sql(f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT repo_id) AS unique_repositories,
                SUM(CASE WHEN is_deleted THEN 1 ELSE 0 END) AS deleted_rows,
                SUM(CASE WHEN NOT is_deleted THEN 1 ELSE 0 END) AS active_rows,
                MIN(last_kafka_offset) AS min_offset,
                MAX(last_kafka_offset) AS max_offset
            FROM {SILVER_TABLE}
        """).show(truncate=False)

        print("=== OPERATIONS REPRESENTED IN SILVER ===")
        spark.sql(f"""
            SELECT source_op, is_deleted, COUNT(*) AS records
            FROM {SILVER_TABLE}
            GROUP BY source_op, is_deleted
            ORDER BY source_op, is_deleted
        """).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
