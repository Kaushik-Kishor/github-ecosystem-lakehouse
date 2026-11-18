from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_changes(incremental_df: DataFrame) -> DataFrame:
    """Convert Debezium CDC messages into the latest repository changes."""

    events = incremental_df.select(
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
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
            ),
        ).alias("event_updated_at"),
        F.get_json_object(
            "message_value", "$.payload.ts_ms"
        ).cast("long").alias("source_ts_ms"),
    )

    # Null-valued Kafka tombstones do not represent state transitions.
    events = events.filter(
        F.col("operation").isin("r", "c", "u", "d")
    )

    events = (
        events
        .withColumn(
            "repo_id",
            F.coalesce("after_repo_id", "before_repo_id", "key_repo_id"),
        )
        .filter(F.col("repo_id").isNotNull())
    )

    window = Window.partitionBy("repo_id").orderBy(
        F.col("kafka_offset").desc()
    )

    latest = (
        events
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    return latest.select(
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
