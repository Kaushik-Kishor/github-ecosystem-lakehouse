from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_daily_metrics(events: DataFrame) -> DataFrame:
    return (
        events.groupBy("event_date", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("actor_id").alias("unique_actors"),
            F.countDistinct("repo_id").alias("unique_repositories"),
        )
    )


def build_hourly_metrics(events: DataFrame) -> DataFrame:
    return (
        events
        .withColumn("event_hour", F.hour("event_timestamp"))
        .groupBy("event_date", "event_hour", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("actor_id").alias("unique_actors"),
            F.countDistinct("repo_id").alias("unique_repositories"),
        )
    )


def build_repo_activity(events: DataFrame) -> DataFrame:
    return (
        events.groupBy("event_date", "repo_id", "repo_name")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("event_type").alias("distinct_event_types"),
            F.countDistinct("actor_id").alias("unique_contributors"),
        )
    )


def build_developer_activity(events: DataFrame) -> DataFrame:
    developer_events = events.withColumn(
        "is_bot",
        F.lower(F.col("actor_login")).endswith("[bot]"),
    )

    return (
        developer_events
        .groupBy("event_date", "actor_id", "actor_login", "is_bot")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("repo_id").alias("unique_repositories"),
            F.countDistinct("event_type").alias("distinct_event_types"),
        )
    )
