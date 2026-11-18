from datetime import date, datetime

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.silver_transformations import transform_events


def test_silver_deduplicates_and_standardizes(spark):
    timestamp = datetime(2024, 1, 15, 8, 0, 0)

    raw_json = (
        '{"payload":{"action":"opened",'
        '"ref":"main","ref_type":"branch"}}'
    )

    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("actor_id", StringType(), True),
        StructField("actor_login", StringType(), True),
        StructField("repo_id", StringType(), True),
        StructField("repo_name", StringType(), True),
        StructField("org_id", StringType(), True),
        StructField("org_login", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("parse_status", StringType(), True),
        StructField("raw_json", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ])

    rows = [
        ("event-1", "PushEvent", timestamp, "10", "alice",
         "100", "org/repo", None, None, "source.json",
         "valid", raw_json, timestamp),
        ("event-1", "PushEvent", timestamp, "10", "alice",
         "100", "org/repo", None, None, "source.json",
         "valid", raw_json, timestamp),
    ]

    bronze = spark.createDataFrame(rows, schema)
    silver = transform_events(bronze)

    assert silver.count() == 1

    result = silver.first()

    assert result.event_id == "event-1"
    assert result.actor_id == 10
    assert result.repo_id == 100
    assert result.event_date == date(2024, 1, 15)
    assert result.action == "opened"
    assert result.ref == "main"
    assert result.ref_type == "branch"
    assert result.payload_json is not None
    assert "created_at" not in silver.columns