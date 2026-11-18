from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_events(bronze: DataFrame) -> DataFrame:
    """Deduplicate and standardize raw Bronze GitHub events."""

    standardized = (
        bronze
        .dropDuplicates(["event_id"])
        .withColumn("actor_id", F.col("actor_id").cast("long"))
        .withColumn("repo_id", F.col("repo_id").cast("long"))
        .withColumn("org_id", F.col("org_id").cast("long"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("action", F.get_json_object("raw_json", "$.payload.action"))
        .withColumn("ref", F.get_json_object("raw_json", "$.payload.ref"))
        .withColumn("ref_type", F.get_json_object("raw_json", "$.payload.ref_type"))
        .withColumn("payload_json", F.get_json_object("raw_json", "$.payload"))
        .drop("created_at")
    )

    return standardized
