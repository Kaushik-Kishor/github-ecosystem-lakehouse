
from pyspark.sql import functions as F

from spark.iceberg_session import get_spark

spark = get_spark("Nexus-Duplicate-Investigation")

try:
    events = spark.read.table("nexus.bronze.events")

    duplicates = (
        events.groupBy("event_id")
        .agg(
            F.count("*").alias("row_count"),
            F.countDistinct("raw_json").alias("distinct_payloads"),
        )
        .filter(F.col("row_count") > 1)
    )

    print("\nDuplicate event IDs:")
    duplicates.show(truncate=False)

    duplicate_rows = events.join(
        duplicates.select("event_id"),
        on="event_id",
        how="inner",
    )

    print("\nFull duplicate records:")
    duplicate_rows.select(
        "event_id",
        "event_type",
        "created_at",
        "actor_login",
        "repo_name",
        "source_file",
        "raw_json",
    ).show(20, truncate=False)

finally:
    spark.stop()
