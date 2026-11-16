
from pyspark.sql import functions as F

from spark.iceberg_session import get_spark

spark = get_spark("Nexus-Bronze-Quality-Check")

try:
    events = spark.read.table("nexus.bronze.events")

    print("\n1. Overall record count")
    print(f"Total: {events.count():,}")

    print("\n2. Parse status")
    events.groupBy("parse_status").count().show()

    print("\n3. Missing essential fields")
    events.select(
        F.count(F.when(F.col("event_id").isNull(), 1)).alias("missing_event_id"),
        F.count(F.when(F.col("event_type").isNull(), 1)).alias("missing_event_type"),
        F.count(F.when(F.col("event_timestamp").isNull(), 1)).alias("missing_timestamp"),
    ).show()

    print("\n4. Archive coverage")
    events.select("source_file").distinct().count()
    events.select("source_file").distinct().orderBy("source_file").show(30, False)

    print("\n5. Duplicate event IDs")
    duplicate_ids = (
        events.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
    )

    print(f"Duplicate IDs: {duplicate_ids.count():,}")

finally:
    spark.stop()
