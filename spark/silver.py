from pyspark.sql import functions as F

from spark.iceberg_session import get_spark


def build_silver():
    spark = get_spark("Nexus-Silver")

    # Smaller shuffle partitions reduce per-task memory pressure.
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    print("Reading Bronze events...")
    bronze = spark.table("nexus.bronze.events")

    # --------------------------------------------------
    # 1. Deduplicate by event_id
    # --------------------------------------------------
    # --------------------------------------------------
    # 2. Standardize common fields
    # --------------------------------------------------
    print("Standardizing fields...")

    standardized = (
        bronze
        .withColumn("actor_id", F.col("actor_id").cast("long"))
        .withColumn("repo_id", F.col("repo_id").cast("long"))
        .withColumn("org_id", F.col("org_id").cast("long"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn(
            "action",
            F.get_json_object("raw_json", "$.payload.action")
        )
        .withColumn(
            "ref",
            F.get_json_object("raw_json", "$.payload.ref")
        )
        .withColumn(
            "ref_type",
            F.get_json_object("raw_json", "$.payload.ref_type")
        )
        .withColumn(
            "payload_json",
            F.get_json_object("raw_json", "$.payload")
        )
        .drop("created_at")
    )

    # --------------------------------------------------
    # 3. Create Silver namespace
    # --------------------------------------------------
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nexus.silver")

    # --------------------------------------------------
    # 4. Write Iceberg table
    # --------------------------------------------------
    table_name = "nexus.silver.events"

    print("Writing Silver events to Iceberg...")

    if spark.catalog.tableExists(table_name):
        standardized.writeTo(table_name).overwritePartitions()
    else:
        (
            standardized.writeTo(table_name)
            .partitionedBy("event_date")
            .create()
        )

    print("Silver write completed successfully.")

    spark.stop()


if __name__ == "__main__":
    build_silver()
