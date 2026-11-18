from spark.iceberg_session import get_spark
from spark.silver_transformations import transform_events


def build_silver():
    spark = get_spark("Nexus-Silver")

    try:
        spark.conf.set("spark.sql.shuffle.partitions", "32")

        print("Reading Bronze events...")
        bronze = spark.table("nexus.bronze.events")

        print("Deduplicating and standardizing events...")
        standardized = transform_events(bronze)

        spark.sql("CREATE NAMESPACE IF NOT EXISTS nexus.silver")

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

    finally:
        spark.stop()


if __name__ == "__main__":
    build_silver()
