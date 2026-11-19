from spark.iceberg_session import get_spark
from spark.silver_transformations import transform_events


def build_silver():
    spark = get_spark("Nexus-Silver")
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading Bronze events...")
        bronze = spark.table("nexus.bronze.events")

        print("Applying Silver transformations...")
        silver = transform_events(bronze)

        spark.sql("CREATE NAMESPACE IF NOT EXISTS nexus.silver")

        print("Writing nexus.silver.events...")
        (
            silver.writeTo("nexus.silver.events")
            .using("iceberg")
            .partitionedBy("event_date")
            .createOrReplace()
        )

        print("Silver layer build completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    build_silver()
