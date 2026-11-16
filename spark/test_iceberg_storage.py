
from iceberg_session import get_spark

spark = get_spark("Nexus-Iceberg-Storage-Test")

table = "nexus.bronze.iceberg_smoke_test"

try:
    # Create a tiny test dataset
    data = [
        (1, "PushEvent"),
        (2, "PullRequestEvent"),
        (3, "WatchEvent"),
    ]

    df = spark.createDataFrame(data, ["event_id", "event_type"])

    # Write to Iceberg (stored in Silo)
    df.writeTo(table).create()

    print("\nSuccessfully wrote test table!")

    # Read it back from Iceberg
    result = spark.read.table(table)

    print("\nReading data back:")
    result.show()

    assert result.count() == 3, "Unexpected record count!"

    print("ICEBERG STORAGE TEST PASSED!")

finally:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.stop()
