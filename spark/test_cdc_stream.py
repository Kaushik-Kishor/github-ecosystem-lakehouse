
from pyspark.sql import functions as F

from spark.iceberg_session import get_spark


def test_cdc_stream():
    spark = get_spark("Nexus-CDC-Test")
    spark.sparkContext.setLogLevel("WARN")

    print("Connecting to Redpanda...")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:19092")
        .option("subscribe", "nexus.public.repos")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        raw_stream
        .select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp"),
        )
        .where(F.col("value").isNotNull())
    )

    print("Starting CDC stream. Waiting for new events...")

    query = (
        events.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="5 seconds")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping CDC test stream...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    test_cdc_stream()
