
from pyspark.sql import functions as F

from spark.iceberg_session import get_spark


def test_cdc_historical():
    spark = get_spark("Nexus-CDC-Historical-Test")
    spark.sparkContext.setLogLevel("WARN")

    print("Connecting to Redpanda historical offsets...")

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:19092")
        .option("subscribe", "nexus.public.repos")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "10000")
        .option("failOnDataLoss", "false")
        .load()
    )

    events = stream.select(
        F.col("value").cast("string").alias("value"),
        F.col("partition"),
        F.col("offset"),
    ).filter(F.col("value").isNotNull())

    def inspect_batch(batch_df, batch_id):
        count = batch_df.count()

        if count:
            bounds = batch_df.agg(
                F.min("offset").alias("first_offset"),
                F.max("offset").alias("last_offset"),
            ).first()

            print(
                f"Batch {batch_id}: {count} records | "
                f"offsets {bounds['first_offset']} "
                f"to {bounds['last_offset']}"
            )
        else:
            print(f"Batch {batch_id}: no records")

    query = (
        events.writeStream
        .foreachBatch(inspect_batch)
        .option("checkpointLocation", "checkpoints/cdc_historical_test")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("Historical test running. Stop with Ctrl+C.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping historical test...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    test_cdc_historical()
