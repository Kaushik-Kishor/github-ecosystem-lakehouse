import os
from pathlib import Path

from pyspark.sql import functions as F

from spark.iceberg_session import get_spark


TOPIC = "nexus.public.repos"
BOOTSTRAP_SERVERS = "localhost:19092"
TARGET_TABLE = "nexus.bronze.repo_cdc"

CHECKPOINT_PATH = str(
    Path("checkpoints/repos_bronze_v2").resolve()
)

MAX_OFFSETS_PER_TRIGGER = 25_000


def main():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    spark = get_spark("Nexus-Repos-Bronze-Stream")

    try:
        spark.sparkContext.setLogLevel("WARN")

        print("Starting Nexus Bronze CDC ingestion", flush=True)
        print(f"Kafka topic: {TOPIC}", flush=True)
        print(f"Target table: {TARGET_TABLE}", flush=True)
        print(f"Checkpoint: {CHECKPOINT_PATH}", flush=True)

        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option(
                "maxOffsetsPerTrigger",
                MAX_OFFSETS_PER_TRIGGER,
            )
            .load()
        )

        bronze_df = kafka_df.select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("message_value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingested_at"),
        )

        query = (
            bronze_df.writeStream
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime="60 seconds")
            .toTable(TARGET_TABLE)
        )

        print(
            "Bronze ingestion is running. "
            "Press Ctrl+C to stop gracefully.",
            flush=True,
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
