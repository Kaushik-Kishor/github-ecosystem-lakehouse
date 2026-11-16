
import os
import tempfile
from pathlib import Path

import boto3
from dotenv import load_dotenv
from pyspark.sql import functions as F

from spark.iceberg_session import get_spark


load_dotenv()

ARCHIVE_DATE = os.getenv("GH_ARCHIVE_DATE", "2024-01-15")
BUCKET = os.environ["S3_BUCKET"]

LANDING_PREFIX = f"landing/dt={ARCHIVE_DATE}/"
TABLE = "nexus.bronze.events"


def get_archive_files():
    """List all hourly GH Archive files for the configured date."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    paginator = s3.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=LANDING_PREFIX,
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json.gz"):
                keys.append(obj["Key"])

    return sorted(keys), s3


def download_archives(keys, s3, directory):
    """Download archives to temporary local storage."""
    local_files = []

    for index, key in enumerate(keys, start=1):
        filename = Path(key).name
        destination = Path(directory) / filename

        print(f"[{index}/{len(keys)}] Downloading {filename}...")
        s3.download_file(BUCKET, key, str(destination))

        local_files.append(str(destination))

    return local_files


def main():
    keys, s3 = get_archive_files()

    if not keys:
        raise RuntimeError(
            f"No GH Archive files found under {LANDING_PREFIX}"
        )

    print(f"Found {len(keys)} archive files.")

    spark = get_spark("Nexus-Bronze-Ingestion")

    try:
        with tempfile.TemporaryDirectory(prefix="nexus-bronze-") as temp_dir:
            local_files = download_archives(keys, s3, temp_dir)

            print("\nReading compressed JSON archives...")

            raw = spark.read.text(local_files)

            bronze = (
                raw
                .select(
                    F.col("value").alias("raw_json"),
                    F.regexp_extract(
                        F.input_file_name(), r"([^/]+)$", 1
                    ).alias("source_file"),
                )
                .withColumn("event_id", F.get_json_object("raw_json", "$.id"))
                .withColumn("event_type", F.get_json_object("raw_json", "$.type"))
                .withColumn(
                    "created_at",
                    F.get_json_object("raw_json", "$.created_at"),
                )
                .withColumn(
                    "event_timestamp",
                    F.to_timestamp("created_at"),
                )
                .withColumn(
                    "actor_id",
                    F.get_json_object("raw_json", "$.actor.id"),
                )
                .withColumn(
                    "actor_login",
                    F.get_json_object("raw_json", "$.actor.login"),
                )
                .withColumn(
                    "repo_id",
                    F.get_json_object("raw_json", "$.repo.id"),
                )
                .withColumn(
                    "repo_name",
                    F.get_json_object("raw_json", "$.repo.name"),
                )
                .withColumn(
                    "org_id",
                    F.get_json_object("raw_json", "$.org.id"),
                )
                .withColumn(
                    "org_login",
                    F.get_json_object("raw_json", "$.org.login"),
                )
                .withColumn(
                    "ingestion_date",
                    F.lit(ARCHIVE_DATE).cast("date"),
                )
                .withColumn(
                    "parse_status",
                    F.when(
                        F.col("event_id").isNotNull()
                        & F.col("event_type").isNotNull()
                        & F.col("created_at").isNotNull()
                        & F.col("event_timestamp").isNotNull(),
                        F.lit("valid"),
                    ).otherwise(F.lit("needs_review")),
                )
                .select(
                    "event_id",
                    "event_type",
                    "created_at",
                    "event_timestamp",
                    "actor_id",
                    "actor_login",
                    "repo_id",
                    "repo_name",
                    "org_id",
                    "org_login",
                    "source_file",
                    "ingestion_date",
                    "parse_status",
                    "raw_json",
                )
            )

            print("\nBronze schema:")
            bronze.printSchema()

            total = bronze.count()
            print(f"\nTotal source records: {total:,}")

            print("\nEvent type distribution:")
            (
                bronze.groupBy("event_type")
                .count()
                .orderBy(F.desc("count"))
                .show(30, truncate=False)
            )

            existing = spark.catalog.tableExists(TABLE)

            if existing:
                print(f"\nReplacing existing partition in {TABLE}...")
                bronze.coalesce(4).writeTo(TABLE).overwritePartitions()
            else:
                print(f"\nCreating Iceberg table {TABLE}...")
                (
                    bronze.coalesce(4)
                    .writeTo(TABLE)
                    .partitionedBy("ingestion_date")
                    .create()
                )

            print("\nVerifying persisted Iceberg table...")

            saved = spark.read.table(TABLE)

            saved_count = (
                saved
                .filter(F.col("ingestion_date") == ARCHIVE_DATE)
                .count()
            )

            print(f"Source records: {total:,}")
            print(f"Stored records: {saved_count:,}")

            if total != saved_count:
                raise RuntimeError("Bronze record count verification failed!")

            print("\nStored event type distribution:")
            (
                saved.filter(F.col("ingestion_date") == ARCHIVE_DATE)
                .groupBy("event_type")
                .count()
                .orderBy(F.desc("count"))
                .show(30, truncate=False)
            )

            print("\nSample Bronze records:")
            saved.select(
                "event_id",
                "event_type",
                "actor_login",
                "repo_name",
                "source_file",
                "parse_status",
            ).show(10, truncate=False)

            print("\nBRONZE INGESTION SUCCESSFUL!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
