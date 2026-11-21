import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import functions as F

from spark.iceberg_session import get_spark


def main():
    # Environment variables are supplied by Docker Compose

    spark = get_spark("Nexus-Repos-CDC-Reconciliation")

    try:
        source = (
            spark.read.format("jdbc")
            .option("url", f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:5432/app")
            .option("dbtable", "public.repos")
            .option("user", os.environ["POSTGRES_USER"])
            .option("password", os.environ["POSTGRES_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
            .alias("s")
        )

        silver = (
            spark.table("nexus.silver.repos_v2")
            .filter(~F.col("is_deleted"))
            .alias("t")
        )

        joined = source.join(
            silver,
            F.col("s.repo_id") == F.col("t.repo_id"),
            "full_outer"
        )

        missing = (
            F.col("s.repo_id").isNotNull() &
            F.col("t.repo_id").isNull()
        )

        extra = (
            F.col("s.repo_id").isNull() &
            F.col("t.repo_id").isNotNull()
        )

        matched = (
            F.col("s.repo_id").isNotNull() &
            F.col("t.repo_id").isNotNull()
        )

        mismatch = (
            ~F.col("s.name").eqNullSafe(F.col("t.name")) |
            ~F.col("s.owner").eqNullSafe(F.col("t.owner")) |
            ~F.col("s.stars").eqNullSafe(F.col("t.stars")) |
            ~F.col("s.is_archived").eqNullSafe(F.col("t.is_archived")) |
            ~F.col("s.updated_at").eqNullSafe(F.col("t.updated_at"))
        )

        summary = joined.agg(
            F.sum(missing.cast("long")).alias("missing_in_silver"),
            F.sum(extra.cast("long")).alias("extra_in_silver"),
            F.sum((matched & mismatch).cast("long")).alias("attribute_mismatches"),
            F.sum(matched.cast("long")).alias("matched_records"),
        ).first()

        source_count = source.count()
        silver_count = silver.count()

        deleted_count = (
            spark.table("nexus.silver.repos_v2")
            .filter(F.col("is_deleted"))
            .count()
        )

        print("\n========== NEXUS CDC RECONCILIATION ==========")
        print(f"PostgreSQL records:       {source_count:,}")
        print(f"Silver active records:    {silver_count:,}")
        print(f"Silver soft-deleted:      {deleted_count:,}")
        print(f"Matched records:          {int(summary['matched_records'] or 0):,}")
        print(f"Missing in Silver:        {int(summary['missing_in_silver'] or 0):,}")
        print(f"Extra in Silver:          {int(summary['extra_in_silver'] or 0):,}")
        print(f"Attribute mismatches:     {int(summary['attribute_mismatches'] or 0):,}")

        passed = (
            source_count == silver_count
            and int(summary["missing_in_silver"] or 0) == 0
            and int(summary["extra_in_silver"] or 0) == 0
            and int(summary["attribute_mismatches"] or 0) == 0
        )

        if not passed:
            print("\nRECONCILIATION FAILED — inspect the source/Silver differences.")

            joined.filter(
                missing | extra | (matched & mismatch)
            ).select(
                F.coalesce(
                    F.col("s.repo_id"), F.col("t.repo_id")
                ).alias("repo_id"),
                F.col("s.name").alias("source_name"),
                F.col("t.name").alias("silver_name"),
                F.col("s.owner").alias("source_owner"),
                F.col("t.owner").alias("silver_owner"),
                F.col("s.stars").alias("source_stars"),
                F.col("t.stars").alias("silver_stars"),
                F.col("s.is_archived").alias("source_is_archived"),
                F.col("t.is_archived").alias("silver_is_archived"),
                F.col("s.updated_at").alias("source_updated_at"),
                F.col("t.updated_at").alias("silver_updated_at"),
            ).show(50, truncate=False)

            raise SystemExit(1)

        print("\nRECONCILIATION PASSED.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
