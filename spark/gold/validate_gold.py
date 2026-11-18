from pyspark.sql import functions as F

from spark.iceberg_session import get_spark


def validate_gold():
    spark = get_spark("Nexus-Gold-Quality-Checks")
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "32")

    source_count = spark.table("nexus.silver.events").count()

    tables = {
        "daily_event_metrics": {
            "keys": ["event_date", "event_type"],
            "metrics": ["event_count", "unique_actors", "unique_repositories"],
        },
        "hourly_event_metrics": {
            "keys": ["event_date", "event_hour", "event_type"],
            "metrics": ["event_count", "unique_actors", "unique_repositories"],
        },
        "repo_activity_metrics": {
            "keys": ["event_date", "repo_id", "repo_name"],
            "metrics": ["event_count", "distinct_event_types", "unique_contributors"],
        },
        "developer_activity_metrics": {
            "keys": ["event_date", "actor_id", "actor_login", "is_bot"],
            "metrics": ["event_count", "unique_repositories", "distinct_event_types"],
        },
    }

    failures = []

    try:
        print(f"Silver source rows: {source_count:,}")

        for name, config in tables.items():
            table_name = f"nexus.gold.{name}"
            df = spark.table(table_name)

            print(f"\n=== {table_name} ===")

            row_count = df.count()
            event_total = df.agg(
                F.sum("event_count").alias("total")
            ).first()["total"] or 0

            print(f"Rows: {row_count:,}")
            print(f"Event total: {event_total:,}")

            # Event totals must reconcile to Silver.
            if event_total != source_count:
                failures.append(
                    f"{name}: event total {event_total} != Silver {source_count}"
                )
                print("FAIL: event total mismatch")
            else:
                print("PASS: event total reconciles")

            # Aggregation keys must be unique.
            duplicate_key = (
                df.groupBy(*config["keys"])
                .count()
                .filter(F.col("count") > 1)
                .limit(1)
                .count()
            )

            if duplicate_key:
                failures.append(f"{name}: duplicate aggregation keys")
                print("FAIL: duplicate aggregation keys")
            else:
                print("PASS: aggregation keys are unique")

            # Grouping keys must not be null.
            null_condition = None
            for key in config["keys"]:
                condition = F.col(key).isNull()
                null_condition = (
                    condition if null_condition is None
                    else null_condition | condition
                )

            null_keys = df.filter(null_condition).limit(1).count()

            if null_keys:
                failures.append(f"{name}: null grouping keys")
                print("FAIL: null grouping keys")
            else:
                print("PASS: grouping keys contain no nulls")

            # Metrics must be non-negative.
            negative_condition = None
            for metric in config["metrics"]:
                condition = F.col(metric) < 0
                negative_condition = (
                    condition if negative_condition is None
                    else negative_condition | condition
                )

            negative_rows = df.filter(negative_condition).limit(1).count()

            if negative_rows:
                failures.append(f"{name}: negative metric values")
                print("FAIL: negative metric values")
            else:
                print("PASS: metrics are non-negative")

        # Bot classification must reconcile.
        print("\n=== BOT ACTIVITY CHECK ===")

        bot_totals = (
            spark.table("nexus.gold.developer_activity_metrics")
            .groupBy("is_bot")
            .agg(F.sum("event_count").alias("event_count"))
        )

        bot_totals.show(truncate=False)

        bot_total = (
            bot_totals.agg(F.sum("event_count")).first()[0] or 0
        )

        if bot_total != source_count:
            failures.append("Bot/non-bot totals do not reconcile")
            print("FAIL: bot totals mismatch")
        else:
            print("PASS: bot and non-bot totals reconcile")

        print("\n=== FINAL RESULT ===")

        if failures:
            print(f"FAILED: {len(failures)} check(s)")
            for failure in failures:
                print("-", failure)
            raise SystemExit(1)

        print("ALL GOLD QUALITY CHECKS PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    validate_gold()
