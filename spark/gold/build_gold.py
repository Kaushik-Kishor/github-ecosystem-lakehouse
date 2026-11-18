from spark.gold.transformations import (
    build_daily_metrics,
    build_developer_activity,
    build_hourly_metrics,
    build_repo_activity,
)
from spark.iceberg_session import get_spark


def build_gold():
    spark = get_spark("Nexus-Gold")
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.conf.set("spark.sql.shuffle.partitions", "32")
        spark.conf.set("spark.sql.adaptive.enabled", "true")

        print("Reading Silver events...")

        events = spark.table("nexus.silver.events").select(
            "event_id",
            "event_type",
            "event_timestamp",
            "event_date",
            "actor_id",
            "actor_login",
            "repo_id",
            "repo_name",
        )

        spark.sql("CREATE NAMESPACE IF NOT EXISTS nexus.gold")

        tables = {
            "daily_event_metrics": build_daily_metrics(events),
            "hourly_event_metrics": build_hourly_metrics(events),
            "repo_activity_metrics": build_repo_activity(events),
            "developer_activity_metrics": build_developer_activity(events),
        }

        for name, dataframe in tables.items():
            table_name = f"nexus.gold.{name}"
            print(f"Writing {table_name}...")

            (
                dataframe.writeTo(table_name)
                .using("iceberg")
                .partitionedBy("event_date")
                .createOrReplace()
            )

            print(f"Written: {table_name}")

        print("\nGold layer build completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    build_gold()
