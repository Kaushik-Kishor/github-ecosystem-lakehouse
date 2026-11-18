import time

import pytest
from pyspark.sql import functions as F

from spark.processing.cdc_transformations import build_changes


@pytest.mark.parametrize("record_count", [10_000, 50_000, 100_000])
def test_cdc_transformation_performance(spark, record_count):
    # Generate valid Debezium CDC JSON directly inside Spark.
    source = (
        spark.range(1, record_count + 1)
        .select(
            F.concat(
                F.lit('{"payload":{"repo_id":'),
                F.col("id").cast("string"),
                F.lit("}}"),
            ).alias("message_key"),

            F.concat(
                F.lit(
                    '{"payload":{"op":"c","after":{"repo_id":'
                ),
                F.col("id").cast("string"),
                F.lit(
                    ',"name":"repo-'
                ),
                F.col("id").cast("string"),
                F.lit(
                    '","owner":"benchmark","stars":10,'
                    '"is_archived":false,'
                    '"updated_at":"2024-01-15T08:00:00Z"},'
                    '"before":null,"ts_ms":1705300000000}}'
                ),
            ).alias("message_value"),

            F.lit(0).alias("partition"),
            F.col("id").alias("offset"),
        )
    )

    start = time.perf_counter()

    changes = build_changes(source)
    output_count = changes.count()

    elapsed = time.perf_counter() - start
    throughput = record_count / elapsed

    print("\n========== CDC PERFORMANCE ==========")
    print(f"Input records:       {record_count:,}")
    print(f"Output changes:      {output_count:,}")
    print(f"Execution time:      {elapsed:.2f} seconds")
    print(f"Throughput:          {throughput:,.0f} records/second")
    print("=====================================")

    assert output_count == record_count
