import pytest

from spark.iceberg_session import get_spark

TEST_NAMESPACE = "integration_test"
TEST_TABLE = "nexus.integration_test.repos_silver_test"


@pytest.fixture(scope="session")
def iceberg_spark():
    spark = get_spark("Nexus-CDC-Integration-Tests")
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS nexus.{TEST_NAMESPACE}"
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
            repo_id BIGINT,
            name STRING,
            owner STRING,
            stars INT,
            is_archived BOOLEAN,
            updated_at TIMESTAMP,
            is_deleted BOOLEAN,
            last_kafka_offset BIGINT,
            source_op STRING,
            source_ts_ms BIGINT,
            applied_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (bucket(32, repo_id))
        LOCATION 's3://lake/integration-tests/repos_silver_test'
        TBLPROPERTIES ('format-version' = '2')
    """)

    yield spark

    spark.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    spark.stop()


@pytest.fixture
def clean_test_table(iceberg_spark):
    iceberg_spark.sql(f"DELETE FROM {TEST_TABLE}")
    yield


import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

TEST_CHECKPOINT_SCHEMA = "pipeline_control_test"
TEST_CHECKPOINT_TABLE = "pipeline_control_test.cdc_offsets_test"


@pytest.fixture
def checkpoint_db():
    load_dotenv(Path.cwd() / ".env")

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="iceberg",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE SCHEMA IF NOT EXISTS {TEST_CHECKPOINT_SCHEMA}"
            )

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TEST_CHECKPOINT_TABLE} (
                    pipeline_name TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    last_applied_offset BIGINT NOT NULL,
                    last_bronze_snapshot_id BIGINT,
                    last_successful_run TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY (pipeline_name, topic, partition_id)
                )
            """)

            cur.execute(f"DELETE FROM {TEST_CHECKPOINT_TABLE}")

            cur.execute(f"""
                INSERT INTO {TEST_CHECKPOINT_TABLE} (
                    pipeline_name,
                    topic,
                    partition_id,
                    last_applied_offset,
                    last_bronze_snapshot_id
                )
                VALUES (
                    'nexus-repos-silver',
                    'nexus.public.repos',
                    0,
                    100,
                    1000
                )
            """)

        conn.commit()
        yield conn

    finally:
        conn.rollback()
        conn.close()
