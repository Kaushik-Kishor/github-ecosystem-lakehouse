CREATE NAMESPACE IF NOT EXISTS nexus.silver;

CREATE TABLE IF NOT EXISTS nexus.silver.repos_v2 (
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
LOCATION 's3://lake/warehouse/silver/repos_v2'
TBLPROPERTIES (
    'format-version' = '2'
);
