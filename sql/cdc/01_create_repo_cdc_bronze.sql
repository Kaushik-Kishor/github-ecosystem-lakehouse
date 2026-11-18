CREATE NAMESPACE IF NOT EXISTS nexus.bronze;

CREATE TABLE IF NOT EXISTS nexus.bronze.repo_cdc (
    message_key STRING,
    message_value STRING,
    topic STRING,
    partition INT,
    offset BIGINT,
    kafka_timestamp TIMESTAMP,
    ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingested_at))
LOCATION 's3://lake/warehouse/bronze/repo_cdc_v2'
TBLPROPERTIES (
    'format-version' = '2'
);
