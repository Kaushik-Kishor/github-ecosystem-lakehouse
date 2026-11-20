# Nexus — GitHub Ecosystem Lakehouse

An end-to-end data engineering project that transforms historical GitHub activity and repository changes into analytics-ready datasets using **Apache Spark, Apache Iceberg, Debezium, Redpanda, PostgreSQL, MinIO, Amazon S3, and Apache Airflow**.

Nexus implements a Medallion Architecture with two complementary data pipelines:

- **Historical batch pipeline:** Ingests GitHub activity data from GH Archive and transforms it into analytical datasets.
- **Repository CDC pipeline:** Captures PostgreSQL repository changes through Debezium and Redpanda, then incrementally maintains repository state in Iceberg.

Apache Airflow orchestrates historical batch processing and incremental CDC workflows.

The project focuses on reliable ingestion, incremental processing, idempotent transformations, checkpoint management, data quality, workflow orchestration, and source-to-target reconciliation.

---

## Architecture

### Pipeline A — Historical GitHub Activity

Processes historical GitHub events from GH Archive through Bronze, Silver, and Gold layers.

```text
GH Archive (.json.gz)
        |
        v
Object Storage (MinIO / Amazon S3)
        |
        v
Apache Spark
        |
        v
Iceberg — Bronze
(Raw GitHub Events)
        |
        v
Iceberg — Silver
(Normalized and Deduplicated Events)
        |
        v
Iceberg — Gold
(Analytics-Ready Aggregations)
```

**Initial dataset**

- 24 hourly GH Archive files from 2024-01-15.
- 5,528,301 raw GitHub events.
- 15 observed event types.
- Original event JSON preserved alongside extracted fields.

### Pipeline B — Repository Change Data Capture

Captures repository inserts, updates, and deletes from PostgreSQL and incrementally maintains the current repository state in Iceberg.

```text
PostgreSQL (app.public.repos)
        |
        v
Debezium PostgreSQL Connector
        |
        v
Redpanda
(Kafka-Compatible Broker)
        |
        v
Spark Structured Streaming
        |
        v
Iceberg — Bronze
(Append-Only Raw CDC)
        |
        v
Incremental Spark Processing
        |
        v
Iceberg — Silver
(Current Repository State)
```

The CDC architecture separates continuous message ingestion from incremental state application.

**Bronze CDC**

- Consumes Debezium messages from Redpanda.
- Preserves original Kafka message keys and values.
- Stores topic, partition, offset, Kafka timestamp, and ingestion timestamp.
- Retains null-valued Kafka tombstone messages.
- Uses Spark Structured Streaming checkpoints for recovery.

**Silver CDC**

- Reads new Bronze Iceberg snapshots incrementally.
- Applies inserts, updates, and deletes using Iceberg `MERGE`.
- Uses Kafka offsets to prevent stale events from overwriting newer state.
- Retains deleted repositories as soft-delete records.
- Tracks processing progress using a PostgreSQL-backed checkpoint.
- Supports idempotent replay following successful Iceberg writes.

### Pipeline C — Workflow Orchestration

Apache Airflow coordinates batch processing and incremental CDC workflows.

**Historical DAG — `nexus_historical_pipeline`**

```text
download_gh_archive
        |
        v
build_bronze
        |
        v
build_silver
        |
        v
build_gold
        |
        v
validate_gold
```

Runs on demand through the Airflow UI.

**CDC DAG — `nexus_cdc_pipeline`**

```text
process_incremental_cdc
        |
        v
reconcile_cdc_state
```

Runs every 15 minutes.

The continuous Spark Structured Streaming job that ingests Redpanda messages into Iceberg Bronze runs independently. Airflow orchestrates downstream incremental Silver processing and source-to-Silver reconciliation.

### Medallion Architecture

| Layer | Responsibility | Status |
|---|---|---|
| Bronze | Raw historical events and append-only CDC messages | Implemented |
| Silver | Normalized events and current repository state | Implemented |
| Gold | Analytics-ready event and activity aggregations | Implemented |

---

## Technology Stack

| Category | Technologies |
|---|---|
| Languages | Python, SQL |
| Data Processing | Apache Spark 3.5.6, PySpark |
| Table Format | Apache Iceberg 1.9.2 |
| Object Storage | MinIO, Amazon S3 |
| CDC | Debezium 2.5.4 |
| Streaming Infrastructure | Redpanda 24.2.14 |
| Operational Database | PostgreSQL 16 |
| Iceberg Catalog | PostgreSQL-backed JDBC Catalog |
| Workflow Orchestration | Apache Airflow 2.10.5 |
| Airflow Executor | LocalExecutor |
| Infrastructure | Docker Compose, AWS EC2 |
| Testing | Pytest, PySpark |
| Code Quality | Ruff |

---

## Implementation

### 1. Infrastructure and Storage

- Deployed PostgreSQL, Redpanda, Debezium, and MinIO using Docker Compose.
- Configured a PostgreSQL-backed Iceberg JDBC catalog.
- Integrated Apache Spark with Apache Iceberg.
- Configured S3-compatible storage through Iceberg S3FileIO.
- Established a local development environment using Docker and WSL.
- Configured Amazon S3 as the object storage layer for the temporary AWS deployment.

### 2. Historical Event Ingestion

Implemented a batch ingestion pipeline for GH Archive data.

- Downloaded 24 hourly compressed JSON archives.
- Implemented object existence checks to avoid unnecessary downloads.
- Inspected raw event structures and profiled schemas.
- Ingested historical events into Iceberg Bronze.
- Preserved original JSON alongside extracted event fields.
- Built normalized and deduplicated Silver event data.

**Historical dataset results**

| Metric | Result |
|---|---:|
| Archive date | 2024-01-15 |
| Hourly files | 24 |
| Raw events | 5,528,301 |
| Observed event types | 15 |
| Bronze records | 5,528,301 |
| Silver records | 5,528,300 |

The Silver layer removes duplicate event IDs while preserving the original event payload.

### 3. Repository CDC — End-to-End Implementation

Implemented a PostgreSQL-to-Iceberg CDC pipeline using Debezium, Redpanda, Spark Structured Streaming, and Apache Iceberg.

**Source database**

- Seeded PostgreSQL with 852,777 distinct repository records.
- Configured Debezium PostgreSQL CDC.
- Configured PostgreSQL logical replication and the `pgoutput` plugin.
- Verified insert, update, and delete event capture.

The repository records are seeded test data. Their attributes should not be interpreted as verified live GitHub repository metadata.

**Bronze CDC ingestion**

- Captured Debezium messages from Redpanda.
- Implemented append-only Iceberg ingestion.
- Preserved raw message keys, values, Kafka metadata, and tombstones.
- Configured checkpoint-based streaming recovery.
- Ingested the initial CDC history and subsequent test events.

**Incremental Silver processing**

- Implemented initial repository state construction from Bronze history.
- Implemented incremental processing from new Bronze Iceberg snapshots.
- Applied inserts, updates, and deletes using Iceberg `MERGE`.
- Used Kafka offsets to prevent stale events from overwriting newer repository state.
- Retained deleted repositories as soft-delete records.
- Implemented persistent progress tracking in PostgreSQL.
- Added PostgreSQL advisory locking to prevent concurrent Silver writers.
- Implemented idempotent merge conditions for replay handling.

**Processing reliability**

- Validated contiguous Kafka offset ranges.
- Advanced the processing checkpoint only after successful Iceberg writes.
- Designed replay handling for failures between the Iceberg commit and PostgreSQL checkpoint update.
- Added source-to-Silver reconciliation.

The Iceberg write and PostgreSQL checkpoint update are separate operations. The design uses idempotent merges and checkpoint comparison to handle retries rather than claiming a distributed exactly-once transaction.

### 4. Gold Analytics

Implemented four analytics-ready Iceberg tables from the normalized historical Silver events.

| Gold table | Grain | Records |
|---|---|---:|
| `nexus.gold.daily_event_metrics` | Date and event type | 15 |
| `nexus.gold.hourly_event_metrics` | Date, hour, and event type | 360 |
| `nexus.gold.repo_activity_metrics` | Date and repository | 856,792 |
| `nexus.gold.developer_activity_metrics` | Date, developer, and bot classification | 646,518 |

The models provide:

- Daily event counts and distinct activity metrics.
- Hourly event distribution.
- Repository-level activity metrics.
- Developer-level activity and bot classification.

Bot classification uses the `[bot]` suffix in actor logins.

The historical dataset covers a single day, 2024-01-15. These models demonstrate aggregation and analytical modeling, not longitudinal trends.

Synthetic repository metadata is not treated as genuine GitHub metadata or joined to historical events as though it were verified source data.

### 5. CDC Validation Results

The initial repository state and subsequent incremental changes were reconciled against PostgreSQL.

**Final AWS deployment reconciliation**

| Metric | Result |
|---|---:|
| PostgreSQL records | 852,777 |
| Silver active records | 852,777 |
| Silver soft-deleted records | 0 |
| Missing records | 0 |
| Extra records | 0 |
| Attribute mismatches | 0 |

**Incremental processing test**

| Metric | Result |
|---|---:|
| New Kafka messages processed | 8 |
| Kafka offset range | 852810–852817 |
| Distinct offsets | 8 |
| Partitions processed | 1 |
| Repository changes applied | 1 |
| Iceberg MERGE | Committed |
| PostgreSQL checkpoint | Advanced |

The eight messages include the test operations and associated tombstones. Offset accounting and repository state application are handled separately.

**Source-to-Silver reconciliation: PASSED**

The final AWS reconciliation confirmed that all 852,777 PostgreSQL records matched their corresponding active Silver records, with zero missing records, extra records, or attribute mismatches.

### 6. Automated Testing

Implemented an automated test suite covering transformations, CDC processing, checkpoint management, Iceberg operations, and performance.

| Test category | Tests |
|---|---:|
| Unit tests | 10 |
| Iceberg MERGE integration tests | 6 |
| PostgreSQL checkpoint integration tests | 3 |
| CDC end-to-end test | 1 |
| Performance benchmarks | 3 |
| **Total** | **23** |

All 23 tests passed in the latest test run.

Coverage includes:

- Silver event deduplication and standardization.
- Daily, hourly, repository, and developer aggregations.
- Bot classification.
- CDC insert, update, delete, and tombstone handling.
- Iceberg MERGE behavior.
- Stale-offset protection.
- Idempotent replay.
- PostgreSQL checkpoint compare-and-set behavior.
- CDC transformation and reconciliation.
- Transformation performance at 10,000, 50,000, and 100,000 input records.

**Local transformation benchmarks**

| Input records | Execution time | Throughput |
|---:|---:|---:|
| 10,000 | 2.42 s | 4,125 records/s |
| 50,000 | 0.78 s | 64,088 records/s |
| 100,000 | 0.83 s | 120,782 records/s |

These benchmarks measure local Spark transformation performance, not end-to-end Kafka-to-Iceberg throughput. The smaller run includes Spark startup and warm-up overhead.

### 7. Workflow Orchestration with Apache Airflow

Implemented Apache Airflow 2.10.5 to orchestrate historical batch processing and incremental CDC processing.

**Airflow deployment**

- Uses LocalExecutor.
- Uses a dedicated PostgreSQL database named `airflow` for metadata.
- Shares the project directory with Airflow containers so DAG tasks can execute project scripts.
- Provides the Airflow web UI at `http://localhost:8080` in the local environment.

**Historical DAG — `nexus_historical_pipeline`**

Coordinates the complete historical workflow:

1. Download GH Archive data.
2. Build Bronze.
3. Build Silver.
4. Build Gold.
5. Validate Gold.

**CDC DAG — `nexus_cdc_pipeline`**

Runs every 15 minutes and coordinates:

1. Incremental CDC processing from Bronze to Silver.
2. Source-to-Silver reconciliation.

The long-running Bronze streaming job remains independent of Airflow. This keeps continuous ingestion separate from downstream processing and validation.

Both DAGs were successfully executed through the Airflow UI during local development and the AWS deployment.

### 8. AWS Deployment

Deployed Nexus to AWS to validate the pipelines in a cloud environment.

**Infrastructure**

| Component | AWS configuration |
|---|---|
| Region | Asia Pacific (Mumbai) — `ap-south-1` |
| Compute | Amazon EC2 |
| Instance type | `m7i-flex.large` |
| Operating system | Ubuntu Server 24.04 LTS |
| Container orchestration | Docker Compose |
| Object storage | Amazon S3 |
| Access management | IAM instance role |
| Pipeline orchestration | Apache Airflow |

The EC2 instance hosted the containerized PostgreSQL, Redpanda, Debezium, and Airflow services. Spark processed the historical and CDC datasets, while Iceberg tables were stored in Amazon S3.

**AWS deployment validation**

- Successfully deployed the Docker-based data engineering stack to EC2.
- Configured EC2 access to S3 through an IAM instance role rather than hardcoded AWS access keys.
- Executed the historical GH Archive pipeline through Airflow.
- Successfully completed all five historical DAG tasks.
- Executed the CDC pipeline through Airflow.
- Successfully completed incremental processing and reconciliation tasks.
- Verified 852,777 PostgreSQL records against 852,777 active Silver records.
- Confirmed zero missing records, extra records, and attribute mismatches.

**Deployment status**

The AWS environment was provisioned temporarily for deployment testing and portfolio validation. After successful execution and evidence collection, the EC2 instance was terminated and the S3 bucket was deleted to avoid ongoing infrastructure costs.

The AWS deployment is therefore a completed deployment and validation exercise, not a continuously running production environment.

---

## Engineering Considerations

### Incremental Processing

Processes new Bronze Iceberg snapshots instead of repeatedly scanning the entire CDC history.

### Idempotency

Uses Kafka offsets and conditional Iceberg merges to prevent older events from overwriting newer repository state.

### Checkpointing

Maintains processing progress in PostgreSQL independently of Spark Structured Streaming checkpoints.

### Concurrency Control

Uses PostgreSQL advisory locking to prevent multiple incremental processors from applying changes concurrently.

### Soft Deletes

Preserves delete history in Silver without allowing replayed older events to resurrect deleted repositories.

### Data Reconciliation

Compares active Silver repository state against PostgreSQL and validates missing records, extra records, and attribute mismatches.

### Separation of Concerns

Decouples continuous Kafka ingestion from incremental repository state application so ingestion and processing can operate independently.

---

## Repository Structure

```text
.
├── airflow/
│   ├── dags/
│   │   ├── nexus_historical_pipeline.py
│   │   └── nexus_cdc_pipeline.py
│   ├── logs/
│   ├── plugins/
│   └── Dockerfile
│
├── ingest/
│   ├── download.py
│   ├── inspect_raw.py
│   ├── profile_schema.py
│   └── bronze.py
│
├── scripts/
│   └── simulate_repo_changes.py
│
├── spark/
│   ├── iceberg_session.py
│   ├── silver_transformations.py
│   ├── seed_repos.py
│   ├── gold/
│   │   ├── build_gold.py
│   │   ├── transformations.py
│   │   └── validate_gold.py
│   ├── ingestion/
│   │   └── repos_bronze_stream.py
│   ├── processing/
│   │   ├── cdc_transformations.py
│   │   ├── repos_silver_batch.py
│   │   └── repos_silver_incremental.py
│   └── validation/
│       └── repos_cdc_reconciliation.py
│
├── sql/
│   └── cdc/
│       ├── 01_create_repo_cdc_bronze.sql
│       └── 02_create_repos_silver_v2.sql
│
├── tests/
│   ├── unit/
│   │   ├── test_silver.py
│   │   ├── test_gold.py
│   │   └── test_cdc_transformations.py
│   ├── integration/
│   │   ├── conftest.py
│   │   ├── test_cdc_e2e.py
│   │   ├── test_checkpoint.py
│   │   └── test_iceberg_cdc_merge.py
│   └── performance/
│       └── test_cdc_performance.py
│
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Running Locally

### Prerequisites

- Docker Desktop with WSL 2 integration, or Docker Engine with Docker Compose.
- Python 3.12.
- WSL 2 / Linux environment.
- Sufficient local storage for historical data and Iceberg tables.

### 1. Clone the repository

```bash
git clone <repository-url>
cd github-ecosystem-lakehouse
```

### 2. Configure environment variables

Create a local `.env` file using `.env.example` as a reference.

```bash
cp .env.example .env
```

Configure the required PostgreSQL credentials, MinIO endpoint and credentials, S3 bucket, and AWS region.

The Airflow setup also requires its configured image name, executor settings, and admin credentials. Keep all actual credentials in `.env`.

**Never commit `.env` or real credentials to GitHub.**

### 3. Start the infrastructure and Airflow

Build and start the Docker Compose services:

```bash
docker compose up -d --build
```

Check the service status:

```bash
docker compose ps
```

This starts the core infrastructure and Airflow components, including:

- PostgreSQL
- Redpanda
- Debezium
- MinIO
- Airflow initialization
- Airflow webserver
- Airflow scheduler

Open the Airflow UI:

```text
http://localhost:8080
```

Sign in using the Airflow admin username and password configured in `.env`.

### 4. Run the historical pipeline

In the Airflow UI:

1. Open `nexus_historical_pipeline`.
2. Trigger the DAG manually.
3. Monitor the task sequence:

```text
download_gh_archive
        |
        v
build_bronze
        |
        v
build_silver
        |
        v
build_gold
        |
        v
validate_gold
```

The DAG coordinates the historical download, ingestion, transformations, and validation.

### 5. Verify the Debezium connector

Check that the PostgreSQL CDC connector is running:

```bash
curl http://localhost:8083/connectors/nexus-postgres-connector/status
```

The connector and its task should both report `RUNNING`.

### 6. Start continuous Bronze CDC ingestion

The Bronze streaming job runs independently of Airflow.

Activate the project's Python environment:

```bash
source .venv/bin/activate
```

Start the stream:

```bash
python -m spark.ingestion.repos_bronze_stream
```

The streaming job consumes messages from Redpanda and appends them to Iceberg Bronze. It resumes from its existing Spark checkpoint.

Keep this process running while CDC ingestion is required.

### 7. Run incremental CDC processing with Airflow

Enable `nexus_cdc_pipeline` in the Airflow UI.

The DAG runs every 15 minutes and executes:

```text
process_incremental_cdc
        |
        v
reconcile_cdc_state
```

It processes new Bronze CDC snapshots, applies repository changes to Silver, and reconciles the resulting state against PostgreSQL.

The Bronze streaming process and the Airflow scheduler must remain available for the complete CDC workflow.

### 8. Run individual pipeline scripts manually

For development and debugging, individual scripts can also be executed outside Airflow.

**Historical ingestion**

```bash
python -m ingest.download
python -m ingest.bronze
```

**Build Silver and Gold**

```bash
python -m spark.processing.repos_silver_batch
python -m spark.gold.build_gold
python -m spark.gold.validate_gold
```

**Incremental CDC processing**

```bash
python -m spark.processing.repos_silver_incremental
```

**Repository reconciliation**

```bash
python -m spark.validation.repos_cdc_reconciliation
```

### 9. Run automated tests

With development dependencies installed and the required local services running:

```bash
python -m pytest tests/ -v
```

Run lint checks:

```bash
ruff check .
```

### 10. Stop the services

To stop the Docker Compose services without deleting persistent data:

```bash
docker compose down
```

To start them again:

```bash
docker compose up -d
```

Persistent PostgreSQL, Redpanda, and MinIO data are stored in Docker volumes. Do not use `docker compose down -v` unless you intentionally want to delete those volumes and their data.

---

## Roadmap

- [x] Deploy local infrastructure using Docker Compose.
- [x] Implement GH Archive historical data ingestion.
- [x] Build Iceberg Bronze, Silver, and Gold layers.
- [x] Implement PostgreSQL CDC using Debezium and Redpanda.
- [x] Stream raw CDC messages into Iceberg Bronze.
- [x] Implement incremental Silver repository processing.
- [x] Add persistent PostgreSQL processing checkpoints.
- [x] Implement offset-based idempotent merges and soft deletes.
- [x] Add source-to-Silver reconciliation.
- [x] Add automated unit and integration tests.
- [x] Add local transformation performance benchmarks.
- [x] Orchestrate historical processing with Apache Airflow.
- [x] Orchestrate incremental CDC processing with Apache Airflow.
- [x] Deploy and validate historical and CDC pipelines on AWS.
- [x] Verify cloud pipeline execution and source-to-Silver reconciliation.
- [x] Terminate temporary AWS infrastructure after validation.

---

## Project Status

**Core historical batch, repository CDC, Gold analytics, workflow orchestration, and AWS deployment validation completed.**

Historical GitHub events are stored in Iceberg Bronze and Silver, with four Gold aggregation tables built from normalized event data.

Repository changes are captured through Debezium and Redpanda, ingested into append-only Bronze, and incrementally applied to Silver using Spark and Iceberg.

The repository CDC pipeline has passed source-to-Silver reconciliation, with 852,777 active records matching PostgreSQL and zero attribute mismatches.

The automated test suite passes all 23 tests.

Apache Airflow orchestrates the historical workflow and incremental CDC processing through two successfully executed DAGs.

Nexus was also deployed and validated on AWS, with both historical and CDC pipelines completing successfully. The temporary cloud infrastructure was subsequently decommissioned to avoid ongoing costs.

**The implementation and cloud validation milestones are complete.**