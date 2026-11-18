# Nexus — GitHub Ecosystem Lakehouse

An end-to-end data engineering project that transforms historical GitHub activity and repository changes into analytics-ready datasets using **Apache Spark, Apache Iceberg, Debezium, Redpanda, PostgreSQL, and S3-compatible object storage**.

Nexus implements a Medallion Architecture with two complementary data pipelines:

- **Historical batch pipeline:** Ingests GitHub activity data from GH Archive and transforms it into analytical datasets.
- **Repository CDC pipeline:** Captures PostgreSQL repository changes through Debezium and Redpanda, then incrementally maintains repository state in Iceberg.

The project focuses on reliable ingestion, incremental processing, idempotent transformations, checkpoint management, data quality, and source-to-target reconciliation.

---

## Architecture

### Pipeline A — Historical GitHub Activity

Processes historical GitHub events from GH Archive through Bronze, Silver, and Gold layers.

```text
GH Archive (.json.gz)
        |
        v
S3-Compatible Object Storage
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
Debezium
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
| Object Storage | MinIO, S3-compatible storage |
| CDC | Debezium |
| Streaming Infrastructure | Redpanda |
| Operational Database | PostgreSQL 16 |
| Iceberg Catalog | PostgreSQL-backed JDBC Catalog |
| Storage Interface | Iceberg S3FileIO |
| Infrastructure | Docker Compose |
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

The Iceberg write and PostgreSQL checkpoint update are separate operations; the design uses idempotent merges and checkpoint comparison to handle retries rather than claiming a distributed exactly-once transaction.

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

**Repository reconciliation**

| Metric | Result |
|---|---:|
| PostgreSQL records | 852,777 |
| Silver active records | 852,777 |
| Silver soft-deleted records | 2 |
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
│   ├── silver.py
│   ├── seed_repos.py
│   ├── repos_cdc_stream.py
│   ├── gold/
│   │   ├── build_gold.py
│   │   ├── transformations.py
│   │   └── validate_gold.py
│   ├── ingestion/
│   │   └── repos_bronze_stream.py
│   ├── processing/
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

- Docker and Docker Compose
- Python 3.12
- WSL 2 / Linux environment
- Sufficient local storage for historical data and Iceberg tables

### 1. Start infrastructure

```bash
docker compose up -d
docker compose ps
```

### 2. Configure the Python environment

```bash
python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 3. Configure environment variables

Create a local `.env` file in the project root, using `.env.example` as a reference.

Configure the required PostgreSQL credentials, MinIO endpoint and credentials, S3 bucket, and AWS region.

**Never commit `.env` or real credentials to GitHub.**

### 4. Run historical GH Archive ingestion

Download the hourly archives:

```bash
python -m ingest.download
```

Run Bronze ingestion:

```bash
python -m ingest.bronze
```

### 5. Start repository CDC infrastructure

Ensure PostgreSQL, Redpanda, Debezium, and MinIO are running.

Verify the Debezium connector:

```bash
curl http://localhost:8083/connectors/nexus-postgres-source/status
```

The connector and its task should both report `RUNNING`.

### 6. Start Bronze CDC ingestion

```bash
python -m spark.ingestion.repos_bronze_stream
```

The streaming job resumes from its existing Spark checkpoint.

### 7. Run incremental Silver processing

In a separate terminal, after Bronze has ingested new CDC messages:

```bash
python -m spark.processing.repos_silver_incremental
```

This processes new Bronze snapshots and applies repository changes to Silver.

### 8. Build and validate Gold analytics

Run the Gold builder:

```bash
python -m spark.gold.build_gold
```

Validate the resulting Gold tables:

```bash
python -m spark.gold.validate_gold
```

### 9. Validate repository state

```bash
python -m spark.validation.repos_cdc_reconciliation
```

This compares active Silver repository state against the PostgreSQL source.

### 10. Run automated tests

With the development dependencies installed and the required local services running:

```bash
python -m pytest tests/ -v
```

Run lint checks:

```bash
ruff check .
```

---

## Roadmap

- [x] Deploy local infrastructure using Docker Compose.
- [x] Implement GH Archive historical data ingestion.
- [x] Build Iceberg Bronze event tables.
- [x] Build normalized Silver event tables.
- [x] Seed PostgreSQL repository data.
- [x] Implement PostgreSQL CDC using Debezium.
- [x] Stream raw CDC messages into Iceberg Bronze.
- [x] Implement incremental Silver repository processing.
- [x] Add persistent PostgreSQL processing checkpoints.
- [x] Implement offset-based idempotent merges.
- [x] Implement soft-delete handling.
- [x] Add source-to-Silver reconciliation.
- [x] Build Gold analytical aggregations.
- [x] Add automated unit and integration tests.
- [x] Add local transformation performance benchmarks.
- [ ] Complete operational restart and replay recovery testing.
- [ ] Build analytical dashboards using Power BI.
- [ ] Explore AWS deployment and infrastructure automation.

---

## Project Status

**Core historical batch, repository CDC, and Gold analytics pipelines implemented.**

Historical GitHub events are stored in Iceberg Bronze and Silver, with four Gold aggregation tables built from normalized event data.

Repository changes are captured through Debezium and Redpanda, ingested into append-only Bronze, and incrementally applied to Silver using Spark and Iceberg.

The repository CDC pipeline has passed source-to-Silver reconciliation, and the automated test suite currently passes all 23 tests.

Operational restart/replay recovery testing, dashboard development, and cloud deployment remain future milestones.
