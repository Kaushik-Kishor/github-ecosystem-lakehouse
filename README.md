# Nexus — GitHub Ecosystem Lakehouse

An end-to-end data engineering project that transforms GitHub activity data and repository changes into an analytics-ready lakehouse using Apache Spark, Apache Iceberg, Debezium, Redpanda, PostgreSQL, and S3-compatible object storage.

Nexus implements a Medallion Architecture with two complementary data pipelines:

- **Historical batch ingestion:** Processes GitHub activity data from GH Archive.
- **Change Data Capture (CDC):** Captures PostgreSQL repository changes and incrementally maintains repository state in Apache Iceberg.

The project focuses on scalable data ingestion, incremental processing, reliable checkpointing, idempotent transformations, and source-to-target data validation.

---

## Architecture

### Pipeline A — Historical GitHub Activity

Ingests historical GitHub events from GH Archive and transforms them into structured Iceberg tables.

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
Apache Iceberg — Bronze
(Raw Events)
        |
        v
Apache Iceberg — Silver
(Normalized Events)
        |
        v
Gold Analytics
(Planned)
```

**Initial dataset:**
- 24 hourly GH Archive files from 2024-01-15.
- 5.5 million+ GitHub events.
- 15 observed event types.
- Original event JSON preserved alongside extracted common fields.

### Pipeline B — Repository Change Data Capture

Captures inserts, updates, and deletes from PostgreSQL and incrementally maintains repository state in Iceberg.

```text
PostgreSQL (app.repos)
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

The CDC architecture separates continuous event ingestion from repository state application.

**Bronze layer**
- Consumes Debezium messages from Redpanda.
- Stores original Kafka message keys and values.
- Preserves topic, partition, offset, Kafka timestamp, and ingestion timestamp.
- Retains null-valued Kafka tombstone messages.
- Uses Spark Structured Streaming checkpoints for recovery.

**Silver layer**
- Reads new Bronze Iceberg snapshots incrementally.
- Applies repository inserts, updates, and deletes using Iceberg `MERGE`.
- Uses Kafka offsets to preserve event ordering and prevent stale updates.
- Retains deleted repositories as soft-delete records.
- Uses a PostgreSQL-backed processing checkpoint.
- Supports idempotent replay following successful Iceberg writes.

### Medallion Architecture

| Layer | Responsibility | Status |
|---|---|---|
| Bronze | Raw GitHub events and append-only CDC messages | Implemented |
| Silver | Normalized event data and current repository state | Implemented |
| Gold | Analytics-ready aggregations and dimensional models | Planned |

---

## Technology Stack

| Category | Technologies |
|---|---|
| Languages | Python, SQL |
| Data Processing | Apache Spark 3.5.6, PySpark |
| Table Format | Apache Iceberg 1.9.2 |
| Object Storage | S3-compatible storage, MinIO |
| CDC | Debezium |
| Streaming Infrastructure | Redpanda |
| Operational Database | PostgreSQL 16 |
| Iceberg Catalog | PostgreSQL-backed JDBC Catalog |
| Storage Interface | Iceberg S3FileIO |
| Infrastructure | Docker Compose |
| Analytics | dbt (planned) |
| Visualization | Power BI (planned) |

---

## Implementation Progress

### 1. Infrastructure and Storage

- Deployed PostgreSQL, Redpanda, Debezium, and MinIO using Docker Compose.
- Configured a PostgreSQL-backed Iceberg JDBC catalog.
- Integrated Apache Spark 3.5.6 with Apache Iceberg 1.9.2.
- Configured S3-compatible storage access through Iceberg S3FileIO.
- Established local development infrastructure using Docker and WSL.

### 2. Historical Event Ingestion — Bronze and Silver

- Implemented GH Archive hourly data ingestion.
- Downloaded 24 compressed JSON archives.
- Implemented object existence checks to avoid unnecessary downloads.
- Inspected raw event structures and profiled schemas.
- Ingested 5.5 million+ events into Iceberg Bronze.
- Preserved original event JSON alongside extracted event fields.
- Built normalized Silver event data.
- Verified record counts and Iceberg read-back integrity.

**Initial dataset metrics**

| Metric | Result |
|---|---:|
| Archive date | 2024-01-15 |
| Hourly files | 24 |
| Total events | 5,528,301 |
| Observed event types | 15 |
| Bronze records | 5,528,301 |
| Silver records | 5,528,301 |

### 3. Repository CDC — End-to-End Implementation

Implemented a PostgreSQL-to-Iceberg CDC pipeline using Debezium, Redpanda, Spark Structured Streaming, and Apache Iceberg.

**Source database**
- Seeded PostgreSQL with 852,777 distinct repository records.
- Configured Debezium PostgreSQL CDC.
- Verified insert, update, and delete event capture.

**Bronze CDC ingestion**
- Captured Debezium messages from Redpanda.
- Implemented append-only Iceberg ingestion.
- Preserved raw message keys, values, Kafka metadata, and tombstones.
- Configured checkpoint-based streaming recovery.
- Ingested the complete initial CDC history and subsequent test events.

**Incremental Silver processing**
- Implemented initial repository state construction from Bronze history.
- Implemented incremental processing from new Bronze Iceberg snapshots.
- Applied inserts, updates, and deletes using Iceberg `MERGE`.
- Used Kafka offsets to prevent stale events from overwriting newer repository state.
- Retained deleted repositories as soft-delete records.
- Implemented persistent progress tracking in PostgreSQL.
- Added a PostgreSQL advisory lock to prevent concurrent Silver writers.
- Implemented idempotent merge conditions for replay handling.

**Processing reliability**
- Validated contiguous Kafka offset ranges.
- Advanced the processing checkpoint only after successful Iceberg writes.
- Designed replay handling for failures between the Iceberg commit and PostgreSQL checkpoint update.
- Added source-to-Silver reconciliation.

### 4. CDC Validation Results

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

The incremental test successfully processed new CDC messages, applied the latest repository change, and advanced the persistent checkpoint.

**Source-to-Silver reconciliation: PASSED**

### 5. Current Dataset State

| Table | Records | Description |
|---|---:|---|
| `nexus.bronze.events` | 5,528,301 | Raw historical GitHub events |
| `nexus.silver.events` | 5,528,301 | Normalized GitHub events |
| `nexus.bronze.repo_cdc` | 852,818 | Raw repository CDC messages |
| `nexus.silver.repos_v2` | 852,779 | Current repository state and soft deletes |

The repository CDC Bronze count includes the original history and eight subsequent test messages. The Silver table contains active repository state and retained soft-delete records.

---

## Engineering Considerations

### Medallion Architecture
Separates raw ingestion, normalized data, and analytical transformations into independently managed layers.

### Incremental Processing
Processes new Bronze Iceberg snapshots rather than repeatedly scanning the entire CDC history.

### Idempotency
Uses Kafka offsets and conditional Iceberg merges to prevent older events from overwriting newer repository state.

### Checkpointing
Maintains processing progress in PostgreSQL independently of Spark Structured Streaming checkpoints.

### Soft Deletes
Preserves delete history in Silver without allowing replayed older events to resurrect deleted repositories.

### Data Reconciliation
Compares active Silver repository state against PostgreSQL and validates missing records, extra records, and attribute mismatches.

### Separation of Concerns
Decouples continuous Kafka ingestion from incremental repository state application, allowing ingestion and processing to operate independently.

---

## Repository Structure

```text
.
├── archive/
│   └── legacy-cdc/                  # Reserved for archived CDC experiments
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
│   │
│   ├── ingestion/
│   │   ├── create_repo_cdc_bronze.py
│   │   └── repos_bronze_stream.py
│   │
│   ├── processing/
│   │   ├── create_repos_silver_v2.py
│   │   ├── repos_silver_batch.py
│   │   └── repos_silver_incremental.py
│   │
│   ├── validation/
│   │   └── repos_cdc_reconciliation.py
│   │
│   ├── maintenance/
│   │   └── __init__.py
│   │
│   ├── repos_cdc_stream.py           # Legacy CDC implementation
│   ├── silver.py                     # Legacy Silver implementation
│   ├── seed_repos.py
│   ├── test_cdc_stream.py
│   ├── test_cdc_historical.py
│   ├── inspect_duplicates.py
│   ├── test_iceberg_storage.py
│   └── verify_bronze.py
│
├── sql/
│   └── cdc/
│       ├── 01_create_repo_cdc_bronze.sql
│       └── 02_create_repos_silver_v2.sql
│
├── benchmark/                         # Reserved for future benchmarks
├── cdc/                               # Reserved
├── dbt/                               # Reserved for future transformations
├── terraform/                         # Reserved for future infrastructure
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

The root-level scripts `repos_cdc_stream.py` and `silver.py` represent earlier implementations and are not part of the active incremental CDC execution path. Other root-level scripts include seeding, testing, inspection, and validation utilities.

---

## Running Locally

### Prerequisites

- Docker and Docker Compose
- Python 3.12
- WSL 2 / Linux environment
- Sufficient local storage for the historical dataset and Iceberg tables

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
```

### 3. Configure environment variables

Create a local `.env` file in the project root.

Configure the required PostgreSQL credentials, MinIO endpoint and credentials, S3 bucket, and AWS region.

**Do not commit `.env` or any real credentials to GitHub.**

### 4. Run historical GH Archive ingestion

Download the hourly archives:

```bash
python -m ingest.download
```

Run Bronze ingestion:

```bash
python -m ingest.bronze
```

Verify the Bronze table:

```bash
python -m spark.verify_bronze
```

### 5. Start the repository CDC infrastructure

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

### 8. Validate repository state

```bash
python -m spark.validation.repos_cdc_reconciliation
```

This compares active Silver repository state against the PostgreSQL source.

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
- [ ] Complete restart and replay recovery testing.
- [ ] Build Gold analytical models.
- [ ] Add analytical dashboards using Power BI.
- [ ] Explore AWS deployment and infrastructure automation.

---

## Future Gold Analytics

The planned Gold layer will transform historical GitHub activity and repository state into analytics-ready datasets.

Potential analytical models include:

- Daily GitHub activity and event trends.
- Event type distribution and activity patterns.
- Repository activity and contribution metrics.
- Developer activity and contributor trends.
- Repository-level engagement analytics.

Gold models will be designed around reliable source attributes and observed activity. Synthetic repository attributes will not be presented as genuine GitHub metrics.

---

## Project Status

**Core batch ingestion and repository CDC pipelines implemented.**

Historical GitHub events are stored in Iceberg Bronze and Silver. Repository changes are captured through Debezium and Redpanda, ingested into append-only Bronze, and incrementally applied to Silver using Spark and Iceberg.

The initial repository state and incremental CDC changes have passed source-to-Silver reconciliation.

Restart/replay recovery testing, Gold analytics, and dashboard development remain future milestones.