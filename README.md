
# GitHub Ecosystem Lakehouse

An end-to-end data engineering project that transforms GitHub activity data into an analytics-ready lakehouse using Apache Spark, Apache Iceberg, Kafka-compatible streaming infrastructure, PostgreSQL, and dbt.

## Architecture

The project follows a Medallion Architecture and separates batch ingestion from operational CDC.

### Pipeline A — Batch ingestion

```text
GH Archive (.json.gz)
        |
        v
S3-compatible Object Storage (Silo)
        |
        v
Apache Spark
        |
        v
Apache Iceberg — Bronze
        |
        v
Silver (planned)
        |
        v
Gold / dbt (planned)
```

### Pipeline B — Change Data Capture

```text
Repository Simulator
        |
        v
PostgreSQL (app.repos)
        |
        v
Debezium
        |
        v
Redpanda
        |
        v
Spark Structured Streaming
        |
        v
Iceberg Silver (planned)
```

The two pipelines converge in the analytics layer.

## Technology Stack

- **Processing:** Python, Apache Spark
- **Table format:** Apache Iceberg
- **Object storage:** Silo (S3-compatible)
- **CDC:** Debezium, Redpanda
- **Database:** PostgreSQL
- **Transformation:** dbt (planned)
- **Infrastructure:** Docker Compose

## Current Progress

### Completed

- Local infrastructure deployed using Docker Compose.
- GH Archive hourly ingestion into S3-compatible object storage.
- Idempotent archive downloader with existing-object checks.
- Raw event inspection and schema profiling.
- Apache Spark 3.5.6 and Apache Iceberg 1.9.2 integration.
- PostgreSQL-backed Iceberg JDBC catalog.
- Bronze ingestion of 24 hourly GH Archive files.
- Preservation of original event JSON alongside extracted common fields.
- Iceberg write and read-back verification.

### Bronze Layer — Initial Dataset

| Metric | Result |
|---|---:|
| Archive date | 2024-01-15 |
| Hourly files | 24 |
| Total events | 5,528,301 |
| Event types | 15 |
| Stored records | 5,528,301 |

All 15 observed event types are retained in Bronze.

## Project Structure

```text
.
├── ingest/
│   ├── download.py
│   ├── inspect_raw.py
│   ├── profile_schema.py
│   └── bronze.py
├── spark/
│   ├── iceberg_session.py
│   ├── test_iceberg_storage.py
│   └── verify_bronze.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Running Locally

1. Start the infrastructure:

```bash
docker compose up -d
```

2. Create and activate a Python virtual environment.

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Configure credentials and storage settings in `.env`.

5. Download GH Archive data:

```bash
python -m ingest.download
```

6. Run Bronze ingestion:

```bash
python -m ingest.bronze
```

7. Verify the Bronze table:

```bash
python -m spark.verify_bronze
```

## Roadmap

- [ ] Complete Bronze data-quality validation.
- [ ] Build normalized Silver event tables.
- [ ] Seed the operational repository database.
- [ ] Implement PostgreSQL CDC using Debezium and Redpanda.
- [ ] Process CDC events using Spark Structured Streaming.
- [ ] Build Gold analytical models using dbt.
- [ ] Add dashboards and analytical insights.
- [ ] Explore AWS deployment.

---

**Note:** This is an actively developed portfolio project. Components marked as planned are not yet implemented end-to-end.
