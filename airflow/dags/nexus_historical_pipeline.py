from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "nexus",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nexus_historical_pipeline",
    description="Historical GH Archive ingestion and transformation",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["nexus", "historical", "spark"],
) as dag:

    download = BashOperator(
        task_id="download_gh_archive",
        bash_command="export PYTHONPATH=/opt/nexus:$PYTHONPATH && cd /opt/nexus && python ingest/download.py",
    )

    bronze = BashOperator(
        task_id="build_bronze",
        bash_command="export PYTHONPATH=/opt/nexus:$PYTHONPATH && cd /opt/nexus && python ingest/bronze.py",
    )

    silver = BashOperator(
        task_id="build_silver",
        bash_command="export PYTHONPATH=/opt/nexus:$PYTHONPATH && cd /opt/nexus && python spark/silver_batch.py",
    )

    gold = BashOperator(
        task_id="build_gold",
        bash_command="export PYTHONPATH=/opt/nexus:$PYTHONPATH && cd /opt/nexus && python spark/gold/build_gold.py",
    )

    validate = BashOperator(
        task_id="validate_gold",
        bash_command="export PYTHONPATH=/opt/nexus:$PYTHONPATH && cd /opt/nexus && python spark/gold/validate_gold.py",
    )

    download >> bronze >> silver >> gold >> validate
