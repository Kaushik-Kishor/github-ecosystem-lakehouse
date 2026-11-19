from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "nexus",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nexus_cdc_pipeline",
    description="Apply Bronze repository CDC changes to Silver and reconcile state",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["nexus", "cdc", "spark"],
) as dag:

    process_incremental = BashOperator(
        task_id="process_incremental_cdc",
        bash_command=(
            "export PYTHONPATH=/opt/nexus:$PYTHONPATH && "
            "cd /opt/nexus && "
            "python spark/processing/repos_silver_incremental.py"
        ),
    )

    reconcile = BashOperator(
        task_id="reconcile_cdc_state",
        bash_command=(
            "export PYTHONPATH=/opt/nexus:$PYTHONPATH && "
            "cd /opt/nexus && "
            "python spark/validation/repos_cdc_reconciliation.py"
        ),
    )

    process_incremental >> reconcile
