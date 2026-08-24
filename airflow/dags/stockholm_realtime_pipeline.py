from datetime import datetime, timedelta
import subprocess

from airflow.sdk import DAG, task


PROJECT_ROOT = "/opt/airflow/project"


with DAG(
    dag_id="stockholm_realtime_pipeline",
    description="Orchestrates Stockholm GTFS-Realtime ingestion, transformation, and validation",
    start_date=datetime(2026, 8, 24),
    schedule=None,
    catchup=False,
    tags=["stockholm", "gtfs", "realtime"],
) as dag:

    @task(
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    def ingest_realtime():
        subprocess.run(
            [
                "python",
                f"{PROJECT_ROOT}/src/ingestion/ingest_realtime.py",
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def transform_realtime_silver():
        subprocess.run(
            [
                "python",
                f"{PROJECT_ROOT}/src/realtime.py",
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def validate_realtime_silver():
        subprocess.run(
            [
                "python",
                f"{PROJECT_ROOT}/src/validation/validate_realtime_silver.py",
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )

    ingest = ingest_realtime()
    transform = transform_realtime_silver()
    validate = validate_realtime_silver()

    ingest >> transform >> validate