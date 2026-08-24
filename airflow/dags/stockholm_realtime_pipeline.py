from datetime import datetime, timedelta
import os
import subprocess
import time

import requests
from dotenv import load_dotenv

from airflow.sdk import DAG, task


PROJECT_ROOT = "/opt/airflow/project"
ENV_FILE = f"{PROJECT_ROOT}/.env"

load_dotenv(ENV_FILE)


with DAG(
    dag_id="stockholm_realtime_pipeline",
    description=(
        "Orchestrates Stockholm GTFS-Realtime "
        "ingestion, transformation, and validation"
    ),
    start_date=datetime(2026, 8, 24),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
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

        load_dotenv(ENV_FILE)

        databricks_host = os.getenv("DATABRICKS_HOST")
        databricks_token = os.getenv("DATABRICKS_TOKEN")
        databricks_job_id = os.getenv("DATABRICKS_JOB_ID")

        if not databricks_host:
            raise ValueError(
                "DATABRICKS_HOST is missing."
            )

        if not databricks_token:
            raise ValueError(
                "DATABRICKS_TOKEN is missing."
            )

        if not databricks_job_id:
            raise ValueError(
                "DATABRICKS_JOB_ID is missing."
            )

        headers = {
            "Authorization": (
                f"Bearer {databricks_token}"
            ),
            "Content-Type": "application/json",
        }

        # --------------------------------------------------
        # Trigger Databricks Job
        # --------------------------------------------------

        print(
            "Triggering Databricks realtime "
            "Silver job..."
        )

        run_response = requests.post(
            (
                f"{databricks_host}"
                "/api/2.1/jobs/run-now"
            ),
            headers=headers,
            json={
                "job_id": int(
                    databricks_job_id
                )
            },
            timeout=30,
        )

        run_response.raise_for_status()

        run_id = run_response.json()["run_id"]

        print(
            f"Databricks run started: {run_id}"
        )

        # --------------------------------------------------
        # Wait for Databricks Job
        # --------------------------------------------------

        while True:

            status_response = requests.get(
                (
                    f"{databricks_host}"
                    "/api/2.1/jobs/runs/get"
                ),
                headers=headers,
                params={
                    "run_id": run_id
                },
                timeout=30,
            )

            status_response.raise_for_status()

            run_data = status_response.json()
            state = run_data["state"]

            lifecycle_state = state.get(
                "life_cycle_state"
            )

            result_state = state.get(
                "result_state"
            )

            print(
                "Databricks run status: "
                f"{lifecycle_state}"
            )

            if lifecycle_state in {
                "TERMINATED",
                "SKIPPED",
                "INTERNAL_ERROR",
            }:
                break

            time.sleep(10)

        # --------------------------------------------------
        # Validate Databricks Job result
        # --------------------------------------------------

        if result_state != "SUCCESS":
            raise RuntimeError(
                "Databricks realtime Silver job "
                f"failed. "
                f"Run ID: {run_id}, "
                f"Lifecycle state: "
                f"{lifecycle_state}, "
                f"Result state: {result_state}"
            )

        print(
            "Databricks realtime Silver "
            "job completed successfully."
        )

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def validate_realtime_silver():
        subprocess.run(
            [
                "python",
                (
                    f"{PROJECT_ROOT}/src/validation/"
                    "validate_realtime_silver.py"
                ),
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )

    ingest = ingest_realtime()
    transform = transform_realtime_silver()
    validate = validate_realtime_silver()

    ingest >> transform >> validate