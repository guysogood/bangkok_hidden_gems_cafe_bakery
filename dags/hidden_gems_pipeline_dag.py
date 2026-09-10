"""
Orchestrates the silver -> gold -> BigQuery portion of the Bangkok
hidden-gems pipeline.

This DAG demonstrates orchestration structure - bronze_to_silver
intentionally reads from a fixed, already-ingested bronze snapshot
rather than triggering new ingestion each run.

Task graph:

    bronze_to_silver >> silver_to_gold >> load_to_bigquery

Idempotency: safe to rerun or retry any task. 
- bronze_to_silver merges with existing silver data by place_id
- silver_to_gold and load_to_bigquery fully overwrite their outputs each run.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

import os
from dotenv import load_dotenv

# --- Configuration  ---
load_dotenv(override=True)
PROJECT_ROOT = os.getenv("PROJECT_ROOT")
PYTHON_BIN = f"{PROJECT_ROOT}/.venv/bin/python"

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")
GCP_PROJECT = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BIGQUERY_DATASET")

# Fixed bronze snapshot this DAG processes 
FIXED_BRONZE_DATE = "2026-08-27"  # update to match actual bronze snapshot

SILVER_PATH = f"gs://{GCS_BUCKET}/silver"
GOLD_PATH = f"gs://{GCS_BUCKET}/gold"
BRONZE_INPUT_PATH = f"gs://{GCS_BUCKET}/bronze/{FIXED_BRONZE_DATE}/bronze_places_raw.json"

default_args = {
    "owner": "potcharaphon",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hidden_gems_pipeline",
    description="Bangkok cafe/bakery hidden-gem scoring: silver -> gold -> BigQuery",
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["data-engineering", "gcp"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"{PYTHON_BIN} {PROJECT_ROOT}/src/transform/bronze_to_silver.py "
            f"--input {BRONZE_INPUT_PATH} "
            f"--output {SILVER_PATH}"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"{PYTHON_BIN} {PROJECT_ROOT}/src/transform/silver_to_gold.py "
            f"--input {SILVER_PATH} "
            f"--output {GOLD_PATH}"
        ),
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=(
            f"{PYTHON_BIN} {PROJECT_ROOT}/src/loader/load_to_bigquery.py "
            f"--project {GCP_PROJECT} "
            f"--dataset {BQ_DATASET} "
            f"--gold-path {GOLD_PATH}"
        ),
    )

    bronze_to_silver >> silver_to_gold >> load_to_bigquery