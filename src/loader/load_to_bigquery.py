"""
Loads the two gold-layer Parquet tables from GCS into BigQuery, overwriting each table fully on every run (WRITE_TRUNCATE) 
matching how the gold layer itself is fully recomputed each pipeline run.

Tables created/replaced:
    <dataset>.scored_places
    <dataset>.unqualified_places

Usage:
    python load_to_bigquery.py \\
        --project <gcp-project-id> \\
        --dataset <dataset-name> \\
        --gold-path gs://<bucket-name>/gold
"""

import argparse
import logging
import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
SERVICE_ACCOUNT_KEY_PATH = os.getenv("BIGQUERY_SERVICE_ACCOUNT_KEY_PATH")

TABLES = ["scored_places", "unqualified_places"]


def get_client(project_id: str) -> bigquery.Client:
    if SERVICE_ACCOUNT_KEY_PATH:
        return bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_PATH, project=project_id)
    # Falls back to Application Default Credentials if no key file is set
    return bigquery.Client(project=project_id)


def ensure_dataset(client: bigquery.Client, project_id: str, dataset_id: str, location: str) -> None:
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset %s already exists", dataset_ref)
    except NotFound:
        logger.info("Creating dataset %s in %s", dataset_ref, location)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)


def load_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
    source_uri: str,
) -> None:
    table_ref = f"{project_id}.{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logger.info("Loading %s -> %s (WRITE_TRUNCATE)", source_uri, table_ref)
    load_job = client.load_table_from_uri(source_uri, table_ref, job_config=job_config)
    load_job.result()  # blocks until the job finishes, raises on failure

    table = client.get_table(table_ref)
    logger.info("Loaded %d rows into %s", table.num_rows, table_ref)


def main():
    parser = argparse.ArgumentParser(description="Load gold Parquet tables into BigQuery")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="bangkok_hidden_gems", help="BigQuery dataset name")
    parser.add_argument("--gold-path", required=True, help="Base gold GCS path, e.g. gs://bucket/gold")
    parser.add_argument("--location", default="asia-southeast1", help="BigQuery dataset location")
    args = parser.parse_args()

    client = get_client(args.project)
    ensure_dataset(client, args.project, args.dataset, args.location)

    gold_path = args.gold_path.rstrip("/")
    for table_name in TABLES:
        source_uri = f"{gold_path}/{table_name}/*.parquet"
        load_table(client, args.project, args.dataset, table_name, source_uri)

    logger.info("Done")


if __name__ == "__main__":
    main()