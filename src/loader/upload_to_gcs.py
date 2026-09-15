"""
Uploads a local bronze-layer file to Cloud Storage under a dated path,
so each ingestion run is preserved as its own immutable snapshot
instead of overwriting the previous one.

Usage:
    python upload_to_gcs.py data/raw/bronze_places_raw.json
    python upload_to_gcs.py data/raw/bronze_places_raw.json --run-date 2026-08-24
"""

import argparse
import logging
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage
from google.cloud.exceptions import NotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
SERVICE_ACCOUNT_KEY_PATH = os.getenv("GCS_SERVICE_ACCOUNT_KEY_PATH")


def get_client() -> storage.Client:
    if SERVICE_ACCOUNT_KEY_PATH:
        return storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_PATH)
    # Falls back to Application Default Credentials if no key file is set
    return storage.Client()


def upload_file(local_path: Path, bucket_name: str, run_date: str) -> str:
    """
    Upload local_path to gs://bucket_name/bronze/<run_date>/<filename>.
    Returns the gs:// URI of the uploaded blob.
    """
    if not bucket_name:
        raise ValueError("GCS_BUCKET_NAME is not set")
    if not local_path.exists():
        raise FileNotFoundError(f"No such file: {local_path}")

    client = get_client()

    try:
        bucket = client.get_bucket(bucket_name)
    except NotFound as exc:
        raise ValueError(
            f"Bucket '{bucket_name}' not found or not accessible with current credentials"
        ) from exc

    blob_path = f"bronze/{run_date}/{local_path.name}"
    blob = bucket.blob(blob_path)

    logger.info("Uploading %s to gs://%s/%s ...", local_path, bucket_name, blob_path)
    blob.upload_from_filename(str(local_path), content_type="application/json")

    gs_uri = f"gs://{bucket_name}/{blob_path}"
    logger.info("Upload complete: %s (%.1f KB)", gs_uri, blob.size / 1024)
    return gs_uri


def main():
    parser = argparse.ArgumentParser(description="Upload a bronze JSON file to GCS")
    parser.add_argument("local_path", type=Path, help="Path to the local JSON file")
    parser.add_argument(
        "--run-date",
        default=date.today().isoformat(),
        help="Date folder to upload under (default: today, YYYY-MM-DD)",
    )
    args = parser.parse_args()

    upload_file(args.local_path, BUCKET_NAME, args.run_date)


if __name__ == "__main__":
    main()