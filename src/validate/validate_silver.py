"""
Quality gate for the silver layer. Run after bronze_to_silver and before silver_to_gold.
Exits with code 1 if any check fails so the Airflow task is marked as failed
and downstream tasks are blocked.

Checks:
  1. row_count          — silver is not empty
  2. required_columns   — all expected columns are present
  3. no_null_place_ids  — place_id is the pipeline's primary key
  4. no_duplicate_place_ids — merge logic should have deduplicated
  5. rating_range       — rating in [1.0, 5.0] (Google Places scale)
  6. user_rating_count  — all counts >= 1 (filter in transform should guarantee this)
  7. province_is_bangkok — Bangkok-only filter should have removed everything else

Usage:
    python validate_silver.py --path gs://<bucket>/silver
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
SERVICE_ACCOUNT_KEY_PATH = os.getenv("GCS_SERVICE_ACCOUNT_KEY_PATH")
GCS_CONNECTOR_JAR_PATH = os.getenv("GCS_CONNECTOR_JAR_PATH")

REQUIRED_COLUMNS = {
    "place_id", "name", "place_types", "rating", "user_rating_count",
    "price_level", "latitude", "longitude", "khet", "province",
    "ingestion_date", "first_seen_date",
}

VALID_PROVINCES = {"bangkok", "krung thep maha nakhon"}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("validate_silver")
        .config("spark.jars", GCS_CONNECTOR_JAR_PATH)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", SERVICE_ACCOUNT_KEY_PATH)
        .getOrCreate()
    )


def run_checks(spark: SparkSession, silver_path: str) -> list[str]:
    """Return a list of failure messages. Empty list means all checks passed."""
    logger.info("Reading silver layer from %s", silver_path)
    df = spark.read.parquet(silver_path)
    failures = []

    # 1. Row count
    count = df.count()
    if count == 0:
        failures.append("silver layer is empty (0 rows) — transform may have failed silently")
        logger.error("FAIL  row_count: 0 rows")
        return failures  # no point running further checks on an empty DataFrame
    logger.info("PASS  row_count: %d rows", count)

    # 2. Required columns
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        failures.append(f"missing required columns: {sorted(missing)}")
        logger.error("FAIL  required_columns: %s missing", sorted(missing))
        return failures  # column-dependent checks below would crash
    logger.info("PASS  required_columns: all %d present", len(REQUIRED_COLUMNS))

    # 3. Null place_ids
    null_ids = df.filter(col("place_id").isNull()).count()
    if null_ids > 0:
        failures.append(f"{null_ids} rows have null place_id")
        logger.error("FAIL  no_null_place_ids: %d nulls found", null_ids)
    else:
        logger.info("PASS  no_null_place_ids")

    # 4. Duplicate place_ids
    distinct_ids = df.select("place_id").distinct().count()
    if distinct_ids != count:
        failures.append(
            f"duplicate place_ids: {count} rows but only {distinct_ids} distinct ids"
        )
        logger.error("FAIL  no_duplicate_place_ids: %d duplicates", count - distinct_ids)
    else:
        logger.info("PASS  no_duplicate_place_ids: %d unique", distinct_ids)

    # 5. Rating range [1.0, 5.0]
    out_of_range = df.filter((col("rating") < 1.0) | (col("rating") > 5.0)).count()
    if out_of_range > 0:
        failures.append(f"{out_of_range} rows have rating outside [1.0, 5.0]")
        logger.error("FAIL  rating_range: %d rows out of range", out_of_range)
    else:
        logger.info("PASS  rating_range: all in [1.0, 5.0]")

    # 6. user_rating_count >= 1
    bad_counts = df.filter(col("user_rating_count") < 1).count()
    if bad_counts > 0:
        failures.append(f"{bad_counts} rows have user_rating_count < 1")
        logger.error("FAIL  user_rating_count: %d rows < 1", bad_counts)
    else:
        logger.info("PASS  user_rating_count: all >= 1")

    # 7. Province is Bangkok
    non_bkk = df.filter(~lower(col("province")).isin(VALID_PROVINCES)).count()
    if non_bkk > 0:
        failures.append(
            f"{non_bkk} rows have a non-Bangkok province — Bangkok filter may have failed"
        )
        logger.error("FAIL  province_is_bangkok: %d non-Bangkok rows", non_bkk)
    else:
        logger.info("PASS  province_is_bangkok: all rows are Bangkok")

    return failures


def main():
    parser = argparse.ArgumentParser(description="Validate the silver layer")
    parser.add_argument("--path", required=True, help="Path to silver Parquet (gs:// or local)")
    args = parser.parse_args()

    spark = build_spark_session()
    failures = run_checks(spark, args.path)

    if failures:
        logger.error("Silver validation FAILED — %d issue(s) found:", len(failures))
        for i, msg in enumerate(failures, 1):
            logger.error("  %d. %s", i, msg)
        sys.exit(1)

    logger.info("Silver validation PASSED — all checks OK")


if __name__ == "__main__":
    main()
