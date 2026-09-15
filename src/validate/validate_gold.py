"""
Quality gate for the gold layer. Run after silver_to_gold and before load_to_bigquery.
Exits with code 1 if any check fails so the Airflow task is marked as failed
and no corrupted data is loaded into BigQuery.

Checks on scored_places:
  1. row_count              — scored_places is not empty
  2. no_null_gem_score      — every scored place must have a score
  3. no_null_khet_median    — every scored place must have its khet's median
  4. gem_score_math         — gem_score == round(rating - khet_median_rating, 2)

Checks on unqualified_places:
  5. valid_reasons          — reason is one of the two known values
  6. no_null_reasons        — every unqualified place must have a reason

Cross-table:
  7. no_place_id_overlap    — a place_id must appear in exactly one table

Usage:
    python validate_gold.py --path gs://<bucket>/gold
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, round as spark_round

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
SERVICE_ACCOUNT_KEY_PATH = os.getenv("GCS_SERVICE_ACCOUNT_KEY_PATH")
GCS_CONNECTOR_JAR_PATH = os.getenv("GCS_CONNECTOR_JAR_PATH")

VALID_REASONS = {"low_review_count", "khet_sample_too_small"}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("validate_gold")
        .config("spark.jars", GCS_CONNECTOR_JAR_PATH)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", SERVICE_ACCOUNT_KEY_PATH)
        .getOrCreate()
    )


def run_checks(spark: SparkSession, gold_path: str) -> list[str]:
    """Return a list of failure messages. Empty list means all checks passed."""
    gold_path = gold_path.rstrip("/")
    scored_path = f"{gold_path}/scored_places"
    unqualified_path = f"{gold_path}/unqualified_places"

    logger.info("Reading scored_places from %s", scored_path)
    scored = spark.read.parquet(scored_path)

    logger.info("Reading unqualified_places from %s", unqualified_path)
    unqualified = spark.read.parquet(unqualified_path)

    failures = []

    # --- scored_places checks ---

    # 1. Row count
    scored_count = scored.count()
    if scored_count == 0:
        failures.append("scored_places is empty — no places qualified for scoring")
        logger.error("FAIL  scored_row_count: 0 rows")
    else:
        logger.info("PASS  scored_row_count: %d rows", scored_count)

    # 2. No null gem_score
    null_scores = scored.filter(col("gem_score").isNull()).count()
    if null_scores > 0:
        failures.append(f"{null_scores} rows in scored_places have null gem_score")
        logger.error("FAIL  no_null_gem_score: %d nulls", null_scores)
    else:
        logger.info("PASS  no_null_gem_score")

    # 3. No null khet_median_rating
    null_medians = scored.filter(col("khet_median_rating").isNull()).count()
    if null_medians > 0:
        failures.append(f"{null_medians} rows in scored_places have null khet_median_rating")
        logger.error("FAIL  no_null_khet_median: %d nulls", null_medians)
    else:
        logger.info("PASS  no_null_khet_median")

    # 4. gem_score math: gem_score == round(rating - khet_median_rating, 2)
    if null_scores == 0 and null_medians == 0:
        wrong = scored.filter(
            spark_abs(col("gem_score") - spark_round(col("rating") - col("khet_median_rating"), 2)) > 0.001
        ).count()
        if wrong > 0:
            failures.append(
                f"{wrong} rows have gem_score that doesn't equal round(rating - khet_median_rating, 2)"
            )
            logger.error("FAIL  gem_score_math: %d rows with wrong score", wrong)
        else:
            logger.info("PASS  gem_score_math: all scores match formula")

    # --- unqualified_places checks ---

    unqualified_count = unqualified.count()
    logger.info("unqualified_places: %d rows", unqualified_count)

    # 5. Valid reasons
    invalid_reasons = unqualified.filter(~col("reason").isin(list(VALID_REASONS))).count()
    if invalid_reasons > 0:
        failures.append(
            f"{invalid_reasons} rows in unqualified_places have an unrecognised reason "
            f"(expected one of {VALID_REASONS})"
        )
        logger.error("FAIL  valid_reasons: %d rows with unknown reason", invalid_reasons)
    else:
        logger.info("PASS  valid_reasons: all reasons are known")

    # 6. No null reasons
    null_reasons = unqualified.filter(col("reason").isNull()).count()
    if null_reasons > 0:
        failures.append(f"{null_reasons} rows in unqualified_places have null reason")
        logger.error("FAIL  no_null_reasons: %d nulls", null_reasons)
    else:
        logger.info("PASS  no_null_reasons")

    # 7. No place_id in both tables
    overlap = scored.select("place_id").intersect(unqualified.select("place_id")).count()
    if overlap > 0:
        failures.append(
            f"{overlap} place_ids appear in both scored_places and unqualified_places"
        )
        logger.error("FAIL  no_place_id_overlap: %d overlapping ids", overlap)
    else:
        logger.info("PASS  no_place_id_overlap")

    return failures


def main():
    parser = argparse.ArgumentParser(description="Validate the gold layer")
    parser.add_argument("--path", required=True, help="Base gold GCS path, e.g. gs://bucket/gold")
    args = parser.parse_args()

    spark = build_spark_session()
    failures = run_checks(spark, args.path)

    if failures:
        logger.error("Gold validation FAILED — %d issue(s) found:", len(failures))
        for i, msg in enumerate(failures, 1):
            logger.error("  %d. %s", i, msg)
        sys.exit(1)

    logger.info("Gold validation PASSED — all checks OK")


if __name__ == "__main__":
    main()
