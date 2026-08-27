"""
Scores each place's "hidden gem" potential relative to its khet (district), and splits the result into two gold tables:

    gold_scored_places       - places that qualify for scoring
    gold_unqualified_places  - places that don't, with a reason why

Scoring rule (locked):
  1. Only places with user_rating_count >= MIN_REVIEWS_FOR_SCORING are candidates for scoring.
  2. A khet must have at least MIN_PLACES_PER_KHET such candidates before its median rating is trusted.
  3. For places in a trusted khet: gem_score = rating - khet_median_rating 
     (a continuous score, not a binary flag - higher means a bigger positive gap from what's typical in that khet).
  4. Everything else lands in gold_unqualified_places with a reason:
     - "low_review_count"      : user_rating_count < MIN_REVIEWS_FOR_SCORING
     - "khet_sample_too_small" : khet has fewer than MIN_PLACES_PER_KHET qualifying places

category is derived from place_types (cafe / bakery / cafe and bakery).
Since ingestion already restricts included_types=["cafe", "bakery"], every place should match one or both.

Usage:
    python silver_to_gold.py \\
        --input gs://bkk_hidden_gems_cafe_bakery/silver \\
        --output gs://bkk_hidden_gems_cafe_bakery/gold
"""

import argparse
import logging
import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
SERVICE_ACCOUNT_KEY_PATH = os.getenv("GCS_SERVICE_ACCOUNT_KEY_PATH")
GCS_CONNECTOR_JAR_PATH = os.getenv("GCS_CONNECTOR_JAR_PATH")

MIN_REVIEWS_FOR_SCORING = 5
MIN_PLACES_PER_KHET = 5

GOLD_SCORED_COLUMNS = [
    "place_id", "name", "category", "khet", "province",
    "latitude", "longitude", "rating", "user_rating_count", "price_level",
    "khet_median_rating", "gem_score",
]

GOLD_UNQUALIFIED_COLUMNS = [
    "place_id", "name", "category", "khet", "province",
    "latitude", "longitude", "rating", "user_rating_count", "price_level",
    "reason",
]


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_to_gold")
        .config("spark.jars", GCS_CONNECTOR_JAR_PATH)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", SERVICE_ACCOUNT_KEY_PATH)
        .getOrCreate()
    )


def add_category_column(df: DataFrame) -> DataFrame:
    """
    Derive a simplified category from place_types.
    Expects the ingestion search to already be restricted to
    included_types=["cafe", "bakery"], so "other" should be rare/never -
    it's a defensive fallback, logged so it's visible if it ever happens.
    """
    has_cafe = array_contains(col("place_types"), "cafe")
    has_bakery = array_contains(col("place_types"), "bakery")

    df = df.withColumn(
        "category",
        when(has_cafe & has_bakery, lit("cafe and bakery"))
        .when(has_cafe, lit("cafe"))
        .when(has_bakery, lit("bakery"))
        .otherwise(lit("other")),
    )

    other_count = df.filter(col("category") == "other").count()
    if other_count > 0:
        logger.warning(
            "%d places matched neither 'cafe' nor 'bakery' in place_types "
            "- unexpected given the ingestion filter, worth investigating",
            other_count,
        )

    return df


def score_places(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Returns (gold_scored_places, gold_unqualified_places).
    """
    # Enough reviews to be a scoring candidate at all
    low_review = df.filter(col("user_rating_count") < MIN_REVIEWS_FOR_SCORING) \
        .withColumn("reason", lit("low_review_count"))

    candidates = df.filter(col("user_rating_count") >= MIN_REVIEWS_FOR_SCORING)

    # Khet needs enough qualifying places before its median is trusted
    khet_counts = candidates.groupBy("khet").agg(count("*").alias("khet_candidate_count"))
    trusted_khets = khet_counts.filter(col("khet_candidate_count") >= MIN_PLACES_PER_KHET)
    small_khets = khet_counts.filter(col("khet_candidate_count") < MIN_PLACES_PER_KHET)

    khet_sample_too_small = candidates.join(
        small_khets.select("khet"), on="khet", how="inner"
    ).withColumn("reason", lit("khet_sample_too_small"))

    scoreable = candidates.join(trusted_khets.select("khet"), on="khet", how="inner")

    # Median rating per trusted khet, computed only from the qualifying population
    khet_medians = scoreable.groupBy("khet").agg(
        expr("percentile_approx(rating, 0.5)").alias("khet_median_rating")
    )

    scored = scoreable.join(khet_medians, on="khet", how="left") \
        .withColumn("gem_score", round((col("rating") - col("khet_median_rating")), 2))

    gold_scored = scored.select(*GOLD_SCORED_COLUMNS)

    gold_unqualified = low_review.unionByName(khet_sample_too_small).select(*GOLD_UNQUALIFIED_COLUMNS)

    return gold_scored, gold_unqualified


def main():
    parser = argparse.ArgumentParser(description="Score places and split into gold tables")
    parser.add_argument("--input", required=True, help="Path to silver Parquet (local or gs://)")
    parser.add_argument("--output", required=True, help="Base gold output path (local or gs://)")
    args = parser.parse_args()

    spark = build_spark_session()

    logger.info("Reading silver data from %s", args.input)
    silver_df = spark.read.parquet(args.input)
    logger.info("Loaded %d silver rows", silver_df.count())

    categorized_df = add_category_column(silver_df)
    gold_scored, gold_unqualified = score_places(categorized_df)

    scored_count = gold_scored.count()
    unqualified_count = gold_unqualified.count()
    logger.info(
        "Split into %d scored places and %d unqualified places",
        scored_count, unqualified_count,
    )

    scored_path = f"{args.output.rstrip('/')}/scored_places"
    unqualified_path = f"{args.output.rstrip('/')}/unqualified_places"

    logger.info("Writing scored places to %s", scored_path)
    gold_scored.write.mode("overwrite").parquet(scored_path)

    logger.info("Writing unqualified places to %s", unqualified_path)
    gold_unqualified.write.mode("overwrite").parquet(unqualified_path)

    logger.info("Done")


if __name__ == "__main__":
    main()