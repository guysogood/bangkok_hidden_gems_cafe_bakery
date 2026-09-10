"""
PySpark job that turns raw bronze Places API JSON into the clean, flat silver schema:

    place_id, name, place_types, rating, user_rating_count,
    price_level, latitude, longitude, khet, province,
    ingestion_date, first_seen_date

Rules applied:
  - Rows missing rating, user_rating_count, or coordinates are dropped
    (nothing to score them on).
  - user_rating_count == 0 is dropped for the same reason.
  - khet is parsed from formattedAddress.split(',')[-3],
    province from [-2] with the postal code stripped out.
  - Any row where khet or province contains Thai script is dropped
    (per the locked schema decision - English-formatted addresses only).
  - If a silver table already exists at --output, this run merges into
    it: place_id rows are deduped keeping the newest ingestion_date's
    data, but first_seen_date is preserved as the earliest date that
    place_id has ever appeared across all runs.

Usage:
    python bronze_to_silver.py \\
        --input bronze_places_raw.json \\
        --output silver_places \\
        --run-date 2026-08-24
"""

import argparse
import logging
from datetime import date

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

THAI_SCRIPT_PATTERN = r"[\u0E00-\u0E7F]"

PRICE_LEVEL_MAP = {
    "PRICE_LEVEL_FREE": 0,
    "PRICE_LEVEL_INEXPENSIVE": 1,
    "PRICE_LEVEL_MODERATE": 2,
    "PRICE_LEVEL_EXPENSIVE": 3,
    "PRICE_LEVEL_VERY_EXPENSIVE": 4,
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze_to_silver")
        .config("spark.jars", GCS_CONNECTOR_JAR_PATH)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", SERVICE_ACCOUNT_KEY_PATH)
        .getOrCreate()
    )


def build_price_level_column() -> Column:
    """Map the priceLevel enum string to an int, NULL if unmapped/missing."""
    price_level_col = when(lit(False), lit(None))  # base case, always false, gives None type anchor
    for enum_str, value in PRICE_LEVEL_MAP.items():
        price_level_col = price_level_col.when(col("priceLevel") == enum_str, lit(value))
    return price_level_col.otherwise(lit(None).cast(IntegerType()))


def load_bronze(spark: SparkSession, input_path: str) -> DataFrame:
    logger.info("Reading bronze data from %s", input_path)
    df = spark.read.option("multiLine", "true").json(input_path)
    logger.info("Loaded %d raw records", df.count())
    return df


def flatten_and_clean(df: DataFrame, run_date: str) -> DataFrame:
    # Split formattedAddress into parts once, reused for khet/province
    address_parts = split(col("formattedAddress"), r",\s*")
    khet_raw = trim(element_at(address_parts, -3))
    province_with_postal = trim(element_at(address_parts, -2))
    # Strip trailing postal code digits (and any leftover whitespace) from province
    province_raw = trim(regexp_replace(province_with_postal, r"\d+", ""))

    if "types" not in df.columns:
        logger.warning(
            "'types' field not present in bronze data - place_types will be NULL. "
            "Add 'places.types' to the FIELD_MASK in places_api_client.py to populate it."
        )
        place_types_col = lit(None).cast(ArrayType(StringType()))
    else:
        place_types_col = col("types")

    flat = df.select(
        col("id").alias("place_id"),
        col("displayName.text").alias("name"),
        place_types_col.alias("place_types"),
        col("rating").cast(DoubleType()).alias("rating"),
        col("userRatingCount").cast(IntegerType()).alias("user_rating_count"),
        build_price_level_column().alias("price_level"),
        col("location.latitude").cast(DoubleType()).alias("latitude"),
        col("location.longitude").cast(DoubleType()).alias("longitude"),
        khet_raw.alias("khet"),
        province_raw.alias("province"),
        lit(run_date).cast(DateType()).alias("ingestion_date"),
    )

    before = flat.count()

    # Drop rows with no usable rating signal or coordinates
    flat = flat.filter(
        col("rating").isNotNull()
        & col("user_rating_count").isNotNull()
        & (col("user_rating_count") > 0)
        & col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & col("khet").isNotNull()
        & col("province").isNotNull()
    )
    after_null_filter = flat.count()
    logger.info(
        "Dropped %d rows missing rating/reviews/coordinates/address parts",
        before - after_null_filter,
    )

    # Drop rows where khet or province is Thai script, per locked schema decision
    flat = flat.filter(
        ~col("khet").rlike(THAI_SCRIPT_PATTERN)
        & ~col("province").rlike(THAI_SCRIPT_PATTERN)
    )
    after_thai_filter = flat.count()
    logger.info(
        "Dropped %d rows with Thai-script khet/province and ",
        after_null_filter - after_thai_filter,
    )

    # Keep record inside Bangkok only
    only_bkk = flat.filter(lower(col("province")).isin(["bangkok", "krung thep maha nakhon"]))
    only_bkk_count = only_bkk.count()
    logger.info("Dropped %d rows outside Bangkok", after_thai_filter - only_bkk_count)

    return only_bkk


def merge_with_existing(new_df: DataFrame, output_path: str, spark: SparkSession) -> DataFrame:
    """
    If a silver table already exists at output_path, merge new_df into it:
    keep the newest row per place_id, but preserve the earliest
    ingestion_date ever seen as first_seen_date.
    """
    try:
        existing_df = spark.read.parquet(output_path)
        logger.info("Found existing silver table with %d rows, merging", existing_df.count())
    except Exception:
        logger.info("No existing silver table found at %s, this is the first run", output_path)
        return new_df.withColumn("first_seen_date", col("ingestion_date"))

    # first_seen_date per place_id across existing history + this new batch
    existing_first_seen = existing_df.select("place_id", "first_seen_date")
    new_first_seen = new_df.select("place_id", col("ingestion_date").alias("first_seen_date"))
    all_first_seen = existing_first_seen.union(new_first_seen)
    min_first_seen = all_first_seen.groupBy("place_id").agg(
        min("first_seen_date").alias("first_seen_date")
    )

    # Newest data per place_id: prefer new_df's row if the place_id is in both
    existing_only = existing_df.join(
        new_df.select("place_id"), on="place_id", how="left_anti"
    ).drop("first_seen_date")
    combined_latest = existing_only.unionByName(new_df)

    merged = combined_latest.join(min_first_seen, on="place_id", how="left")
    return merged


def main():
    parser = argparse.ArgumentParser(description="Transform bronze Places data to silver schema")
    parser.add_argument("--input", required=True, help="Path to bronze JSON (local or gs://)")
    parser.add_argument("--output", required=True, help="Path to write silver Parquet (local or gs://)")
    parser.add_argument("--run-date", default=date.today().isoformat(), help="Ingestion date for this batch")
    args = parser.parse_args()

    spark = build_spark_session()

    raw_df = load_bronze(spark, args.input)
    clean_df = flatten_and_clean(raw_df, args.run_date)
    final_df = merge_with_existing(clean_df, args.output, spark)

    final_df = final_df.select(
        "place_id",
        "name",
        "place_types",
        "rating",
        "user_rating_count",
        "price_level",
        "latitude",
        "longitude",
        "khet",
        "province",
        "ingestion_date",
        "first_seen_date",
    )

    logger.info("Writing %d rows to %s", final_df.count(), args.output)
    final_df.write.mode("overwrite").parquet(args.output)
    logger.info("Done")


if __name__ == "__main__":
    main()