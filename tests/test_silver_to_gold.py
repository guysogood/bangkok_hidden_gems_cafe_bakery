"""
Tests for silver_to_gold transform.

Creates silver DataFrames directly (no GCS or Spark session with GCS config needed).
"""

from datetime import date

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.transform.silver_to_gold import MIN_PLACES_PER_KHET, MIN_REVIEWS_FOR_SCORING, add_category_column, score_places

SILVER_SCHEMA = StructType([
    StructField("place_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("place_types", ArrayType(StringType()), True),
    StructField("rating", DoubleType(), True),
    StructField("user_rating_count", IntegerType(), True),
    StructField("price_level", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("khet", StringType(), True),
    StructField("province", StringType(), True),
    StructField("ingestion_date", DateType(), True),
    StructField("first_seen_date", DateType(), True),
])

_TODAY = date(2026, 8, 27)


def silver_row(
    place_id="p001",
    name="Test Cafe",
    place_types=None,
    rating=4.5,
    user_rating_count=50,
    price_level=2,
    khet="Khet Watthana",
    province="Bangkok",
):
    return {
        "place_id": place_id,
        "name": name,
        "place_types": place_types if place_types is not None else ["cafe"],
        "rating": float(rating),
        "user_rating_count": user_rating_count,
        "price_level": price_level,
        "latitude": 13.73,
        "longitude": 100.57,
        "khet": khet,
        "province": province,
        "ingestion_date": _TODAY,
        "first_seen_date": _TODAY,
    }


def make_silver_df(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows], schema=SILVER_SCHEMA)


def trusted_khet_rows(khet="Khet Big", n=None, ratings=None):
    """Return n rows for a single khet, all with enough reviews to qualify."""
    n = n or MIN_PLACES_PER_KHET + 1  # one more than threshold
    ratings = ratings or [4.0 + i * 0.1 for i in range(n)]
    return [
        silver_row(place_id=f"{khet}_{i}", khet=khet, rating=r, user_rating_count=MIN_REVIEWS_FOR_SCORING + 5)
        for i, r in enumerate(ratings)
    ]


# ---------------------------------------------------------------------------
# add_category_column
# ---------------------------------------------------------------------------

def test_category_cafe(spark):
    df = make_silver_df(spark, [silver_row(place_types=["cafe", "food"])])
    assert add_category_column(df).first().category == "cafe"


def test_category_bakery(spark):
    df = make_silver_df(spark, [silver_row(place_types=["bakery", "food"])])
    assert add_category_column(df).first().category == "bakery"


def test_category_both(spark):
    df = make_silver_df(spark, [silver_row(place_types=["cafe", "bakery", "food"])])
    assert add_category_column(df).first().category == "cafe and bakery"


def test_category_other_fallback(spark):
    df = make_silver_df(spark, [silver_row(place_types=["food", "restaurant"])])
    assert add_category_column(df).first().category == "other"


# ---------------------------------------------------------------------------
# score_places
# ---------------------------------------------------------------------------

def test_low_review_count_goes_to_unqualified(spark):
    rows = trusted_khet_rows() + [silver_row(place_id="low_review", user_rating_count=MIN_REVIEWS_FOR_SCORING - 1)]
    df = add_category_column(make_silver_df(spark, rows))
    scored, unqualified = score_places(df)

    assert scored.filter(col("place_id") == "low_review").count() == 0
    low_row = unqualified.filter(col("place_id") == "low_review").first()
    assert low_row is not None
    assert low_row.reason == "low_review_count"


def test_small_khet_goes_to_unqualified(spark):
    # Khet with fewer than MIN_PLACES_PER_KHET qualifying places
    small_khet_rows = [
        silver_row(place_id=f"small_{i}", khet="Khet Small")
        for i in range(MIN_PLACES_PER_KHET - 1)
    ]
    df = add_category_column(make_silver_df(spark, small_khet_rows))
    scored, unqualified = score_places(df)

    assert scored.filter(col("khet") == "Khet Small").count() == 0
    assert unqualified.filter(col("khet") == "Khet Small").count() == MIN_PLACES_PER_KHET - 1
    assert unqualified.filter(col("khet") == "Khet Small").first().reason == "khet_sample_too_small"


def test_trusted_khet_gets_scored(spark):
    rows = trusted_khet_rows()
    df = add_category_column(make_silver_df(spark, rows))
    scored, unqualified = score_places(df)

    assert scored.count() == len(rows)
    assert unqualified.count() == 0


def test_gem_score_equals_rating_minus_khet_median(spark):
    rows = trusted_khet_rows()
    df = add_category_column(make_silver_df(spark, rows))
    scored, _ = score_places(df)

    for row in scored.collect():
        expected_gem_score = round(row.rating - row.khet_median_rating, 2)
        assert row.gem_score == expected_gem_score


def test_all_records_land_in_exactly_one_table(spark):
    rows = (
        trusted_khet_rows(khet="Khet Big")
        + [silver_row(place_id=f"small_{i}", khet="Khet Small") for i in range(MIN_PLACES_PER_KHET - 1)]
        + [silver_row(place_id="low_review", user_rating_count=MIN_REVIEWS_FOR_SCORING - 1)]
    )
    df = add_category_column(make_silver_df(spark, rows))
    scored, unqualified = score_places(df)

    assert scored.count() + unqualified.count() == len(rows)
