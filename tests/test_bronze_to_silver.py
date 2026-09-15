"""
Tests for bronze_to_silver transform.

Uses spark.read.json from an in-memory RDD to create test DataFrames that
mirror the structure produced by the real Places API ingestion.
No GCS credentials or network access required.
"""

import json

import pytest

from src.transform.bronze_to_silver import flatten_and_clean, merge_with_existing

RUN_DATE = "2026-08-27"

# Minimal valid bronze record matching the Places API (New) response shape.
VALID_RECORD = {
    "id": "place_001",
    "displayName": {"text": "Test Cafe", "languageCode": "en"},
    "formattedAddress": "123 Street, Khwaeng Sub, Khet Watthana, Krung Thep Maha Nakhon 10110, Thailand",
    "location": {"latitude": 13.73, "longitude": 100.57},
    "rating": 4.5,
    "priceLevel": "PRICE_LEVEL_MODERATE",
    "userRatingCount": 50,
    "types": ["cafe", "food"],
}


def make_bronze_df(spark, records):
    """Create a bronze-like DataFrame from a list of dicts via spark.read.json."""
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
    return spark.read.json(rdd)


# ---------------------------------------------------------------------------
# flatten_and_clean
# ---------------------------------------------------------------------------

def test_valid_record_passes_through(spark):
    df = make_bronze_df(spark, [VALID_RECORD])
    result = flatten_and_clean(df, RUN_DATE)
    assert result.count() == 1
    row = result.first()
    assert row.place_id == "place_001"
    assert row.name == "Test Cafe"
    assert row.khet == "Watthana"
    assert row.province == "Krung Thep Maha Nakhon"
    assert row.rating == 4.5
    assert row.price_level == 2


def test_drops_row_with_missing_rating(spark):
    # In production, Spark infers the schema from the whole file, so 'rating' exists
    # as a nullable column even if a specific record omits it.  We simulate that by
    # including a valid record alongside the one with no rating.
    no_rating = {**{k: v for k, v in VALID_RECORD.items() if k != "rating"}, "id": "no_rating_place"}
    df = make_bronze_df(spark, [VALID_RECORD, no_rating])
    result = flatten_and_clean(df, RUN_DATE)
    assert result.filter("place_id = 'no_rating_place'").count() == 0


def test_drops_row_with_zero_user_rating_count(spark):
    record = {**VALID_RECORD, "userRatingCount": 0}
    df = make_bronze_df(spark, [record])
    assert flatten_and_clean(df, RUN_DATE).count() == 0


def test_drops_row_with_thai_script_khet(spark):
    # Address where the [-3] comma-split position contains Thai script
    record = {
        **VALID_RECORD,
        "formattedAddress": "123 Street, แขวงบางมด, Krung Thep Maha Nakhon 10110, Thailand",
    }
    df = make_bronze_df(spark, [record])
    assert flatten_and_clean(df, RUN_DATE).count() == 0


def test_drops_row_outside_bangkok(spark):
    record = {
        **VALID_RECORD,
        "formattedAddress": "123 Street, Subdistrict, Mueang Chiang Mai, Chiang Mai 50200, Thailand",
    }
    df = make_bronze_df(spark, [record])
    assert flatten_and_clean(df, RUN_DATE).count() == 0


def test_accepts_province_spelled_bangkok(spark):
    # Both "Bangkok" and "Krung Thep Maha Nakhon" are valid province spellings
    record = {
        **VALID_RECORD,
        "formattedAddress": "123 Street, Khwaeng Sub, Khet Watthana, Bangkok 10110, Thailand",
    }
    df = make_bronze_df(spark, [record])
    assert flatten_and_clean(df, RUN_DATE).count() == 1


@pytest.mark.parametrize("price_enum,expected_int", [
    ("PRICE_LEVEL_FREE", 0),
    ("PRICE_LEVEL_INEXPENSIVE", 1),
    ("PRICE_LEVEL_MODERATE", 2),
    ("PRICE_LEVEL_EXPENSIVE", 3),
    ("PRICE_LEVEL_VERY_EXPENSIVE", 4),
])
def test_price_level_enum_maps_to_int(spark, price_enum, expected_int):
    record = {**VALID_RECORD, "priceLevel": price_enum}
    df = make_bronze_df(spark, [record])
    row = flatten_and_clean(df, RUN_DATE).first()
    assert row.price_level == expected_int


def test_unknown_price_level_maps_to_null(spark):
    record = {**VALID_RECORD, "priceLevel": "PRICE_LEVEL_UNKNOWN"}
    df = make_bronze_df(spark, [record])
    row = flatten_and_clean(df, RUN_DATE).first()
    assert row.price_level is None


# ---------------------------------------------------------------------------
# merge_with_existing
# ---------------------------------------------------------------------------

def test_first_run_assigns_ingestion_date_as_first_seen(spark, tmp_path):
    df = make_bronze_df(spark, [VALID_RECORD])
    clean_df = flatten_and_clean(df, "2026-08-01")
    merged = merge_with_existing(clean_df, str(tmp_path / "silver"), spark)
    assert str(merged.first().first_seen_date) == "2026-08-01"


def test_second_run_preserves_first_seen_date(spark, tmp_path):
    silver_path = str(tmp_path / "silver")

    # First run
    df1 = make_bronze_df(spark, [VALID_RECORD])
    clean1 = flatten_and_clean(df1, "2026-08-01")
    merge_with_existing(clean1, silver_path, spark).write.mode("overwrite").parquet(silver_path)

    # Second run — same place_id, updated rating
    df2 = make_bronze_df(spark, [{**VALID_RECORD, "rating": 4.7}])
    clean2 = flatten_and_clean(df2, "2026-08-27")
    merged = merge_with_existing(clean2, silver_path, spark)

    row = merged.first()
    assert str(row.first_seen_date) == "2026-08-01"  # preserved from first run
    assert row.rating == 4.7                          # updated from second run


def test_second_run_retains_places_not_in_new_batch(spark, tmp_path):
    silver_path = str(tmp_path / "silver")

    # First run: two places
    record_a = VALID_RECORD
    record_b = {**VALID_RECORD, "id": "place_002", "displayName": {"text": "Other Cafe", "languageCode": "en"}}
    df1 = make_bronze_df(spark, [record_a, record_b])
    clean1 = flatten_and_clean(df1, "2026-08-01")
    merge_with_existing(clean1, silver_path, spark).write.mode("overwrite").parquet(silver_path)

    # Second run: only place_001 reappears
    df2 = make_bronze_df(spark, [record_a])
    clean2 = flatten_and_clean(df2, "2026-08-27")
    merged = merge_with_existing(clean2, silver_path, spark)

    # place_002 should still be in the merged result
    assert merged.count() == 2
