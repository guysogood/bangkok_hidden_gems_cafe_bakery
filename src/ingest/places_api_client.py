"""
Ingestion module for the Bangkok hidden-gems pipeline.
Pulls cafe and bakery data from Google Places API (New) using a grid of
overlapping search circles to cover a wide area (Nearby Search New caps
out at 15 results per call).

Usage:
    python places_api_client.py
"""

import json
import logging
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

BASE_URL = "https://places.googleapis.com/v1/places:searchNearby"
FIELD_MASK = (
    "places.id,places.displayName,places.rating,"
    "places.userRatingCount,places.location,places.priceLevel,"
    "places.formattedAddress,places.types"
)

# Rough bounding box for central + inner Bangkok. Tune as needed.
BANGKOK_BBOX = {
    "min_lat": 13.65,
    "max_lat": 13.85,
    "min_lon": 100.45,
    "max_lon": 100.65,
}

SEARCH_RADIUS_M = 700  # per-circle search radius
REQUEST_DELAY_SEC = 0.2  # simple client-side rate limiting


@dataclass
class GridPoint:
    lat: float
    lon: float


def generate_grid(bbox: dict, radius_m: float) -> list[GridPoint]:
    """
    Tile a bounding box with circle centers spaced so consecutive
    circles overlap slightly, avoiding coverage gaps at the edges.
    """
    step_deg_lat = (radius_m * 1.5) / 111_320  # meters per degree latitude is ~constant
    points: list[GridPoint] = []

    lat = bbox["min_lat"]
    while lat <= bbox["max_lat"]:
        # longitude degrees shrink as you move away from the equator
        step_deg_lon = step_deg_lat / math.cos(math.radians(lat))
        lon = bbox["min_lon"]
        while lon <= bbox["max_lon"]:
            points.append(GridPoint(lat=lat, lon=lon))
            lon += step_deg_lon
        lat += step_deg_lat

    return points


class PlacesAPIClient:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("GOOGLE_PLACES_API_KEY is not set")
        self.api_key = api_key

    def search_nearby(
        self,
        lat: float,
        lon: float,
        radius_m: float,
        included_types: list[str],
        max_retries: int = 3,
    ) -> list[dict[str, Any]]:
        """
        Call Places API (New) Nearby Search for one grid point.
        Retries with exponential backoff on transient errors (429, 5xx).
        Returns an empty list on repeated failure rather than raising,
        so one bad grid point doesn't kill the whole run.
        """
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": FIELD_MASK,
        }
        body = {
            "includedTypes": included_types,
            "maxResultCount": 15,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lon},
                    "radius": radius_m,
                }
            },
        }

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.post(BASE_URL, headers=headers, json=body, timeout=10)
            except requests.RequestException as exc:
                logger.warning("Request error at (%s, %s): %s", lat, lon, exc)
                time.sleep(2 ** attempt)
                continue

            if resp.status_code == 200:
                return resp.json().get("places", [])

            if resp.status_code in (429, 500, 502, 503):
                wait = 2 ** attempt
                logger.warning(
                    "Retryable error %s at (%s, %s), waiting %ss",
                    resp.status_code, lat, lon, wait,
                )
                time.sleep(wait)
                continue

            # Non-retryable error (403, 400, etc.) - log and give up on this point
            logger.error(
                "Non-retryable error %s at (%s, %s): %s",
                resp.status_code, lat, lon, resp.text,
            )
            return []

        logger.error("Exhausted retries at (%s, %s)", lat, lon)
        return []

    def fetch_all(
        self, grid_points: list[GridPoint], included_types: list[str]
    ) -> list[dict[str, Any]]:
        """
        Sweep every grid point and return deduplicated places, keyed by
        Google's place id (the same cafe will appear in multiple
        overlapping circles).
        """
        seen_ids: set[str] = set()
        results: list[dict[str, Any]] = []

        for i, point in enumerate(grid_points, start=1):
            places = self.search_nearby(point.lat, point.lon, SEARCH_RADIUS_M, included_types)
            new_count = 0
            for place in places:
                place_id = place.get("id")
                if place_id and place_id not in seen_ids:
                    seen_ids.add(place_id)
                    results.append(place)
                    new_count += 1

            logger.info(
                "Grid point %d/%d (%.4f, %.4f): %d returned, %d new, %d total",
                i, len(grid_points), point.lat, point.lon,
                len(places), new_count, len(results),
            )
            time.sleep(REQUEST_DELAY_SEC)

        return results


def main():
    grid = generate_grid(BANGKOK_BBOX, SEARCH_RADIUS_M)
    logger.info("Generated %d grid points covering the bounding box", len(grid))

    client = PlacesAPIClient(API_KEY)
    places = client.fetch_all(grid, included_types=["cafe", "bakery"])

    out_path = Path("data/raw/bronze_places_raw.json")
    out_path.write_text(json.dumps(places, ensure_ascii=False, indent=2))
    logger.info("Saved %d unique places to %s", len(places), out_path)


if __name__ == "__main__":
    main()