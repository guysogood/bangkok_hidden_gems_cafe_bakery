"""
Tests for the grid generation logic in places_api_client.
No API calls are made — only the pure-Python grid math is tested.
"""

from src.ingest.places_api_client import GridPoint, generate_grid

BBOX = {"min_lat": 13.65, "max_lat": 13.85, "min_lon": 100.45, "max_lon": 100.65}
RADIUS_M = 700


def test_generate_grid_returns_gridpoints():
    grid = generate_grid(BBOX, RADIUS_M)
    assert len(grid) > 0
    assert all(isinstance(p, GridPoint) for p in grid)


def test_generate_grid_all_points_within_bbox():
    grid = generate_grid(BBOX, RADIUS_M)
    for point in grid:
        assert BBOX["min_lat"] <= point.lat <= BBOX["max_lat"]
        assert BBOX["min_lon"] <= point.lon <= BBOX["max_lon"]


def test_generate_grid_covers_bbox_with_enough_points():
    # Bangkok central bbox at 700m radius should produce hundreds of grid points
    grid = generate_grid(BBOX, RADIUS_M)
    assert len(grid) > 100


def test_generate_grid_no_duplicate_coordinates():
    grid = generate_grid(BBOX, RADIUS_M)
    coords = [(round(p.lat, 6), round(p.lon, 6)) for p in grid]
    assert len(coords) == len(set(coords))


def test_generate_grid_smaller_radius_produces_more_points():
    grid_small = generate_grid(BBOX, radius_m=500)
    grid_large = generate_grid(BBOX, radius_m=1500)
    assert len(grid_small) > len(grid_large)
