import json
from pathlib import Path

import pytest

from dockwatch.geo.build import VIEWS, build_view, clip, project, projection_for, simplify

ROOT = Path(__file__).resolve().parents[1]


def test_projection_fits_viewbox():
    for view, spec in VIEWS.items():
        west, south, east, north = spec["bounds"]
        p = projection_for(spec["bounds"])
        corners = [project(lon, lat, p) for lon in (west, east) for lat in (south, north)]
        for x, y in corners:
            assert -1e-6 <= x <= 1000 + 1e-6, view
            assert -1e-6 <= y <= 1000 + 1e-6, view
        # The longer side fills the box exactly; north is up.
        xs = [x for x, _ in corners]
        ys = [y for _, y in corners]
        assert max(max(xs) - min(xs), max(ys) - min(ys)) == pytest.approx(1000)
        assert project(west, north, p)[1] < project(west, south, p)[1]
        assert project((west + east) / 2, (south + north) / 2, p) == pytest.approx((500, 500))


def test_simplify_keeps_endpoints_and_reduces_points():
    # A nearly straight line with small wiggles and one real corner.
    line = [(float(x), 0.1 * (x % 2)) for x in range(11)] + [(10.0, 10.0)]
    out = simplify(line, tolerance=0.5)
    assert out[0] == line[0]
    assert out[-1] == line[-1]
    assert len(out) < len(line)
    assert (10.0, 0.0) in out  # the corner survives
    assert simplify(line, tolerance=0.0) == line
    assert simplify(line[:2], tolerance=5) == line[:2]


def test_clip_to_rectangle():
    square = [(-5.0, -5.0), (5.0, -5.0), (5.0, 5.0), (-5.0, 5.0)]
    out = clip(square, 0, 0, 10, 10)
    assert sorted(out) == sorted([(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (0.0, 5.0)])
    assert clip(square, 20, 20, 30, 30) == []
    inside = [(1.0, 1.0), (2.0, 1.0), (2.0, 2.0)]
    assert clip(inside, 0, 0, 10, 10) == inside
    # A closed ring (first point repeated) is handled the same way.
    assert sorted(clip(square + [square[0]], 0, 0, 10, 10)) == sorted(out)


def test_build_view_document_shape():
    ring = [(-122.6, 37.6), (-122.3, 37.6), (-122.3, 37.9), (-122.6, 37.9)]
    doc = build_view("sf", [[ring]])
    assert doc["view"] == "sf"
    assert "TIGER" in doc["source"]
    assert doc["viewBox"] == "0 0 1000 1000"
    assert len(doc["land"]) == 1
    assert doc["land"][0].startswith("M")
    assert 3 <= len(doc["landmarks"]) <= 5


def test_fixture_stations_project_inside_their_view():
    live = json.loads((ROOT / "tests/web/fixtures/live.json").read_text(encoding="utf-8"))
    stations = live["stations"]
    assert len(stations) >= 600
    for view in VIEWS:
        geo = json.loads((ROOT / "web/geo" / f"{view}.json").read_text(encoding="utf-8"))
        mine = [s for s in stations if s["view"] == view]
        assert mine, view
        for s in mine:
            x, y = project(s["lon"], s["lat"], geo["projection"])
            assert 0 <= x <= 1000, (view, s["code"], x)
            assert 0 <= y <= 1000, (view, s["code"], y)
