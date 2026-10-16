"""Insights export format 2 (Loop K): station lookup + window header + compact rows, through the real marts_export
task (Trino faked) and the real SIZE_BUDGET, on synthetic marts with every real column at 1,000 stations x 30 dates."""

import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta

import pytest

from dockwatch.batch import dag_tasks, marts

COLUMNS = {
    "mart_station_availability_daily": [
        "station_id", "station_name", "region_id", "service_date", "windows", "coverage_hours", "share_empty",
        "share_full", "minutes_empty", "minutes_full", "avg_bikes", "first_window", "last_window",
    ],
    "mart_rebalancing_need": [
        "station_id", "station_name", "region_id", "hours_empty", "hours_full", "need_rank", "episodes",
        "alerted_episodes", "longest_empty_hours", "coverage_hours", "window_first_day", "window_last_day",
    ],
    "mart_trips_vs_availability": [
        "station_id", "station_name", "region_id", "hour_of_day", "trips_started_per_day", "trips_ended_per_day",
        "share_empty", "share_full", "live_days", "trip_days", "trip_first_month", "trip_last_month",
    ],
}  # fmt: skip
MOVED = ("station_id", "station_name", "region_id", "first_window", "last_window", "trip_days", "trip_first_month",
         "trip_last_month", "window_first_day", "window_last_day")  # fmt: skip
STREETS = ("Market St", "Valencia St", "Mission St", "Shattuck Ave", "Telegraph Ave", "San Pablo Ave", "Santa Clara St",
           "The Embarcadero", "Martin Luther King Jr Way", "Folsom St", "Divisadero St", "Grand Ave", "Broadway",
           "Townsend St", "Fruitvale Ave", "Julian St", "Los Gatos Creek Trail", "Golden Gate Ave")  # fmt: skip
LAST_DAY = date(2026, 10, 14)


def stations(n: int, rng: random.Random) -> list[tuple]:
    """Like bronze.station_information: mostly UUIDs, some long numeric ids, names "<street> at <street>"."""
    out = []
    for i in range(n):
        sid = str(uuid.UUID(int=rng.getrandbits(128))) if i % 10 else str(rng.randrange(10**18, 10**19))
        a, b = rng.sample(STREETS, 2)
        out.append((sid, f"{a} at {b}", rng.choice(("3", "5", "12", "14", None))))
    return out


def mart_rows(n_stations: int = 1000, n_dates: int = 30, seed: int = 7) -> dict[str, list[list]]:
    """Synthetic marts with full-precision floats (rounded by the export, as Trino's doubles are)."""
    rng = random.Random(seed)
    st = stations(n_stations, rng)
    daily, need, trips = [], [], []
    for d in range(n_dates):
        day = LAST_DAY - timedelta(days=d)
        start = datetime(day.year, day.month, day.day, 7, tzinfo=UTC)
        for sid, name, region in st:
            windows = rng.randint(200, 288)
            se, sf = rng.random(), rng.random() * 0.5
            daily.append([sid, name, region, day, windows, windows / 12.0, se, sf, se * windows * 5, sf * windows * 5,
                          rng.random() * 40, start, start + timedelta(minutes=5 * (windows - 1))])  # fmt: skip
    for rank, (sid, name, region) in enumerate(st, start=1):
        need.append([sid, name, region, rng.random() * 60, rng.random() * 60, rank, rng.randint(0, 40),
                     rng.randint(0, 30), rng.random() * 30, 168 * rng.random(), LAST_DAY - timedelta(days=6),
                     LAST_DAY])  # fmt: skip
        for hour in range(24):
            trips.append([sid, name, region, hour, rng.random() * 8, rng.random() * 8, rng.random(), rng.random(),
                          rng.randint(1, 30), 365, date(2025, 10, 1), date(2026, 9, 1)])  # fmt: skip
    return {"mart_station_availability_daily": daily, "mart_rebalancing_need": need,
            "mart_trips_vs_availability": trips}  # fmt: skip


class FakeTrino:
    """Answers marts_export's queries from in-memory marts (dicts by column name, like dag_tasks.trino_execute)."""

    def __init__(self, data: dict[str, list[list]]) -> None:
        self.data = data

    def rows(self, mart: str) -> list[dict]:
        return [dict(zip(COLUMNS[mart], r, strict=True)) for r in self.data[mart]]

    def __call__(self, sql: str) -> list[dict]:
        mart = next(m for m in COLUMNS if f"lake.marts.{m}" in sql)
        rows = self.rows(mart)
        if sql.startswith("SELECT min("):
            a, b = (
                ("window_first_day", "window_last_day")
                if "window_first_day" in sql
                else ("trip_first_month", "trip_last_month")
            )
            return [{"a": min(r[a] for r in rows), "b": max(r[b] for r in rows)}]
        if mart == "mart_station_availability_daily":
            last = max(r["service_date"] for r in rows)
            rows = [r for r in rows if r["service_date"] > last - timedelta(days=marts.DAILY_DATES)]
        return rows


def export(tmp_path, monkeypatch, data) -> dict[str, dict]:
    """Run dag_tasks.marts_export (the real task) against the fake Trino; return the written documents."""
    data_dir = tmp_path / "data"
    (data_dir / "dbt").mkdir(parents=True)
    (data_dir / "dbt" / "quality_gate.json").write_text('{"passed": true, "tests": 28, "warnings": 0}', "utf-8")
    monkeypatch.setenv("DOCKWATCH_DATA_DIR", str(data_dir))
    monkeypatch.setattr(dag_tasks, "trino_execute", lambda: FakeTrino(data))

    class Run:
        run_id = "manual__2026-10-15T11:12:52+00:00"

    dag_tasks.marts_export(dag_run=Run())
    out = tmp_path / "web" / "data" / "insights"
    return {p.stem: json.loads(p.read_text(encoding="utf-8")) | {"bytes": p.stat().st_size} for p in out.glob("*.json")}


def decode(doc: dict) -> list[dict]:
    """A format-2 file back to mart rows (by column name): station lookup + window constants + the row."""
    scols = doc["stations"]["columns"]
    names = {"station_id": "station_id", "name": "station_name", "region_id": "region_id"}
    window = {k: v for k, v in doc["window"].items() if k in MOVED}
    out = []
    for r in doc["rows"]:
        row = dict(zip(doc["columns"], r, strict=True))
        if "service_dates" in doc["window"]:
            row["service_date"] = doc["window"]["service_dates"][row["service_date"]]
        station = dict(zip((names[c] for c in scols), doc["stations"]["rows"][row.pop("station")], strict=True))
        out.append({**station, **window, **row})
    return out


def test_v2_moves_station_and_window_columns_out_of_the_rows(tmp_path, monkeypatch):
    docs = export(tmp_path, monkeypatch, mart_rows(n_stations=40, n_dates=3))
    man = docs.pop("manifest")
    assert man["format"] == 2
    assert man["dag_run_id"] == "manual__2026-10-15T11:12:52+00:00"
    assert set(docs) == set(marts.INSIGHTS)
    for name, doc in docs.items():
        assert doc["format"] == 2, name
        assert doc["source"] == f"lake.marts.{marts.INSIGHTS[name][0]}"
        assert doc["generated_at"] == man["generated_at"]
        assert doc["columns"][0] == "station", name
        assert not set(doc["columns"]) & set(MOVED), name
        assert doc["stations"]["columns"] == ["station_id", "name", "region_id"]
        ids = [s[0] for s in doc["stations"]["rows"]]
        assert len(ids) == len(set(ids)) == 40, name  # one lookup row per station, every one used
        assert {r[0] for r in doc["rows"]} == set(range(40)), name
        assert man["files"][name]["rows"] == len(doc["rows"])
    assert docs["rebalancing_need"]["window"] == {"window_first_day": "2026-10-08", "window_last_day": "2026-10-14"}
    assert docs["trips_vs_availability"]["window"] == {"trip_days": 365, "trip_first_month": "2025-10-01",
                                                       "trip_last_month": "2026-09-01"}  # fmt: skip
    daily = docs["station_availability_daily"]
    assert daily["window"] == {"service_dates": ["2026-10-12", "2026-10-13", "2026-10-14"]}
    assert {r[daily["columns"].index("service_date")] for r in daily["rows"]} == {0, 1, 2}  # index into the window
    assert "service_date" in docs["station_availability_daily"]["columns"]
    assert "hour_of_day" in docs["trips_vs_availability"]["columns"]
    # a window column that is not constant would be lost: the export refuses
    need = mart_rows(n_stations=3, n_dates=1)["mart_rebalancing_need"]
    need[1][COLUMNS["mart_rebalancing_need"].index("window_last_day")] = LAST_DAY - timedelta(days=1)
    with pytest.raises(marts.GateError, match="window_last_day has 2 different values"):
        marts.insights_v2("rebalancing_need", COLUMNS["mart_rebalancing_need"], need)


def test_v2_rows_decode_back_to_the_mart_rows(tmp_path, monkeypatch):
    data = mart_rows(n_stations=25, n_dates=4)
    docs = export(tmp_path, monkeypatch, data)
    for name, (mart, _order) in marts.INSIGHTS.items():
        cols = [c for c in COLUMNS[mart] if c not in marts.DROPPED_COLUMNS]
        want = [{c: marts.plain(v) for c, v in zip(COLUMNS[mart], r, strict=True) if c in cols} for r in data[mart]]
        got = decode(docs[name])
        key = (lambda r: (r["station_id"], r.get("service_date"), r.get("hour_of_day"))) if name != "rebalancing_need" \
            else (lambda r: r["station_id"])  # fmt: skip
        assert sorted(got, key=key) == sorted(want, key=key), name
        assert len({key(r) for r in got}) == len(got), name  # grain keys stay unique


def test_v2_files_fit_the_budget_at_1000_stations_and_30_dates(tmp_path, monkeypatch):
    data = mart_rows(n_stations=1000, n_dates=30)
    docs = export(tmp_path, monkeypatch, data)
    assert len(docs["station_availability_daily"]["rows"]) == 30_000
    assert len(docs["trips_vs_availability"]["rows"]) == 24_000
    assert len(docs["rebalancing_need"]["rows"]) == 1000
    for name in marts.INSIGHTS:
        assert docs[name]["bytes"] <= marts.SIZE_BUDGET, (name, docs[name]["bytes"])
        assert docs["manifest"]["files"][name]["bytes"] == docs[name]["bytes"]
    assert docs["station_availability_daily"]["bytes"] < 0.95 * marts.SIZE_BUDGET  # headroom: worst-case digits here
    # the format-1 layout (station_id, name and window on every row) does not fit at this scale
    mart = "mart_station_availability_daily"
    rows = [r for r in data[mart] if r[3] > LAST_DAY - timedelta(days=marts.DAILY_DATES)]
    with pytest.raises(marts.GateError, match="bytes >"):
        marts.export_insights(tmp_path / "v1", {"station_availability_daily": (COLUMNS[mart], rows)}, {})
