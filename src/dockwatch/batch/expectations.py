"""Great Expectations on silver (Loop L, M5): the suite silver_station_status, its batch (the newest 24 hours of
lake.silver.station_status as a query asset), the results document data/quality/expectations.json and how it joins
marts_daily's quality gate. Pure and stdlib only (no GE import): the GE venv runs it through dockwatch.batch.gx_run,
Airflow's python through dag_tasks.marts_quality_gate / health_export.

Blocking expectations fail the gate (no insights export); freshness only warns: Spark never runs beside Trino, so
during marts_daily silver is always older than 5 minutes.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dockwatch.streaming.episodes import STATES

SUITE = "silver_station_status"
CHECKPOINT = "marts_daily_quality"
TABLE = "lake.silver.station_status"
RESULTS = "data/quality/expectations.json"
WINDOW = timedelta(hours=24)
FRESHNESS_S = 300
COLUMNS = (
    "station_id", "snapshot_ts", "last_reported_ts", "num_bikes_available", "num_bikes_disabled",
    "num_ebikes_available", "num_docks_available", "num_docks_disabled", "capacity", "is_installed", "is_renting",
    "is_returning", "state", "fetched_ts", "kafka_ts", "processed_ts", "num_scooters_available",
)  # fmt: skip
ROW_COUNT, MIN = "expect_table_row_count_to_be_between", "expect_column_min_to_be_between"
# (GE type, column key, kwargs, blocking, label). Column key: None = table, "col", or "A,B" for a pair / compound.
EXPECTATIONS = (
    (ROW_COUNT, None, {"min_value": 1}, True, "The newest 24 hours of silver have rows"),
    ("expect_table_columns_to_match_set", None, {"column_set": list(COLUMNS), "exact_match": False}, True,
     "Silver has every expected column (new columns are allowed)"),
    ("expect_column_values_to_not_be_null", "station_id", {}, True, "Every row has a station id"),
    ("expect_column_values_to_not_be_null", "snapshot_ts", {}, True, "Every row has a snapshot time"),
    ("expect_column_values_to_be_in_set", "state", {"value_set": list(STATES)}, True,
     "Every state is one of the six station states"),
    ("expect_column_values_to_be_between", "num_bikes_available", {"min_value": 0}, True,
     "Bikes available is never negative"),
    ("expect_column_values_to_be_between", "num_docks_available", {"min_value": 0}, True,
     "Docks available is never negative"),
    # capacity includes disabled bikes and docks: that is the tolerance
    ("expect_column_pair_values_a_to_be_greater_than_b", "capacity,bikes_plus_docks", {"or_equal": True}, True,
     "Bikes plus docks available never exceed capacity"),
    ("expect_compound_columns_to_be_unique", "station_id,snapshot_ts", {}, True, "One row per station per snapshot"),
    (MIN, "age_s", {"max_value": FRESHNESS_S}, False, "The newest row reached silver less than 5 min ago"),
)  # fmt: skip


# ---------------------------------------------------------------- suite and batch


def gx_kwargs(kind: str, key: str | None, kwargs: dict) -> dict:
    """The GE constructor kwargs of one suite entry."""
    if key is None:
        return dict(kwargs)
    cols = key.split(",")
    if kind.startswith("expect_column_pair_"):
        return {"column_A": cols[0], "column_B": cols[1], **kwargs}
    if kind.startswith("expect_compound_columns_"):
        return {"column_list": cols, **kwargs}
    return {"column": key, **kwargs}


def column_key(kwargs: dict) -> str | None:
    """The suite's column key from a GE result's kwargs."""
    if kwargs.get("column_list"):
        return ",".join(kwargs["column_list"])
    if "column_A" in kwargs:
        return f"{kwargs['column_A']},{kwargs['column_B']}"
    return kwargs.get("column")


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="microseconds")


def lit(ts: datetime) -> str:
    return f"TIMESTAMP '{ts.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f} UTC'"


def window(newest: datetime) -> tuple[datetime, datetime]:
    """(start, end]: the 24 hours ending at silver's newest snapshot_ts."""
    return newest - WINDOW, newest


def query(start: datetime, end: datetime, now: datetime) -> str:
    """The query asset: the window's rows plus the two derived columns the pair and freshness checks need."""
    return (
        f"SELECT *, num_bikes_available + num_docks_available AS bikes_plus_docks, "
        f"date_diff('second', processed_ts, {lit(now)}) AS age_s FROM {TABLE} "
        f"WHERE snapshot_ts > {lit(start)} AND snapshot_ts <= {lit(end)}"
    )


# ---------------------------------------------------------------- results


def number(value):
    """GE reports some observed values as text; integral values become int."""
    if value is None:
        return None
    x = float(value)
    return int(x) if x.is_integer() else x


def results(raw: list[dict]) -> list[dict]:
    """GE results [{type, kwargs, success, result}] (any order) -> one row per suite entry, in suite order. A suite
    entry GE did not report counts as not successful."""
    got = {(r["type"], column_key(r.get("kwargs") or {})): r for r in raw}
    out = []
    for kind, key, kwargs, blocking, label in EXPECTATIONS:
        r = got.get((kind, key), {})
        res = r.get("result") or {}
        success = r.get("success") is True
        out.append({
            "expectation": kind, "column": key, "label": label, "blocking": blocking, "success": success,
            "status": "ok" if success else "fail" if blocking else "warn",
            "observed": number(res.get("observed_value")) if kind in (ROW_COUNT, MIN) else None,
            "unexpected_count": res.get("unexpected_count"), "element_count": res.get("element_count"),
            "kwargs": kwargs,
        })  # fmt: skip
    return out


def results_doc(rows: list[dict], *, dag_run_id: str, run_at: datetime, gx_version: str, seconds: float,
                start: datetime, end: datetime, now: datetime) -> dict:  # fmt: skip
    """The data/quality/expectations.json document (format 1)."""
    st = [r["status"] for r in rows]
    count = next((r["observed"] for r in rows if r["expectation"] == ROW_COUNT), None)
    return {
        "format": 1, "suite": SUITE, "checkpoint": CHECKPOINT, "dag_run_id": dag_run_id, "run_at": iso(run_at),
        "gx_version": gx_version, "seconds": round(seconds, 1),
        "batch": {"table": TABLE, "window_start": iso(start), "window_end": iso(end), "now": iso(now), "rows": count},
        "results": rows, "evaluated": len(rows), "passed": st.count("ok"), "warnings": st.count("warn"),
        "failed": st.count("fail"), "success": st.count("fail") == 0,
    }  # fmt: skip


def read_results(path: Path) -> dict | None:
    """The results document, or None when it is missing or not JSON."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def unusable(doc, dag_run_id: str) -> str | None:
    """Why a results document cannot be used by this run's gate, or None."""
    if not isinstance(doc, dict):
        return "no results document"
    if doc.get("format") != 1 or doc.get("suite") != SUITE:
        return f"not a format 1 {SUITE} document"
    if doc.get("dag_run_id") != dag_run_id:
        return f"written by {doc.get('dag_run_id')}, not this run"
    keys = sorted((str(r.get("expectation")), str(r.get("column"))) for r in doc.get("results") or [])
    if keys != sorted((kind, str(key)) for kind, key, *_ in EXPECTATIONS):
        return "its results do not match the suite"
    return None


def merge_gate(gate: dict, doc, dag_run_id: str, note: str = "") -> dict:
    """quality_gate.json with the expectations summary: one `gx:` problem per failed blocking expectation, and
    `passed` only when the dbt part passed and no blocking expectation failed. A missing or unusable document fails
    the gate (`gx: no results (...)`); it never passes silently."""
    out = {**gate, "problems": list(gate.get("problems") or [])}
    why = unusable(doc, dag_run_id)
    if why:
        out["problems"].append(f"gx: no results ({'; '.join(x for x in (why, note) if x)})")
        out["expectations"] = {"suite": SUITE, "evaluated": None, "passed": None, "warnings": None, "failed": None,
                               "path": RESULTS}  # fmt: skip
        out["passed"] = False
        return out
    for r in doc["results"]:
        if r["status"] == "fail":
            found = r["observed"] if r["unexpected_count"] is None else f"{r['unexpected_count']} unexpected rows"
            out["problems"].append(f"gx: {r['label']} ({r['expectation']}, {r['column'] or 'table'}): {found}")
    out["expectations"] = {"suite": SUITE, **{k: doc[k] for k in ("evaluated", "passed", "warnings", "failed")},
                           "path": RESULTS}  # fmt: skip
    out["passed"] = gate.get("passed") is True and doc["failed"] == 0
    return out
