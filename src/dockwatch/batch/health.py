"""pipeline_health (M5): per-minute pipeline metrics from lake.silver.station_status into lake.metrics.pipeline_minutely
and web/data/health.json for the Pipeline page. Pure rules and SQL builders; Trino and pyiceberg come in through
dag_tasks.py.

Per minute m of the 1,440 minutes before the run's start minute (plan D5):
  events(m)     = silver rows with kafka_ts in m (published that minute)
  processed(m)  = rows with processed_ts in m (landed in silver), with p50 / p95 of processed_ts - fetched_ts (s):
                  DockWatch's own path, fetch -> Spark micro-batch
  lag_records(m)= rows with kafka_ts < end(m) <= processed_ts: published but not yet in silver (consumer lag as the
                  lake sees it; records not processed at all yet are invisible here, freshness shows that case)
lag is computed exactly from two GROUP BYs over the rows processed since the window start (R):
  lag(b) = #{R: kafka_ts < b} - #{R: greatest(kafka_ts, processed_ts) < b}
which needs no ordering assumption between the broker's and Spark's clocks.
"""

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dockwatch.batch import maintenance

MINUTES = 1440
SILVER = "lake.silver.station_status"
MINUTELY_TABLE = "lake.metrics.pipeline_minutely"
MINUTELY_COLUMNS = (
    ("minute", "timestamp(6) with time zone"),
    ("events", "bigint"),
    ("processed", "bigint"),
    ("latency_p50_s", "double"),
    ("latency_p95_s", "double"),
    ("lag_records", "bigint"),
    ("run_id", "varchar"),
    ("computed_at", "timestamp(6) with time zone"),
)
SERIES = ["minute", "events", "processed", "latency_p50_s", "latency_p95_s", "lag_records"]
HEALTH_BUDGET = 500_000
STREAM_OK_S = 300  # the site's Paused threshold
WARN_S = 86_400
BATCH_OK_S = 172_800  # 48 h: daily jobs may skip a day
RANK = {"ok": 0, "warn": 1, "fail": 2}
DIGITS = 3
COMPACTION = ("table_name", "status", "files_before", "files_after", "bytes_before", "bytes_after")  # maintenance_runs


class HealthError(RuntimeError):
    pass


# ---------------------------------------------------------------- window and SQL


def window(run_start: datetime) -> tuple[datetime, datetime]:
    """[start, end): the 1,440 whole minutes before the run's start minute (UTC)."""
    end = run_start.astimezone(UTC).replace(second=0, microsecond=0)
    return end - timedelta(minutes=MINUTES), end


def ts(value: datetime) -> str:
    """A naive TIMESTAMP literal in UTC wall time (silver's timestamp(6) columns hold UTC)."""
    return f"TIMESTAMP '{value.astimezone(UTC):%Y-%m-%d %H:%M:%S}'"


def events_sql(w0: datetime, w1: datetime) -> str:
    return (
        f"SELECT date_trunc('minute', kafka_ts) AS m, count(*) AS n FROM {SILVER} "
        f"WHERE kafka_ts >= {ts(w0)} AND kafka_ts < {ts(w1)} GROUP BY 1"
    )


def processed_sql(w0: datetime, w1: datetime) -> str:
    lat = "to_milliseconds(processed_ts - fetched_ts) / 1000.0"
    return (
        f"SELECT date_trunc('minute', processed_ts) AS m, count(*) AS n, approx_percentile({lat}, 0.5) AS p50, "
        f"approx_percentile({lat}, 0.95) AS p95 FROM {SILVER} "
        f"WHERE processed_ts >= {ts(w0)} AND processed_ts < {ts(w1)} GROUP BY 1"
    )


def lag_sql(w0: datetime, w1: datetime, column: str) -> str:
    """Rows processed since w0 per minute of `column` (kafka_ts, or greatest(kafka_ts, processed_ts))."""
    return (
        f"SELECT date_trunc('minute', {column}) AS m, count(*) AS n FROM {SILVER} "
        f"WHERE processed_ts >= {ts(w0)} AND kafka_ts < {ts(w1)} GROUP BY 1"
    )


def create_sql() -> list[str]:
    cols = ", ".join(f"{n} {t}" for n, t in MINUTELY_COLUMNS)
    return ["CREATE SCHEMA IF NOT EXISTS lake.metrics", f"CREATE TABLE IF NOT EXISTS {MINUTELY_TABLE} ({cols})"]


def in_window(w0: datetime, w1: datetime) -> str:
    return f"minute >= {maintenance.literal(w0)} AND minute < {maintenance.literal(w1)}"


def count_sql(w0: datetime, w1: datetime) -> str:
    return f"SELECT count(*) AS n FROM {MINUTELY_TABLE} WHERE {in_window(w0, w1)}"


def window_sql(w0: datetime, w1: datetime) -> str:
    cols = ", ".join(SERIES)
    return f"SELECT {cols} FROM {MINUTELY_TABLE} WHERE {in_window(w0, w1)} ORDER BY minute"


EXISTING_SQL = (
    "SELECT table_schema || '.' || table_name AS t FROM lake.information_schema.tables "
    "WHERE table_schema IN ('bronze', 'silver', 'ops', 'metrics', 'marts')"
)
NEWEST_RUN_SQL = (
    f"SELECT * FROM {maintenance.METRICS_TABLE} WHERE run_id = "
    f"(SELECT max_by(run_id, finished_at) FROM {maintenance.METRICS_TABLE})"
)


def table_sql(table: str) -> str:
    """Newest commit that is not a compaction, data files and partitions of one lake table."""
    snaps = maintenance.meta(table, "snapshots")
    return (
        f"SELECT (SELECT max(committed_at) FROM {snaps} WHERE operation <> 'replace') AS last_commit, "
        f"(SELECT count(*) FROM {maintenance.meta(table, 'files')}) AS files, "
        f"(SELECT count(*) FROM {maintenance.meta(table, 'partitions')}) AS partitions"
    )


# ---------------------------------------------------------------- the series


def utc(value) -> datetime | None:
    """Trino timestamp (naive = UTC) or ISO text -> aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00").replace(" UTC", "+00:00"))
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def by_minute(rows: list[dict]) -> dict[datetime, dict]:
    return {utc(r["m"]): r for r in rows}


def num(value):
    return None if value is None or (isinstance(value, float) and math.isnan(value)) else round(float(value), DIGITS)


def minutely_rows(w0: datetime, events: list[dict], processed: list[dict], published: list[dict],
                  done: list[dict]) -> list[list]:  # fmt: skip
    """[minute, events, processed, p50, p95, lag_records] for each of the 1,440 minutes from w0."""
    ev, pr, pub, dn = by_minute(events), by_minute(processed), by_minute(published), by_minute(done)
    pub_cum = sum(int(r["n"]) for m, r in pub.items() if m < w0)  # published before the window, processed in it
    done_cum = sum(int(r["n"]) for m, r in dn.items() if m < w0)
    out = []
    for i in range(MINUTES):
        m = w0 + timedelta(minutes=i)
        pub_cum += int(pub.get(m, {}).get("n", 0))
        done_cum += int(dn.get(m, {}).get("n", 0))
        p = pr.get(m, {})
        out.append([m, int(ev.get(m, {}).get("n", 0)), int(p.get("n", 0)), num(p.get("p50")), num(p.get("p95")),
                    pub_cum - done_cum])  # fmt: skip
    return out


def table_rows(result: list[dict]) -> list[list]:
    """pipeline_minutely rows read back through Trino -> the series rows (minute as aware UTC datetime)."""
    return [[utc(r["minute"]), int(r["events"]), int(r["processed"]), num(r["latency_p50_s"]), num(r["latency_p95_s"]),
             int(r["lag_records"])] for r in result]  # fmt: skip


def tiles(rows: list[list], freshness_s: int | None) -> dict:
    """The last active hour = the 60 minutes ending at the newest minute with processed > 0."""
    active = [i for i, r in enumerate(rows) if r[2] > 0]
    out = {"latency_p95_s": None, "events_per_min": None, "lag_records": rows[-1][5] if rows else None,
           "freshness_s": freshness_s}  # fmt: skip
    if active:
        hour = rows[max(0, active[-1] - 59) : active[-1] + 1]
        out["events_per_min"] = round(sum(r[1] for r in hour) / len(hour), 1)
        p95 = [r[4] for r in hour if r[4] is not None]
        out["latency_p95_s"] = max(p95) if p95 else None
    return out


# ---------------------------------------------------------------- statuses


def stream_status(age_s: int | None) -> str:
    if age_s is None:
        return "fail"
    return "ok" if age_s <= STREAM_OK_S else "warn" if age_s <= WARN_S else "fail"


def table_status(table: str, age_s: int | None) -> str:
    if age_s is None:
        return "warn"
    if table in maintenance.STREAM_TABLES:
        return stream_status(age_s)
    return "ok" if age_s <= BATCH_OK_S else "warn"


def quality_status(gate: dict | None) -> str:
    return "warn" if gate is None else "ok" if gate.get("passed") is True else "fail"


def maintenance_status(run: list[dict], now: datetime) -> str:
    if not run:
        return "warn"
    if any(r.get("status") == "error" for r in run):
        return "fail"
    finished = max((utc(r.get("finished_at")) for r in run if r.get("finished_at")), default=None)
    return "warn" if finished is None or (now - finished).total_seconds() > BATCH_OK_S else "ok"


def ago(seconds: int) -> str:
    """A rough age for the status details: "45 s", "6 min", "3 h", "2 days"."""
    if seconds < 120:
        return f"{seconds} s"
    if seconds < 7200:
        return f"{seconds // 60} min"
    return f"{seconds // 3600} h" if seconds < 172_800 else f"{seconds // 86_400} days"


def worst(statuses) -> str:
    return max(statuses, key=RANK.get)


def age(now: datetime, then) -> int | None:
    t = utc(then)
    return None if t is None else int((now - t).total_seconds())


# ---------------------------------------------------------------- health.json


def health_doc(now: datetime, run_id: str, rows: list[list], last_processed, tables: list[dict],
               gate: dict | None, tests: list[dict], run: list[dict]) -> dict:  # fmt: skip
    """The health.json document (format 1). `tables`: [{table, last_commit, files, partitions}] for the tables that
    exist; `run`: the newest maintenance run's rows; `tests`: [{name, status}] from dbt's run_results.json."""
    now = now.astimezone(UTC).replace(microsecond=0)
    fresh = age(now, last_processed)
    finished = max((utc(r.get("finished_at")) for r in run if r.get("finished_at")), default=None)
    components = [
        {"name": "stream", "status": stream_status(fresh),
         "detail": "no silver rows yet" if fresh is None else f"newest row reached silver {ago(fresh)} ago"},
        {"name": "quality", "status": quality_status(gate),
         "detail": "no quality gate yet" if gate is None else
         f"{gate.get('tests_passed')} of {gate.get('tests')} dbt tests passed" if gate.get("passed") else
         f"quality gate failed: {'; '.join(map(str, gate.get('problems') or []))[:300]}"},
        {"name": "maintenance", "status": maintenance_status(run, now),
         "detail": "no maintenance run yet" if not run else
         f"{sum(r.get('status') == 'error' for r in run)} of {len(run)} tables failed in the newest run, "
         f"{'finished ' + ago(age(now, finished)) + ' ago' if finished else 'not finished'}"},
    ]  # fmt: skip
    table_rows = []
    for t in tables:
        a = age(now, t.get("last_commit"))
        commit = utc(t.get("last_commit"))
        table_rows.append([t["table"], commit.isoformat() if commit else None, a, int(t["files"]),
                           int(t["partitions"]), table_status(t["table"], a)])  # fmt: skip
    keep = ("passed", "tests", "tests_passed", "warnings", "problems", "dbt_generated_at", "dag_run_id")
    doc = {
        "format": 1,
        "generated_at": now.isoformat(timespec="seconds"),
        "dag_run_id": run_id,
        "status": {"overall": worst(c["status"] for c in components), "components": components},
        "tiles": tiles(rows, fresh),
        "minutely": {"columns": SERIES, "rows": [[r[0].isoformat(timespec="seconds"), *r[1:]] for r in rows]},
        "tables": {"columns": ["table", "last_commit", "age_s", "files", "partitions", "status"], "rows": table_rows},
        "quality": {"gate": {k: gate.get(k) for k in keep} if gate else None, "tests": tests},
        "compaction": {"columns": ["table", *COMPACTION[1:]], "rows": [[r.get(k) for k in COMPACTION] for r in run]},
    }
    return doc


def dbt_tests(run_results: dict) -> list[dict]:
    return [{"name": str(r.get("unique_id", "")).split(".")[2] if r.get("unique_id", "").count(".") >= 2 else
             r.get("unique_id"), "status": r.get("status")}
            for r in run_results.get("results") or [] if str(r.get("unique_id", "")).startswith("test.")]  # fmt: skip


def write_health(path: Path, doc: dict) -> int:
    text = json.dumps(doc, separators=(",", ":")) + "\n"
    size = len(text.encode("utf-8"))
    if size > HEALTH_BUDGET:
        raise HealthError(f"health.json would be {size} bytes > {HEALTH_BUDGET}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    tmp.replace(path)
    return size


# ---------------------------------------------------------------- Arrow and pyiceberg (Airflow image only)


def minutely_arrow(rows: list[list], run_id: str, computed_at: datetime):
    """The series rows -> one Arrow table with pipeline_minutely's columns and types."""
    from dockwatch.batch import trips

    return trips.rows_table(MINUTELY_COLUMNS, [(*r, run_id, computed_at) for r in rows])


def replace_window(w0: datetime, data) -> None:
    """One pyiceberg transaction (one commit): delete every minute >= w0, append the window's rows. Readers see the
    old window or the new one; no minute is ever there twice."""
    from pyiceberg.expressions import GreaterThanOrEqual

    from dockwatch.batch import trips

    tbl = trips.catalog().load_table(MINUTELY_TABLE.removeprefix("lake."))
    with tbl.transaction() as tx:
        tx.delete(GreaterThanOrEqual("minute", w0.astimezone(UTC).isoformat()))
        tx.append(data)
