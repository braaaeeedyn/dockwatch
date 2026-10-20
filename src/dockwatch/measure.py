"""Local measurements for docs/METRICS.md (Loop O), read from Trino. Stdlib only, plus dockwatch.config.

  python -m dockwatch.measure window <table> <from> <to>   latency, micro-batches and rate of one window
  python -m dockwatch.measure files <table> [--runs N]      data files, and the median time of one fixed query

window (docs/METRICS.md "How it was measured"): the rows of <table> with processed_ts in [from, to). A micro-batch is
one distinct processed_ts (Spark's batch timestamp). fetch -> silver = processed_ts - fetched_ts per row; each batch
is paired with the table's first unused `append` snapshot committed after it whose added-records equals the batch's
rows (snapshots committed in [from, to + 15 min]); fetch -> queryable = committed_at - fetched_ts per row; batch
duration = committed_at - processed_ts. Percentiles are nearest-rank over (value, count) groups. Sustained = every
batch paired and finished within the trigger.

Each command prints one JSON line, saves it under data/metrics/ and exits 1 on a Trino error.
"""

import argparse
import json
import math
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dockwatch.config import get_settings

METRICS_DIR = Path("data/metrics")
PAIR_SLACK = timedelta(minutes=15)  # a batch that starts just before `to` commits after it
FILES_QUERY = "SELECT station_id, count(*) AS n, max(snapshot_ts) AS newest FROM {table} GROUP BY station_id"


class TrinoError(RuntimeError):
    pass


def _fetch(req: urllib.request.Request) -> dict:
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=300) as r:  # noqa: S310 - the configured local Trino URL
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code not in (502, 503, 504) or attempt == 4:  # Trino's protocol: retry these
                raise
            time.sleep(0.1)
    raise AssertionError("unreachable")


def trino(sql: str, url: str | None = None, user: str = "dockwatch-measure") -> tuple[list[dict], dict]:
    """One statement through Trino's HTTP API: POST /v1/statement, follow nextUri. Returns (rows, final stats)."""
    base = (url or get_settings().trino_url).rstrip("/")
    headers = {"X-Trino-User": user}
    doc = _fetch(urllib.request.Request(f"{base}/v1/statement", data=sql.encode(), headers=headers, method="POST"))
    columns, data = None, []
    while True:
        if doc.get("error"):
            raise TrinoError(doc["error"].get("message", str(doc["error"])))
        if columns is None and doc.get("columns"):
            columns = [c["name"] for c in doc["columns"]]
        data += doc.get("data") or []
        if not doc.get("nextUri"):
            return [dict(zip(columns or [], row, strict=True)) for row in data], doc.get("stats") or {}
        doc = _fetch(urllib.request.Request(doc["nextUri"], headers=headers))


def utc(text: str) -> datetime:
    """Trino text ('2026-10-20 00:39:47.913 UTC', '2026-10-20 00:39:30.660000') or ISO 'Z' -> aware UTC."""
    d = datetime.fromisoformat(str(text).strip().replace(" UTC", "+00:00").replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=UTC)


def sql_ts(iso: str) -> str:
    return f"TIMESTAMP '{utc(iso):%Y-%m-%d %H:%M:%S} UTC'"


def meta(table: str, kind: str) -> str:
    """lake.silver.station_status -> lake.silver."station_status$files"."""
    cat, schema, name = table.split(".")
    return f'{cat}.{schema}."{name}${kind}"'


def nearest_rank(groups: list[tuple[float, int]], q: float) -> float | None:
    """The value at rank ceil(q * N) in ascending order, N = the sum of the counts."""
    total = sum(n for _, n in groups)
    if not total:
        return None
    rank, seen = max(1, math.ceil(q * total)), 0
    for value, n in sorted(groups):
        seen += n
        if seen >= rank:
            return value
    return None


def summary(groups: list[tuple[float, int]]) -> dict:
    out = {
        "p50": nearest_rank(groups, 0.5),
        "p95": nearest_rank(groups, 0.95),
        "max": max((v for v, n in groups if n), default=None),
    }
    return {k: None if v is None else round(v, 3) for k, v in out.items()}


def pair(batches: dict[datetime, int], snapshots: list[tuple[datetime, int | None]]) -> dict[datetime, datetime]:
    """Batch start -> commit time: each batch, in time order, takes the first unused snapshot committed after it
    whose added-records equals its rows (no-data snapshots have no added-records and never pair)."""
    commit, cur = {}, 0
    for p in sorted(batches):
        for j in range(cur, len(snapshots)):
            if snapshots[j][0] > p and snapshots[j][1] == batches[p]:
                commit[p], cur = snapshots[j][0], j + 1
                break
    return commit


def window_report(groups, snapshots, kafka, frm: str, to: str, trigger_s: int) -> dict:
    """groups: (processed_ts, fetched_ts, rows); snapshots: (committed_at, added_records) in commit order; kafka:
    (min kafka_ts, max kafka_ts, fetch-to-Kafka p50 s)."""
    batches: dict[datetime, int] = {}
    for p, _, n in groups:
        batches[p] = batches.get(p, 0) + n
    lo, hi = utc(frm), utc(to) + PAIR_SLACK
    commit = pair(batches, [(c, a) for c, a in snapshots if lo <= c <= hi])
    rows = sum(batches.values())
    k_lo, k_hi, k_p50 = kafka
    span = (k_hi - k_lo).total_seconds() if k_lo and k_hi else 0
    batch_s = summary([((c - p).total_seconds(), 1) for p, c in commit.items()])
    unpaired = len(batches) - len(commit)
    return {
        "rows": rows,
        "micro_batches": len(batches),
        "events_per_min": round(rows * 60 / span, 1) if span > 0 else None,
        "latency_s": summary([((p - f).total_seconds(), n) for p, f, n in groups]),
        "commit_latency_s": summary([((commit[p] - f).total_seconds(), n) for p, f, n in groups if p in commit]),
        "batch_s": batch_s,
        "fetch_to_kafka_p50_s": None if k_p50 is None else round(float(k_p50), 3),
        "unpaired_batches": unpaired,
        "trigger_s": trigger_s,
        "sustained": unpaired == 0 and bool(commit) and batch_s["max"] < trigger_s,
    }


def window(table: str, frm: str, to: str, q=trino) -> dict:
    where = f"processed_ts >= {sql_ts(frm)} AND processed_ts < {sql_ts(to)}"
    groups, _ = q(
        f"SELECT CAST(processed_ts AS varchar) AS p, CAST(fetched_ts AS varchar) AS f, count(*) AS n "
        f"FROM {table} WHERE {where} GROUP BY 1, 2"
    )
    (k,), _ = q(
        f"SELECT CAST(min(kafka_ts) AS varchar) AS lo, CAST(max(kafka_ts) AS varchar) AS hi, "
        f"approx_percentile(to_milliseconds(kafka_ts - fetched_ts) / 1000.0, 0.5) AS fk "
        f"FROM {table} WHERE {where}"
    )
    snaps, _ = q(
        f"SELECT CAST(committed_at AS varchar) AS c, element_at(summary, 'added-records') AS a "
        f"FROM {meta(table, 'snapshots')} WHERE operation = 'append' AND "
        f"element_at(summary, 'added-records') IS NOT NULL ORDER BY committed_at"
    )
    report = window_report(
        [(utc(r["p"]), utc(r["f"]), int(r["n"])) for r in groups],
        [(utc(r["c"]), None if r["a"] is None else int(r["a"])) for r in snaps],
        (k["lo"] and utc(k["lo"]), k["hi"] and utc(k["hi"]), k["fk"]),
        frm,
        to,
        get_settings().trigger_seconds,
    )
    return {"table": table, "from": frm, "to": to, **report}


def files(table: str, runs: int = 5, q=trino) -> dict:
    """Files / bytes / partitions of `table`, then FILES_QUERY timed: one discarded warm-up run + `runs` runs, the
    median of Trino's own elapsedTimeMillis."""
    at = datetime.now(UTC).isoformat(timespec="seconds")
    (f,), _ = q(f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes FROM {meta(table, 'files')}")
    (p,), _ = q(f"SELECT count(*) AS partitions FROM {meta(table, 'partitions')}")
    snap, _ = q(f"SELECT snapshot_id FROM {meta(table, 'snapshots')} ORDER BY committed_at DESC LIMIT 1")
    sql = FILES_QUERY.format(table=table)
    timed = [int(q(sql)[1].get("elapsedTimeMillis", 0)) for _ in range(runs + 1)]
    return {
        "table": table,
        "at": at,
        "files": int(f["files"]),
        "bytes": int(f["bytes"]),
        "partitions": int(p["partitions"]),
        "snapshot_id": snap[0]["snapshot_id"] if snap else None,
        "query": sql,
        "warmup_ms": timed[0],
        "runs_ms": timed[1:],
        "median_ms": statistics.median(timed[1:]),
    }


def save(kind: str, doc: dict) -> Path:
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    path = METRICS_DIR / f"{kind}-{doc['table']}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dockwatch.measure", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("window", help="latency / micro-batches / rate of processed_ts in [from, to)")
    w.add_argument("table")
    w.add_argument("frm", metavar="from", help="YYYY-MM-DDTHH:MM:SSZ")
    w.add_argument("to", help="YYYY-MM-DDTHH:MM:SSZ")
    f = sub.add_parser("files", help="data files and the median time of one query")
    f.add_argument("table")
    f.add_argument("--runs", type=int, default=5)
    args = parser.parse_args(argv)
    try:
        doc = window(args.table, args.frm, args.to) if args.cmd == "window" else files(args.table, args.runs)
    except (TrinoError, OSError) as e:  # OSError covers URLError / HTTPError
        print(f"measure {args.cmd}: Trino error: {e}", file=sys.stderr)
        sys.exit(1)
    path = save(args.cmd, doc)
    print(json.dumps(doc, default=str))
    print(f"saved {path}", file=sys.stderr)


if __name__ == "__main__":
    main()
