"""Great Expectations runner (Loop L, M5), run by the GE venv in the Airflow image (marts_daily's quality_gate task):

  /opt/gx-venv/bin/python -m dockwatch.batch.gx_run trino --run-id ID --window-end ISO --out .../expectations.json
  /opt/gx-venv/bin/python -m dockwatch.batch.gx_run frame --frames IN.json --out OUT.json   (tests: same suite, pandas)

An ephemeral GE context, an SQL datasource on Trino (DOCKWATCH_TRINO_URL, catalog lake) without temp tables, the query
asset from dockwatch.batch.expectations, its suite, a ValidationDefinition and the Checkpoint marts_daily_quality
(result format SUMMARY). Imports great_expectations, trino (through SQLAlchemy) and stdlib-only dockwatch.batch
modules, never dockwatch.config: the GE venv has no pydantic-settings.
"""

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import great_expectations as gx
from great_expectations.data_context.types.base import ProgressBarsConfig
from great_expectations.expectations.registry import get_expectation_impl

from dockwatch.batch import expectations, marts


def ephemeral():
    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = ProgressBarsConfig(globally=False)  # no tqdm bars in the task log
    return context


def checkpoint(context, batch_definition):
    suite = gx.ExpectationSuite(name=expectations.SUITE)
    for kind, key, kwargs, *_ in expectations.EXPECTATIONS:
        suite.add_expectation(get_expectation_impl(kind)(**expectations.gx_kwargs(kind, key, kwargs)))
    vd = gx.ValidationDefinition(name=expectations.SUITE, data=batch_definition, suite=context.suites.add(suite))
    cp = gx.Checkpoint(name=expectations.CHECKPOINT, validation_definitions=[context.validation_definitions.add(vd)],
                       result_format={"result_format": "SUMMARY"})  # fmt: skip
    return context.checkpoints.add(cp)


def raw_results(result) -> list[dict]:
    """A CheckpointResult -> [{type, kwargs, success, result}] as plain JSON values."""
    out = []
    for run in result.run_results.values():
        for r in run.results:
            d = r.to_json_dict()
            cfg = d["expectation_config"]
            out.append({"type": cfg["type"], "kwargs": cfg["kwargs"], "success": d["success"],
                        "result": d.get("result") or {}})  # fmt: skip
    return out


def reflect_trino_queries() -> None:
    """GE 1.24 finds no columns in a Trino query asset (SQLAlchemy's TextClause has no selected_columns, and GE's
    Trino fallback looks the query's FROM table up in information_schema: wrong schema, no derived columns), so every
    column expectation fails with "column does not exist". Read the names from the query itself (LIMIT 0: planned,
    not scanned)."""
    import sqlalchemy as sa
    from great_expectations.expectations.metrics import util

    fallback = util.column_reflection_fallback

    def columns(selectable, dialect, sqlalchemy_engine, schema_name=None):
        if dialect.name != "trino" or not isinstance(selectable, sa.TextClause):
            return fallback(selectable, dialect, sqlalchemy_engine, schema_name)
        with sqlalchemy_engine.connect() as conn:
            result = conn.execute(sa.text(f"SELECT * FROM ({selectable.text}) LIMIT 0"))
            return [{"name": k} for k in result.keys()]  # noqa: SIM118 - a Result, not a dict

    # ponytail: patches GE 1.24's module global; re-check on every GE upgrade and drop once GE reflects query assets
    util.column_reflection_fallback = columns


def run_trino(args) -> dict:
    t0, now = time.monotonic(), datetime.now(UTC)
    start, end = expectations.window(datetime.fromisoformat(args.window_end))
    u = urlparse(os.environ["DOCKWATCH_TRINO_URL"])
    reflect_trino_queries()
    context = ephemeral()
    source = context.data_sources.add_sql(
        "lake", connection_string=f"trino://dockwatch@{u.hostname}:{u.port or 8080}/lake", create_temp_table=False
    )
    asset = source.add_query_asset("silver_newest_24h", query=expectations.query(start, end, now))
    result = checkpoint(context, asset.add_batch_definition_whole_table("batch")).run()
    rows = expectations.results(raw_results(result))
    return expectations.results_doc(rows, dag_run_id=args.run_id, run_at=now, gx_version=gx.__version__,
                                    seconds=time.monotonic() - t0, start=start, end=end, now=now)  # fmt: skip


def run_frames(args) -> dict:
    """{name: rows} -> {name: result rows}: the same suite on each small pandas DataFrame."""
    import pandas as pd

    frames = json.loads(Path(args.frames).read_text(encoding="utf-8"))
    context = ephemeral()
    asset = context.data_sources.add_pandas("frames").add_dataframe_asset("frame")
    cp = checkpoint(context, asset.add_batch_definition_whole_dataframe("batch"))
    return {name: expectations.results(raw_results(cp.run(batch_parameters={"dataframe": pd.DataFrame(rows)})))
            for name, rows in frames.items()}  # fmt: skip


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser(prog="gx_run")
    p.add_argument("mode", choices=("trino", "frame"))
    p.add_argument("--out", required=True)
    p.add_argument("--run-id")
    p.add_argument("--window-end", help="silver's newest snapshot_ts (ISO 8601 with offset)")
    p.add_argument("--frames")
    args = p.parse_args(argv)
    doc = run_trino(args) if args.mode == "trino" else run_frames(args)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    marts.write_atomic(out, json.dumps(doc, indent=1) + "\n")
    if args.mode == "trino":
        print(json.dumps({k: doc[k] for k in ("evaluated", "passed", "warnings", "failed", "seconds")}), flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
