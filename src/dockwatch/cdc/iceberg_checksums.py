"""Iceberg side of cdc-verify: rows, columns and checksum of every lake.ops.* table. Spark, Python 3.10.

    spark-submit .../cdc/iceberg_checksums.py --columns <base64 JSON {table: [postgres columns in order]}>
                                              [--nonnull rebalancing_jobs.priority]

The host passes Postgres's column lists so both sides hash the same columns in the same order (cdc/checksum.py).
Tables are small, so rows are collected to the driver. Prints one line `CDC_ICEBERG {json}`.
"""

import argparse
import base64
import json
import sys

from dockwatch.cdc.checksum import table_checksum


def table_report(spark, target: str, columns: list[str]) -> dict:
    from pyspark.sql.utils import AnalysisException

    try:
        df = spark.table(target)
    except AnalysisException:
        return {"exists": False, "iceberg_rows": 0, "iceberg_columns": [], "iceberg_checksum": None}
    have = df.columns
    missing = [c for c in columns if c not in have]
    out = {"exists": True, "iceberg_columns": have, "missing_columns": missing}
    if missing:
        out |= {"iceberg_rows": df.count(), "iceberg_checksum": None}
        return out
    rows = [tuple(r) for r in df.select(*columns).collect()]
    out |= {"iceberg_rows": len(rows), "iceberg_checksum": table_checksum(rows)}
    return out


def main(argv: list[str] | None = None) -> None:
    from dockwatch.cdc.apply import build_session

    p = argparse.ArgumentParser(prog="cdc/iceberg_checksums.py")
    p.add_argument("--columns", required=True, help="base64 of JSON {table: [columns]}")
    p.add_argument("--nonnull", action="append", default=[], help="table.column to count non-null values of")
    args = p.parse_args(argv)
    columns = json.loads(base64.urlsafe_b64decode(args.columns.encode("ascii")))
    s, spark = build_session()
    ns = f"{s.iceberg_catalog}.{s.cdc_namespace}"
    tables = {t: table_report(spark, f"{ns}.{t}", cols) for t, cols in sorted(columns.items())}
    nonnull = {}
    for spec in args.nonnull:
        table, column = spec.split(".", 1)
        info = tables.get(table) or {}
        if column in info.get("iceberg_columns", []):
            nonnull[spec] = spark.table(f"{ns}.{table}").where(f"`{column}` IS NOT NULL").count()
        else:
            nonnull[spec] = None
    spark.stop()
    print("CDC_ICEBERG " + json.dumps({"tables": tables, "nonnull": nonnull}, sort_keys=True), flush=True)


if __name__ == "__main__":
    sys.exit(main())
