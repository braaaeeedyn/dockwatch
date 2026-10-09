"""One table checksum for Postgres rows (host, psycopg) and Iceberg rows (Spark, collected). Pure, Python 3.10-safe.

Each value is canonicalised to text: None -> \\N, bool -> t / f, int -> str, float -> repr, Decimal -> normalised
plain string, datetime -> UTC ISO-8601 with microseconds (a naive datetime is taken as UTC: Spark collects
timestamps as naive UTC when TZ=UTC), str as is. A row is its values joined with \\x1f and hashed with sha256; the
table checksum is the sha256 of the sorted row hashes, so row order does not matter. Columns are Postgres's, in
ordinal order; `_`-prefixed CDC metadata columns on the Iceberg side are left out.
"""

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal

SEP = "\x1f"
NULL = "\\N"
UTC = timezone.utc  # noqa: UP017 - datetime.UTC is 3.11+, and this module also runs on the Spark image's 3.10


def canonical(value) -> str:
    if value is None:
        return NULL
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        utc = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        return utc.isoformat(timespec="microseconds")
    if isinstance(value, str):
        return value
    raise TypeError(f"no canonical form for {type(value).__name__}: {value!r}")


def row_hash(values: Sequence) -> str:
    return hashlib.sha256(SEP.join(canonical(v) for v in values).encode("utf-8")).hexdigest()


def table_checksum(rows: Iterable[Sequence]) -> str:
    return hashlib.sha256("\n".join(sorted(row_hash(r) for r in rows)).encode("utf-8")).hexdigest()


def data_columns(columns: Iterable[str]) -> list[str]:
    """Drop the `_`-prefixed CDC metadata columns (_lsn, _op, ...)."""
    return [c for c in columns if not c.startswith("_")]


def project(rows: Iterable[Mapping], columns: Sequence[str]) -> list[tuple]:
    """Rows as dicts -> tuples in `columns` order (KeyError if a column is missing)."""
    return [tuple(r[c] for c in columns) for r in rows]
