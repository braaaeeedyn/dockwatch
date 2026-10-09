"""Debezium envelopes -> rows for the Iceberg MERGE. Pure (no Spark), Python 3.10-safe: runs in the Spark image.

Messages are Kafka Connect JSON with schemas (key and value): the table comes from the topic (ops.public.<table>),
the primary key from the key schema, the column types from the value schema's `after` / `before` struct. A delete
takes its key from `before` (REPLICA IDENTITY FULL, so `before` is the whole old row). Within a batch the newest
event per key wins, ordered by (source.lsn, Kafka offset). New Postgres columns are added to Iceberg before the
MERGE (add-only); a type change or an unknown Connect type fails loudly.
"""

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

UTC = timezone.utc  # noqa: UP017 - datetime.UTC is 3.11+, and this module runs on the Spark image's 3.10

# Connect schema (type, semantic name) -> Iceberg / Spark SQL type. Postgres smallint arrives as int16.
SEMANTIC_TYPES = {
    "io.debezium.time.ZonedTimestamp": "timestamp",
    "io.debezium.time.MicroTimestamp": "timestamp",
}
PLAIN_TYPES = {
    "int16": "int",
    "int32": "int",
    "int64": "bigint",
    "float64": "double",
    "double": "double",  # JsonConverter writes Connect's FLOAT64 as "double"
    "boolean": "boolean",
    "string": "string",
}
# CDC metadata columns on every lake.ops.* table (excluded from checksums: they start with `_`).
METADATA = (
    ("_lsn", "bigint"),
    ("_op", "string"),
    ("_source_ts", "timestamp"),
    ("_kafka_offset", "bigint"),
    ("_applied_ts", "timestamp"),
)
OPS = ("c", "u", "d", "r")


class SchemaError(ValueError):
    """A Connect type the apply does not know, or a column whose type changed."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # Spark SQL type name: int, bigint, double, boolean, string, timestamp
    semantic: str | None = None  # Connect schema name (io.debezium.time.*), used to decode the value


@dataclass
class Event:
    table: str
    op: str  # c / u / d / r
    lsn: int
    offset: int
    key: tuple  # primary-key values (from `before` for a delete)
    key_names: tuple  # primary-key columns, from the key schema
    row: dict  # decoded values: `after`, or `before` for a delete
    columns: list[Column] = field(default_factory=list)
    source_ts: datetime | None = None


def table_of(topic: str) -> str:
    """ops.public.rebalancing_jobs -> rebalancing_jobs."""
    return topic.rsplit(".", 1)[-1]


def iceberg_type(field_schema: dict) -> Column:
    name = field_schema.get("field", "")
    semantic = field_schema.get("name")
    if semantic in SEMANTIC_TYPES:
        return Column(name, SEMANTIC_TYPES[semantic], semantic)
    if semantic:
        raise SchemaError(f"column {name}: Connect type {semantic} ({field_schema.get('type')}) is not supported")
    t = field_schema.get("type")
    if t not in PLAIN_TYPES:
        raise SchemaError(f"column {name}: Connect type {t} is not supported")
    return Column(name, PLAIN_TYPES[t])


def row_columns(value_schema: dict) -> list[Column]:
    """The table's columns, in Postgres ordinal order, from the envelope's `after` (or `before`) struct."""
    for wanted in ("after", "before"):
        for f in value_schema.get("fields", []):
            if f.get("field") == wanted and f.get("type") == "struct":
                return [iceberg_type(c) for c in f["fields"]]
    raise SchemaError(f"value schema {value_schema.get('name')} has no before / after struct")


def key_fields(key_schema: dict) -> list[str]:
    fields = [f["field"] for f in key_schema.get("fields", [])]
    if not fields:
        raise SchemaError(f"key schema {key_schema.get('name')} has no fields (a table without a primary key?)")
    return fields


_TS = re.compile(r"(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$")


def parse_zoned(text: str) -> datetime:
    """Debezium ZonedTimestamp (ISO-8601, trailing zeros of the fraction trimmed, usually `Z`) -> aware UTC."""
    m = _TS.match(text)
    if not m:
        raise SchemaError(f"not a ZonedTimestamp: {text!r}")
    day, clock, frac, zone = m.groups()
    micros = int(((frac or "") + "000000")[:6])
    ts = datetime.fromisoformat(f"{day}T{clock}").replace(microsecond=micros)
    offset = timedelta(0) if zone == "Z" else timedelta(hours=int(zone[1:3]), minutes=int(zone[4:6]))
    if zone.startswith("-"):
        offset = -offset
    return (ts - offset).replace(tzinfo=UTC)


def decode(value, column: Column):
    if value is None:
        return None
    if column.semantic == "io.debezium.time.ZonedTimestamp":
        return parse_zoned(value)
    if column.semantic == "io.debezium.time.MicroTimestamp":
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=int(value))
    if column.type in ("int", "bigint"):
        return int(value)
    if column.type == "double":
        return float(value)
    if column.type == "boolean":
        return bool(value)
    return str(value)


def parse_event(topic: str, offset: int, key: bytes | str | None, value: bytes | str | None) -> Event | None:
    """One Kafka record -> Event; None for a tombstone (tombstones.on.delete is off, but be safe)."""
    if value is None:
        return None
    env = json.loads(value)
    payload, schema = env.get("payload"), env.get("schema")
    if payload is None:
        return None
    if schema is None:
        raise SchemaError(f"{topic}@{offset}: message has no schema (value.converter.schemas.enable must be true)")
    op = payload["op"]
    if op not in OPS:
        raise SchemaError(f"{topic}@{offset}: unsupported op {op!r} (truncate is not handled)")
    columns = row_columns(schema)
    image = payload["before"] if op == "d" else payload["after"]
    if image is None:
        raise SchemaError(f"{topic}@{offset}: op {op} without a {'before' if op == 'd' else 'after'} image")
    types = {c.name: c for c in columns}
    row = {c.name: decode(image.get(c.name), c) for c in columns}
    kschema = json.loads(key)["schema"] if key is not None else None
    keys = key_fields(kschema) if kschema else []
    source = payload.get("source") or {}
    ts_ms = source.get("ts_ms")
    return Event(
        table=table_of(topic),
        op=op,
        lsn=int(source["lsn"]),
        offset=int(offset),
        key=tuple(decode(image.get(k), types[k]) for k in keys),
        key_names=tuple(keys),
        row=row,
        columns=columns,
        source_ts=datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=ts_ms) if ts_ms is not None else None,
    )


def latest_per_key(events: list[Event]) -> list[Event]:
    """The newest event per primary key, by (lsn, Kafka offset). Older events in the batch are dropped."""
    best: dict[tuple, Event] = {}
    for ev in events:
        cur = best.get(ev.key)
        if cur is None or (ev.lsn, ev.offset) > (cur.lsn, cur.offset):
            best[ev.key] = ev
    return sorted(best.values(), key=lambda e: (e.lsn, e.offset))


def merged_columns(events: list[Event]) -> list[Column]:
    """Every column seen in the batch, oldest schema first (a column added mid-batch comes last)."""
    out: dict[str, Column] = {}
    for ev in sorted(events, key=lambda e: (e.lsn, e.offset)):
        for c in ev.columns:
            seen = out.get(c.name)
            if seen is None:
                out[c.name] = c
            elif seen.type != c.type:
                raise SchemaError(f"{ev.table}.{c.name}: type changed from {seen.type} to {c.type} within a batch")
    return list(out.values())


def plan_evolution(existing: list[tuple[str, str]], incoming: list[Column]) -> list[Column]:
    """Columns to add to the Iceberg table before the MERGE. Add-only: a type change is refused."""
    have = {name: t for name, t in existing}
    adds = []
    for c in incoming:
        if c.name not in have:
            adds.append(c)
        elif have[c.name] != c.type:
            raise SchemaError(
                f"column {c.name}: Postgres type is now {c.type}, Iceberg has {have[c.name]}; type changes are not "
                "applied automatically"
            )
    return adds


def q(name: str) -> str:
    return f"`{name}`"


def create_table_sql(target: str, columns: list[Column]) -> str:
    cols = ", ".join(f"{q(c.name)} {c.type}" for c in columns)
    meta = ", ".join(f"{n} {t}" for n, t in METADATA)
    return f"CREATE TABLE IF NOT EXISTS {target} ({cols}, {meta}) USING iceberg TBLPROPERTIES ('format-version'='2')"


def add_column_sql(target: str, column: Column) -> str:
    return f"ALTER TABLE {target} ADD COLUMN {q(column.name)} {column.type}"


def merge_sql(target: str, source: str, keys: list[str], columns: list[str]) -> str:
    """MERGE with the stale-event guard: only a newer LSN updates or deletes; a delete never inserts."""
    every = [*columns, *(n for n, _ in METADATA)]
    on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
    sets = ", ".join(f"t.{q(c)} = s.{q(c)}" for c in every)
    names = ", ".join(q(c) for c in every)
    values = ", ".join(f"s.{q(c)}" for c in every)
    return (
        f"MERGE INTO {target} t USING {source} s ON {on}\n"
        "WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd' THEN DELETE\n"
        f"WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET {sets}\n"
        f"WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT ({names}) VALUES ({values})"
    )


def source_rows(events: list[Event], columns: list[str], applied: datetime) -> list[tuple]:
    """MERGE source rows: the table's columns (None where an older event lacks one) + the metadata columns."""
    return [(*(ev.row.get(c) for c in columns), ev.lsn, ev.op, ev.source_ts, ev.offset, applied) for ev in events]


def spark_schema(columns: list[tuple[str, str]]) -> str:
    """DDL schema string for spark.createDataFrame."""
    return ", ".join(f"{q(n)} {t}" for n, t in [*columns, *METADATA])
