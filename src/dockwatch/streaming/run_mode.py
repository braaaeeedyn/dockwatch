"""How the streaming job runs (pyspark-free so host tests can cover it).

processing_time: the long-running job (python tasks.py stream), one micro-batch every `trigger_seconds`.
available_now:   catch-up (python tasks.py catchup) processes everything in Kafka up to now, in batches bounded by
                 maxOffsetsPerTrigger, then every query stops and the process exits.
lab mode:        (python tasks.py stream-lab) the same job on lake.lab_* tables, .lab topics and its own checkpoints.
"""

import re
from datetime import datetime

SPARK_LINEAGE_NAMESPACE = "dockwatch-spark"
# Production's checkpoint root and topics (the Settings defaults; a host test pins them). A lab run uses none of them.
PRODUCTION_CHECKPOINT_ROOT = "/checkpoints"
PRODUCTION_TOPIC_STATION_STATUS = "gbfs.station_status"
PRODUCTION_TOPIC_ALERTS = "gbfs.alerts"
PRODUCTION_TOPIC_STATION_STATE = "dockwatch.station_state"
LAB_PREFIX = re.compile(r"[a-z][a-z0-9]*_")


def trigger_options(s) -> dict:
    """Keyword arguments for DataStreamWriter.trigger()."""
    if s.trigger_mode == "available_now":
        return {"availableNow": True}
    return {"processingTime": f"{s.trigger_seconds} seconds"}


def lineage_conf(s, app: str, now: datetime) -> dict[str, str]:
    """Spark conf for the OpenLineage listener: one JSON-lines file per run under `s.spark_lineage_dir` (only
    catchup sets it); {} when it is empty."""
    if not s.spark_lineage_dir:
        return {}
    return {
        "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        "spark.openlineage.transport.type": "file",
        "spark.openlineage.transport.location": f"{s.spark_lineage_dir}/spark-{app}-{now:%Y%m%dT%H%M%SZ}.jsonl",
        "spark.openlineage.namespace": SPARK_LINEAGE_NAMESPACE,
    }


def table(s, name: str) -> str:
    """Full Iceberg name of `name` ("silver.station_status"): lake.silver.station_status in production,
    lake.lab_silver.station_status with namespace_prefix "lab_" (Loop O's lab mode)."""
    return f"{s.iceberg_catalog}.{s.namespace_prefix}{name}"


def lab_refusal(s) -> str | None:
    """None if the streaming job may start with these settings, else why not. Production (no prefix) must read from
    the earliest offset; a lab run (a prefix) must have a safe prefix and none of production's outputs."""
    if not s.namespace_prefix:
        if s.starting_offsets != "earliest":
            return "production must read gbfs.station_status from the earliest offset: latest would skip the backlog"
        return None
    if not LAB_PREFIX.fullmatch(s.namespace_prefix):
        return f"namespace_prefix {s.namespace_prefix!r} must match ^[a-z][a-z0-9]*_$"
    production = {
        "checkpoint_root": PRODUCTION_CHECKPOINT_ROOT,
        "topic_station_status": PRODUCTION_TOPIC_STATION_STATUS,
        "topic_alerts": PRODUCTION_TOPIC_ALERTS,
        "topic_station_state": PRODUCTION_TOPIC_STATION_STATE,
    }
    clash = [k for k, v in production.items() if str(getattr(s, k)).rstrip("/") == v]
    if clash:
        return f"a lab run (namespace_prefix {s.namespace_prefix!r}) must not use production's {', '.join(clash)}"
    return None
