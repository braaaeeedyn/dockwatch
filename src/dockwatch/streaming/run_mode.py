"""How the streaming job runs (pyspark-free so host tests can cover it).

processing_time: the long-running job (python tasks.py stream), one micro-batch every `trigger_seconds`.
available_now:   catch-up (python tasks.py catchup) processes everything in Kafka up to now, in batches bounded by
                 maxOffsetsPerTrigger, then every query stops and the process exits.
"""

from datetime import datetime

SPARK_LINEAGE_NAMESPACE = "dockwatch-spark"


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
