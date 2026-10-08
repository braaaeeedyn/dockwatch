"""How the streaming job runs (pyspark-free so host tests can cover it).

processing_time: the long-running job (python tasks.py stream), one micro-batch every `trigger_seconds`.
available_now:   catch-up (python tasks.py catchup) processes everything in Kafka up to now, in batches bounded by
                 maxOffsetsPerTrigger, then every query stops and the process exits.
"""


def trigger_options(s) -> dict:
    """Keyword arguments for DataStreamWriter.trigger()."""
    if s.trigger_mode == "available_now":
        return {"availableNow": True}
    return {"processingTime": f"{s.trigger_seconds} seconds"}
