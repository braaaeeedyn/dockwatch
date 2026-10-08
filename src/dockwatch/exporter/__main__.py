"""Exporter: keep the latest station state, station information and alerts from Kafka in memory, and write
web/data/live.json + alerts.json every minute. Run: python tasks.py export
"""

import json
import logging
import os
import signal
import tempfile
import time
import uuid
from pathlib import Path

from dockwatch.config import get_settings
from dockwatch.exporter.build import build_alerts, build_live

log = logging.getLogger("dockwatch.exporter")


def write_json(path: Path, doc: dict) -> None:
    """Write atomically, so the site never reads a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(doc, f, separators=(",", ":"))
    os.replace(tmp, path)


def main() -> None:
    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    s = get_settings()
    out = Path(s.export_dir)
    topics = [s.topic_station_state, s.topic_station_information, s.topic_alerts]
    # No committed offsets: each start rebuilds its in-memory view from the beginning (state topics are compacted).
    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": f"dockwatch-exporter-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    meta = consumer.list_topics(timeout=10)
    consumer.assign([TopicPartition(t, p, OFFSET_BEGINNING) for t in topics for p in meta.topics[t].partitions])

    states: dict[str, dict] = {}
    infos: dict[str, dict] = {}
    alerts: dict[str, dict] = {}
    target = {s.topic_station_state: states, s.topic_station_information: infos, s.topic_alerts: alerts}

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    next_write = time.time() + 5  # first write soon after catching up
    while not stop["flag"]:
        for msg in consumer.consume(num_messages=1000, timeout=1.0):
            if msg.error() or msg.key() is None:
                continue
            target[msg.topic()][msg.key().decode()] = json.loads(msg.value())
        if time.time() >= next_write:
            now = time.time()
            write_json(out / "live.json", build_live(states, infos, now))
            write_json(out / "alerts.json", build_alerts(alerts, infos, now))
            log.info("wrote %d stations, %d alerts", len(states), len(alerts))
            next_write = now + s.export_interval_s
    consumer.close()


if __name__ == "__main__":
    main()
