"""Kafka (Redpanda) plumbing: topic definitions, an idempotent producer, and a small publisher interface."""

import logging
from dataclasses import dataclass
from typing import Protocol

log = logging.getLogger(__name__)

WEEK_MS = 7 * 24 * 3600 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    config: dict[str, str]


def topic_specs(settings) -> list[TopicSpec]:
    return [
        # Keyed by station_id, so each station's snapshots stay in order within one partition.
        TopicSpec(settings.topic_station_status, 6, {"retention.ms": str(WEEK_MS)}),
        # Compacted: the latest station_information per station is kept forever.
        TopicSpec(settings.topic_station_information, 1, {"cleanup.policy": "compact"}),
        TopicSpec(settings.topic_dlq, 1, {"retention.ms": str(4 * WEEK_MS)}),
        TopicSpec(f"{settings.topic_station_status}.replay", 6, {"retention.ms": str(WEEK_MS)}),
        # Written by the Spark episodes query (M2): alerts keyed by alert_id, latest state per station (compacted).
        TopicSpec(settings.topic_alerts, 1, {"retention.ms": str(4 * WEEK_MS)}),
        TopicSpec(settings.topic_station_state, 1, {"cleanup.policy": "compact"}),
    ]


class Publisher(Protocol):
    def publish(self, topic: str, key: str, value: bytes) -> None: ...
    def flush(self, timeout: float = 30.0) -> int: ...


class KafkaPublisher:
    def __init__(self, bootstrap: str, client_id: str = "dockwatch-producer") -> None:
        from confluent_kafka import Producer

        self.failed = 0
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": client_id,
                "enable.idempotence": True,  # implies acks=all and safe retries: no duplicates from retries
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 50,
            }
        )

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed for %s[%s]: %s", msg.topic(), msg.key(), err)

    def publish(self, topic: str, key: str, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(topic, key=key.encode(), value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(0.5)  # local queue full: let deliveries drain, then retry
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        return self._producer.flush(timeout)


def create_topics(settings) -> list[str]:
    """Create any missing topics; returns the names created."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [
        NewTopic(spec.name, num_partitions=spec.partitions, replication_factor=1, config=spec.config)
        for spec in topic_specs(settings)
        if spec.name not in existing
    ]
    if not new:
        return []
    for name, future in admin.create_topics(new).items():
        future.result()
        log.info("created topic %s", name)
    return [t.topic for t in new]
