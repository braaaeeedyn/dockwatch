"""The GBFS poll loop: fetch each feed when its ttl expires, archive it, and publish per-station messages."""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from dockwatch.gbfs.client import FetchedFeed, GbfsClient
from dockwatch.producer.archive import Archive, archive_key
from dockwatch.producer.kafka import Publisher
from dockwatch.producer.messages import (
    Exploded,
    content_hash,
    encode,
    explode_station_information,
    explode_station_status,
)

log = logging.getLogger(__name__)

MIN_INTERVAL_S = 15.0  # never poll faster than this, whatever ttl says
ERROR_BACKOFF_S = 30.0

EXPLODERS: dict[str, Callable] = {
    "station_status": explode_station_status,
    "station_information": explode_station_information,
}


@dataclass
class TickStats:
    feed: str
    skipped: str | None = None  # why nothing was published ("unchanged", "same snapshot", "error")
    messages: int = 0
    rejects: int = 0


@dataclass
class Poller:
    client: GbfsClient
    publisher: Publisher
    archive: Archive | None
    topics: dict[str, str]  # feed name -> topic
    dlq_topic: str
    clock: Callable[[], float] = time.time
    next_due: dict[str, float] = field(default_factory=dict)
    last_updated: dict[str, int] = field(default_factory=dict)
    last_hash: dict[str, str] = field(default_factory=dict)

    def poll_feed(self, feed: str) -> TickStats:
        stats = TickStats(feed)
        now = self.clock()
        try:
            fetched: FetchedFeed = self.client.fetch(feed)
        except Exception as exc:  # network, HTTP, JSON: log, back off, keep running
            log.warning("fetch %s failed: %s", feed, exc)
            self.next_due[feed] = now + ERROR_BACKOFF_S
            stats.skipped = "error"
            return stats

        env = fetched.envelope
        self.next_due[feed] = now + max(float(env.ttl), MIN_INTERVAL_S)
        if self.last_updated.get(feed) == env.last_updated:
            stats.skipped = "same snapshot"
            return stats
        self.last_updated[feed] = env.last_updated

        if self.archive is not None:
            try:
                self.archive.put(archive_key(feed, env.last_updated), fetched.raw)
            except Exception as exc:  # the archive must never stop the live stream
                log.error("archive write failed for %s: %s", feed, exc)

        if feed == "station_information":
            digest = content_hash(env)
            if self.last_hash.get(feed) == digest:
                stats.skipped = "unchanged"
                return stats
            self.last_hash[feed] = digest

        exploded: Exploded = EXPLODERS[feed](env, now)
        for msg in exploded.messages:
            self.publisher.publish(self.topics[feed], msg.key, encode(msg.value))
        for msg in exploded.rejects:
            self.publisher.publish(self.dlq_topic, msg.key, encode(msg.value))
        stats.messages, stats.rejects = len(exploded.messages), len(exploded.rejects)
        log.info("%s @%s: %d messages, %d rejects", feed, env.last_updated, stats.messages, stats.rejects)
        return stats

    def due_feeds(self) -> list[str]:
        now = self.clock()
        return [feed for feed in self.topics if self.next_due.get(feed, 0.0) <= now]

    def seconds_until_next(self) -> float:
        now = self.clock()
        return max(0.0, min((self.next_due.get(f, 0.0) for f in self.topics), default=0.0) - now)

    def run(self, should_stop: Callable[[], bool], sleep: Callable[[float], None] = time.sleep) -> None:
        while not should_stop():
            for feed in self.due_feeds():
                self.poll_feed(feed)
            self.publisher.flush(10.0)
            # Sleep in short steps so Ctrl+C / SIGTERM is handled quickly.
            remaining = self.seconds_until_next()
            while remaining > 0 and not should_stop():
                step = min(remaining, 1.0)
                sleep(step)
                remaining -= step
        self.publisher.flush(30.0)
