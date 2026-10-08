import json

import httpx

from dockwatch.gbfs.client import FetchedFeed
from dockwatch.gbfs.models import Envelope
from dockwatch.producer.archive import FsArchive
from dockwatch.producer.poller import ERROR_BACKOFF_S, MIN_INTERVAL_S, Poller

TOPICS = {"station_status": "status", "station_information": "info"}


class FakeClient:
    def __init__(self, docs: dict[str, dict]) -> None:
        self.docs = docs
        self.fail = False

    def fetch(self, name: str) -> FetchedFeed:
        if self.fail:
            raise httpx.ConnectError("offline")
        doc = self.docs[name]
        return FetchedFeed(name, f"u/{name}", Envelope.model_validate(doc), json.dumps(doc).encode())


class Clock:
    def __init__(self) -> None:
        self.t = 1_000_000.0

    def __call__(self) -> float:
        return self.t


def _poller(status_doc, info_doc, publisher, archive=None):
    clock = Clock()
    client = FakeClient({"station_status": status_doc, "station_information": info_doc})
    poller = Poller(client, publisher, archive, TOPICS, "dlq", clock=clock)
    return poller, client, clock


def test_publishes_archives_and_schedules_by_ttl(status_doc, info_doc, publisher, tmp_path):
    poller, _, clock = _poller(status_doc, info_doc, publisher, FsArchive(tmp_path))
    stats = poller.poll_feed("station_status")
    assert stats.messages == len(status_doc["data"]["stations"])
    assert len(publisher.on("status")) == stats.messages
    assert poller.next_due["station_status"] == clock.t + max(status_doc["ttl"], MIN_INTERVAL_S)
    assert len(FsArchive(tmp_path).list_keys("raw/gbfs/station_status/")) == 1


def test_same_snapshot_is_not_republished(status_doc, info_doc, publisher):
    poller, _, _ = _poller(status_doc, info_doc, publisher)
    poller.poll_feed("station_status")
    again = poller.poll_feed("station_status")
    assert again.skipped == "same snapshot"
    assert len(publisher.on("status")) == len(status_doc["data"]["stations"])


def test_unchanged_station_information_is_skipped_even_with_new_timestamp(status_doc, info_doc, publisher):
    poller, client, _ = _poller(status_doc, info_doc, publisher)
    poller.poll_feed("station_information")
    client.docs["station_information"] = {**info_doc, "last_updated": info_doc["last_updated"] + 60}
    assert poller.poll_feed("station_information").skipped == "unchanged"
    assert len(publisher.on("info")) == len(info_doc["data"]["stations"])


def test_fetch_error_backs_off_and_keeps_running(status_doc, info_doc, publisher):
    poller, client, clock = _poller(status_doc, info_doc, publisher)
    client.fail = True
    assert poller.poll_feed("station_status").skipped == "error"
    assert poller.next_due["station_status"] == clock.t + ERROR_BACKOFF_S
    assert not publisher.sent


def test_rejects_go_to_dlq(status_doc, info_doc, publisher):
    status_doc["data"]["stations"][0]["num_docks_available"] = "lots"
    poller, _, _ = _poller(status_doc, info_doc, publisher)
    poller.poll_feed("station_status")
    assert len(publisher.on("dlq")) == 1


def test_run_polls_due_feeds_until_stopped(status_doc, info_doc, publisher):
    poller, _, clock = _poller(status_doc, info_doc, publisher)
    loops = {"n": 0}

    def sleep(s: float) -> None:
        clock.t += s

    def should_stop() -> bool:
        loops["n"] += 1
        return loops["n"] > 3

    poller.run(should_stop, sleep=sleep)
    assert publisher.on("status")
    assert publisher.on("info")
