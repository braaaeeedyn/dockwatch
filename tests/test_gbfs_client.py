import json

import httpx
import pytest

from dockwatch.gbfs.client import GbfsClient, feed_urls_from_discovery, pick_version_url
from dockwatch.gbfs.models import SystemRegion, dedupe_regions
from tests.conftest import load_fixture

ENTRY = "https://gbfs.example/gbfs.json"
V11 = "https://gbfs.example/1.1/gbfs.json"
V23 = "https://gbfs.example/2.3/gbfs.json"


def _discovery(version: str, base: str, extra: tuple[str, ...] = ()) -> dict:
    names = ("station_status", "station_information", "gbfs_versions", *extra)
    return {
        "last_updated": 1,
        "ttl": 60,
        "version": version,
        "data": {"en": {"feeds": [{"name": n, "url": f"{base}/en/{n}.json"} for n in names]}},
    }


def _client(routes: dict[str, object], calls: list[str] | None = None) -> GbfsClient:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if calls is not None:
            calls.append(url)
        if url not in routes:
            return httpx.Response(404)
        body = routes[url]
        return body if isinstance(body, httpx.Response) else httpx.Response(200, json=body)

    http = httpx.Client(transport=httpx.MockTransport(handler))
    return GbfsClient(ENTRY, "2.3", "en", "test-agent", http=http)


def test_feed_urls_falls_back_to_first_language():
    doc = {"data": {"fr": {"feeds": [{"name": "a", "url": "u"}]}}}
    assert feed_urls_from_discovery(doc, "en") == {"a": "u"}


def test_pick_version_url_accepts_v_prefix():
    doc = {"data": {"versions": [{"version": "v1.1", "url": V11}, {"version": "v2.3", "url": V23}]}}
    assert pick_version_url(doc, "2.3") == V23
    assert pick_version_url(doc, "3.0") is None


def test_discover_switches_from_1_1_entry_point_to_2_3():
    routes = {
        ENTRY: _discovery("1.1", "https://gbfs.example/1.1"),
        "https://gbfs.example/1.1/en/gbfs_versions.json": {
            "data": {"versions": [{"version": "v1.1", "url": V11}, {"version": "v2.3", "url": V23}]}
        },
        V23: _discovery("2.3", "https://gbfs.example/2.3"),
    }
    feeds = _client(routes).discover()
    assert feeds["station_status"] == "https://gbfs.example/2.3/en/station_status.json"


def test_fetch_rediscovers_once_after_404():
    calls: list[str] = []
    status = load_fixture("station_status")
    routes = {
        ENTRY: _discovery("2.3", "https://gbfs.example/2.3"),
        "https://gbfs.example/2.3/en/station_status.json": status,
    }
    client = _client(routes, calls)
    client._feeds = {"station_status": "https://gbfs.example/old/station_status.json"}
    fetched = client.fetch("station_status")
    assert fetched.envelope.last_updated == status["last_updated"]
    assert json.loads(fetched.raw) == status
    assert calls.count(ENTRY) == 1


def test_fetch_raises_on_server_error():
    routes = {
        ENTRY: _discovery("2.3", "https://gbfs.example/2.3"),
        "https://gbfs.example/2.3/en/station_status.json": httpx.Response(503),
    }
    with pytest.raises(httpx.HTTPStatusError):
        _client(routes).fetch("station_status")


def test_regions_are_deduplicated_like_the_live_feed():
    doc = load_fixture("system_regions")
    regions = [SystemRegion.model_validate(r) for r in doc["data"]["regions"]]
    deduped = dedupe_regions(regions)
    assert len(regions) == 2 * len(deduped)
    assert {r.name for r in deduped} >= {"San Francisco", "Oakland", "San Jose"}
