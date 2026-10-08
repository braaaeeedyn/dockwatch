"""Fetch GBFS feeds: auto-discovery to feed URLs, then individual feeds, re-discovering when a URL goes stale."""

import json
import logging
from dataclasses import dataclass

import httpx

from dockwatch.gbfs.models import Envelope

log = logging.getLogger(__name__)


class FeedNotFound(Exception):
    pass


@dataclass(frozen=True)
class FetchedFeed:
    name: str
    url: str
    envelope: Envelope
    raw: bytes


def feed_urls_from_discovery(doc: dict, language: str) -> dict[str, str]:
    """Map feed name -> URL from a gbfs.json document for one language."""
    data = doc.get("data", {})
    lang = data.get(language) or next(iter(data.values()), None)
    if not lang or "feeds" not in lang:
        raise ValueError("gbfs.json has no feeds list")
    return {feed["name"]: feed["url"] for feed in lang["feeds"]}


def pick_version_url(versions_doc: dict, version: str) -> str | None:
    """From a gbfs_versions document, the gbfs.json URL for `version` ("2.3" matches "v2.3" and "2.3")."""
    for entry in versions_doc.get("data", {}).get("versions", []):
        if entry.get("version", "").lstrip("v") == version.lstrip("v"):
            return entry["url"]
    return None


class GbfsClient:
    def __init__(
        self,
        discovery_url: str,
        version: str,
        language: str,
        user_agent: str,
        timeout_s: float = 20.0,
        http: httpx.Client | None = None,
    ) -> None:
        self.discovery_url = discovery_url
        self.version = version
        self.language = language
        self._http = http or httpx.Client(headers={"User-Agent": user_agent}, timeout=timeout_s, follow_redirects=True)
        self._feeds: dict[str, str] | None = None

    def _get_json(self, url: str) -> tuple[dict, bytes]:
        resp = self._http.get(url)
        resp.raise_for_status()
        return json.loads(resp.content), resp.content

    def discover(self) -> dict[str, str]:
        """Resolve feed URLs, switching to the requested GBFS version if the entry point serves another one."""
        doc, _ = self._get_json(self.discovery_url)
        feeds = feed_urls_from_discovery(doc, self.language)
        served = str(doc.get("version", "")).lstrip("v")
        if served != self.version.lstrip("v") and "gbfs_versions" in feeds:
            versions_doc, _ = self._get_json(feeds["gbfs_versions"])
            url = pick_version_url(versions_doc, self.version)
            if url:
                doc, _ = self._get_json(url)
                feeds = feed_urls_from_discovery(doc, self.language)
            else:
                log.warning("GBFS version %s not offered; using %s", self.version, served or "unknown")
        self._feeds = feeds
        log.info("discovered %d GBFS feeds", len(feeds))
        return feeds

    def fetch(self, name: str) -> FetchedFeed:
        """Fetch one feed by name. On 404 the URLs are re-discovered once (feeds do move)."""
        for attempt in (1, 2):
            feeds = self._feeds or self.discover()
            if name not in feeds:
                raise FeedNotFound(name)
            url = feeds[name]
            try:
                doc, raw = self._get_json(url)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404 and attempt == 1:
                    log.warning("feed %s returned 404; re-discovering", name)
                    self._feeds = None
                    continue
                raise
            return FetchedFeed(name=name, url=url, envelope=Envelope.model_validate(doc), raw=raw)
        raise FeedNotFound(name)

    def close(self) -> None:
        self._http.close()
