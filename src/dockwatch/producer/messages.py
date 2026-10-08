"""Turn one GBFS snapshot into per-station Kafka messages (pure functions, no I/O)."""

import hashlib
import json
from dataclasses import dataclass, field

from pydantic import ValidationError

from dockwatch.gbfs.models import Envelope, StationInformation, StationStatus


@dataclass(frozen=True)
class Message:
    key: str
    value: dict


@dataclass
class Exploded:
    messages: list[Message] = field(default_factory=list)
    rejects: list[Message] = field(default_factory=list)  # for the dead-letter topic


def snapshot_id(feed: str, last_updated: int) -> str:
    return f"{feed}:{last_updated}"


def _explode(
    feed: str, envelope: Envelope, fetched_at: float, model: type[StationStatus] | type[StationInformation]
) -> Exploded:
    out = Exploded()
    sid = snapshot_id(feed, envelope.last_updated)
    stations = envelope.data.get("stations")
    if not isinstance(stations, list):
        out.rejects.append(Message(key=sid, value={"feed": feed, "snapshot_id": sid, "error": "data.stations missing"}))
        return out
    for row in stations:
        try:
            record = model.model_validate(row)
        except ValidationError as exc:
            out.rejects.append(
                Message(
                    key=str(row.get("station_id", sid)) if isinstance(row, dict) else sid,
                    value={
                        "feed": feed,
                        "snapshot_id": sid,
                        "error": exc.errors(include_url=False, include_context=False),
                        "payload": row,
                    },
                )
            )
            continue
        value = record.model_dump(mode="json")
        value.update(
            feed_last_updated=envelope.last_updated,
            feed_version=envelope.version,
            fetched_at=round(fetched_at, 3),
            snapshot_id=sid,
        )
        out.messages.append(Message(key=record.station_id, value=value))
    return out


def explode_station_status(envelope: Envelope, fetched_at: float) -> Exploded:
    """One message per station, keyed by station_id; `last_reported` is the event time downstream."""
    return _explode("station_status", envelope, fetched_at, StationStatus)


def explode_station_information(envelope: Envelope, fetched_at: float) -> Exploded:
    return _explode("station_information", envelope, fetched_at, StationInformation)


def content_hash(envelope: Envelope) -> str:
    """Hash of the feed's data only (not last_updated), to skip re-producing an unchanged station_information.

    Order-insensitive: Bay Wheels returns the same stations in a different order on every poll.
    """
    data = dict(envelope.data)
    if isinstance(data.get("stations"), list):
        data["stations"] = sorted(data["stations"], key=lambda r: str(r.get("station_id", "")))
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def encode(value: dict) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
