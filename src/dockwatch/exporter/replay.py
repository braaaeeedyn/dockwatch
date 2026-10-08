"""Build web/data/replay.json ("Replay a day", DESIGN.md §6/§7) from the raw GBFS archive, on the host (no Spark).

Run: python tasks.py replay-export [--date YYYY-MM-DD] [--step 60] [--out web/data/replay.json]

`--date` is a Pacific date; one Pacific day spans two UTC `dt=` partitions of the archive, so both are read and
filtered by Pacific date. The default is the latest complete Pacific day (before today) with archived snapshots.
States use the one shared rule (streaming.episodes.classify), views the exporter's view_for.

File format v1 (delta-encoded, one frame per `step_s` bucket = the last snapshot in that bucket):
  {"version": 1, "kind": "dockwatch-replay", "day", "step_s", "source", "generated_at", "states",
   "stations": [{"id", "name", "code", "lat", "lon", "view", "capacity"}, ...],
   "frames": [{"t": ISO Z, "d": [index, state index, bikes, docks, ...]}, ...]}
Frame 0 lists every station present; later frames list only stations whose (state, bikes, docks) changed.
Station indices are given by how often a station changes (busiest first), which keeps the indices short.
"""

import argparse
import json
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from datetime import date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from dockwatch.exporter.build import iso, view_for
from dockwatch.gbfs.models import StationInformation, StationStatus
from dockwatch.streaming.episodes import STATES, capacity_of, classify

PACIFIC = ZoneInfo("America/Los_Angeles")
STATUS_FEED = "station_status"
INFO_FEED = "station_information"
SOURCE = "Bay Wheels GBFS raw archive (raw/gbfs/station_status)"
DEFAULT_STEP_S = 60
MAX_BYTES = 1_500_000  # budget for a full day at step_s 60, uncompressed
STATE_INDEX = {s: i for i, s in enumerate(STATES)}


def day_bounds(day: date) -> tuple[int, int]:
    """[start, end) of a Pacific day in epoch seconds (23, 24 or 25 hours long)."""
    start = datetime.combine(day, dtime(0), tzinfo=PACIFIC)
    end = datetime.combine(day + timedelta(days=1), dtime(0), tzinfo=PACIFIC)
    return int(start.timestamp()), int(end.timestamp())


def pacific_date(epoch: float) -> date:
    return datetime.fromtimestamp(epoch, tz=PACIFIC).date()


def key_epoch(key: str) -> int:
    """raw/gbfs/<feed>/dt=YYYY-MM-DD/HH/<last_updated>.json.gz -> last_updated."""
    return int(key.rsplit("/", 1)[-1].split(".", 1)[0])


def partitions(day: date) -> list[str]:
    """The UTC `dt=` partitions a Pacific day touches: Pacific midnight is 07:00 or 08:00 UTC the same date."""
    return [day.isoformat(), (day + timedelta(days=1)).isoformat()]


def keys_for_day(archive, feed: str, day: date) -> list[tuple[int, str]]:
    start, end = day_bounds(day)
    found = []
    for dt in partitions(day):
        for key in archive.list_keys(f"raw/gbfs/{feed}/dt={dt}/"):
            epoch = key_epoch(key)
            if start <= epoch < end:
                found.append((epoch, key))
    return sorted(found)


def last_per_bucket(keys: list[tuple[int, str]], start: int, step: int) -> list[tuple[int, str]]:
    """The last snapshot in each `step`-second bucket counted from `start`."""
    buckets: dict[int, tuple[int, str]] = {}
    for epoch, key in keys:
        buckets[(epoch - start) // step] = (epoch, key)  # keys are sorted, so the last one wins
    return [buckets[b] for b in sorted(buckets)]


def station_values(doc: dict) -> Iterator[tuple[str, int, int, int, int]]:
    """(station_id, state index, bikes, docks, capacity) for each valid row of one station_status snapshot."""
    snapshot_ts = int(doc["last_updated"])
    rows = (doc.get("data") or {}).get("stations")
    if not isinstance(rows, list):
        return
    for row in rows:
        try:
            s = StationStatus.model_validate(row)
        except ValidationError:
            continue  # the producer sends these to the DLQ; the stream never sees them either
        capacity = capacity_of(s.num_bikes_available, s.num_bikes_disabled, s.num_docks_available, s.num_docks_disabled)
        state = classify(
            snapshot_ts=snapshot_ts,
            last_reported=s.last_reported,
            is_installed=s.is_installed,
            is_renting=s.is_renting,
            bikes=s.num_bikes_available,
            docks=s.num_docks_available,
            capacity=capacity,
        )
        yield s.station_id, STATE_INDEX[state], s.num_bikes_available, s.num_docks_available, capacity


def build_replay(
    snapshots: Iterable[dict], infos: dict[str, dict], day: date, step: int, now: float | None = None
) -> dict:
    """snapshots: station_status envelopes, oldest first, one per frame (already bucketed); infos: station_id ->
    station_information row. Streams: only the previous value of each station is kept."""
    previous: dict[str, tuple[int, int, int]] = {}
    capacity: dict[str, int] = {}
    changes: list[tuple[str, list[tuple[str, int, int, int]]]] = []  # (t, [(id, state, bikes, docks)])
    counts: Counter[str] = Counter()
    for doc in snapshots:
        delta = []
        for sid, st, bikes, docks, cap in station_values(doc):
            capacity[sid] = cap
            value = (st, bikes, docks)
            if previous.get(sid) != value:
                previous[sid] = value
                delta.append((sid, *value))
                counts[sid] += 1
        changes.append((iso(int(doc["last_updated"])), delta))

    # Busiest stations get the smallest indices (fewer digits in every frame); ties by id, so builds are stable.
    order = sorted(previous, key=lambda sid: (-counts[sid], sid))
    index = {sid: i for i, sid in enumerate(order)}
    stations = []
    for sid in order:
        info = infos.get(sid, {})
        lat, lon = info.get("lat"), info.get("lon")
        stations.append(
            {
                "id": sid,
                "name": info.get("name") or "Unknown station",
                "code": info.get("short_name"),
                "lat": lat,
                "lon": lon,
                "view": view_for(info.get("region_id"), lat, lon),
                "capacity": info.get("capacity") or capacity.get(sid),
            }
        )
    frames = []
    for t, delta in changes:
        d: list[int] = []
        for sid, st, bikes, docks in sorted(delta, key=lambda c: index[c[0]]):
            d.extend((index[sid], st, bikes, docks))
        frames.append({"t": t, "d": d})
    return {
        "version": 1,
        "kind": "dockwatch-replay",
        "day": day.isoformat(),
        "step_s": step,
        "source": SOURCE,
        "generated_at": iso(time.time() if now is None else now),
        "states": list(STATES),
        "stations": stations,
        "frames": frames,
    }


def load_infos(archive, day: date) -> dict[str, dict]:
    """Station information from the last snapshot of the day (or, failing that, the latest one before it)."""
    keys = keys_for_day(archive, INFO_FEED, day)
    if not keys:
        _, end = day_bounds(day)
        keys = [(key_epoch(k), k) for k in archive.list_keys(f"raw/gbfs/{INFO_FEED}/")]
        keys = sorted(k for k in keys if k[0] < end)
    if not keys:
        return {}
    doc = json.loads(archive.get(keys[-1][1]))
    infos = {}
    for row in (doc.get("data") or {}).get("stations") or []:
        try:
            info = StationInformation.model_validate(row)
        except ValidationError:
            continue
        infos[info.station_id] = info.model_dump()
    return infos


def archived_days(archive) -> set[date]:
    return {pacific_date(key_epoch(k)) for k in archive.list_keys(f"raw/gbfs/{STATUS_FEED}/")}


def default_day(days: Iterable[date], today: date) -> date | None:
    """The latest complete Pacific day (before today) with snapshots; today if there is no earlier one."""
    days = set(days)
    earlier = [d for d in days if d < today]
    if earlier:
        return max(earlier)
    return today if today in days else None


def export(
    archive,
    out: Path,
    day: date | None = None,
    step: int = DEFAULT_STEP_S,
    now: Callable[[], float] = time.time,
    log: Callable[[str], None] = print,
) -> dict:
    if day is None:
        day = default_day(archived_days(archive), pacific_date(now()))
        if day is None:
            raise SystemExit("replay-export: the archive has no station_status snapshots yet")
    start, _ = day_bounds(day)
    keys = last_per_bucket(keys_for_day(archive, STATUS_FEED, day), start, step)
    if not keys:
        raise SystemExit(f"replay-export: no archived station_status snapshots on {day} (Pacific)")
    infos = load_infos(archive, day)
    snapshots = (json.loads(archive.get(key)) for _, key in keys)
    doc = build_replay(snapshots, infos, day, step, now())

    from dockwatch.exporter.__main__ import write_json

    write_json(out, doc)
    size = out.stat().st_size
    log(
        f"wrote {out}: {size:,} bytes, {len(doc['stations'])} stations, {len(doc['frames'])} frames, "
        f"step {step} s, day {day} (Pacific)"
    )
    if size > MAX_BYTES:
        log(f"warning: over the {MAX_BYTES:,} byte budget; try a larger --step")
    return doc


def main(argv: list[str] | None = None) -> None:
    from dockwatch.config import get_settings
    from dockwatch.producer.archive import build_archive

    settings = get_settings()
    parser = argparse.ArgumentParser(prog="dockwatch.exporter.replay", description=__doc__.split("\n")[0])
    parser.add_argument("--date", type=date.fromisoformat, default=None, help="Pacific date, YYYY-MM-DD")
    parser.add_argument("--step", type=int, default=DEFAULT_STEP_S, help="seconds per frame (>= 60)")
    parser.add_argument("--out", type=Path, default=Path(settings.export_dir) / "replay.json")
    args = parser.parse_args(argv)
    if args.step < 60:
        parser.error("--step must be at least 60 seconds (the feed updates once a minute)")
    export(build_archive(settings), args.out, args.date, args.step)


if __name__ == "__main__":
    sys.stdout.reconfigure(errors="replace")
    main()
