"""Replay a day: replay.json from the raw GBFS archive (FsArchive in tmp_path, synthetic snapshots, no network)."""

import json
import random
import uuid
from datetime import UTC, date, datetime

import pytest

from dockwatch.config import get_settings
from dockwatch.exporter.__main__ import write_json
from dockwatch.exporter.replay import (
    build_replay,
    day_bounds,
    default_day,
    export,
    keys_for_day,
    main,
)
from dockwatch.producer.archive import FsArchive, archive_key
from dockwatch.streaming.episodes import STATES, capacity_of, classify

DAY = date(2026, 10, 7)  # Pacific: 2026-10-07T07:00Z .. 2026-10-08T07:00Z (PDT)
START, END = day_bounds(DAY)
INFOS = {
    "a": {"name": "Market St", "short_name": "SF-1", "lat": 37.79, "lon": -122.40, "region_id": "3", "capacity": 20},
    "b": {"name": "Lake Merritt", "short_name": "OK-1", "lat": 37.80, "lon": -122.26, "region_id": "12"},
}

# Size budget (DESIGN_BACKLOG "Replay size budget"): a full day at step_s 60 must stay under MAX_BYTES uncompressed.
BUDGET_BYTES = 1_500_000
BUDGET_STATIONS = 641  # the real network, 2026-10-07
BUDGET_FRAMES = 1440  # one frame a minute for 24 hours
CHANGES_PER_FRAME = 80  # ~1.5x the real mean (53/min, 2026-10-07); see DESIGN_BACKLOG
STREETS = ["Market", "Townsend", "Valencia", "Broadway", "Telegraph", "Santa Clara", "Mission", "Grand", "Folsom"]
CROSSES = ["2nd St", "16th St", "Webster St", "College Ave", "Bryant St", "San Fernando St", "Shattuck Ave"]
REGIONS = [("3", "SF", 37.77, -122.42), ("12", "OK", 37.81, -122.27), ("14", "SJ", 37.33, -121.89)]


def epoch(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp())


def row(sid, bikes, docks, *, ts, reported=None, installed=1, renting=1, bikes_disabled=0, docks_disabled=0):
    return {
        "station_id": sid,
        "num_bikes_available": bikes,
        "num_bikes_disabled": bikes_disabled,
        "num_docks_available": docks,
        "num_docks_disabled": docks_disabled,
        "is_installed": installed,
        "is_renting": renting,
        "is_returning": 1,
        "last_reported": ts - 30 if reported is None else reported,
    }


def status(ts: int, rows: list[dict]) -> dict:
    return {"last_updated": ts, "ttl": 60, "version": "2.3", "data": {"stations": rows}}


def put(archive: FsArchive, feed: str, doc: dict) -> None:
    archive.put(archive_key(feed, doc["last_updated"]), json.dumps(doc).encode())


def info_doc(ts: int) -> dict:
    return {
        "last_updated": ts,
        "ttl": 60,
        "data": {"stations": [{"station_id": sid, **info} for sid, info in INFOS.items()]},
    }


def decode(doc: dict) -> list[dict[str, tuple[str, int, int]]]:
    """Python decoder of format v1: apply each frame's deltas; returns station id -> (state, bikes, docks) per frame."""
    ids = [s["id"] for s in doc["stations"]]
    current: dict[str, tuple[str, int, int]] = {}
    out = []
    for frame in doc["frames"]:
        d = frame["d"]
        for j in range(0, len(d), 4):
            i, st, bikes, docks = d[j : j + 4]
            current[ids[i]] = (doc["states"][st], bikes, docks)
        out.append(dict(current))
    return out


def expected_values(snapshot: dict) -> dict[str, tuple[str, int, int]]:
    """What the shared rule says each station looks like in one snapshot."""
    out = {}
    for r in snapshot["data"]["stations"]:
        cap = capacity_of(
            r["num_bikes_available"], r["num_bikes_disabled"], r["num_docks_available"], r["num_docks_disabled"]
        )
        state = classify(
            snapshot_ts=snapshot["last_updated"],
            last_reported=r["last_reported"],
            is_installed=bool(r["is_installed"]),
            is_renting=bool(r["is_renting"]),
            bikes=r["num_bikes_available"],
            docks=r["num_docks_available"],
            capacity=cap,
        )
        out[r["station_id"]] = (state, r["num_bikes_available"], r["num_docks_available"])
    return out


@pytest.fixture
def fresh_settings():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_first_frame_is_full_and_later_frames_only_list_changes():
    t0 = START + 3600
    snaps = [
        status(t0, [row("a", 8, 12, ts=t0), row("b", 5, 5, ts=t0), row("c", 0, 10, ts=t0)]),
        status(t0 + 60, [row("a", 7, 13, ts=t0 + 60), row("b", 5, 5, ts=t0 + 60), row("c", 0, 10, ts=t0 + 60)]),
        status(t0 + 120, [row("a", 7, 13, ts=t0 + 120), row("b", 5, 5, ts=t0 + 120), row("c", 0, 10, ts=t0 + 120)]),
        # a station that first appears later is added when it shows up
        status(t0 + 180, [row("a", 7, 13, ts=t0 + 180), row("b", 6, 4, ts=t0 + 180), row("d", 3, 9, ts=t0 + 180)]),
    ]
    doc = build_replay(snaps, INFOS, DAY, 60, now=t0 + 200)
    ids = [s["id"] for s in doc["stations"]]
    entries = [[ids[f["d"][j]] for j in range(0, len(f["d"]), 4)] for f in doc["frames"]]
    assert sorted(entries[0]) == ["a", "b", "c"]
    assert entries[1] == ["a"]
    assert entries[2] == []
    assert sorted(entries[3]) == ["b", "d"]
    assert [f["t"] for f in doc["frames"]] == [
        datetime.fromtimestamp(t0 + k * 60, tz=UTC).isoformat().replace("+00:00", "Z") for k in range(4)
    ]
    assert doc["version"] == 1
    assert doc["kind"] == "dockwatch-replay"
    assert doc["day"] == "2026-10-07"
    assert doc["states"] == list(STATES)
    by_id = {s["id"]: s for s in doc["stations"]}
    assert by_id["a"] == {
        "id": "a",
        "name": "Market St",
        "code": "SF-1",
        "lat": 37.79,
        "lon": -122.40,
        "view": "sf",
        "capacity": 20,
    }
    assert by_id["b"]["view"] == "eastbay"
    assert by_id["b"]["capacity"] == 10  # no capacity in station_information: counted from the status row
    assert by_id["c"]["name"] == "Unknown station"  # no station_information row, like build_live


def test_state_uses_the_shared_classify_rule():
    ts = START + 7200
    rows = {
        "not-renting": row("not-renting", 5, 5, ts=ts, renting=0),
        "not-installed": row("not-installed", 5, 5, ts=ts, installed=0),
        "stale": row("stale", 5, 5, ts=ts, reported=ts - 31 * 60),
        "empty": row("empty", 0, 15, ts=ts),
        "full": row("full", 15, 0, ts=ts),
        "low": row("low", 2, 18, ts=ts),
        "low-share": row("low-share", 3, 37, ts=ts),
        "high": row("high", 18, 2, ts=ts),
        "ok": row("ok", 8, 9, ts=ts, bikes_disabled=1, docks_disabled=2),
    }
    snap = status(ts, list(rows.values()))
    doc = build_replay([snap], {}, DAY, 60, now=ts)
    got = decode(doc)[0]
    assert got == expected_values(snap)  # identical to classify() for every row
    words = {sid: value[0] for sid, value in got.items()}
    assert words == {
        "not-renting": "offline",
        "not-installed": "offline",
        "stale": "offline",
        "empty": "empty",
        "full": "full",
        "low": "low",
        "low-share": "low",
        "high": "high",
        "ok": "ok",
    }


def test_pacific_day_reads_both_utc_partitions(tmp_path):
    archive = FsArchive(tmp_path)
    times = {
        "2026-10-07T06:59:00": False,  # 11:59 PM on Oct 6, Pacific
        "2026-10-07T07:00:00": True,  # midnight, Oct 7 Pacific (dt=2026-10-07)
        "2026-10-07T23:30:00": True,
        "2026-10-08T03:00:00": True,  # 8 PM Oct 7 Pacific, but in the dt=2026-10-08 partition
        "2026-10-08T06:59:00": True,
        "2026-10-08T07:00:00": False,  # midnight, Oct 8 Pacific
    }
    for text in times:
        ts = epoch(text)
        put(archive, "station_status", status(ts, [row("a", 4, 16, ts=ts)]))
    included = sorted(epoch(t) for t, inside in times.items() if inside)
    assert [e for e, _ in keys_for_day(archive, "station_status", DAY)] == included
    assert {k.split("/")[3] for _, k in keys_for_day(archive, "station_status", DAY)} == {
        "dt=2026-10-07",
        "dt=2026-10-08",
    }

    doc = export(archive, tmp_path / "replay.json", DAY, 60, now=lambda: included[-1] + 60, log=lambda _: None)
    assert [epoch(f["t"].replace("Z", "")) for f in doc["frames"]] == included


def test_decoded_frames_match_the_snapshots(tmp_path):
    rnd = random.Random(3)
    archive = FsArchive(tmp_path)
    put(archive, "station_information", info_doc(START + 10))
    ids = ["a", "b", "c", "d", "e"]
    caps = {sid: rnd.randint(8, 30) for sid in ids}
    snaps = []
    for k in range(40):  # two snapshots per 120 s bucket: the later one must win
        ts = START + 600 + k * 60 + rnd.randint(0, 5)
        rows = []
        for sid in ids:
            bikes = rnd.randint(0, caps[sid])
            rows.append(row(sid, bikes, caps[sid] - bikes, ts=ts, renting=int(rnd.random() > 0.1)))
        snap = status(ts, rows)
        snaps.append(snap)
        put(archive, "station_status", snap)

    doc = export(archive, tmp_path / "replay.json", DAY, 120, now=lambda: END + 3600, log=lambda _: None)
    assert doc["step_s"] == 120
    last_in_bucket: dict[int, dict] = {}
    for snap in snaps:
        last_in_bucket[(snap["last_updated"] - START) // 120] = snap
    expected = [last_in_bucket[b] for b in sorted(last_in_bucket)]
    decoded = decode(doc)
    assert len(decoded) == len(expected)
    for frame, values, snap in zip(doc["frames"], decoded, expected, strict=True):
        assert frame["t"] == datetime.fromtimestamp(snap["last_updated"], tz=UTC).isoformat().replace("+00:00", "Z")
        assert values == expected_values(snap)
    names = {s["id"]: s["name"] for s in doc["stations"]}
    assert names["a"] == "Market St"
    assert names["e"] == "Unknown station"
    assert json.loads((tmp_path / "replay.json").read_text(encoding="utf-8")) == doc


def test_cli_writes_replay_json_from_an_fs_archive(tmp_path, monkeypatch, fresh_settings, capsys):
    root = tmp_path / "raw"
    archive = FsArchive(root)
    put(archive, "station_information", info_doc(START + 30))
    for k in range(5):
        ts = START + 3600 + k * 60
        put(archive, "station_status", status(ts, [row("a", 4 + k, 16 - k, ts=ts), row("b", 0, 10, ts=ts)]))
    monkeypatch.setenv("DOCKWATCH_ARCHIVE_KIND", "fs")
    monkeypatch.setenv("DOCKWATCH_ARCHIVE_FS_ROOT", str(root))
    out = tmp_path / "web" / "data" / "replay.json"

    main(["--date", "2026-10-07", "--out", str(out)])

    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["day"] == "2026-10-07"
    assert doc["step_s"] == 60
    assert len(doc["frames"]) == 5
    assert {s["id"] for s in doc["stations"]} == {"a", "b"}
    assert decode(doc)[-1] == {"a": ("ok", 8, 12), "b": ("empty", 0, 10)}
    printed = capsys.readouterr().out
    assert "2 stations, 5 frames" in printed
    assert "bytes" in printed

    with pytest.raises(SystemExit, match="no archived station_status snapshots on 2026-10-05"):
        main(["--date", "2026-10-05", "--out", str(out)])


def test_default_day_is_the_latest_complete_pacific_day(tmp_path):
    oct6, oct7, oct8 = date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8)
    assert default_day({oct6, oct7, oct8}, today=oct8) == oct7  # today is not complete yet
    assert default_day({oct6, oct8}, today=oct8) == oct6
    assert default_day({oct8}, today=oct8) == oct8  # nothing earlier: fall back to today
    assert default_day(set(), today=oct8) is None

    archive = FsArchive(tmp_path)
    # 03:00Z on Oct 7 is 8 PM on Oct 6 Pacific; 15:00Z on Oct 8 is 8 AM on Oct 8 Pacific (today).
    for text in ("2026-10-07T03:00:00", "2026-10-08T15:00:00"):
        ts = epoch(text)
        put(archive, "station_status", status(ts, [row("a", 4, 16, ts=ts)]))
    now = epoch("2026-10-08T17:00:00")  # 10 AM Oct 8, Pacific
    doc = export(archive, tmp_path / "replay.json", now=lambda: now, log=lambda _: None)
    assert doc["day"] == "2026-10-06"
    assert len(doc["frames"]) == 1

    with pytest.raises(SystemExit, match="no station_status snapshots"):
        export(FsArchive(tmp_path / "empty"), tmp_path / "x.json", now=lambda: now, log=lambda _: None)


def test_full_day_fits_the_size_budget(tmp_path):
    rnd = random.Random(20261007)
    infos = {}
    caps = {}
    for k in range(BUDGET_STATIONS):
        sid = str(uuid.UUID(int=rnd.getrandbits(128), version=4))  # real ids are UUIDs
        region, prefix, lat, lon = rnd.choice(REGIONS)
        caps[sid] = rnd.randint(11, 35)
        infos[sid] = {
            "name": f"{rnd.choice(STREETS)} St at {rnd.choice(CROSSES)}",  # ~23 characters, like the real names
            "short_name": f"{prefix}-{chr(65 + k % 26)}{rnd.randint(10, 39)}",
            "lat": round(lat + rnd.uniform(-0.05, 0.05), 6),
            "lon": round(lon + rnd.uniform(-0.05, 0.05), 6),
            "region_id": region,
            "capacity": caps[sid],
        }
    ids = list(infos)
    bikes = {sid: rnd.randint(0, caps[sid]) for sid in ids}

    def snapshots():
        for k in range(BUDGET_FRAMES):
            ts = START + k * 60
            if k:
                for sid in rnd.sample(ids, CHANGES_PER_FRAME):  # distinct stations, a +-1 bike move each
                    step = 1 if bikes[sid] == 0 else -1 if bikes[sid] == caps[sid] else rnd.choice((-1, 1))
                    bikes[sid] += step
            # renting and freshly reported, so classify() gives the state from the counts
            yield status(ts, [row(sid, bikes[sid], caps[sid] - bikes[sid], ts=ts) for sid in ids])

    doc = build_replay(snapshots(), infos, DAY, 60, now=END)
    out = tmp_path / "replay.json"
    write_json(out, doc)  # the replay-export writer: same json options
    assert all(START <= epoch(f["t"].replace("Z", "")) < END for f in doc["frames"])  # one Pacific day

    size = len(out.read_bytes())
    assert size <= BUDGET_BYTES, f"{size:,} bytes"
    assert len(doc["stations"]) == BUDGET_STATIONS
    assert len(doc["frames"]) == BUDGET_FRAMES
    assert sum(len(f["d"]) for f in doc["frames"]) // 4 == BUDGET_STATIONS + (BUDGET_FRAMES - 1) * CHANGES_PER_FRAME
