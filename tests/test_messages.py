import copy

from dockwatch.gbfs.models import Envelope
from dockwatch.producer.archive import FsArchive, archive_key
from dockwatch.producer.messages import (
    content_hash,
    explode_station_information,
    explode_station_status,
)


def test_status_explodes_one_message_per_station_keyed_by_id(status_doc):
    env = Envelope.model_validate(status_doc)
    out = explode_station_status(env, fetched_at=1000.5)
    assert not out.rejects
    assert [m.key for m in out.messages] == [s["station_id"] for s in status_doc["data"]["stations"]]
    value = out.messages[0].value
    assert value["last_reported"] == status_doc["data"]["stations"][0]["last_reported"]
    assert value["feed_last_updated"] == status_doc["last_updated"]
    assert value["snapshot_id"] == f"station_status:{status_doc['last_updated']}"
    assert value["fetched_at"] == 1000.5
    # GBFS sends 0/1 for booleans; they come out as real booleans.
    assert value["is_renting"] in (True, False)


def test_unknown_fields_are_kept(status_doc):
    status_doc["data"]["stations"][0]["num_cargo_bikes_available"] = 3
    out = explode_station_status(Envelope.model_validate(status_doc), 0)
    assert out.messages[0].value["num_cargo_bikes_available"] == 3


def test_bad_rows_go_to_dlq_and_good_rows_still_flow(status_doc):
    status_doc["data"]["stations"][1]["num_bikes_available"] = -1
    del status_doc["data"]["stations"][2]["station_id"]
    out = explode_station_status(Envelope.model_validate(status_doc), 0)
    assert len(out.messages) == len(status_doc["data"]["stations"]) - 2
    assert len(out.rejects) == 2
    assert all("error" in r.value and "payload" in r.value for r in out.rejects)


def test_missing_stations_list_is_one_reject(status_doc):
    status_doc["data"] = {}
    out = explode_station_status(Envelope.model_validate(status_doc), 0)
    assert not out.messages
    assert out.rejects[0].value["error"] == "data.stations missing"


def test_station_information_parses(info_doc):
    out = explode_station_information(Envelope.model_validate(info_doc), 0)
    assert not out.rejects
    assert all(m.value["lat"] and m.value["lon"] for m in out.messages)


def test_content_hash_ignores_last_updated(info_doc):
    a = Envelope.model_validate(info_doc)
    changed_time = copy.deepcopy(info_doc)
    changed_time["last_updated"] += 60
    changed_data = copy.deepcopy(info_doc)
    changed_data["data"]["stations"][0]["capacity"] += 1
    assert content_hash(a) == content_hash(Envelope.model_validate(changed_time))
    assert content_hash(a) != content_hash(Envelope.model_validate(changed_data))


def test_content_hash_ignores_station_order(info_doc):
    # Regression: the live feed reshuffles stations every poll, which made every snapshot look "changed".
    shuffled = copy.deepcopy(info_doc)
    shuffled["data"]["stations"].reverse()
    assert content_hash(Envelope.model_validate(info_doc)) == content_hash(Envelope.model_validate(shuffled))


def test_archive_key_is_partitioned_by_utc_date_and_hour():
    assert archive_key("station_status", 1791411244) == ("raw/gbfs/station_status/dt=2026-10-07/22/1791411244.json.gz")


def test_fs_archive_round_trip(tmp_path):
    archive = FsArchive(tmp_path)
    key = archive_key("station_status", 1791411244)
    archive.put(key, b'{"a": 1}')
    assert archive.list_keys("raw/gbfs/station_status/dt=2026-10-07/") == [key]
    assert archive.get(key) == b'{"a": 1}'
