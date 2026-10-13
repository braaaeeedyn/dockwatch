"""trip_history_monthly rules (M4b): listing and month keys, header schemas, the row mapping, the streamed CSV reader,
local times to UTC, the ZIP cache. Host tests: no pyarrow / pyiceberg (those are tested in the Airflow image)."""

import io
import zipfile
from datetime import UTC, date, datetime

import pytest

from dockwatch.batch import trips

LISTING = {
    "2017-fordgobike-tripdata.csv.zip": 1,
    "201901-fordgobike-tripdata.csv.zip": 6_169_344,
    "202003-baywheels-tripdata.csv.zip": 3,
    "202005-baywheels-tripdata.csv.zip": 4,
    "202211-baywheeels-tripdata.csv.zip": 5,
    "202503-baywheels-tripdata.zip": 6,
    "202609-lyftbikes-tripdata.zip": 22_079_484,
    "index.html": 7,
}
LEGACY_HEADER = list(trips.LEGACY) + ["bike_share_for_all_trip"]
RIDE_HEADER = list(trips.RIDE)


def index(header):
    return {h: i for i, h in enumerate(header)}


def legacy_record(**kw):
    rec = dict(zip(LEGACY_HEADER, [
        "598", "2019-01-31 17:57:44.6130", "2019-01-31 18:07:43.0", "229", "Foothill Blvd at 42nd Ave", "37.77",
        "-122.21", "196", "Grand Ave at Perkins St", "37.80", "-122.25", "4861", "Subscriber", "No",
    ], strict=True))  # fmt: skip
    rec.update(kw)
    return [rec[h] for h in LEGACY_HEADER]


def ride_record(**kw):
    rec = dict(zip(RIDE_HEADER, [
        "A1B2C3", "electric_bike", "2026-09-01 08:00:00", "2026-09-01 08:12:30.5", "Market St at 10th St",
        "SF-C26-2", "", "", "37.776", "-122.417", "37.78", "-122.42", "casual",
    ], strict=True))  # fmt: skip
    rec.update(kw)
    return [rec[h] for h in RIDE_HEADER]


def zip_with(tmp_path, name, text, member="trips.csv"):
    path = tmp_path / name
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member, text.encode("utf-8"))
        zf.writestr("__MACOSX/._trips.csv", b"junk")
    return path


def test_month_key_is_found_whatever_the_file_name():
    assert trips.month_key(LISTING, "2019-01") == "201901-fordgobike-tripdata.csv.zip"
    assert trips.month_key(LISTING, "2022-11") == "202211-baywheeels-tripdata.csv.zip"
    assert trips.month_key(LISTING, "2025-03") == "202503-baywheels-tripdata.zip"
    assert trips.month_key(LISTING, "2026-09") == "202609-lyftbikes-tripdata.zip"
    assert trips.month_key(LISTING, "2020-04") is None
    with pytest.raises(trips.TripError):
        trips.month_key({**LISTING, "202609-baywheels-tripdata.zip": 1}, "2026-09")
    with pytest.raises(trips.TripError):
        trips.check_month("2026-13")
    assert trips.month_of("", datetime(2025, 10, 1, tzinfo=UTC)) == "2025-10"
    assert trips.month_of("2019-01", datetime(2025, 10, 1, tzinfo=UTC)) == "2019-01"
    xml = (
        b'<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b"<IsTruncated>false</IsTruncated><Contents><Key>202609-lyftbikes-tripdata.zip</Key><Size>22079484</Size>"
        b"</Contents></ListBucketResult>"
    )
    assert trips.parse_listing(xml) == {"202609-lyftbikes-tripdata.zip": 22_079_484}
    with pytest.raises(trips.TripError):
        trips.parse_listing(xml.replace(b"false", b"true"))


def test_month_status_is_published_missing_or_pending():
    assert trips.month_status(LISTING, "2026-09") == "published"
    assert trips.month_status(LISTING, "2020-04") == "missing"
    assert trips.month_status(LISTING, "2026-10") == "pending"


def test_header_detects_legacy_and_ride_schemas():
    assert trips.schema_of(LEGACY_HEADER) == "legacy"
    assert trips.schema_of(list(trips.LEGACY) + ["rental_access_method"]) == "legacy"
    assert trips.schema_of(RIDE_HEADER) == "ride"


def test_header_missing_a_required_column_is_refused():
    with pytest.raises(trips.TripError, match="lacks a required column"):
        trips.schema_of([h for h in RIDE_HEADER if h != "started_at"])
    with pytest.raises(trips.TripError):
        trips.schema_of([])


def test_header_with_unsafe_column_names_is_refused():
    for bad in ("started_at; DROP TABLE x", "Ride ID", "ride-id", ""):
        with pytest.raises(trips.TripError):
            trips.schema_of([*RIDE_HEADER, bad])
    with pytest.raises(trips.TripError, match="duplicate"):
        trips.schema_of([*RIDE_HEADER, "ride_id"])


def test_rows_map_both_schemas_to_bronze_trips():
    assert tuple(n for n, _ in trips.TRIPS_COLUMNS[3:-1]) == trips.ROW_COLUMNS
    legacy = dict(zip(trips.ROW_COLUMNS, trips.map_row("legacy", index(LEGACY_HEADER), legacy_record()), strict=True))
    assert legacy == {
        "ride_id": None,
        "rideable_type": None,
        "started_at": datetime(2019, 2, 1, 1, 57, 44, 613000, tzinfo=UTC),
        "ended_at": datetime(2019, 2, 1, 2, 7, 43, tzinfo=UTC),
        "duration_s": 598,
        "start_station_code": "229",
        "start_station_name": "Foothill Blvd at 42nd Ave",
        "end_station_code": "196",
        "end_station_name": "Grand Ave at Perkins St",
        "start_lat": 37.77,
        "start_lon": -122.21,
        "end_lat": 37.80,
        "end_lon": -122.25,
        "member_type": "member",
        "bike_id": "4861",
    }
    customer = trips.map_row("legacy", index(LEGACY_HEADER), legacy_record(user_type="Customer", bike_id=""))
    assert customer[13:] == ("casual", None)
    ride = dict(zip(trips.ROW_COLUMNS, trips.map_row("ride", index(RIDE_HEADER), ride_record()), strict=True))
    assert ride == {
        "ride_id": "A1B2C3",
        "rideable_type": "electric_bike",
        "started_at": datetime(2026, 9, 1, 15, 0, tzinfo=UTC),
        "ended_at": datetime(2026, 9, 1, 15, 12, 30, 500000, tzinfo=UTC),
        "duration_s": 750,
        "start_station_code": "SF-C26-2",
        "start_station_name": "Market St at 10th St",
        "end_station_code": None,
        "end_station_name": None,
        "start_lat": 37.776,
        "start_lon": -122.417,
        "end_lat": 37.78,
        "end_lon": -122.42,
        "member_type": "casual",
        "bike_id": None,
    }
    member = trips.map_row("ride", index(RIDE_HEADER), ride_record(member_casual="Member", start_lat="x"))
    assert member[13] == "member"
    assert member[9] is None


def test_csv_reader_counts_records_with_quotes_crlf_and_newlines(tmp_path):
    legacy_rows = [legacy_record(), legacy_record(start_station_name="Shattuck Ave, at Hearst"), legacy_record()]
    legacy_text = "\n".join(",".join(f'"{v}"' if "," in v else v for v in r) for r in [LEGACY_HEADER, *legacy_rows])
    legacy_zip = zip_with(tmp_path, "201901-fordgobike-tripdata.csv.zip", legacy_text + "\n")
    stats: dict = {}
    batches = list(trips.month_batches(legacy_zip, stats, size=2))
    assert [len(b) for b in batches] == [2, 1]
    assert stats == {"header": LEGACY_HEADER, "schema": "legacy", "csv_rows": 3, "bad_timestamps": 0}
    assert batches[0][1][6] == "Shattuck Ave, at Hearst"

    quoted = [ride_record(), ride_record(start_station_name="Pier 1, the\r\nEmbarcadero"), ride_record()]
    lines = [",".join(f'"{v}"' for v in r) for r in [RIDE_HEADER, *quoted]]
    ride_zip = zip_with(tmp_path, "202609-lyftbikes-tripdata.zip", "﻿" + "\r\n".join(lines) + "\r\n\r\n")
    stats = {}
    rows = [r for b in trips.month_batches(ride_zip, stats) for r in b]
    assert stats["schema"] == "ride"
    assert stats["header"] == RIDE_HEADER
    assert stats["csv_rows"] == len(rows) == 3
    assert rows[1][6] == "Pier 1, the\r\nEmbarcadero"
    assert all(r[13] == "casual" for r in rows)  # no stray \r on the last column

    bad = zip_with(tmp_path, "bad.zip", "\n".join(",".join(r) for r in [RIDE_HEADER, ride_record(ended_at="soon")]))
    stats = {}
    list(trips.month_batches(bad, stats))
    assert stats["bad_timestamps"] == 1

    two = tmp_path / "two.zip"
    with zipfile.ZipFile(two, "w") as zf:
        zf.writestr("a.csv", "x")
        zf.writestr("b.csv", "y")
    with pytest.raises(trips.TripError, match="one .csv member"):
        list(trips.month_batches(two, {}))


def test_local_times_map_to_utc_across_dst():
    assert trips.local_time("2026-03-08 02:30:00") == datetime(2026, 3, 8, 10, 30, tzinfo=UTC)  # gap: 03:30 PDT
    assert trips.local_time("2025-11-02 01:30:00") == datetime(2025, 11, 2, 8, 30, tzinfo=UTC)  # first (PDT) 01:30
    assert trips.local_time("2026-01-15 12:00:00") == datetime(2026, 1, 15, 20, 0, tzinfo=UTC)
    assert trips.local_time("2019-01-31 17:57:44.6130") == datetime(2019, 2, 1, 1, 57, 44, 613000, tzinfo=UTC)
    for garbage in ("", None, "soon", "2026-02-30 10:00:00"):
        assert trips.local_time(garbage) is None
    stats: dict = {}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("t.csv", "\n".join(",".join(r) for r in [RIDE_HEADER, ride_record(started_at="garbage")]))
    buf.seek(0)
    rows = [r for b in trips.month_batches(buf, stats) for r in b]
    assert stats["bad_timestamps"] == 1
    assert rows[0][2] is None


def test_cached_zip_is_reused_only_when_its_size_matches(tmp_path):
    key = "202609-lyftbikes-tripdata.zip"
    calls = []

    class Body(io.BytesIO):
        def __enter__(self):
            return self

    def opener(url, timeout):
        calls.append(url)
        return Body(b"0123456789")

    got = trips.download(key, 10, tmp_path, opener=opener)
    assert got == tmp_path / key
    assert got.read_bytes() == b"0123456789"
    assert len(calls) == 1
    assert trips.download(key, 10, tmp_path, opener=opener) == got
    assert len(calls) == 1
    got.write_bytes(b"short")
    assert trips.download(key, 10, tmp_path, opener=opener).read_bytes() == b"0123456789"
    assert len(calls) == 2
    with pytest.raises(trips.TripError, match="listing says"):
        trips.download("202608-lyftbikes-tripdata.zip", 99, tmp_path, opener=opener)
    assert not (tmp_path / "202608-lyftbikes-tripdata.zip").exists()
    with pytest.raises(trips.TripError, match="unexpected key"):
        trips.download("../etc/passwd.zip", 10, tmp_path, opener=opener)
    assert trips.first_day("2026-09") == date(2026, 9, 1)


@pytest.mark.network
def test_live_listing_has_the_latest_months():
    listing = trips.fetch_listing()
    for month in ("2019-01", "2025-10", "2026-09"):
        assert trips.month_status(listing, month) == "published", month
    assert trips.month_status(listing, "2020-04") == "missing"
