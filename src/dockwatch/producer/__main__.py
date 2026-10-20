"""CLI: python -m dockwatch.producer {run,once,topics,replay}."""

import argparse
import json
import logging
import signal
import time

from dockwatch.config import get_settings
from dockwatch.gbfs.client import GbfsClient
from dockwatch.gbfs.models import Envelope
from dockwatch.producer.archive import build_archive
from dockwatch.producer.kafka import KafkaPublisher, create_topics
from dockwatch.producer.messages import encode, explode_station_status
from dockwatch.producer.poller import Poller

log = logging.getLogger("dockwatch.producer")


def _client(s) -> GbfsClient:
    return GbfsClient(s.gbfs_discovery_url, s.gbfs_version, s.gbfs_language, s.user_agent, s.http_timeout_s)


def _poller(s, no_archive: bool) -> Poller:
    return Poller(
        client=_client(s),
        publisher=KafkaPublisher(s.kafka_bootstrap),
        archive=None if no_archive else build_archive(s),
        topics={
            "station_status": s.topic_station_status,
            "station_information": s.topic_station_information,
        },
        dlq_topic=s.topic_dlq,
    )


def cmd_run(args) -> None:
    stop = {"flag": False}

    def _stop(*_):
        log.info("stopping after the current poll")
        stop["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    _poller(get_settings(), args.no_archive).run(lambda: stop["flag"])


def cmd_once(args) -> None:
    poller = _poller(get_settings(), args.no_archive)
    for feed in poller.topics:
        print(poller.poll_feed(feed))
    poller.publisher.flush()


def cmd_setup(_args) -> None:
    """Create the Kafka topics and, on the local target, the S3 bucket."""
    s = get_settings()
    created = create_topics(s)
    print("topics created:", created or "none (all exist)")
    archive = build_archive(s)
    if s.target == "local" and hasattr(archive, "ensure_bucket"):
        print("bucket created:", archive.ensure_bucket())


def cmd_replay(args) -> None:
    """Re-publish an archived day of station_status at N x speed, to the .replay topic by default. --skip / --limit
    take a slice of the day's snapshots in time order. Pacing follows the feed's schedule (snapshot k goes out at
    start + (last_updated_k - last_updated_0) / speed), so time spent publishing does not lower the achieved speed."""
    if args.skip < 0 or (args.limit is not None and args.limit < 1):
        raise SystemExit("--skip must be >= 0 and --limit >= 1")
    s = get_settings()
    archive = build_archive(s)
    keys = archive.list_keys(f"raw/gbfs/station_status/dt={args.date}/")
    if not keys:
        raise SystemExit(f"no archived station_status snapshots for {args.date}")
    day = len(keys)
    keys = keys[args.skip : None if args.limit is None else args.skip + args.limit]
    if not keys:
        raise SystemExit(f"--skip {args.skip} leaves none of the {day} snapshots of {args.date}")
    topic = args.topic or f"{s.topic_station_status}.replay"
    publisher = KafkaPublisher(s.kafka_bootstrap, client_id="dockwatch-replay")
    start, sent, first = time.time(), 0, None
    for key in keys:
        env = Envelope.model_validate(json.loads(archive.get(key)))
        if first is None:
            first = env.last_updated
        elif args.speed > 0:
            time.sleep(max(0.0, start + (env.last_updated - first) / args.speed - time.time()))
        for msg in explode_station_status(env, time.time()).messages:
            publisher.publish(topic, msg.key, encode({**msg.value, "replay": True}))
            sent += 1
    publisher.flush()
    elapsed = max(time.time() - start, 1e-9)
    print(
        f"replayed {len(keys)} snapshots ({args.skip}..{args.skip + len(keys) - 1} of {day}), {sent} messages to "
        f"{topic} in {elapsed:.1f}s ({sent / elapsed:.0f} msg/s)"
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="dockwatch.producer")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name, fn, helptext in (
        ("run", cmd_run, "poll the GBFS feeds forever"),
        ("once", cmd_once, "poll each feed once and exit"),
    ):
        p = sub.add_parser(name, help=helptext)
        p.add_argument("--no-archive", action="store_true", help="skip the raw snapshot archive")
        p.set_defaults(fn=fn)
    sub.add_parser("setup", help="create missing Kafka topics (and the local bucket)").set_defaults(fn=cmd_setup)
    p = sub.add_parser("replay", help="replay an archived day")
    p.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--speed", type=float, default=10.0, help="N x real time; 0 = as fast as possible")
    p.add_argument("--topic", default=None)
    p.add_argument("--skip", type=int, default=0, help="start at the day's Nth snapshot (time order)")
    p.add_argument("--limit", type=int, default=None, help="replay at most N snapshots")
    p.set_defaults(fn=cmd_replay)
    args = parser.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
