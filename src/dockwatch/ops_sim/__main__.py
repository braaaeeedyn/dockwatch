"""Ops simulator CLI. Run through tasks.py: ops-schema, ops-sim, ops-explain.

python -m dockwatch.ops_sim schema [--base-only]       schema.sql (+ migrations unless --base-only) on `ops`
python -m dockwatch.ops_sim seeded --seed 42 --events 1500 [--stations 80]
                                                        bounded deterministic run into empty tables; applies
                                                        migration 001 at tick events // 2
python -m dockwatch.ops_sim live                        reacts to gbfs.alerts every 60 s (needs the Spark stream)
python -m dockwatch.ops_sim explain [--out sql/ops/PLANS.md]
"""

import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path

from dockwatch.config import get_settings
from dockwatch.ops_sim import db
from dockwatch.ops_sim.model import Model, Station, parse_alert
from dockwatch.ops_sim.synthetic import SeededSimulation, plates

log = logging.getLogger("dockwatch.ops_sim")


def cmd_schema(args) -> None:
    with db.connect(get_settings().ops_db_dsn) as conn:
        for f in db.apply_schema(conn, base_only=args.base_only):
            print("applied", f)


def cmd_seeded(args) -> None:
    sim = SeededSimulation(seed=args.seed, events=args.events, n_stations=args.stations)
    t = time.monotonic()
    with db.connect(get_settings().ops_db_dsn) as conn:
        counts = db.table_counts(conn)
        if any(counts.values()):
            sys.exit(f"the ops tables are not empty ({counts}); reset first (python tasks.py cdc-reset)")
        ex = db.run_seeded(conn, sim, range(args.events))
        counts = db.table_counts(conn)
    print(
        json.dumps(
            {
                "seed": args.seed,
                "events": args.events,
                "seconds": round(time.monotonic() - t, 1),
                "rows": counts,
                "expected_events": ex.expected,
                "query_calls": dict(ex.calls.most_common()),
            },
            indent=1,
        )
    )


def cmd_explain(args) -> None:
    from dockwatch.ops_sim.explain import run

    print(json.dumps(run(get_settings().ops_db_dsn, Path(args.out)), indent=1))


def station_from_info(value: dict) -> Station | None:
    try:
        return Station(
            station_id=str(value["station_id"]),
            name=str(value["name"]),
            short_name=value.get("short_name"),
            lat=float(value["lat"]),
            lon=float(value["lon"]),
            capacity=max(int(value.get("capacity") or 0), 0),
            region_id=value.get("region_id"),
        )
    except (KeyError, TypeError, ValueError):
        return None


def cmd_live(args) -> None:
    """Consume gbfs.alerts (group dockwatch-ops-sim) with a 60 s tick; stations come from gbfs.station_information.

    Not part of the checked run. Model state starts empty on each start, so ops that contradict the database
    (e.g. a job left open by an earlier run) are skipped and logged.
    """
    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

    s = get_settings()
    model = Model(seed=args.seed)
    stop = {"now": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(now=True))

    with db.connect(s.ops_db_dsn) as conn:
        model.with_priority = "priority" in [
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'rebalancing_jobs'"
            )
        ]
        ex = db.Executor(conn)

        def apply(ops) -> None:
            for op in ops:
                try:
                    with conn.transaction():
                        ex.apply_one(op)
                except db.Conflict as e:
                    log.warning("skipped %s: %s", op.action, e)

        info = Consumer(
            {"bootstrap.servers": s.kafka_bootstrap, "group.id": "dockwatch-ops-sim-info", "enable.auto.commit": False}
        )
        parts = info.list_topics(s.topic_station_information, timeout=10).topics[s.topic_station_information]
        tps = [TopicPartition(s.topic_station_information, p, OFFSET_BEGINNING) for p in parts.partitions]
        ends = {tp.partition: info.get_watermark_offsets(tp, timeout=10)[1] for tp in tps}
        info.assign(tps)
        latest: dict[str, Station] = {}
        done = {p for p, hi in ends.items() if hi == 0}
        while len(done) < len(ends):
            msg = info.poll(5)
            if msg is None:
                break
            if msg.error():
                continue
            st = station_from_info(json.loads(msg.value()))
            if st is not None:
                latest[st.station_id] = st
            if msg.offset() + 1 >= ends[msg.partition()]:
                done.add(msg.partition())
        info.close()
        ops = [op for sid in sorted(latest) for op in model.add_station(latest[sid], 0)]
        ops += model.setup([], plates(model.params.vans))
        apply(ops)
        log.info("live: %d stations, %d vans", len(latest), len(model.vans))

        alerts = Consumer(
            {
                "bootstrap.servers": s.kafka_bootstrap,
                "group.id": "dockwatch-ops-sim",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        alerts.subscribe([s.topic_alerts])
        tick = 1
        while not stop["now"]:
            deadline = time.monotonic() + 60
            batch = []
            while time.monotonic() < deadline and not stop["now"]:
                msg = alerts.poll(1.0)
                if msg is None or msg.error():
                    continue
                try:
                    batch.append(parse_alert(msg.value()))
                except ValueError as e:
                    log.warning("bad alert: %s", e)
            apply(model.step(tick, int(time.time()), batch))
            if batch:
                alerts.commit(asynchronous=False)
            log.info("tick %d: %d alerts, expected events %s", tick, len(batch), ex.expected)
            tick += 1
        alerts.close()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="python -m dockwatch.ops_sim")
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("schema")
    a.add_argument("--base-only", action="store_true", help="schema.sql only, no migrations")
    a = sub.add_parser("seeded")
    a.add_argument("--seed", type=int, default=42)
    a.add_argument("--events", type=int, default=1500, help="simulated minutes")
    a.add_argument("--stations", type=int, default=80)
    a = sub.add_parser("live")
    a.add_argument("--seed", type=int, default=42)
    a = sub.add_parser("explain")
    a.add_argument("--out", default="sql/ops/PLANS.md")
    args = p.parse_args(argv)
    {"schema": cmd_schema, "seeded": cmd_seeded, "live": cmd_live, "explain": cmd_explain}[args.cmd](args)


if __name__ == "__main__":
    main()
