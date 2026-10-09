"""CDC CLI (host). Run through tasks.py: connect-up, cdc-reset, cdc-catchup, cdc-verify, cdc-e2e.

python -m dockwatch.cdc connect-up             start Kafka Connect, register infra/connect/ops.json, wait RUNNING
python -m dockwatch.cdc connect-stop
python -m dockwatch.cdc reset                  drop lake.ops.* + checkpoints, the slot, the five tables (base schema
                                               re-created) and the ops.public.* / dockwatch-connect-* topics
python -m dockwatch.cdc catchup [--replay]     Spark MERGE INTO lake.ops.* (availableNow); --replay = from earliest
python -m dockwatch.cdc verify                 Postgres vs Iceberg rows / columns / checksums; exit 1 on a mismatch
python -m dockwatch.cdc e2e --seed 42 --events 1500 [--stations 80]
python -m dockwatch.cdc kafka-counts           change events per table and op in ops.public.*
"""

import argparse
import json
import sys

from dockwatch.cdc import runner


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m dockwatch.cdc")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("connect-up")
    sub.add_parser("connect-stop")
    sub.add_parser("reset")
    a = sub.add_parser("catchup")
    a.add_argument("--replay", action="store_true", help="fresh checkpoint from earliest (proves idempotence)")
    sub.add_parser("verify")
    a = sub.add_parser("e2e")
    a.add_argument("--seed", type=int, default=42)
    a.add_argument("--events", type=int, default=1500, help="simulated minutes")
    a.add_argument("--stations", type=int, default=80)
    sub.add_parser("kafka-counts")
    args = p.parse_args(argv)
    try:
        if args.cmd == "connect-up":
            runner.connect_up()
            print("Kafka Connect heap:", runner.connect_heap())
        elif args.cmd == "connect-stop":
            runner.connect_stop()
        elif args.cmd == "reset":
            print(json.dumps(runner.reset(), indent=1))
        elif args.cmd == "catchup":
            print(json.dumps(runner.catchup(replay=args.replay), indent=1))
        elif args.cmd == "verify":
            runner.exit_code(runner.verify()["ok"])
        elif args.cmd == "e2e":
            runner.exit_code(runner.e2e(args.seed, args.events, args.stations)["ok"])
        elif args.cmd == "kafka-counts":
            print(json.dumps(runner.kafka_counts(), indent=1))
    except runner.CdcError as e:
        print(f"cdc {args.cmd}: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
