"""Batch CLI (host). Run through tasks.py: batch-up, batch-stop, airflow, dag-test, test-airflow, scheduler-up,
scheduler-stop, trino-sql.

python -m dockwatch.batch batch-up                     Trino + airflow-db (no deps), airflow db migrate
python -m dockwatch.batch batch-stop                   stop every batch service (volumes kept)
python -m dockwatch.batch airflow <args...>            one-shot Airflow CLI in airflow-cli
python -m dockwatch.batch dag-test <dag_id> [k=v ...]  bounded `airflow dags test`; data/airflow/dag-test-<dag_id>.json
python -m dockwatch.batch test-airflow [pytest args]   tests/airflow in the Airflow image (SQLite metadata)
python -m dockwatch.batch scheduler-up | scheduler-stop
python -m dockwatch.batch trino-sql "<query>"
"""

import sys

from dockwatch.batch import host


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print(__doc__)
        sys.exit(2)
    cmd, rest = args[0], args[1:]
    try:
        if cmd == "batch-up":
            host.batch_up()
        elif cmd == "batch-stop":
            host.batch_stop()
        elif cmd == "airflow":
            host.airflow(*rest)
        elif cmd == "dag-test":
            if not rest:
                raise host.BatchError("usage: dag-test <dag_id> [key=value ...]")
            host.dag_test(rest[0], rest[1:])
        elif cmd == "test-airflow":
            host.test_airflow(*rest)
        elif cmd == "scheduler-up":
            host.scheduler_up()
        elif cmd == "scheduler-stop":
            host.scheduler_stop()
        elif cmd == "trino-sql":
            host.trino_sql(" ".join(rest))
        else:
            print(__doc__)
            sys.exit(2)
    except host.BatchError as e:
        print(f"batch {cmd}: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
