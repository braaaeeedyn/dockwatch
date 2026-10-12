"""Inside the Airflow image: print the real state of one DAG run (`airflow dags test` exits 0 even when the run
failed). Usage: python -m dockwatch.batch.runstate <dag_id> <logical_date ISO-8601>

Prints one line `DAG_TEST_STATE {"state": ..., "run_id": ..., "tasks": {task_id[map_index]: state}}`.

`python -m dockwatch.batch.runstate fail-stale <dag_id>` marks every running / queued run of the DAG failed, with its
unfinished task instances. dag-test calls it only while no scheduler runs: a run left behind by a killed
`airflow dags test` has no job, so zombie detection never ends it, and with max_active_runs=1 it would block the
scheduler forever.
"""

import json
import sys
from datetime import datetime

UNFINISHED_TI = ("running", "queued", "scheduled", "up_for_retry", "up_for_reschedule", "deferred", "restarting")


def plain(state) -> str | None:
    return None if state is None else str(getattr(state, "value", state))


def fail_stale(dag_id: str, session) -> list[str]:
    """Fail the DAG's running / queued runs and their unfinished task instances; return the failed run_ids."""
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.state import DagRunState, TaskInstanceState

    failed = []
    runs = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.state.in_(("running", "queued")))
    for dr in runs.all():
        tis = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id == dr.run_id)
        for ti in tis:
            if ti.state is None or plain(ti.state) in UNFINISHED_TI:
                ti.state = TaskInstanceState.FAILED
        dr.set_state(DagRunState.FAILED)
        failed.append(dr.run_id)
    session.flush()
    return failed


def main() -> None:
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session

    if sys.argv[1] == "fail-stale":
        with create_session() as s:
            for run_id in fail_stale(sys.argv[2], s):
                print(f"FAILED_STALE_RUN {sys.argv[2]} {run_id}", flush=True)
        return
    dag_id, logical = sys.argv[1], datetime.fromisoformat(sys.argv[2])
    with create_session() as s:
        dr = s.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.execution_date == logical).one_or_none()
        doc: dict = {"state": None, "run_id": None, "tasks": {}}
        if dr is not None:
            doc["state"] = plain(dr.state)
            doc["run_id"] = dr.run_id
            tis = s.query(TaskInstance).filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id == dr.run_id)
            for ti in tis:
                key = ti.task_id if ti.map_index < 0 else f"{ti.task_id}[{ti.map_index}]"
                doc["tasks"][key] = plain(ti.state)
    print("DAG_TEST_STATE " + json.dumps(doc), flush=True)


if __name__ == "__main__":
    main()
