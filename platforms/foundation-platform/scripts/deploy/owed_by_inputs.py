#!/usr/bin/env python3
"""The enabled jobs owed a run because an input changed while their DAG was paused (root ADR-0336).

    python -I -B owed_by_inputs.py

Run in the Airflow scheduler container, where the release tree is mounted and Airflow's database is
configured; it only reads. Prints one job id per line (job_specs.owed_by_inputs), and the deploy
triggers each after it unpauses the DAGs (deploy-start-once.sh). A paused DAG gets no queue entry
for an event (2026-10-28: asset event 3912 started nothing for 13 hours).
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "orchestration" / "dags"))
from job_specs import load_specs, owed_by_inputs  # noqa: E402


def airflow_state():
    """(asset name -> newest event, DAG id -> newest run queued, DAG ids with an asset queue entry)."""
    from airflow.models.asset import AssetDagRunQueue, AssetEvent, AssetModel
    from airflow.models.dagrun import DagRun
    from airflow.settings import Session, configure_orm
    from sqlalchemy import func, select

    configure_orm()
    session = Session()
    try:
        events = dict(session.execute(select(AssetModel.name, func.max(AssetEvent.timestamp))
                                      .join(AssetEvent, AssetEvent.asset_id == AssetModel.id)
                                      .group_by(AssetModel.name)).all())
        runs = dict(session.execute(select(DagRun.dag_id, func.max(DagRun.queued_at)).group_by(DagRun.dag_id)).all())
        queued = set(session.scalars(select(AssetDagRunQueue.target_dag_id)).all())
    finally:
        session.rollback()  # read only: nothing to keep
        session.close()
    return events, runs, queued


if __name__ == "__main__":
    for job in owed_by_inputs(load_specs(), *airflow_state()):
        print(job)
