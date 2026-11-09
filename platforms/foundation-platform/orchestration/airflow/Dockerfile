# The release's Airflow (compose.orchestration.yml, every Airflow service): the pinned apache/airflow with
# dagster-airlift's in-airflow part, so a job at `dagster_stage: proxied` can hand its `run` task to Dagster
# (orchestration/dags/foundation_jobs.py, root ADR-0431 §5). Apache Airflow's own advice for production
# is this: extend the reference image, never install at container start (_PIP_ADDITIONAL_REQUIREMENTS).
# The DAGs, the job list and the code are the release's, mounted read-only as before; the image adds one
# wheel and nothing else.
FROM apache/airflow:3.3.2@sha256:e9982ad3f49a60418622e1baf80c34b0daf3fa5091a336cb5ee61f0cdd189371

ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# The wheel, checked against the lock's hash, without dependencies: what it needs is Airflow's and keeps
# Airflow's pins, which `pip check` then holds to the wheel's requirements. The import is the proxy's.
COPY orchestration/airflow/requirements.txt /tmp/airlift-requirements.txt
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: --no-deps -r /tmp/airlift-requirements.txt \
    && pip check \
    && python -c "from dagster_airlift.in_airflow import BaseProxyTaskToDagsterOperator" \
    && rm /tmp/airlift-requirements.txt
