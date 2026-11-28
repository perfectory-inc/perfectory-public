# The Dagster webserver and daemon of root ADR-0333 stage 0 (compose.orchestration.yml, profile
# dagster-observe). The image holds the locked dependencies and the instance config only; the code
# location and the contracts it is generated from are the release's, mounted read-only at
# /opt/foundation-platform/current as Airflow's DAGs are, so both orchestrators read one tree.
FROM python:3.15.2-slim-trixie@sha256:08a4c22c92b79ec821a7ad49768480039ee5b7accac3955228e75366e30abc8e

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DAGSTER_HOME=/opt/dagster/home

# Wheels only, every one checked against the lock's hashes: nothing is compiled or resolved here.
COPY orchestration/dagster/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt \
    && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin dagster \
    && mkdir -p /opt/dagster/home /var/lib/dagster \
    && chown dagster:dagster /var/lib/dagster

COPY orchestration/dagster/dagster.yaml orchestration/dagster/workspace.yaml /opt/dagster/home/
# The writable webserver's gate and what it allows (root ADR-0565 §8): in the image, not the mounted
# release, so a change to them changes the image and compose recreates the webserver that serves them.
COPY orchestration/dagster/writer_gate.py orchestration/dags/dagster_writes.py /opt/dagster/gate/

USER dagster
WORKDIR /var/lib/dagster
