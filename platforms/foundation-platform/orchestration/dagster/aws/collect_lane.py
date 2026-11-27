"""A collector on AWS, with no host under it (root ADR-0616 A3, ADR-0623): the one engine every lane runs.

The lanes are the contract's (config/aws-platform.contract.json, collect.lanes): a job of the job list
(orchestration/jobs.v1.json, same id), the publisher command it runs, its env prefix, its beat (a pointer into its
source contract), its size class, its hand-over kind and its scope. definitions.py makes one asset per lane from
this module; nothing here names a lane. For a run:

1. When: at the beat its source contract declares (ADR-0623 §1), only when no run of it is in progress. A
   handoff lane (a provider quota) runs here only while its job is off on the host in the same release
   (job_specs.runs_on_host), so the one switch that turns the host copy off turns this one on.
2. Its lakehouse access, from the credential vendor (infra/aws/r2-vend) through the lane's own alias under the
   lane's own role, with the run's id: object read-write on its write prefixes and its state prefix (and read-only
   on its read prefixes), with an expiration. Step one of ADR-0623 §2-5: a collector commits as it goes and stops
   and resumes safely, so the credentials are not renewed; a run past the lifetime loses its last request and the
   next beat goes on. Before the owner has placed the token the vendor answers inert.
3. Its source keys (contract collect.source_secrets) from Secrets Manager under its own role; its database is the
   foundation runtime role on RDS (the Bronze ledger, root ADR-0616 A1b).
4. Its state, one R2 object per run (contract collect.state_reason): the newest <state>/<lane>/aws/ object, or with
   none the newest <state>/<lane>/host/ object the host copy wrote. With neither it asks its source nothing.
5. The publisher's command, one subprocess in the run's temporary directory; its lines are the step's log; its
   last `foundation-job-outcome` line decides as on the host (job_specs.job_outcome). Then the state goes back as a
   new object, also when the command failed: what it asked and counted is spent either way.

A lane that is not ready (vendor inert, a source key not placed, no state to start from) asks nothing and says
which; definitions.py turns that into a failing readiness check (ADR-0623 §4). This module is plain Python.
"""

import datetime
import hashlib
import io
import json
import os
import pathlib
import shlex
import subprocess
import sys
import tarfile
import tempfile
import urllib.parse

PLATFORM_ROOT = pathlib.Path(__file__).resolve().parents[3]
CONTRACT = PLATFORM_ROOT / "config" / "aws-platform.contract.json"
JOBS = PLATFORM_ROOT / "orchestration" / "jobs.v1.json"
GRAPH = PLATFORM_ROOT / "docs" / "catalog" / "pipeline-graph.v1.json"
LAKEHOUSE_CONTRACTS = PLATFORM_ROOT / "infra" / "lakehouse" / "contracts"
PUBLISHER = "foundation-outbox-publisher"
OUTCOME = "foundation-job-outcome "
MATERIALIZES = "changed"
QUIET = ("unchanged", "waiting", "skipped")
KIND_TAG = "perfectory-platform/kind"
LANE_TAG = "perfectory-platform/lane"
HANDOFF = "handoff"
OVERLAP = "overlap"
# The publisher's SDK runs `<this file> credential-process <job> <run> writer` for fresh keys (AWS credential_process)
# and again before they expire: one platform time limit for every lane, however long its run (root ADR-0623 §1,
# owner 2026-11-26; W101c 2026-11-27: onbid's first AWS run took 2 h 12 min and outlived its 1 h key).
CREDENTIAL_PROCESS = "credential-process"
CREDENTIAL_PROCESS_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_CREDENTIAL_PROCESS"


def contract():
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def jobs():
    return json.loads(JOBS.read_text(encoding="utf-8"))


def job(job_id, job_list=None):
    """The job list's entry for `job_id`."""
    return next(entry for entry in (job_list or jobs())["jobs"] if entry["id"] == job_id)


def _job_specs():
    dags = PLATFORM_ROOT / "orchestration" / "dags"
    if str(dags) not in sys.path:
        sys.path.insert(0, str(dags))
    import job_specs

    return job_specs


def on_host(job_list=None):
    """The ids of the jobs the host runs in this release (root ADR-0625's one answer)."""
    job_list = job_list or jobs()
    return _job_specs().runs_on_host(job_list, json.loads(GRAPH.read_text(encoding="utf-8")))


def lanes_here(platform=None, job_list=None):
    """{job id: lane} of the lanes that run on AWS in this release: every overlap lane, and a handoff lane only
    while its job is off on the host (contract collect.handover_reason)."""
    platform = platform or contract()
    hosted = on_host(job_list)
    return {job_id: lane for job_id, lane in platform["collect"]["lanes"].items()
            if lane["handover"] == OVERLAP or job_id not in hosted}


def resolve(document, pointer):
    """The value at JSON pointer `pointer` ("/a/b") in `document`."""
    value = document
    for part in pointer.lstrip("/").split("/"):
        value = value[part.replace("~1", "/").replace("~0", "~")]
    return value


def beat(lane):
    """(cron, timezone) of the lane's beat, from the pointer into its source contract."""
    name, _, pointer = lane["beat"].partition("#")
    declared = resolve(json.loads((LAKEHOUSE_CONTRACTS / name).read_text(encoding="utf-8")), pointer)
    return declared["cron"], declared["timezone"]


def vendor_function(platform):
    return f"{platform['tags']['project']}-r2-vend"


def lane_role_arn(platform, lane):
    """The lane's run role, under the contract's run path, the only one Dagster may pass (infra/aws/collect.tf)."""
    project = platform["tags"]["project"]
    return f"arn:aws:iam::{platform['account']['id']}:role{platform['ecs']['run_role_path']}{project}-{lane['role']}"


def run_tags(platform, job_id, lane):
    """The run's ECS task: the lane's role and size class, and the kind and lane tags the run queue's limits read.
    No time number: the platform's one ceiling is the instance's run monitoring (ADR-0623 §3)."""
    size = platform["collect"]["size_classes"][lane["size"]]
    return {
        "ecs/task_overrides": json.dumps({"taskRoleArn": lane_role_arn(platform, lane)}),
        "ecs/cpu": str(size["cpu"]),
        "ecs/memory": str(size["memory"]),
        KIND_TAG: "collect",
        LANE_TAG: job_id,
    }


def state_prefix(platform, job_id):
    return f"{platform['collect']['state_prefix']}{job_id}/"


class RunFailed(Exception):
    """The vendor refused, or the command exited non-zero or said `failed`; the message holds no value."""


class NotReady(Exception):
    """The lane asked its source nothing: what it waits for (a slot's name or the state), never a value."""


def vended(client, function, job_id, run_id):
    """The vendor's credentials for lane `job_id`, through the lane's alias."""
    response = client.invoke(FunctionName=f"{function}:{job_id}", InvocationType="RequestResponse",
                             Payload=json.dumps({"call": "credentials", "lane": job_id, "run_id": run_id}).encode())
    answer = json.loads(response["Payload"].read() or b"{}")
    if response.get("FunctionError"):
        raise RunFailed(f"the vendor failed for {job_id} ({response['FunctionError']}: {answer.get('errorType', '')})")
    if "refused" in answer:
        raise RunFailed(f"the vendor refused {job_id}: {answer['refused']}")
    if "inert" in answer:
        raise NotReady(f"the vendor's secret {answer['inert']} has no value yet (docs/runbooks/aws-platform.md)")
    return answer


def credential_process(job_id, run_id):
    """The command that prints lane `job_id`'s fresh writer keys, as the SDK's credential_process reads them."""
    return shlex.join([sys.executable, str(pathlib.Path(__file__).resolve()), CREDENTIAL_PROCESS, job_id, run_id])


def credential_process_answer(credentials):
    """The vendor's writer keys in credential_process's JSON (Version 1, with the time they expire)."""
    writer = credentials["writer"]
    return {"Version": 1, "AccessKeyId": writer["access_key_id"], "SecretAccessKey": writer["secret_access_key"],
            "SessionToken": writer["session_token"], "Expiration": writer["expiration"]}


def source_keys(client, platform, lane):
    """{env name: value} of the lane's source keys; NotReady names a slot that has no value yet."""
    project = platform["tags"]["project"]
    found = {}
    for name in lane["secrets"]:
        slot = f"{project}/source/{platform['collect']['source_secrets'][name]}"
        try:
            value = client.get_secret_value(SecretId=slot)["SecretString"].strip()
        except client.exceptions.ResourceNotFoundException:
            value = ""
        if not value:
            raise NotReady(f"the source key slot {slot} has no value yet (docs/runbooks/aws-platform.md)")
        found[name] = value
    return found


def database_url(client, platform):
    """The foundation database as its runtime role, over TLS."""
    secret = json.loads(client.get_secret_value(SecretId=platform["database"]["foundation"]["runtime_secret"])["SecretString"])
    return "postgres://{}:{}@{}:{}/{}?sslmode=require".format(
        urllib.parse.quote(secret["username"], safe=""), urllib.parse.quote(secret["password"], safe=""),
        secret["host"], secret["port"], secret["dbname"])


def endpoint(credentials):
    return f"https://{credentials['account_id']}.r2.cloudflarestorage.com"


def object_store(credentials):
    """An S3 client on the lakehouse bucket with the lane's writer credentials (its prefixes only)."""
    import boto3
    from botocore.config import Config

    writer = credentials["writer"]
    return boto3.client("s3", endpoint_url=endpoint(credentials), region_name="auto",
                        aws_access_key_id=writer["access_key_id"], aws_secret_access_key=writer["secret_access_key"],
                        aws_session_token=writer["session_token"],
                        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}))


def newest(store, bucket, prefix):
    """The key of the newest object under `prefix` (keys start with a UTC time), or None."""
    keys = []
    for page in store.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return max(keys) if keys else None


def unpack(blob, into):
    """Unpacks a state tarball into `into`, refusing any member that would land outside it."""
    into = pathlib.Path(into)
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as archive:
        archive.extractall(into, filter="data")


def pack(directory):
    """The directory as a gzipped tarball, its members in name order so the same state packs the same."""
    directory = pathlib.Path(directory)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for path in sorted(directory.rglob("*")):
            if path.is_file():
                info = archive.gettarinfo(str(path), arcname=str(path.relative_to(directory)))
                info.mtime, info.uid, info.gid, info.uname, info.gname = 0, 0, 0, "", ""
                with path.open("rb") as handle:
                    archive.addfile(info, handle)
    return buffer.getvalue()


def digest(directory):
    """A digest of the directory's files and bytes, to write a new state object only when the state changed."""
    directory = pathlib.Path(directory)
    hasher = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            hasher.update(str(path.relative_to(directory)).encode())
            hasher.update(path.read_bytes())
    return hasher.hexdigest()


def restore(store, bucket, prefix, into):
    """Loads the lane's newest state into `into`: (key read, "aws" | "host"). NotReady with no state at all."""
    for side in ("aws", "host"):
        key = newest(store, bucket, f"{prefix}{side}/")
        if key:
            unpack(store.get_object(Bucket=bucket, Key=key)["Body"].read(), into)
            return key, side
    raise NotReady(f"no state under {prefix} yet: the host copy writes one after its next run")


def save(store, bucket, prefix, run_id, directory, now=None):
    """Writes the state as a new object `<prefix>aws/<UTC time>-<run id>.tar.gz`; returns its key."""
    stamp = (now or datetime.datetime.now(datetime.timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix}aws/{stamp}-{run_id}.tar.gz"
    store.put_object(Bucket=bucket, Key=key, Body=pack(directory), ContentType="application/gzip", IfNoneMatch="*")
    return key


def environment(lane, credentials, url, keys, work, refresh):
    """The publisher's environment for one run; `refresh` is its credential_process command."""
    writer = credentials["writer"]
    prefix = lane["env_prefix"]
    env = {
        "FOUNDATION_PLATFORM_RUNTIME_ENV": "production",
        "FOUNDATION_PLATFORM_EXECUTION_CONTEXT": "service",
        # Its last line: time, peak RSS and bytes landed, measured as the host copy measures them.
        "FOUNDATION_PLATFORM_RUN_USAGE": "1",
        "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER": "r2",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": endpoint(credentials),
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET": credentials["bucket"],
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION": "auto",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID": writer["access_key_id"],
        "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY": writer["secret_access_key"],
        "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SESSION_TOKEN": writer["session_token"],
        CREDENTIAL_PROCESS_ENV: refresh,
        "DATABASE_URL": url,
        f"{prefix}_OUTPUT_DIR": str(work / "output"),
        f"{prefix}_STATE_DIR": str(work / "state"),
        f"{prefix}_LIVE_WRITE": "1",
    }
    reader = credentials.get("reader")
    if reader:
        env.update({
            "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID": reader["access_key_id"],
            "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY": reader["secret_access_key"],
            "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SESSION_TOKEN": reader["session_token"],
        })
    for name in lane["work_dirs"]:
        env[f"{prefix}_{name}"] = str(work / name.lower())
    env.update({f"{prefix}_{name}": value for name, value in lane["environment"].items()})
    env.update(keys)
    return env


def outcome(lines):
    """The word of the last outcome line; none counts as `changed`, as job_specs.job_outcome reads it."""
    for line in reversed(lines):
        if line.startswith(OUTCOME):
            return line[len(OUTCOME):].split(" ", 1)[0]
    return MATERIALIZES


def run(lane, env, log, program=None):
    """Runs the lane's command; returns (exit status, lines)."""
    program = program or os.environ.get("FOUNDATION_PLATFORM_PUBLISHER_BIN", PUBLISHER)
    argv = [*program, lane["command"]] if isinstance(program, (list, tuple)) else [program, lane["command"]]
    lines = []
    with subprocess.Popen(argv, env={**os.environ, **env}, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                          encoding="utf-8", errors="replace") as process:
        for line in process.stdout:
            lines.append(line.rstrip("\n"))
            log(lines[-1])
    return process.returncode, lines


def collect(job_id, run_id, secrets_client, lambda_client, log, program=None, store_factory=object_store):
    """One run of lane `job_id`: {"outcome", "state_read", "state_written", "seconds"}. NotReady when it asked
    nothing; RunFailed when the vendor refused or the command failed (after its state was saved)."""
    platform = contract()
    lane = platform["collect"]["lanes"][job_id]
    credentials = vended(lambda_client, vendor_function(platform), job_id, run_id)
    keys = source_keys(secrets_client, platform, lane)
    url = database_url(secrets_client, platform)
    store, bucket, prefix = store_factory(credentials), credentials["bucket"], state_prefix(platform, job_id)
    with tempfile.TemporaryDirectory(prefix=f"{job_id}-") as scratch:
        work = pathlib.Path(scratch)
        for name in ("output", "state", *(name.lower() for name in lane["work_dirs"])):
            (work / name).mkdir()
        read_key, side = restore(store, bucket, prefix, work / "state")
        log(f"{job_id}: state from {read_key} ({side})")
        before = digest(work / "state")
        started = datetime.datetime.now(datetime.timezone.utc)
        try:
            env = environment(lane, credentials, url, keys, work, credential_process(job_id, run_id))
            status, lines = run(lane, env, log, program)
        finally:
            written = None
            if digest(work / "state") != before or side == "host":
                # Fresh keys: the run may have outlived the ones it started with.
                fresh = store_factory(vended(lambda_client, vendor_function(platform), job_id, run_id))
                written = save(fresh, bucket, prefix, run_id, work / "state")
                log(f"{job_id}: state saved as {written}")
        seconds = round((datetime.datetime.now(datetime.timezone.utc) - started).total_seconds(), 1)
    word = outcome(lines)
    if status != 0 or word not in (MATERIALIZES, *QUIET):
        raise RunFailed(f"{lane['command']} exited {status} ({word}) after {seconds} s: {lines[-1] if lines else ''}")
    return {"outcome": word, "state_read": read_key, "state_written": written, "seconds": seconds}


if __name__ == "__main__":
    # credential-process <job id> <run id>: the lane's role asks the vendor through its own alias.
    if len(sys.argv) != 4 or sys.argv[1] != CREDENTIAL_PROCESS:
        sys.exit(f"usage: {sys.argv[0]} {CREDENTIAL_PROCESS} <job id> <run id>")
    import boto3

    _platform = contract()
    _answer = vended(boto3.client("lambda", region_name=_platform["region"]), vendor_function(_platform), sys.argv[2],
                     sys.argv[3])
    print(json.dumps(credential_process_answer(_answer)))
