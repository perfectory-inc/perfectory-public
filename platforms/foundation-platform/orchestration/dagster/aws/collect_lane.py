"""A collector on AWS as a Dagster asset, with no host under it (root ADR-0616 A2, ADR-0623).

The lanes are the contract's (config/aws-platform.contract.json, collect.lanes): a job of the job list
(orchestration/jobs.v1.json), the publisher command it runs, its size class and its scope. A lane carries no
time number (ADR-0623 §5). For a run:

1. When: once a day (every collector refreshes daily, ADR-0623 §1), at the hour its source contract gives
   for the full walk, and only when no run of it is in progress (definitions.py's start condition).
2. Its lakehouse access, from the credential vendor (infra/aws/r2-vend), invoked through the lane's own
   alias under the lane's own role, with the run's id: `pin` once (the read tables' current snapshots, read
   to the end with the publisher's `vended` catalog provider), then `credentials` (read-write on the lane's
   write prefixes, read-only on its tables' locations, with an expiration). Step one: the collector stops
   and resumes safely and commits as it goes, so the credentials are not renewed (ADR-0623 §2-5).
   Before the owner has placed the tokens the vendor answers inert, and the run says so.
3. Its database: the lane role reads the foundation runtime role's secret, the only secret it reads.
4. The publisher's command, one subprocess in the run's own temporary directory. Its lines are the step's
   log; its last `foundation-job-outcome` line decides as on the host (orchestration/dags/job_specs.py
   job_outcome). The only whole-run limit is the platform's ceiling (ADR-0623 §3).

This module is plain Python; definitions.py makes the Dagster objects from it.
"""

import json
import os
import pathlib
import subprocess
import tempfile
import urllib.parse

PLATFORM_ROOT = pathlib.Path(__file__).resolve().parents[3]
CONTRACT = PLATFORM_ROOT / "config" / "aws-platform.contract.json"
JOBS = PLATFORM_ROOT / "orchestration" / "jobs.v1.json"
LAKEHOUSE_CONTRACTS = PLATFORM_ROOT / "infra" / "lakehouse" / "contracts"
PUBLISHER = "foundation-outbox-publisher"
OUTCOME = "foundation-job-outcome "
MATERIALIZES = "changed"
QUIET = ("unchanged", "waiting", "skipped")
KIND_TAG = "perfectory-platform/kind"
LANE_TAG = "perfectory-platform/lane"


def contract():
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def jobs():
    return json.loads(JOBS.read_text(encoding="utf-8"))


def job(job_id):
    """The job list's entry for `job_id` (its description, its landed bound)."""
    return next(entry for entry in jobs()["jobs"] if entry["id"] == job_id)


def source_contract(lane):
    return json.loads((LAKEHOUSE_CONTRACTS / lane["source_contract"]).read_text(encoding="utf-8"))


def start_cron(lane):
    """Once a day, at the hour the source contract gives for the run that walks the whole list."""
    return f"0 {int(source_contract(lane)['full_walk_hour_utc'])} * * *"


def vendor_function(platform):
    return f"{platform['tags']['project']}-r2-vend"


def lane_role_arn(platform, lane):
    """The lane's run role, under the contract's run path, the only one Dagster may pass (infra/aws/collect.tf)."""
    project = platform["tags"]["project"]
    return f"arn:aws:iam::{platform['account']['id']}:role{platform['ecs']['run_role_path']}{project}-{lane['role']}"


def run_tags(platform, job_id, lane):
    """The run's ECS task: the lane's role and size class; the platform's one run ceiling; and the kind and
    lane tags the run queue's limits read."""
    size = platform["collect"]["size_classes"][lane["size"]]
    return {
        "ecs/task_overrides": json.dumps({"taskRoleArn": lane_role_arn(platform, lane)}),
        "ecs/cpu": str(size["cpu"]),
        "ecs/memory": str(size["memory"]),
        "dagster/max_runtime": str(platform["collect"]["run_ceiling_seconds"]),
        KIND_TAG: "collect",
        LANE_TAG: job_id,
    }


class RunFailed(Exception):
    """The vendor refused, or the command exited non-zero or said `failed`; the message holds no value."""


def vended(client, function, job_id, call, run_id):
    """The vendor's answer to `call` for lane `job_id`, through the lane's alias."""
    response = client.invoke(FunctionName=f"{function}:{job_id}", InvocationType="RequestResponse",
                             Payload=json.dumps({"call": call, "lane": job_id, "run_id": run_id}).encode())
    answer = json.loads(response["Payload"].read() or b"{}")
    if response.get("FunctionError"):
        raise RunFailed(f"the vendor failed for {job_id} ({response['FunctionError']}: {answer.get('errorType', '')})")
    if "refused" in answer:
        raise RunFailed(f"the vendor refused {job_id}: {answer['refused']}")
    return answer


def database_url(client, platform):
    """The foundation database as its runtime role, over TLS."""
    secret = json.loads(client.get_secret_value(SecretId=platform["database"]["foundation"]["runtime_secret"])["SecretString"])
    return "postgres://{}:{}@{}:{}/{}?sslmode=require".format(
        urllib.parse.quote(secret["username"], safe=""), urllib.parse.quote(secret["password"], safe=""),
        secret["host"], secret["port"], secret["dbname"])


def environment(lane, pin, credentials, database_url, output_dir):
    """The publisher's environment for one run, from the vendor's pin and credentials."""
    account, bucket = credentials["account_id"], credentials["bucket"]
    writer, reader = credentials["writer"], credentials.get("reader")
    env = {
        "FOUNDATION_PLATFORM_RUNTIME_ENV": "production",
        "FOUNDATION_PLATFORM_EXECUTION_CONTEXT": "service",
        "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER": "r2",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": f"https://{account}.r2.cloudflarestorage.com",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET": bucket,
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION": "auto",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID": writer["access_key_id"],
        "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY": writer["secret_access_key"],
        "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SESSION_TOKEN": writer["session_token"],
        # The pin stands for the catalog: no catalog token, no catalog request.
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER": "vended",
        "FOUNDATION_PLATFORM_LAKEHOUSE_VENDED_TABLES": json.dumps(pin["tables"]),
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI": f"https://catalog.cloudflarestorage.com/{account}/{bucket}",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE": f"{account}_{bucket}",
        "DATABASE_URL": database_url,
        lane["output_dir_env"]: str(output_dir),
    }
    if reader:
        env.update({
            "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID": reader["access_key_id"],
            "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY": reader["secret_access_key"],
            "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SESSION_TOKEN": reader["session_token"],
        })
    env.update(lane["environment"])
    return env


def outcome(lines):
    """The word of the last outcome line; none counts as `changed`, as job_specs.job_outcome reads it."""
    for line in reversed(lines):
        if line.startswith(OUTCOME):
            return line[len(OUTCOME):].split(" ", 1)[0]
    return MATERIALIZES


def summary(lines, prefix):
    for line in reversed(lines):
        if line.startswith(prefix):
            try:
                return json.loads(line[len(prefix):])
            except ValueError:
                return {}
    return {}


def run(lane, env, log, program=None):
    """Runs the lane's command; returns its lines. Raises RunFailed as the host's runner fails a run."""
    program = program or os.environ.get("FOUNDATION_PLATFORM_PUBLISHER_BIN", PUBLISHER)
    process = subprocess.Popen([program, lane["command"]], env={**os.environ, **env}, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
    lines = []
    for line in process.stdout:
        lines.append(line.rstrip("\n"))
        log(lines[-1])
    status = process.wait()
    word = outcome(lines)
    if status != 0 or word not in (MATERIALIZES, *QUIET):
        raise RunFailed(f"{lane['command']} exited {status} ({word}): {lines[-1] if lines else ''}")
    return lines


def collect(job_id, run_id, secrets_client, lambda_client, log, program=None):
    """One run of lane `job_id`: (outcome, summary, inert). `inert` names the secret not placed yet."""
    platform = contract()
    lane = platform["collect"]["lanes"][job_id]
    function = vendor_function(platform)
    pin = vended(lambda_client, function, job_id, "pin", run_id)
    if "inert" in pin:
        log(f"{job_id}: inert, the secret {pin['inert']} has no value yet (docs/runbooks/aws-platform.md)")
        return "waiting", {}, pin["inert"]
    credentials = vended(lambda_client, function, job_id, "credentials", run_id)
    url = database_url(secrets_client, platform)
    with tempfile.TemporaryDirectory(prefix=f"{job_id}-") as work:
        lines = run(lane, environment(lane, pin, credentials, url, pathlib.Path(work)), log, program)
    return outcome(lines), summary(lines, lane["summary_prefix"]), None
