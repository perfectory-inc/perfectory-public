# Foundation Platform Low Cost Production Hardening Runbook

## Purpose

This runbook records the low-cost production hardening baseline for foundation-platform. It does not
replace M3.2 cutover completion evidence. It exists so capacity claims are based on measured
load-test artifacts, not estimates.

## Recovery Design

Cloudflare R2 does not expose S3 bucket versioning. Foundation Platform therefore uses two different
recovery controls for two different storage contracts:

- The governed lakehouse Bronze prefix is immutable raw evidence. Apply the checked-in
  `bronze-raw-30-days` Bucket Lock policy to the configured lakehouse bucket and read it back before
  allowing live collection writes.
- PostgreSQL recovery uses a physically separate bucket supplied through
  `FOUNDATION_RECOVERY_R2_BUCKET`. Do not Bucket Lock the whole pgBackRest repository because
  pgBackRest updates metadata such as `backup.info` and `archive.info`. Its controls are a dedicated
  bucket and credentials, client-side AES-256-CBC encryption, continuous WAL archiving, 35-day
  full-backup retention, and restore rehearsal.

Apply the checked-in lock declaration to the configured logical lakehouse bucket with the official
Cloudflare CLI and verify it immediately:

```bash
CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=... R2_BUCKET_NAME=... \
  wrangler r2 bucket lock set "$R2_BUCKET_NAME" \
  --file infra/cloudflare/foundation-platform-lakehouse-prod.bucket-lock.json --force
CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=... R2_BUCKET_NAME=... \
  wrangler r2 bucket lock list "$R2_BUCKET_NAME"
```

Do not add a lock for the pgBackRest repository unless completed backup objects and mutable repository
metadata have first been moved into physically separate prefixes and an end-to-end backup plus expiry
test has passed.

## Baseline Command

Run the read-path k6 smoke after the API reports readiness:

```bash
FOUNDATION_PLATFORM_API_URL=http://localhost:8080 \
FOUNDATION_PLATFORM_LOAD_DURATION=5m \
FOUNDATION_PLATFORM_LOAD_READ_RPS=20 \
FOUNDATION_PLATFORM_LOAD_HEALTH_RPS=5 \
k6 run --summary-export target/load/summary.json scripts/load/foundation-read-smoke.js
```

The k6 run writes JSON evidence under `target/load`.

## Validation Only

In environments where k6 is not installed, validate the script without running load:

```bash
k6 inspect scripts/load/foundation-read-smoke.js
```

This checks the script parses and its scenarios resolve. It does not make a capacity claim.

## Initial Targets

- `/healthz` returns `200`.
- `/readyz` returns `200` before load starts.
- Hot read p95 stays below 500 ms.
- Hot read p99 stays below 1500 ms.
- Failed request rate stays below 1 percent.
- Overload must degrade as bounded `429`, `503`, or timeout responses, not process collapse.

## Capacity Claim Format

Only claim capacity with evidence:

```text
On instance type X with Postgres config Y and Redis config Z,
foundation-platform handled N read RPS for D duration with:
p95 <= A ms, p99 <= B ms, error rate <= C%, no restart, no OOM, no DB saturation.
```

## Required Evidence

- k6 summary JSON from `target/load`.
- Platform logs for the same time window.
- `/metrics` scrape for request count, error rate, latency, DB pressure, Redis state, and outbox state.
- Deployment target details: host type, CPU, memory, Postgres limits, Redis limits, and commit SHA.

## Process Manager Guardrails

Use a process manager with explicit restart and shutdown behavior before making any production
claim:

- Restart on failure with backoff. Do not use a tight infinite restart loop.
- Set a startup timeout and fail deployment if `/readyz` does not return `200`.
- Set a graceful stop timeout so in-flight requests can finish before the process is killed.
- Load environment variables from an environment file managed outside Git.
- Send logs to journald, Docker logs, or another retained log sink.
- Record the deployed commit SHA, container digest, and environment file version in the rollout
  note.

## PostgreSQL Backup Policy

The recovery image is pinned in `infra/postgres/Dockerfile.recovery` and uses pgBackRest 2.58.0.
`compose.recovery.yml` enables synchronous WAL archive push with `archive_timeout=60s` and keeps the
repository outside the application host in R2.

- Full backup: Sunday, or whenever no valid full backup exists.
- Differential backup: every other day.
- Schedule: daily at 02:15 local server time with up to 15 minutes randomized delay.
- Retention: full backups remain recoverable for 35 days; required WAL is retained by pgBackRest.
- Encryption: pgBackRest AES-256-CBC using a passphrase stored outside Git.
- Access: credentials must be scoped to the dedicated recovery bucket and must not be shared with the
  lakehouse writer.
- RPO objective: at most 5 minutes. The configured 60-second archive timeout is stricter, while the
  larger objective leaves room for network and alerting delay.
- RTO objective: at most 2 hours for the current deployment class.
- Redis remains disposable cache/idempotency state and is not restored as canonical data.

Install releases under `/opt/foundation-platform/releases/<git-sha>`. The deployment entrypoint
atomically switches `/opt/foundation-platform/current`, records the prior target in `previous`, and
keeps mutable recovery evidence outside immutable releases in `/var/lib/foundation-platform/recovery`:

```bash
release_id="$(git rev-parse HEAD)"
git archive --format=tar.gz --output="/tmp/foundation-${release_id}.tar.gz" "${release_id}"
sudo FOUNDATION_PLATFORM_RELEASE_ROOT=/opt/foundation-platform \
  FOUNDATION_PLATFORM_STATE_ROOT=/var/lib/foundation-platform \
  scripts/deploy/foundation-release.sh install \
    "${release_id}" "/tmp/foundation-${release_id}.tar.gz"
```

The same release entrypoint prepares lakehouse compute state outside the immutable source tree:

- `/var/lib/foundation-platform/lakehouse`
- `/var/lib/foundation-platform/remote-lakehouse`

Both directories are owned by the configured lakehouse runtime UID/GID (default `185:185`) and are
mounted by `compose.lakehouse.yml`. A release must never write mutable Spark or lakehouse output under
`/opt/foundation-platform/releases/<git-sha>/target`.

Install the scheduler only after `current` identifies the intended exact commit and
`/etc/foundation-platform/recovery.env` exists with mode `0600`. Set
`FOUNDATION_RECOVERY_EVIDENCE_DIR=/var/lib/foundation-platform/recovery` in that file, or use the
same systemd default:

```bash
sudo install -o root -g root -m 0644 \
  infra/systemd/foundation-postgres-backup.service \
  /etc/systemd/system/foundation-postgres-backup.service
sudo install -o root -g root -m 0644 \
  infra/systemd/foundation-postgres-backup.timer \
  /etc/systemd/system/foundation-postgres-backup.timer
sudo systemctl daemon-reload
sudo systemctl enable --now foundation-postgres-backup.timer
sudo systemctl start foundation-postgres-backup.service
systemctl show foundation-postgres-backup.timer -p ActiveState -p NextElapseUSecRealtime
journalctl -u foundation-postgres-backup.service --since today
```

The first service run must finish successfully before the timer is accepted as operational.

Rollback switches the symlink without mutating either release. Restart affected services and rerun
readiness checks after the switch:

```bash
sudo FOUNDATION_PLATFORM_RELEASE_ROOT=/opt/foundation-platform \
  FOUNDATION_PLATFORM_STATE_ROOT=/var/lib/foundation-platform \
  /opt/foundation-platform/current/scripts/deploy/foundation-release.sh rollback
readlink /opt/foundation-platform/current
```

## Runtime Compose Entrypoint

All production-runtime Compose operations must go through `scripts/deploy/foundation-runtime.sh`.
The entrypoint always merges `docker-compose.yml` with `compose.recovery.yml`, so starting an API or
observability service cannot silently replace the recovery-enabled PostgreSQL image with the local
development image or disable WAL archiving.

```bash
cd /opt/foundation-platform/current
sudo scripts/deploy/foundation-runtime.sh up -d --build \
  postgres redis foundation-api alertmanager prometheus
sudo scripts/deploy/foundation-runtime.sh ps
```

Lakehouse services use the same recovery-safe wrapper but a separate Compose project so compute can
be operated independently from the API and database runtime:

```bash
sudo env FOUNDATION_PLATFORM_COMPOSE_PROJECT=foundation-platform-compute \
  scripts/deploy/foundation-runtime.sh --profile lakehouse-query up -d trino
sudo env FOUNDATION_PLATFORM_COMPOSE_PROJECT=foundation-platform-compute \
  scripts/deploy/foundation-runtime.sh --profile lakehouse-batch up -d spark
```

Use the same entrypoint for a controlled API alert rehearsal:

```bash
sudo scripts/deploy/foundation-runtime.sh stop foundation-api
sudo scripts/deploy/foundation-runtime.sh up -d foundation-api
```

Do not invoke the root Compose file by itself on a recovery-enabled runtime. The root file is also a
local-development contract and intentionally uses the standard PostGIS image; the recovery overlay is
what adds pgBackRest, `archive_mode=on`, and the off-host repository configuration.

## Restore Rehearsal

Run the isolated rehearsal with a dedicated empty repository prefix and a temporary encryption
passphrase:

```bash
run_id="$(date -u +%Y%m%d%H%M%S)"
FOUNDATION_RECOVERY_RUN_ID="${run_id}" \
FOUNDATION_RECOVERY_EVIDENCE_DIR="target/recovery/${run_id}" \
  scripts/recovery/postgres-restore-drill.sh
```

The drill performs the exact bootstrap, migration, runtime-grant, and finalize chain; takes a full
encrypted backup; writes a marker after the full backup; creates a named PostgreSQL restore point;
archives the WAL; restores into a new volume; promotes the restored database; reruns migrations; and
proves both application tables and the post-backup marker are readable.

## Evidence Handling

Every restore rehearsal must emit its schema version, run identifier, start/finish time, named restore
point, migration count, read/PITR assertions, and result into the private operational evidence store.
Retain the deployed and rollback commit identifiers, container digests, timer state, backup identifiers,
RPO/RTO timings, alert transitions, daemon-restart results, and positive/negative bucket-access probes
with the same run. Do not commit those values, live resource bindings, token names, account identifiers,
or host inventory to this public source repository.

A recovery gate is complete only when the private evidence proves all of the following for the target
environment:

- `pgbackrest check`, a full or differential backup, WAL archival, and a restore into an isolated volume;
- application reads and a post-backup marker after PITR;
- active/enabled scheduling and successful execution outside the immutable release directory;
- the recovery credential can access only its dedicated bucket and is denied from the lakehouse bucket;
- controlled service and daemon recovery returns every required health/readiness check;
- observed RPO and RTO are within the declared objectives.

Official references:

- Cloudflare R2 Bucket Locks: https://developers.cloudflare.com/r2/buckets/bucket-locks/
- Cloudflare R2 lock API: https://developers.cloudflare.com/api/resources/r2/subresources/buckets/subresources/locks/
- pgBackRest user guide: https://pgbackrest.org/user-guide.html
