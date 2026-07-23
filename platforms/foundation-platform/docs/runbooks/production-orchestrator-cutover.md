# Production Orchestrator Cutover

## Purpose

Use this runbook when Foundation Platform moves lakehouse ingestion or projection work from manual CLI
execution to a production orchestrator. The orchestrator owns schedule, retry, dependency ordering,
and operator visibility. It does not become the source of truth for catalog data.

## Approval Gate

Do not cut over a production orchestrator until the implementation choice is approved. Introducing
Temporal, Dagster, Airflow, or another runtime can add packages, infrastructure, and operational
state. Record the decision in an ADR before enabling production schedules.

## Preconditions

- Canonical input and output contracts are documented in `crates/lakehouse/lakehouse-domain/src/lakehouse.rs`.
- The lakehouse smoke flow passes (cargo tests plus the Spark jobs under
  `infra/lakehouse/spark/jobs/` running locally against the Docker Spark profile).
- The lakehouse job definitions in `infra/lakehouse/spark/jobs/` are updated and reviewed
  (dependency order: Bronze to Silver, then Silver to Gold, then gold-pointer publish).
- The selected orchestrator has a documented owner, deployment target, rollback path, and audit log.
- Every scheduled job has an idempotent run id, source snapshot id, target table, and expected row count.

> 2026-06-21 note: the former local pre-runtime manifest runner, the
> `infra/orchestration/foundation-platform-lakehouse.jobs.yml` manifest, the GitHub `workflow_dispatch`
> cutover-evidence path, and the dispatch/fetch helper scripts were all removed as ceremony. The
> production orchestrator runtime itself is still unimplemented; when it is adopted, drive it from
> the Spark jobs under `infra/lakehouse/spark/jobs/` and the `foundation-outbox-publisher`
> publish subcommands, and record the runtime and rollback decisions in an ADR.

## Cutover Plan

1. Run the existing manual command and record its batch audit row.
2. Register the same job in the orchestrator with schedules disabled.
3. Execute one ad hoc orchestrated run against a smoke or staging target.
4. Confirm retry policy, timeout, cancellation, and operator logs.
5. Enable the schedule only after the orchestrated output matches the manual output.
6. Keep the manual command documented as the rollback path.

## Retry And Backoff

- Retry only transient provider, storage, or database failures.
- Do not retry deterministic validation failures without operator action.
- Use bounded retry with an explicit max attempt count.
- Preserve every failed attempt in the run summary or audit log.

## Rollback

If orchestrated runs diverge from manual runs:

1. Disable the orchestrator schedule.
2. Keep consumers pinned to the previous known-good pointer.
3. Run the manual command only if input quota and write approval gates allow it.
4. Attach orchestrator run id, source snapshot id, and failing validation output to the incident note.
