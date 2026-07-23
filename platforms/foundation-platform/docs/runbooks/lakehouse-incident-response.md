# Lakehouse Incident Response

## Purpose

Use this runbook when lakehouse writes, read-only smoke, R2 object layout, Gold pointers, or
consumer invalidation events behave incorrectly.

## Severity

- SEV1: canonical pointer or API returns incorrect data to consumers.
- SEV2: canonical write or promotion is blocked, but consumers still serve a known-good snapshot.
- SEV3: smoke, validation, or non-production table is failing without consumer impact.

## Triage

Collect:

- correlation id or request id
- affected contract and table
- source snapshot id and Iceberg snapshot id
- outbox event id when cache invalidation is involved
- operator command and environment profile

Run safe checks first (cargo workspace tests cover the lakehouse and R2 subcommand contracts;
`py_compile` covers the Spark jobs):

```bash
cargo test --workspace --all-features
python -m py_compile infra/lakehouse/spark/jobs/*.py
```

## Mitigation

1. Stop new promotions for the affected table.
2. Keep consumers on the previous known-good pointer.
3. If an event was published with wrong payload, publish a corrected versioned event rather than
   mutating the historical payload.
4. Preserve raw responses and run summaries.
5. If R2 namespace contamination is suspected, switch to `r2-namespace-contamination-recovery.md`.

## Communication

For every SEV1 or SEV2 incident, write an incident note with:

- severity
- start and detection time
- current mitigation
- impacted consumers
- latest known-good snapshot or pointer
- next update time

## Resolution

An incident is resolved only when:

- affected read path is serving a validated snapshot
- outbox retries are drained or explicitly quarantined
- run summary and audit records are consistent
- rollback or forward-fix evidence is linked from the incident note
