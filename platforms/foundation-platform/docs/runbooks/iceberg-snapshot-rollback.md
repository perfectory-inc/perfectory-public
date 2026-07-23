# Iceberg Snapshot Rollback

## Purpose

Use this runbook when a promoted Iceberg snapshot must be rolled back because row counts, schema,
lineage, or consumer-facing behavior is wrong.

## Preconditions

- Identify current and target previous Iceberg snapshot ids.
- Confirm the target snapshot passed prior audit and read-only smoke.
- Freeze further writes to the affected table during rollback.
- Confirm no Postgres, PostGIS, search, or cache layer is treated as canonical.

## Rollback Procedure

1. Record the rollback request with operator, reason, current snapshot id, and target snapshot id.
2. Use the Iceberg catalog tool or approved Spark/Trino command to repoint the table to the target
   snapshot.
3. Run read-only verification against the table.
4. Rebuild derived serving layers from the restored snapshot.
5. Publish a versioned pointer or event so consumers invalidate stale read models.

## Audit Requirements

The audit record must include:

- table contract
- previous snapshot id
- bad snapshot id
- restored snapshot id
- row count before and after rollback
- validation commands
- operator and request id

## Validation

Required validation:

- Spark run summary contract validation when a rebuild is involved
- read-only lakehouse smoke against the restored table
- API or pointer readback for consumer-facing paths
- outbox sender check if cache invalidation events are emitted

## Failure Handling

If rollback fails:

1. Keep the table frozen.
2. Do not manually edit derived caches.
3. Escalate as SEV1 if consumers can read incorrect data.
4. Prefer forward-fix into a new validated snapshot over untracked manual edits.
