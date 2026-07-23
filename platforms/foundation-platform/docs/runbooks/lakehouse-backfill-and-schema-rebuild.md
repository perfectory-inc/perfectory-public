# Lakehouse Backfill And Schema Rebuild

## Purpose

Use this runbook when Foundation Platform must replay Bronze data into Silver or rebuild a Gold
projection after a schema change, Spark job fix, or source correction. Iceberg remains the
canonical lakehouse source; Postgres, PostGIS, search, vector tiles, and service caches are
derived outputs.

## Preconditions

- Confirm the target table contract in `crates/lakehouse/lakehouse-domain/src/lakehouse.rs`.
- Confirm the Spark jobs and workspace contracts are green:
  - `python -m py_compile infra/lakehouse/spark/jobs/*.py`
  - `cargo test --workspace --all-features`
- Identify the source snapshot id, target table, partition range, expected row count, and rollback
  snapshot before writing.
- Do not run quota-impacting public API ingestion from this runbook. Backfill starts from already
  archived Bronze or approved handoff input.

## Backfill Plan

1. Create a run record with the planned input, target table, expected row count, and operator.
2. Run the Spark job against a staging or smoke table first.
3. Validate the emitted `foundation-platform.spark_run_summary.v1`:
   - `contract` matches the static Rust table contract.
   - `row_count` and `persisted_row_count` match expectations.
   - blocking quality metrics are zero.
   - lineage includes the source snapshot id.
4. Run the read-only smoke against the staged table.
5. Promote only after the audit row is recorded and reviewed.

## Schema Rebuild

1. Add or update the Rust lakehouse contract first.
2. Update the Spark projection to match the Rust contract artifact.
3. Run `python -m py_compile infra/lakehouse/spark/jobs/*.py`.
4. Write the new table version to a staging table.
5. Compare row counts, required columns, and representative ids against the prior snapshot.
6. Record the rebuild as a new batch audit row.

## Live DB Schema Drift Check

The migration-backed contract fixture is `docs/db/catalog-schema-contract.v1.example.json`. The CI
`postgres-integration` job verifies that critical extensions, tables, and columns remain present in
migration SQL and compares the contract against the live `pg_extension` and
`information_schema.columns` after `sqlx migrate run`. The migration state itself can be inspected
locally with:

```bash
sqlx migrate info
```

For incident notes or manual drift comparison against a staging database, export the live schema as
a JSON object with `columns` rows from `information_schema.columns` and an `extensions` array from
`pg_extension`, and diff it against the contract fixture.

## Validation

Required validation before promotion:

- `cargo test --workspace --all-features`
- `python -m py_compile infra/lakehouse/spark/jobs/*.py`
- the CI `postgres-integration` DB schema drift gate when a DB stack is part of the change
- target-specific ignored integration tests when the local database stack is part of the change

## Rollback

If validation fails after a backfill:

1. Stop promotion immediately.
2. Keep the failed output for audit unless it contains sensitive data.
3. Repoint consumers to the previous known-good pointer or Iceberg snapshot.
4. Record the failed source snapshot id, target snapshot id, row count, and failing quality metric.
5. Use `iceberg-snapshot-rollback.md` if a canonical Iceberg snapshot was already promoted.
