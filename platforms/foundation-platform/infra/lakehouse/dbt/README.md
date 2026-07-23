# Foundation Lakehouse dbt

This directory contains the Foundation Platform dbt project.

dbt is responsible for SQL models, SQL tests, model documentation, and model-level lineage evidence over Trino.

dbt is not responsible for source acquisition, checksum truth, AI calls, human review, authorization, publish authority, or rollback authority.

## Local shape

1. Start Trino with the opt-in lakehouse query profile.
2. Copy `profiles.example.yml` to your dbt profile directory.
3. Run dbt from this directory once dbt Core and dbt Trino are available.

Example:

```powershell
docker compose -f compose.lakehouse.yml --profile lakehouse-query up -d trino
dbt parse --profiles-dir .
dbt test --profiles-dir . --exclude tag:full_quality
```

The exact dbt installation method is intentionally not pinned in this slice.
This slice was locally verified with `dbt-core 1.11.12` and `dbt-trino 1.10.2`
on Python 3.13. Python 3.14 failed in the local smoke because one dbt dependency
was not yet compatible with that runtime.

## Current static verification

Before dbt is installed, run:

```powershell
python infra/lakehouse/dbt/tests/no_dbt_forbidden_responsibilities.py
python infra/lakehouse/dbt/tests/test_model_contracts.py
```

These tests verify the project boundary and first model contract without requiring dbt packages.

## Runtime verification, after dbt is available

```powershell
cd infra/lakehouse/dbt
dbt parse --profiles-dir .
dbt test --profiles-dir .
```

Runtime dbt execution requires a live Trino catalog that exposes the declared source tables.
The expected local Trino catalog name is `foundation_platform`. `dbt parse`
validates the project without a live catalog; `dbt compile`, `dbt run`, and
`dbt test` require the Trino/Iceberg metadata store to be initialized.
For local JDBC-backed Iceberg, generate the live ignored catalog properties file
from `infra/lakehouse/trino/templates/foundation-platform-jdbc-iceberg.properties.template`;
the generated file must be named `foundation_platform.properties` because the
catalog file name is the Trino catalog name used by dbt's `database` setting.
initialize the Postgres-backed Iceberg JDBC metadata tables with
`infra/lakehouse/trino/init/foundation-platform-jdbc-iceberg-catalog.sql`.

For isolated local smoke runs, apply `smoke/source-fixtures.sql` and run dbt with
target `smoke` plus `FOUNDATION_DBT_SOURCE_SCHEMA=smoke_source`. This writes
models into `smoke_staging`, `smoke_intermediate`, and `smoke_silver`, not the
canonical layer schemas.

Court-auction source models fail closed unless the source snapshot and lineage
run identifiers are explicit. For fixture smoke runs, use stable fixture ids. For
live Gongzzang-published sources, set these to the actual Gongzzang publish
snapshot and lineage run ids:

```powershell
$env:FOUNDATION_DBT_COURT_AUCTION_SOURCE_SNAPSHOT_ID='smoke-court-auction-property'
$env:FOUNDATION_DBT_COURT_AUCTION_LINEAGE_RUN_ID='smoke-court-auction-lineage'
```

When Gongzzang and Foundation sources live in different schemas, set them
independently instead of relying on `FOUNDATION_DBT_SOURCE_SCHEMA`:

```powershell
$env:FOUNDATION_DBT_GONGZZANG_SOURCE_SCHEMA='gongzzang_silver'
$env:FOUNDATION_DBT_FOUNDATION_SOURCE_SCHEMA='silver'
```

## Smoke versus full-quality tests

Smoke runs should prove that model SQL, source wiring, candidate generation, and
output contracts still work. They must not scan the full Foundation Silver
building-register tables for uniqueness or completeness.

Use this for fast smoke:

```powershell
dbt run --target smoke --exclude tag:full_quality --profiles-dir .
dbt test --target smoke --exclude tag:full_quality --profiles-dir .
```

Use this for cutover, nightly, or release-quality checks:

```powershell
dbt run --target smoke --select tag:full_quality --profiles-dir .
dbt test --target smoke --profiles-dir . --select tag:full_quality
```
