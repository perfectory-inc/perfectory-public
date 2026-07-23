# R2 Namespace Contamination Recovery

## Purpose

Use this runbook when unexpected objects appear in the Foundation Platform R2 namespace, when legacy
prefixes conflict with the current object layout, or when cleanup candidates must be reviewed.

## Principles

- R2 cleanup starts with inventory and classification.
- R2 Data Catalog metadata, canonical manifest pointers, runtime Gold artifacts, and canonical
  Bronze contract objects are protected.
- Date-partitioned Bronze keys such as
  `bronze/source=<source>/ingest_date=<date>/run_id=<run_id>/...` are legacy objects. They must be
  copied to `bronze/source=<source>/run_id=<run_id>/...` and verified before any old key deletion.
- No object is deleted without an explicit dry-run plan, allowed prefixes, and confirmation phrase.

## Recovery Steps

1. Run inventory audit:

```bash
cargo run -p foundation-outbox-publisher -- audit-r2-inventory
```

2. Review `review` objects manually and assign ownership.
3. Generate a dry-run delete plan:

```bash
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_ALLOWED_PREFIXES=$'bronze/2026-05/\ngold/staging/' \
cargo run -p foundation-outbox-publisher -- delete-r2-candidates
```

4. Confirm `mode` is `dry_run`, `executed_count` is `0`, and every key is inside the allowed
   prefixes.
5. Execute only after review:

```bash
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_ALLOWED_PREFIXES=$'bronze/2026-05/\ngold/staging/' \
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_EXECUTE=true \
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_CONFIRM_PHRASE="DELETE FOUNDATION PLATFORM R2 CANDIDATES" \
cargo run -p foundation-outbox-publisher -- delete-r2-candidates
```

6. Run a second inventory audit and cleanup verification.

## Protected Prefixes

Never delete through this workflow:

- `__r2_data_catalog/`
- `gold/manifest.json`
- `gold/v*/`
- `bronze/source=*/run_id=*/partition=*`

Legacy date-partitioned Bronze keys are not protected as current contract objects, but they are not
deleted directly by cleanup. Use `write-r2-bronze-key-migration-plan` and
`migrate-r2-bronze-keys` first.

## Validation

Run the R2 inventory/cleanup subcommand tests:

```bash
cargo test -p foundation-outbox-publisher r2
```

The recovery is complete only when:

- all planned delete candidates are absent after cleanup
- all before-audit keep objects remain with the same key and size
- after-audit `review_count` is zero
- verification report status is `passed`
