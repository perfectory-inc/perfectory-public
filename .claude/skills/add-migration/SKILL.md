---
name: add-migration
description: Use when adding a database migration (SQL schema change) in any perfectory area — 14-digit naming rule, sqlx migrate add usage and its sequential-inference trap, per-area migrations/ directories, enforcing guards, and the local-DB recreate caveat.
---

# Add a database migration

## Rule (ADR-0001 §7)

`YYYYMMDDHHMMSS_<snake_case>.sql` — 14-digit UTC timestamp version + snake_case
intent slug (change intent like `add_listing_index`, not a table name).
Forward-only: a merged migration file is immutable; fix mistakes with a new
`revert_x` / `fix_<cause>` migration.

## Where

Each area owns one `migrations/` directory:

- `products/gongzzang/migrations/`
- `platforms/foundation-platform/migrations/`
- `platforms/identity-platform/migrations/`
- `platforms/intelligence-platform/migrations/`

gongzzang detail SSOT: `products/gongzzang/migrations/README.md`.
gongzzang rule: DB schema changes need user approval BEFORE creating the
migration (gongzzang AGENTS.md §6).

## Command

From the area root (sqlx-cli 0.8.6):

```bash
sqlx migrate add --timestamp <snake_case_intent>
```

Why `--timestamp` explicitly: sqlx infers SEQUENTIAL versioning when the last
two existing versions differ by exactly 1 — true today in gongzzang
(`...000119`/`...000120`) and foundation (`...000003`/`...000004`) — and would
emit `last+1` instead of the current UTC timestamp. Both forms pass the guards,
but `+1` versions collide when two branches add migrations in parallel;
`--timestamp` avoids that.

## Guards that enforce this

- `bash scripts/guard/migration-naming.sh` — all four areas, filename regex
  `^[0-9]{14}_[a-z0-9_]+\.sql$` (part of `scripts/guard/monorepo-guard.sh`).
- gongzzang additionally: repo-guard `migration-version-prefixes` (same regex
  + rejects duplicate version prefixes). Runs on pre-push via lefthook
  (`products/gongzzang/scripts/lefthook/migration-version-prefixes.sh`);
  manual run from `products/gongzzang/`:

  ```bash
  cargo run -q -p repo-guard -- migration-version-prefixes
  ```

  Expected output: `migration-version-prefixes-ok files=<n>`.

## Local DB recreate caveat

Renaming existing migration files (permitted once pre-launch, ADR-0001 §7)
invalidates previously-applied local sqlx histories (`_sqlx_migrations` stores
the version). After any rename — or a partially-failed migration — recreate the
local DB:

```bash
sqlx database drop -y && sqlx database create && sqlx migrate run --source migrations
```

Never hand-edit `_sqlx_migrations` outside local dev.
