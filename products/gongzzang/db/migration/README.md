# Deprecated Migration Path

`db/migration/` is not the migration source of truth.

Use `migrations/` for every SQLx migration. The active naming convention is
`YYYYMMDDHHMMSS_<snake_case>.sql`, documented in [`../../migrations/README.md`](../../migrations/README.md).

Do not add new SQL files here.
