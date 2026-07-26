# Postgres JobBus contract test

`foundation_outbox::PostgresJobBus` is the durable collection-dispatch adapter. The legacy
data.go.kr national async command is now disabled by the bulk-only policy, so its JobBus settings
must not be used to run production collection. The current `hub.go.kr` bulk collection command
uses the bulk streaming path and, in live-write mode, claims/acks through `PostgresJobBus`. The proof below is a
protected integration test, not a credential-free unit test: it starts disposable PostgreSQL,
applies the repository migrations, and verifies lease fencing, retry/dead-letter behavior, and the
transactional `collection.raw_written` outbox insert.

The old Postgres mode remains incompatible with `FOUNDATION_PLATFORM_NATIONAL_ASYNC_PAGE_QUEUE=1`;
the command rejects the entire legacy API executor before it can make a provider request.

From the repository root, with Docker Desktop running:

For the repeatable disposable test, start the same pinned `postgis/postgis:17-3.5-alpine` image as
the Foundation CI database on a free local port, apply the migrations, set `DATABASE_URL` only in
the test process environment, and remove the container in a finally/cleanup step. Then run:

```text
cargo test --locked -p foundation-outbox --test postgres_jobbus -- --ignored --nocapture
```

The ordinary `cargo test -p foundation-outbox` run intentionally reports these protected tests as
`ignored`; it must remain credential-free. A passing contract test proves the adapter against real
PostgreSQL, but it does not prove production database availability or a successful real-provider
collection run.
