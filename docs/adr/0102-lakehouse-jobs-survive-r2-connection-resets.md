# ADR 0102: Lakehouse jobs survive R2 connection resets

- Status: Accepted
- Date: 2026-09-11

## Context

Reloading `silver.unit_official_price` province by province, the Seoul job died
twice at exactly the same shape: `java.net.SocketException: Connection reset`
thrown from `org.apache.iceberg.aws.s3.S3InputStream.readFully` while a task read
a source Parquet row group from R2, surfacing as
`org.apache.iceberg.exceptions.RuntimeIOException` and aborting the whole job.
Jeju (a smaller province) succeeded with identical code, so this is transient,
not a defect in the job.

Three facts were measured against the running jars (Iceberg 1.11.0), not assumed:

1. The only synchronous HTTP client on the classpath is
   `software.amazon.awssdk.http.apache.ApacheSdkHttpService` — the Apache client,
   which **pools and reuses connections**. R2 (Cloudflare) closes idle
   server-side connections on its own schedule; reusing one the server has
   already closed produces a connection reset on the next read. This is the root
   cause of most of these resets — a stale reused connection, not a bad network.
2. `org.apache.iceberg.aws.s3.S3FileIOProperties` accepts `s3.retry.num-retries`,
   `s3.retry.min-wait-ms`, `s3.retry.max-wait-ms` and builds an
   `EqualJitterBackoffStrategy` — exponential backoff with jitter. `S3InputStream`
   carries a `retryPolicy` field, so this retry covers the exact streaming-read
   path that failed.
3. `org.apache.iceberg.aws.HttpClientProperties` accepts
   `http-client.apache.connection-time-to-live-ms`,
   `connection-max-idle-time-ms`, and `use-idle-connection-reaper-enabled` — the
   knobs that bound a pooled connection's age.

Separately, in plain `local[N]` Spark master mode `spark.task.maxFailures` is
ignored and fixed at 1, so a single task failure aborts the job regardless of the
conf. The `local[N,maxFailures]` master form is the only way to raise it in local
mode.

Raising a retry count alone is a symptom fix: the number is arbitrary and, with
no backoff, N immediate retries burn through a bad window in milliseconds.

## Decision

R2 access resilience lives in one place — `_assemble_catalog_settings` in
`infra/lakehouse/spark/jobs/lakehouse_engine.py`, the single spot every job's
catalog settings come from (root ADR-0069 SSOT). Four layers, defence in depth:

1. **Prevent the reset.** Recycle pooled connections before R2 closes them:
   `http-client.type=apache`,
   `http-client.apache.connection-time-to-live-ms=60000`,
   `http-client.apache.connection-max-idle-time-ms=10000`,
   `http-client.apache.use-idle-connection-reaper-enabled=true`,
   `http-client.apache.tcp-keep-alive-enabled=true`.
2. **Retry the read with backoff + jitter** when a reset still happens:
   `s3.retry.num-retries=10`, `s3.retry.min-wait-ms=200`,
   `s3.retry.max-wait-ms=30000`. This covers the `S3InputStream` path that failed.
3. **Re-run the whole task** as a coarser net: every job's Spark master uses the
   `local[N,8]` form so local mode actually honours task retries.
4. **Re-run the whole job** as the final net: loads are idempotent
   (`append_batch_once` keys each province by its `source_record_id` token), so a
   province that already committed is a no-op on replay and one that did not is
   appended. There is no point at which the "Nth failure" loses data — sources are
   immutable, the append is idempotent, the job is replayable.

## Consequences

- Every lakehouse job that reaches R2 gains layers 1 and 2 for free, because they
  are added to the shared catalog settings rather than per job. `local[N,8]`
  (layer 3) is set at each job's `spark-submit` in the runbook.
- Connection recycling costs an occasional extra TCP/TLS handshake; that is far
  cheaper than a reset plus a task re-run plus a re-read from the start.
- The keys are pinned to the shapes Iceberg 1.11.0 actually exposes. A future
  Iceberg upgrade must re-confirm them against the new jars; unknown catalog
  properties are ignored silently, so a renamed key would quietly stop applying.
- `test_one_place_reaches_the_catalog.py` still passes: the settings gained no new
  required environment variable, so the SSOT variable list is unchanged.
