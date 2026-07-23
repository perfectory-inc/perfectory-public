# SLO Alert Policy

## Purpose

Use this runbook when defining or reviewing Foundation Platform lakehouse and API SLOs. The policy links
SLO targets to alert rules, dashboard ownership, and on-call response expectations.

## Required Signals

Track these signals before declaring an SLO production-ready:

- API liveness and readiness status.
- Catalog read latency and error rate.
- Ingestion run duration, row count, and validation failure count.
- Freshness lag from source snapshot to Gold pointer publication.
- Outbox publish lag and retry count.
- R2 request count and storage error count.

## Initial SLO Targets

- API readiness: 99.9 percent of 5-minute windows return ready.
- API 5xx rate: page when more than 1 percent of requests return 5xx over 5 minutes.
- API request timeouts: create an operational ticket when any timeout is observed for 5 minutes.
- API overload rejection: create an operational ticket when concurrency-limit rejections occur for
  5 minutes.
- DB pool exhaustion: create an operational ticket when the pool has no idle connections and is at
  the configured maximum for 5 minutes.
- Catalog read availability: 99.9 percent monthly.
- Gold freshness: critical catalog tables publish within the approved max staleness window.
- Outbox fan-out: 99 percent of publishable events delivered or quarantined within 10 minutes.

The per-contract and per-source baseline policy source lives in
`docs/observability/slo-policy.v1.example.json`. It fixes the initial freshness, duration, and
outbox pending-age thresholds that the dashboard and alert rules must not silently drift from.

## Dashboard

The dashboard must show:

- current liveness and readiness state
- active incidents
- latest successful source snapshot id and Gold pointer
- latest successful lakehouse batch created time, recorded time, and row count by contract
- latest Bronze ingestion finished time, duration, records seen, objects written, and raw response
  bytes by source
- public API quota-impacting request count, dependency duration, and dependency error count
- R2 smoke request count, smoke bytes verified, inventory size, estimated list request cost,
  billing request count, billing bytes, and billing cost
- failed validation count by job
- alert state and owning on-call rotation

The baseline dashboard source lives in
`infra/observability/grafana/foundation-api-dashboard.json`. It includes the API scrape contract,
lakehouse freshness, Bronze ingestion raw response bytes, public API quota/dependency artifacts,
R2 smoke, R2 inventory, and R2 billing metrics. The dashboard is an optional presentation layer;
Prometheus alert evaluation and Alertmanager routing do not depend on Grafana being deployed.

## Alert Policy

- Page on-call for SEV1 consumer data correctness or readiness loss.
- Alert during business hours for freshness lag, repeated provider failures, or retry backlog.
- Create a ticket for quota burn rate, cost anomaly, or non-critical dashboard drift.
- Every alert must include service, environment, correlation id or run id, and the first runbook link.

## Rule Source

The baseline Prometheus alert rule lives in
`infra/observability/prometheus/foundation-api.rules.yml`. `compose.observability.yml` deploys
Prometheus and Alertmanager, loads this rule file, and scrapes `foundation-api:8080/metrics` on the
private Compose network. It covers the API scrape contract
exported by `GET /metrics`, including API process, database readiness, API 5xx rate,
request timeout count, app-level overload rejection count, lakehouse batch staleness,
DB pool pressure, API p95 latency,
ingestion staleness, ingestion failure, ingestion duration, outbox pending age, and outbox retry
backlog gauges. The pre-launch Alertmanager receiver is `prelaunch-audit`: it persists and exposes
routed alerts for an operator rehearsal without sending them to an external paging provider. Before
public launch, an owned staff notification route and its secret must be provisioned and its delivery
tested. A pre-launch alert rehearsal passes only when the same controlled outage alert is active in
both Prometheus and Alertmanager, and then resolves after the API recovers.
The same scrape endpoint also exports latest successful lakehouse batch created time, recorded
time, and row count by contract for freshness dashboarding. It also exports latest Bronze
ingestion finished time, duration, records seen, objects written, raw response bytes by source and
status, Catalog outbox pending/retry/oldest-age metrics, and
`foundation_api_http_requests_total` by method, canonical route, and status. Timeout responses
are also aggregated as `foundation_api_http_request_timeout_total` by canonical route. App-level
traffic budget rejections are exported as `foundation_api_http_overload_rejected_total` by
reason. PostgreSQL pool pressure is exported as `foundation_api_db_pool_size`,
`foundation_api_db_pool_idle_connections`, and `foundation_api_db_pool_max_connections`.
Request latency is exported as `foundation_api_http_request_duration_seconds_bucket` by method,
canonical route, status, and histogram bucket.

The initial staleness threshold is 24 hours, the initial slow-ingestion threshold is 3600 seconds,
the initial outbox pending-age threshold is 600 seconds, and the initial API p95 latency threshold
is 1 second. These are baseline operational tripwires, not final business SLOs.

## Review

Review SLOs after every SEV1 or SEV2 incident and before enabling a new production schedule. Do not
raise targets until the dashboard and alert history show that current targets are consistently met.
