# Provider Outage And Quota Runbook

## Purpose

Use this runbook when V-World, data.go.kr, or another public data provider is unavailable,
rate-limited, returning invalid envelopes, or approaching quota exhaustion.

## Scope

This runbook covers provider-facing ingestion commands only. It does not approve new batch size,
new public API redistribution, or quota-impacting live jobs. Those still require user approval.

## Detection

Signals:

- repeated transient HTTP errors from V-World or data.go.kr clients
- provider-domain error envelopes in raw responses
- request timeout spikes
- quota headers or provider portal showing low remaining quota
- ingestion run failure rate above the normal baseline

## Immediate Mitigation

1. Stop optional ingestion jobs for the impacted provider.
2. Keep read APIs serving the latest canonical data.
3. Do not delete Bronze raw responses while the incident is open.
4. Prefer replay from archived Bronze over refetching from the provider.
5. Record provider, endpoint, time window, request count, and sample request ids.

## Client Circuit Breaker

Provider HTTP clients use bounded retry, per-attempt timeout, and an in-process circuit breaker for
retryable transient failures. After retry attempts are exhausted, the next request fails fast while
the circuit is open instead of immediately hitting the provider again.

Current scope:

- `DataGoKrServiceApiClient`
- `VWorldDataApiClient`
- `VWorldNedAttributeClient`

The circuit breaker is process-local. It does not replace persisted DLQ/quarantine tables, cross-pod
provider outage state, or operator-facing incident records.

## Quota Protection

Before resuming:

- treat `docs/catalog/provider-rate-policy.v1.json` as the SSOT for provider lanes, initial speed,
  max speed, quota signals, retry policy, and defer-without-drop behavior
- use `foundation-outbox-publisher resume-national-data-collection-ledger` in its default
  `ProviderLaneMode=provider_policy` mode for national Bronze collection resume; this makes chunks
  lane-homogeneous, schedules lanes separately, and passes the lane-derived
  `ProviderMinPageIntervalMs` hard cap to the ledger executor
- do not use `ProviderLaneMode=off` for national collection except fixture tests or an explicitly
  documented proof run
- after changing provider speed or signal rules, exercise the changed policy with
  `foundation-outbox-publisher provider-rate-controller` (initialize mode per changed lane); it
  fails fast when `docs/catalog/provider-rate-policy.v1.json` does not parse or the lane id is
  unknown. (The former standalone `check-provider-rate-policy` / `check-provider-rate-controller`
  gates were deleted in the 2026-06-22 self-verifying evidence-gate ceremony purge.)
- reduce page range or bounding box scope
- keep retry limits bounded
- prefer read-only smoke checks over live provider calls
- check that provider-facing subcommands still require explicit quota-impact confirmation for
  data.go.kr or V-World calls before they issue live requests
- write a Prometheus-compatible quota/dependency artifact by setting the command's
  `*_QUOTA_METRICS_PATH` environment variable when running
  approved live smoke checks; the artifact uses `foundation_platform_public_api_quota_request_total`,
  `foundation_platform_public_api_dependency_request_duration_seconds`, and
  `foundation_platform_public_api_dependency_error_total`
- confirm no operator is running parallel ingestion for the same provider

## Event Fabric Boundary

Kafka/MSK or Redpanda is a future event-fabric adapter for outbox fanout, search, notification, AI,
and downstream consumers. It is not the authority for public-provider rate control. Public-provider
collection speed is governed by the provider rate policy, lane scheduler, per-page request spacing,
and quota evidence. Future Kafka publishers must stay behind the existing event publisher contract
and must not make ingestion code depend directly on a Kafka client.

## Failover

Failover options are limited by source authority:

- V-World cadastral geometry: use cached Bronze/Silver outputs until provider recovers.
- data.go.kr building register: use the latest archived Bronze and mark freshness degraded.
- R2/Iceberg read path: keep serving the latest known-good snapshot.

Do not silently substitute a non-authoritative dataset as canonical data. If a temporary derived view
is needed for users, label it as stale and trace it to the last source snapshot id.

## Recovery

1. Run a narrow read-only smoke for the provider.
2. Resume ingestion with the smallest approved window.
3. Compare row counts and schema profile changes against the previous successful run.
4. Record the incident and recovery run ids.
5. Notify consumers if freshness SLO was missed.
