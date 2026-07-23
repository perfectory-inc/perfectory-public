# Load Testing

## Public/Private Boundary

This repository owns reusable k6 scenario code, the scenario registry schema,
and safety validation. Private operations own every deploy-specific binding:
approved target hosts, credentials, measured capacity ceilings, run evidence,
and launch decisions.

The committed registry therefore uses a non-routable `.invalid` target and an
intentionally minimal synthetic safety ceiling. It is not measured capacity and
must never be presented as a launch estimate. A private load runner supplies the
approved target and its reviewed capacity profile at run time without writing
either value back to this repository.

## Safety Rules

- Never run stress, spike, or soak tests against production user traffic paths.
- Never consume VWorld or OpenDataPortal quota from Gongzzang load scenarios.
- Never use production PII or write authentication material to evidence.
- Treat local and CI smoke results only as scenario validation.
- Keep target allowlists, measured ceilings, raw results, and launch evidence in
  the private operations boundary.

The runner must fail closed unless an operator supplies an approved target. It
must reject URL credentials, paths, queries, fragments, and production hosts
before k6 starts. Authenticated scenarios receive tokens only from the private
runner environment.

## Run Types

- `smoke`: validates the scenario, target wiring, credentials, and evidence
  writer at the public synthetic ceiling.
- `baseline`: measures a representative read workload in an approved private
  environment.
- `stress`: searches for a ceiling in an approved non-production environment.
- `spike`: validates burst behavior in an approved non-production environment.
- `soak`: validates long-running stability in an approved non-production
  environment.

Only `smoke` has a committed default. Every other run type requires a private,
reviewed capacity profile.

## Scenario Matrix

| Scenario | Purpose |
| --- | --- |
| `api-read-mix` | Mixed API read-path validation. |
| `map-marker-mix` | Listing marker base, delta, tombstone, and cache-path validation. |
| `capacity-stress` | Private non-production capacity discovery. |
| `foundation-platform-events` | Foundation Platform event-consumer validation. |

`map-marker-mix` must exercise the runtime composition:

```text
visible markers = base tile + delta overlay - tombstone overlay - unauthorized records
```

A run cannot become launch evidence if deleted or private markers are exposed,
if a successful tile drops eligible markers, or if saturation appears under the
privately approved profile.

## Evidence

Set `LOAD_EVIDENCE_DIR` to an operator-owned private destination and pass its
path to `k6 run --summary-export`. Evidence must preserve the scenario, profile,
target, timestamps, k6 summary, threshold output, comparison, and result
classification. Do not commit raw results or a private evidence location.

## Result Classification

- `pass`: the private profile's thresholds and evidence requirements pass.
- `warn`: the run completes with concerns that require operator review.
- `fail`: a threshold fails, evidence is incomplete, or a safety rule is broken.
