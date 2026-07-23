# Load Scenarios

These k6 scenarios are operator tooling for perf/staging capacity discovery.
They are not imported by `apps/`, `services/`, `crates/`, or `packages/`.

Run each scenario with `k6 run --summary-export`. The evidence destination,
approved targets, and measured capacity bindings are private operator inputs
(see `docs/testing/load.md`).

Example:

```bash
test -n "$LOAD_EVIDENCE_DIR"
k6 run --summary-export "$LOAD_EVIDENCE_DIR/k6-summary.json" \
  tests/load/scenarios/api-read-mix.js
```

The committed target is deliberately non-routable. Runs require an approved
target host supplied by the private load runner through
`LOAD_APPROVED_TARGET_HOSTS`, using comma-separated hostnames without scheme,
path, port, query, or credentials.

For authenticated API read paths, set `LOAD_AUTH_BEARER_TOKEN` in the runner
environment. Do not put bearer tokens in workflow inputs or committed files.

For marker runs, set `LOAD_FILTER_HASH` and optionally `LOAD_FILTER_HASH_MISS`
to known fixture hashes from the perf dataset. The default miss path reuses the
same valid hash and changes only the requested tile.
