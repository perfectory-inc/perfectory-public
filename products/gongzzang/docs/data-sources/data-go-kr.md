# data.go.kr Source Boundary

Catalog-related data.go.kr integrations are Foundation Platform Catalog input
sources.

Gongzzang must not add a data.go.kr Catalog client, parser, scheduled ingest
job, raw-response capture path, or drift monitor. Gongzzang consumes building
and parcel facts through Foundation Platform published contracts only.

## Gongzzang Contract

Allowed Gongzzang usage:

- Foundation Platform Catalog HTTP API pinned by
  `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
- Foundation Platform events pinned by
  `docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json`
- Route-facing translation from Foundation Platform building responses into
  Gongzzang API response shapes

Disallowed Gongzzang usage:

- Direct data.go.kr HTTP calls for Catalog data
- `data-go-kr-client` or replacement Catalog ACL crates
- building-register sync jobs
- `parcel_external_data` writes
- raw capture binaries or R2 raw archive writers
- data.go.kr-specific drift smoke workflows

## Ownership

Foundation Platform owns:

- data.go.kr credentials and quota handling
- request/response parsing
- raw response lineage
- schema drift monitoring
- canonical building and parcel Catalog facts

Gongzzang owns:

- `/api/buildings` route shape
- Listing semantics
- Listing marker serving

## Guardrails

- Foundation Platform catalog boundary — `scripts/lefthook/foundation-ownership-boundary.sh`
- Foundation Platform boundary / dependency-boundary contract — `docs/architecture/foundation-platform-boundary.v1.json`
- Foundation Platform Catalog API consumer contract — `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`

If data.go.kr source behavior changes, update Foundation Platform first. Gongzzang
should only update pinned Foundation Platform contracts after the Foundation Platform
API/event contract changes.
