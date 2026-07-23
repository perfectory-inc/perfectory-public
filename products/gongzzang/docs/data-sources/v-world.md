# V-World Source Boundary

V-World is a Foundation Platform Catalog input source.

Gongzzang must not add a V-World client, scheduled V-World job, raw-response
capture path, or V-World drift monitor. Gongzzang consumes Catalog facts through
Foundation Platform published contracts only.

## Gongzzang Contract

Allowed Gongzzang usage:

- Foundation Platform Catalog HTTP API pinned by
  `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
- Foundation Platform events pinned by
  `docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json`
- Immutable PNU anchor artifacts imported into the Gongzzang read model

Disallowed Gongzzang usage:

- Direct V-World HTTP calls
- `vworld-client` or replacement Catalog ACL crates
- `parcel_external_data` writes
- raw capture binaries or R2 raw archive writers
- V-World-specific drift smoke workflows

## Ownership

Foundation Platform owns:

- V-World credentials and quota handling
- Request/response parsing
- raw response lineage
- schema drift monitoring
- canonical parcel geometry and public/reference spatial layers

Gongzzang owns:

- Listing semantics
- Listing marker serving
- the PNU anchor read-model copy required by listing marker serving

## Guardrails

- Foundation Platform catalog boundary — `scripts/lefthook/foundation-ownership-boundary.sh`
- Foundation Platform boundary / dependency-boundary contract — `docs/architecture/foundation-platform-boundary.v1.json`
- Foundation Platform Catalog API consumer contract — `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`

If V-World source behavior changes, update Foundation Platform first. Gongzzang should
only update pinned Foundation Platform contracts after the Foundation Platform API/event
contract changes.
