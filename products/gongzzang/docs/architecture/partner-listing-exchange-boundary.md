# Partner Listing Exchange Boundary

Status: maintained public contract

## Purpose

This contract defines the ownership boundary for provider-neutral listing
exchange. Provider names, account bindings, endpoint inventories, field
catalogs, source documents, measured capacity, and current rollout state belong
in private operations, not in this repository.

## Ownership

| Area | Owner | Rule |
|---|---|---|
| Listing canonical state | Gongzzang | Create, update, review, and publish through Gongzzang application commands. |
| Exchange payload and lineage | Gongzzang | Preserve the received or emitted payload under the product's approved audit and retention policy. |
| Provider mapping adapter | Gongzzang | Translate an external contract into a listing candidate or outbound request. |
| Parcel, building, PNU, and address reference data | Foundation Platform | Consume published contracts; do not read Foundation storage directly. |
| Staff and service authorization | Identity Platform | Use issued identities and published authorization contracts. |

## Canonical Flow

```text
provider-neutral exchange payload
  -> immutable exchange evidence
  -> provider adapter and validation
  -> listing candidate
  -> policy or staff review when required
  -> Gongzzang listing command
  -> canonical listing state
  -> outbound exchange request and delivery status
```

The exchange transport may be push or pull. Transport choice does not change
ownership or permit a provider adapter to write canonical tables directly.

## Invariants

- External identifiers remain namespaced provider identities and never become
  Gongzzang listing identifiers.
- Replayed inbound messages and outbound retries are idempotent.
- Raw exchange evidence is not canonical listing state.
- Canonical writes pass through the Gongzzang application layer.
- Cross-service direct database access is forbidden.
- Foundation Platform reference data is used only through published contracts
  for enrichment and validation.
- Provider-specific schemas and live bindings are private runtime inputs. Their
  absence from this public contract is intentional.

## Non-Goals

- Moving Gongzzang listing ownership into Foundation Platform.
- Publishing a provider's proprietary integration material.
- Exposing Identity Platform internals to exchange adapters.
- Treating a current partner rollout or operational queue as architecture.
