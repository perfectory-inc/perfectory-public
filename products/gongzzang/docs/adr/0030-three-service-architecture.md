# ADR 0030 - Historical Shared-Platform Extraction

| Field | Value |
|---|---|
| Date | 2026-05-11 |
| Status | Superseded by [ADR 0048](./0048-horizontal-platform-redefinition.md) |

## Historical Decision

This decision first removed shared Catalog and staff-identity responsibilities
from Gongzzang and Dawneer. It established that product services should consume
shared capabilities through published contracts instead of duplicating master
data or joining another service's database.

The original topology grouped those capabilities into one shared service. ADR
0048 replaces that topology with three horizontal platforms:

- Foundation Platform for canonical data and data infrastructure;
- Identity Platform for staff/service identity and authorization;
- Intelligence Platform for model execution, retrieval, and proposal
  generation.

## Preserved Invariants

- Gongzzang owns listings, auctions, product users, and product behavior.
- Dawneer is rebuilt as a consumer of published platform contracts.
- Shared canonical data is not duplicated into product-owned masters.
- Cross-platform direct database access is forbidden.
- AI output is a proposal; the data owner controls canonical writes.

## Current Source Of Truth

[ADR 0048](./0048-horizontal-platform-redefinition.md) is the architecture
SSOT. This file remains only as decision lineage and must not be used as a
current topology or naming guide.
