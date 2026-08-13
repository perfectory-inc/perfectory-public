# ADR 0030 - 역사적 공용 플랫폼 분리

| Field | Value |
|---|---|
| Date | 2026-05-11 |
| Status | Superseded by [ADR 0048](./0048-horizontal-platform-redefinition.md) |

## 역사적 결정

이 결정은 먼저 shared Catalog와 staff identity 책임을 Gongzzang과 Dawneer에서
from Gongzzang and Dawneer. It established that product services should consume
shared capabilities through published contracts instead of duplicating master
data or joining another service's database.

원래 topology는 이 capability를 하나의 shared service로 묶었다. ADR
0048 replaces that topology with three horizontal platforms:

- Foundation Platform for canonical data and data infrastructure;
- Identity Platform for staff/service identity and authorization;
- Intelligence Platform for model execution, retrieval, and proposal
  generation.

## 유지하는 불변식

- Gongzzang owns listings, auctions, product users, and product behavior.
- Dawneer is rebuilt as a consumer of published platform contracts.
- Shared canonical data is not duplicated into product-owned masters.
- Cross-platform direct database access is forbidden.
- AI output is a proposal; the data owner controls canonical writes.

## 현재 정본

[ADR 0048](./0048-horizontal-platform-redefinition.md) is the architecture
SSOT. This file remains only as decision lineage and must not be used as a
current topology or naming guide.
