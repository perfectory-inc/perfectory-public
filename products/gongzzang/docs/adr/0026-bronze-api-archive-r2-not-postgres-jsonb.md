# ADR 0026 - 공공 원천 Bronze는 Foundation 객체 저장소 소유

| Field | Value |
|---|---|
| Date | 2026-05-08 |
| Status | Accepted; ownership moved to Foundation Platform |
| Current ownership | [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md) |

## 결정

Raw V-World, data.go.kr, and other canonical public/reference source payloads
are Bronze objects in Foundation Platform object storage. They are not stored as
Gongzzang PostgreSQL JSONB에 저장하지 않으며 Gongzzang 원천 client가 캡처하지 않는다.

Foundation owns:

- provider acquisition and raw-byte preservation;
- object identity, checksum, lineage, and collection ledger records;
- Bronze commit/recovery semantics;
- retention and promotion into Silver/Gold.

Gongzzang consumes only published Foundation APIs, events, and immutable
artifacts. Product-owned external adapters require a separate owner-specific
archive/lineage decision and cannot reuse the Foundation namespace implicitly.

## 영향

- Gongzzang's fresh database contains no public-source raw-response table.
- Object keys are storage locators, not canonical business meaning.
- Checksums, source identity, snapshot time, and lineage live in the Foundation
  catalog rather than being inferred from paths.
- Bronze naming and storage implementation details remain private to Foundation
  unless explicitly published as a contract.

이 ADR은 저장·소유 원칙만 보존한다. 과거 local
raw-capture implementation plans were removed after physical extraction.
