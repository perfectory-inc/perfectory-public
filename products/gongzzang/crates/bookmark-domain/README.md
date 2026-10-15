---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# bookmark-domain

`Bookmark` 도메인 (RDS 동적) crate에요.

## 책임

- spec § 5.2 `bookmark_listing` + `bookmark_external` 두 테이블 매핑하는
  Aggregate 2종 정의해요.
- `BookmarkListing` — 매물 북마크 (composite PK `user_id + listing_id`,
  `Listing` FK + `ON DELETE CASCADE`).
- `BookmarkExternal` — 외부 R2 entity 북마크 (polymorphic
  `target_kind` + `target_id`).
- `BookmarkExternalKind` enum 4값 (`parcel`, `court_auction`,
  `manufacturer`, `industrial_complex`). `industrial_complex` 의 `target_id` 는 lakehouse
  complex id(UUIDv5)만 — DB 가 강제해요
  ([루트 ADR-0226](../../../../docs/adr/0226-gongzzang-complex-target-id-is-the-lakehouse-complex-id.md)).
- `BookmarkRepository` trait — 구현체는 sub-project 5에서 추가.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `ListingMarker`,
  `BookmarkExternalMarker`).
