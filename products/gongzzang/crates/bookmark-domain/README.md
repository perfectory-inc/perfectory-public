---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-11
---

# bookmark-domain

`Bookmark` 도메인(RDS 동적 데이터) 크레이트예요.

## 책임

- `bookmark_listing` + `bookmark_external` 두 테이블(`migrations/20260719000103_insights_tables.sql`)에
  대응하는 애그리거트 두 종을 정의해요.
- `BookmarkListing`: 매물 북마크(복합 기본 키 `user_id + listing_id`,
  `Listing` 외래 키 + `ON DELETE CASCADE`).
- `BookmarkExternal`: 외부 R2 대상 북마크(다형 키
  `target_kind` + `target_id`).
- `BookmarkExternalKind` 열거 4값 (`parcel`, `court_auction`,
  `manufacturer`, `industrial_complex`). `industrial_complex` 의 `target_id` 는 레이크하우스
  산업단지 ID(UUIDv5)만이고 데이터베이스가 강제해요
  ([루트 ADR-0226](../../../../docs/adr/0226-gongzzang-complex-target-id-is-the-lakehouse-complex-id.md)).
- `BookmarkRepository` 트레이트: `Postgres` 구현은 `gongzzang-persistence`의 `PgBookmarkRepository`.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `ListingMarker`,
  `BookmarkExternalMarker`, `MutationContext`).
