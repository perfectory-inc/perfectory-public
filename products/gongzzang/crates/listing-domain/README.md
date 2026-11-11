---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-11
---

# listing-domain

`Listing` 애그리거트(RDS 동적 데이터) 크레이트예요.

## 책임

- `listing` 테이블(`migrations/20260719000102_core_tables.sql`)에 대응하는 `Listing` 애그리거트를 정의해요.
- `try_new_draft` 생성자에서 `20260719000108_listing_transaction_type_check.sql` 필드 간 불변식
  (`transaction_type` ↔ `deposit`/`monthly_rent`)을 강제해요.
- 상태 기계(`Draft` → `PendingReview` → `Active` → `{Sold, Expired}`) 도메인 메서드:
  `submit_for_review`, `approve`, `reject`, `revise_after_rejection`, `mark_sold`, `expire`,
  `update_editable_fields`(`ListingUpdate`).
- `ListingRepository` 트레이트: 저장·조회, 카드 검색(`CardSearchQuery`/`ListingCardSummary`), 상세
  (`ListingDetail`), 조회수, 필지 비정규화, 매물 표지 타일·마스크·변경분·삭제 표시·개수·필터 계약.
  `Postgres` 구현은 `gongzzang-persistence`의 `PgListingRepository`.
- `marker_filter`: 매물 표지 필터 정규화와 해시(`ListingMarkerFilterSpec`, `ListingMarkerFilter`).

## 의존

- `shared-kernel` (`Id`, `MoneyKrw`, `AreaM2`, `Pnu`, `ListingType`, `TransactionType`, `ListingStatus`,
  `ContactVisibility`, `ListingTitle`, `Description`, `LandUseType`, `Zoning`, `EupmyeondongCode`,
  `MutationContext`).
- `utoipa`: API 스키마 생성.
- `user-domain` 에는 의존하지 *않아요*. `UserMarker` 는 `shared-kernel::id` 를 거쳐요.

## 예시

```rust,ignore
use listing_domain::entity::Listing;
let listing = Listing::try_new_draft(/* … */)?;
```
