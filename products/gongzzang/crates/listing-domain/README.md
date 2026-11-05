---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# listing-domain

`Listing` Aggregate (RDS 동적) crate에요.

## 책임

- `listing` 테이블(`migrations/20260719000102_core_tables.sql`)에 대응하는 `Listing` Aggregate를 정의해요.
- `try_new_draft` 생성자에서 `20260719000108_listing_transaction_type_check.sql` cross-field invariant
  (`transaction_type` ↔ `deposit`/`monthly_rent`)를 강제해요.
- 상태 머신 (`Draft` → `PendingReview` → `Active` → `{Sold, Expired}`) 도메인 메서드:
  `submit_for_review`, `approve`, `reject`, `revise_after_rejection`, `mark_sold`, `expire`,
  `update_editable_fields`(`ListingUpdate`).
- `ListingRepository` trait — 저장·조회, 카드 검색(`CardSearchQuery`/`ListingCardSummary`), 상세
  (`ListingDetail`), 조회수, 필지 비정규화, listing marker tile·mask·delta·tombstone·count·filter 계약.
  `Postgres` 구현은 `gongzzang-persistence`의 `PgListingRepository`.
- `marker_filter` — listing marker 필터 정규화와 해시(`ListingMarkerFilterSpec`, `ListingMarkerFilter`).

## 의존

- `shared-kernel` (`Id`, `MoneyKrw`, `AreaM2`, `Pnu`, `ListingType`, `TransactionType`, `ListingStatus`,
  `ContactVisibility`, `ListingTitle`, `Description`, `LandUseType`, `Zoning`, `EupmyeondongCode`,
  `MutationContext`).
- `utoipa` — API 스키마 derive.
- `user-domain` 의존 *없어요* — `UserMarker`는 `shared-kernel::id` 거쳐요.

## 예시

```rust,ignore
use listing_domain::entity::Listing;
let listing = Listing::try_new_draft(/* … */)?;
```
