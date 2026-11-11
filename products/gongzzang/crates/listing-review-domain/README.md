---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-11
---

# listing-review-domain

`ListingReview`(매물 검토 대기열) 도메인(RDS 동적 데이터) 크레이트예요.

## 책임

- `listing_review_queue` 테이블(`migrations/20260719000105_operations_tables.sql`)에 대응하는 애그리거트를 정의해요.
- **결정 기반 흐름**: `decision = None`(대기) →
  `Some(Approve)` / `Some(Reject)` / `Some(RequestChanges)` (종결).
- **12시간 처리 기한**: `submitted_at + 12h` 로 `sla_due_at` 을 자동 계산.
- **낙관적 동시성**: `version` 컬럼(`bigint`)으로 동시 검토 충돌을 막아요.
- **자동 점검**: 규칙 기반 자동 점수 (`auto_check_score` 0-100) 와 플래그
  (`auto_check_flags`, 예: `["suspected_duplicate", "price_anomaly"]`) 를 큐 생성 시 기록.
- 검토자 (`reviewer_id`) 와 검토 메모 (`reviewer_note`, ≤2000자) 는 결정 시점에 기록.
- `reject` / `request_changes` 는 **메모 필수** (사용자에게 사유 안내).
- `ListingReviewRepository` 트레이트: `find_by_id` / `find_pending` / `find_by_listing` / `save` (낙관적 동시성).
  `Postgres` 구현은 `gongzzang-persistence`의 `PgListingReviewRepository`.

## 결정 흐름

```text
                   decide_approve
        Pending ─────────────────►  Approved (terminal)
        (None)  ─────────────────►  Rejected (terminal)
                   decide_reject
                ─────────────────►  RequestChanges (terminal)
                   decide_request_changes
```

- 한 번 `decision` 이 `Some(_)` 으로 채워지면 **이후 모든 결정 시도는 `AlreadyDecided`** 오류.
- 사업자 확인 대기열과 달리 매물 검토 대기열은 `status` 열거 없이 *선택값 `ListingReviewDecision`* 으로 대기·결정을 구분해요.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `ListingMarker`, `ListingReviewMarker`, `MutationContext`).
