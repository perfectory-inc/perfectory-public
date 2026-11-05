---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# business-verification-domain

`BusinessVerification` (Business Verification Queue) 도메인 (RDS 동적) crate에요.

## 책임

- `business_verification_queue` 테이블(`migrations/20260719000105_operations_tables.sql`)에 대응하는 Aggregate를 정의해요.
  사업자 인증의 유일한 출처는 이 큐예요 ([루트 ADR-0258](../../../../docs/adr/0258-gongzzang-business-verification-lives-only-in-its-queue-with-an-append-only-history.md)).
- **5-status workflow** — `pending` → `approved`/`rejected`/`needs_more_info`,
  `needs_more_info` → `pending` (재제출), `approved` → `revoked` (승인 취소).
- **24h SLA** — `submitted_at + 24h` 로 `sla_due_at` 자동 계산.
- **Optimistic concurrency** — `version` 컬럼 (`bigint`) 으로 동시 검토 충돌 차단.
- 검토자 (`reviewer_id`) 와 검토 메모 (`reviewer_note`, ≤2000자) 는 상태 전이 시점에 기록.
- `reject` / `request_more_info` / `revoke` 는 **메모 필수** (사용자에게 사유 안내). `approve` 메모는 선택.
- `BusinessVerificationRepository` trait — `find_by_id` / `find_pending` / `find_by_user` /
  `find_verified_business` (지금 효력 있는 최근 승인 항목) / `save` (OCC).
  `Postgres` 구현은 `gongzzang-persistence`의 `PgBusinessVerificationRepository`.

## 상태 머신

```text
                    approve                      revoke
        Pending  ─────────────►  Approved  ─────────────►  Revoked (terminal)
           │  ─────────────►     Rejected (terminal)
           │     reject
           │  ─────────────►     NeedsMoreInfo
           │     request_more_info
           ▲                          │
           └──────────────────────────┘
                resubmit
```

- `Rejected` / `Revoked` 는 terminal — 어떤 전이도 허용 안 해요. 새 인증은 새 큐 항목으로.
- `resubmit` 은 `submitted_documents` 를 새 R2 키로 교체하고 reviewer 필드를 초기화해요.
- 허용 전이 목록의 정본은 `src/status.rs`의 `can_transition_to`예요.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `BusinessVerificationMarker`, `BusinessNumber`, `MutationContext`).
