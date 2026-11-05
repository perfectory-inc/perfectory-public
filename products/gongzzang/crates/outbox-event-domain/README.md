---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# outbox-event-domain

`OutboxEvent` 도메인 (RDS 동적) crate에요.

## 책임

- `outbox_event` 테이블(`migrations/20260719000104_system_tables.sql`, lease 컬럼은
  `20260719000120_outbox_delivery_leases.sql`)에 대응하는 Aggregate를 정의해요.
- `Aggregate` 도메인 메서드가 `DomainEvent` 를 emit 하면, application layer 에서
  `OutboxEvent::from_domain` 으로 wrap 해 *Aggregate save 와 같은 트랜잭션* 에서
  `OutboxRepository::save` 로 INSERT 해요 (transactional outbox 패턴).
- Publisher(`crates/gongzzang-outbox` + `services/gongzzang-outbox-publisher`)가
  `OutboxRepository::claim_unpublished`(lease)로 미발행 row 를 가져와 발행한 뒤 `mark_published` 호출.
- `mark_published` 는 *idempotent* — 이미 발행된 경우 변경 없음.
- `Postgres` 구현은 `gongzzang-persistence`의 `PgOutboxRepository`.

## ID

`evt_<26-char ULID>` (총 30자) — `shared-kernel`의 `OutboxEventMarker` prefix `evt`.

## 의존

- `shared-kernel` (`Id`, `OutboxEventMarker`, `domain_event::DomainEvent`).
