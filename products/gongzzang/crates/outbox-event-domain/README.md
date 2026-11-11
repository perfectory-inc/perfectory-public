---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-11
---

# outbox-event-domain

`OutboxEvent` 도메인(RDS 동적 데이터) 크레이트예요.

## 책임

- `outbox_event` 테이블(`migrations/20260719000104_system_tables.sql`, 임차 컬럼은
  `20260719000120_outbox_delivery_leases.sql`)에 대응하는 애그리거트를 정의해요.
- 애그리거트의 도메인 메서드가 `DomainEvent` 를 내면, 응용 계층이
  `OutboxEvent::from_domain` 으로 감싸 *애그리거트 저장과 같은 트랜잭션* 에서
  `OutboxRepository::save` 로 넣어요(트랜잭션 아웃박스 방식).
- 발행기(`crates/gongzzang-outbox` + `services/gongzzang-outbox-publisher`)가
  `OutboxRepository::claim_unpublished`(임차)로 미발행 행을 가져와 발행한 뒤 `mark_published` 를 불러요.
- `mark_published` 는 *멱등*이에요. 이미 발행된 행은 바뀌지 않아요.
- `Postgres` 구현은 `gongzzang-persistence`의 `PgOutboxRepository`.

## ID

`evt_<26자 ULID>`(총 30자). 접두사 `evt` 는 `shared-kernel` 의 `OutboxEventMarker` 가 정해요.

## 의존

- `shared-kernel` (`Id`, `OutboxEventMarker`, `domain_event::DomainEvent`).
