# ADR 0032 - 플랫폼 간 최종 일관성 전략

| Field | Value |
|---|---|
| Date | 2026-05-11; amended 2026-07-15 |
| Status | Accepted |
| Ownership | [ADR 0048](./0048-horizontal-platform-redefinition.md) |
| Event transport | [ADR 0046](./0046-kafka-kubernetes-preliminary-design.md), [ADR 0047](./0047-collection-event-fabric.md) |

## 결정

각 플랫폼은 하나의 로컬 데이터베이스 transaction에서 자기 정본 상태를 커밋한다.
Cross-platform propagation is asynchronous and uses published events or
immutable artifacts. A distributed transaction and cross-platform database
write are forbidden.

일관성 protocol은 다섯 부분으로 구성한다.

1. **Transactional outbox** - canonical state and its outbound event are written
   atomically in the owner's database.
2. **Idempotent inbox** - consumers deduplicate by immutable event id before
   applying side effects.
3. **Versioned read models** - copied data carries source version, checksum, and
   source timestamp so stale state is observable.
4. **Reconciliation** - owners and consumers can compare catalog/manifest state
   and repair missing projections without inventing facts.
5. **Quarantine** - checksum conflicts, malformed contracts, and exhausted
   retries are isolated for review instead of being silently accepted.

## 소유권 예시

- Foundation commits canonical Catalog data and publishes artifact/event
  references. Gongzzang imports only its product-serving read models.
- Identity signs or publishes identity assertions. Foundation and Gongzzang
  store only the principal references required for authorization and audit.
- Intelligence submits normalization proposals. Foundation approval/apply
  commands alone may change Foundation canonical records.

## 전송 독립성

Webhook, SQS/SNS, and Kafka are delivery adapters. Moving between them must not
change event identity, schema, idempotency, ownership, or replay semantics.
Kafka may become the high-throughput event log, but it is not the consistency
model itself.

## 실패 규칙

- At-least-once delivery is expected; duplicate effects are a consumer bug.
- An event is acknowledged only after durable inbox/side-effect handling.
- A missing event is repaired from the owner outbox or immutable manifest.
- A conflicting checksum fails loudly and enters quarantine.
- Consumer lag affects freshness, never ownership of the canonical fact.

## 검증

Focused tests cover outbox atomicity, duplicate delivery, stale-version
rejection, checksum conflict, retry exhaustion, and replay recovery on the
runtime paths that implement this protocol.
