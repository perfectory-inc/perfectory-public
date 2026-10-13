# ADR 0212: Identity 서비스 권한은 덮어쓰지 않고 쌓는 원장이며, 바뀔 때마다 이벤트를 낸다

- Status: Accepted
- Date: 2026-10-14
- Related: [ADR-0080](./0080-the-issuer-answers-on-every-loopback.md) (Identity 운영 기동)

## Context

워크로드(서비스) 주체의 권한은 `config/workload-principal-policy.v1.json` 이 정하고,
`tools/identity-service-provisioner` 가 DB 에 맞춘다. 맞추는 방식은 `identity.service_capability_grant`
에서 정책에 없는 행을 `DELETE` 하고 새 행을 `INSERT ... ON CONFLICT DO NOTHING` 하는 것이었다.

- 권한이 회수되면 행이 사라질 뿐 **언제·어느 정책 때문에** 회수됐는지 남지 않는다.
- 아무 이벤트도 나가지 않는다. 직원 역할 부여(`identity.staff_role` + `identity.staff.role_assigned.v1`
  아웃박스 이벤트, 한 트랜잭션)는 이미 그렇게 하는데 서비스 권한만 예외였다.
- 프로비저너 DB 역할이 `DELETE` 권한을 가졌다.

## Decision

1. **원장.** `identity.service_capability_grant_record` 가 서비스 권한의 유일한 정본이다. 한 행 =
   한 변화(`change` = `granted` | `revoked`), 순서는 `record_seq`(identity 열), 원인은
   `source`(`workload_policy` 면 정책 문서 바이트의 SHA-256 `policy_sha256` 필수,
   `baseline_backfill` 은 이번 이주가 옛 표에서 옮긴 행).
2. **덮어쓰기 금지.** 원장의 `UPDATE`·`DELETE`·`TRUNCATE` 는 트리거가 거부한다(소유자 포함).
   프로비저너 역할은 원장에 `SELECT, INSERT` 만 가진다. 회수는 `revoked` 행을 덧붙이는 것이다.
3. **현재 권한은 파생.** `identity.service_capability_grant` 는 표가 아니라 뷰다 —
   `(service_principal_id, capability)` 마다 가장 늦은 변화가 `granted` 인 것. 열 세 개
   (`service_principal_id, capability, granted_at`)는 그대로라 읽는 쪽(`identity-api` 정책 결정)은
   바뀌지 않는다. 뷰는 갱신할 수 없으므로 옛 `DELETE` 경로는 기계적으로 막힌다.
4. **이벤트.** 변화 하나마다 같은 트랜잭션에서 `identity.outbox_event` 에
   `identity.service_principal.capability_granted.v1` 또는 `.capability_revoked.v1`
   (`ServiceCapabilityChangedV1`: 주체·권한·시각·`policy_sha256`)를 쓴다. 정책이 그대로면 아무 행도,
   이벤트도 생기지 않는다(멱등).
5. **동시 실행.** 프로비저너는 트랜잭션 advisory lock 하나를 잡는다. 두 실행이 같은 현재 상태를 읽고
   같은 변화를 두 번 쓰는 일을 막아 `record_seq` 순서가 커밋 순서와 같다.
6. 원장 행이 있는 주체는 지울 수 없다(외래키 `ON DELETE` 없음). 주체 제거는 지금처럼 명시적 운영
   작업이고, 이력이 남는 주체는 지우지 않는다.

검증: `tools/identity-service-provisioner/tests/live_provisioning.rs` 가 실제 PostgreSQL 에서
부여·재실행(무변화)·축소(회수 행+이벤트)·전부 회수와 원장 수정 거부를 확인한다.
`plan_capability_changes` 단위 시험, `identity-contracts` 와이어 시험, `deploy_contract.rs` 의
권한 문구 단언, `scripts/compose-smoke.sh` 의 역할 권한 확인이 함께 지킨다.

## Consequences

- 운영 DB 이주는 옛 표의 행을 `baseline_backfill` 로 옮긴 뒤 표를 지우고 같은 이름의 뷰를 만든다.
  런타임 권한 SQL(`infra/compose/grant-identity-runtime-access.sql`)이 새 권한을 준다.
- 프로비저너는 아웃박스에 `INSERT` 할 수 있게 됐다(이벤트를 쓰려면 필요). 그 밖의 직원·세션
  접근은 여전히 없다.
- 원장은 커진다. 정책 변경은 드물고 주체는 수 개라 무시할 크기다.

## Alternatives rejected

- **현재 표를 두고 옆에 이력 표 추가**: 같은 사실(현재 권한)이 두 곳에 남아 어긋날 수 있다.
  현재는 원장에서 파생한다.
- **`valid_from/valid_to` 시간 구간 표**: 회수 때 기존 행의 `valid_to` 를 고쳐야 한다(덮어쓰기).
- **이벤트만 쓰고 표는 그대로**: 회수가 여전히 `DELETE` 라 DB 에서 이력을 물을 수 없다.
