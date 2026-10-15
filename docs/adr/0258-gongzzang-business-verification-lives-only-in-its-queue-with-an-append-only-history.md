# ADR 0258: gongzzang 사업자 인증은 인증 큐에만 있고, 상태 이력은 덧붙이기만 한다

- Status: Accepted
- Date: 2026-10-15
- Related: ADR-0221 (매물 상태·가격 이력), ADR-0232 (Zitadel subject 단일 출처)

## Context

같은 사실 — "이 사용자의 사업자는 인증됐다, 번호는 이것" — 이 두 곳에 있었다.

- `"user".business_number`, `"user".business_verified_at` — `User::verify_business` /
  `revoke_business_verification` 이 쓰는 칸.
- `business_verification_queue` — 제출 문서·검토자·메모·상태를 가진 인증 워크플로.

둘을 맞추는 코드는 없었다. 큐에서 승인해도 사용자 칸은 비어 있고, 사용자 칸을 채워도 큐에는 근거가 없다.
또 큐 행은 고쳐 쓰는 행이라 재제출(`resubmit`)이 검토자·메모를 지우면 그 전 결정이 남지 않았다.

## Decision

1. 사업자 인증의 유일한 출처는 `business_verification_queue` 다. 지금 효력이 있는 인증은 그 사용자의
   가장 최근 `approved` 행이다(`BusinessVerificationRepository::find_verified_business`).
   `"user".business_number`·`business_verified_at` 열과 `User` 의 사업자 필드·메서드·오류 변형은 없앤다.
2. 승인 취소는 큐의 상태다: `Approved → Revoked`(`BusinessVerification::revoke`, 메모 필수, terminal).
   상태 CHECK 에 `revoked` 를 더한다.
3. `business_verification_status_history` 가 큐 행이 지난 모든 상태를 한 줄씩 남긴다. 쓰는 주체는 트리거
   (`trg_business_verification_status_history_insert`/`_update`, `security definer`) 하나 — 어떤 쓰기 경로도
   빠뜨릴 수 없다. `UPDATE` 는 트리거가 거부한다(SQLSTATE 45000). 행은 큐 행과 함께(`on delete cascade`)만 떠난다.
4. 마이그레이션 `20261015130000_business_verification_lives_in_its_queue.sql` 은 열을 지우기 전에 사용자 칸의
   사실을 큐로 옮긴다(`-- backfill:begin` 구역):
   - `business_verified_at` 이 있고 같은 번호의 승인 행이 없으면 → `approved` 행
     (`reviewed_at = business_verified_at`).
   - 번호만 있고 인증 시각이 없으면 → `revoked` 행. 옛 `revoke_business_verification` 이 번호는 두고 시각만
     지웠기 때문이다; 번호를 잃지 않는다.
   - 큐 id 는 사용자 ULID 를 그대로 쓴다(`usr_<ulid>` → `bvq_<ulid>`): 유효한 id 이고 사용자당 한 행이다.
   - 이미 있던 큐 행은 현재 상태 한 줄로 이력을 시작한다.

## Consequences

- 통합 테스트 `the_user_row_copy_moves_into_the_queue_without_losing_a_row` 가 배포된 마이그레이션의 backfill
  구역을 옛 모양의 사용자 행 위에서 그대로 실행해, 사실 하나당 큐 행 하나(중복·유실 없음)와 열 제거를 확인한다.
  `every_status_is_kept_and_the_verified_business_is_the_approved_row` 가 pending → approved → revoked 이력과
  취소 뒤 인증 없음을 확인한다.
- 영속 계층의 상태 문자열 대응표(`parse_status`)는 손으로 쓴 match 대신 도메인 `FromStr` 를 부른다 — `revoked`
  를 더하면서 두 번째 목록이 어긋날 자리를 없앴다.
- 기각한 대안: (a) 사용자 칸을 큐 승인 때 같이 쓰기 — 두 칸을 맞추는 코드가 하나 더 생길 뿐 원본은 둘이다.
  (b) 큐를 덧붙이기만 하는 결정 표로 바꾸기 — OCC 워크큐(어드민 동시 검토)를 다시 짓는 일이고, 이력만 필요하면
  트리거 이력 표로 충분하다.
- 운영 행 수는 조회하지 않았다(런칭 전, AWS 접근 없음).
