# ADR 0232: 공짱 사용자의 Zitadel subject 는 `external_account` 한 곳에만 산다

- Status: Accepted
- Date: 2026-10-14
- Area: `products/gongzzang`

## Context

같은 Zitadel subject 가 두 곳에 저장됐다.

- `"user".zitadel_sub` (`20260719000102_core_tables.sql`, `NOT NULL UNIQUE`) — 모든 조회가 여기를 읽었다.
- `external_account(provider = 'zitadel')` (`20260719000113_user_ci_external_account.sql`) — 다중 IdP 연결 표.
  첫 로그인 미들웨어가 `save` 를 커밋한 **뒤** 별도 쓰기로 채웠고, 실패하면 `warn!` 만 남기고 계속했다.

두 번째 쓰기가 실패하면 사용자는 로그인되지만 `external_account` 에는 없다. 카카오·네이버 연동(SP6-Social)이
`external_account` 를 기준으로 붙으면, 그 사용자는 연동 표에서 보이지 않는 채로 남는다. 같은 사실이 두 곳에
있고 둘의 쓰기가 원자적이지 않아 어긋날 수 있었다.

## Decision

1. **유일한 출처는 `external_account`.** `"user".zitadel_sub` 를 지운다(전진 마이그레이션
   `20261014100000_external_account_is_the_only_zitadel_subject.sql`). 지우기 전에 연결이 없는 사용자를
   `INSERT ... SELECT` 로 채운다(런칭 전 사용자 0명이지만 형식상).
2. **조회는 조인.** `PgUserRepository` 의 모든 조회는 `"user" u join external_account ea on ea.user_id = u.id
   and ea.provider = 'zitadel'` 하나의 `select` 를 쓴다. 도메인 `User.zitadel_sub` 는 그 열을 받는다.
3. **쓰기는 한 트랜잭션.** `save` 가 새 사용자(`before_state` 없음)일 때 `user` INSERT 와 같은 트랜잭션에서
   `external_account('zitadel')` 를 INSERT 한다. 다른 사용자의 subject 면 `UNIQUE(provider, external_id)` 위반 →
   `Conflict`, 사용자 행도 롤백. `UserRepository::link_zitadel_account` 와 미들웨어의 경고 후 계속은 없앤다.
4. **사용자당 Zitadel subject 하나**: 부분 유일 인덱스 `external_account(user_id) where provider = 'zitadel'`.

## Rejected

- **`external_account` 쓰기를 실패 시 오류로만 바꾸기**: 두 벌은 그대로 남아 다음 경로가 또 어긋날 수 있다.
- **`"user".zitadel_sub` 를 남기고 `external_account` 의 zitadel 행을 없애기**: 연동 표가 IdP 마다 다른 규칙을
  갖게 된다. SP6-Social 이 붙을 곳은 `external_account` 다.
- **"모든 사용자는 zitadel 행이 있다"를 지연 제약 트리거로 강제**: 쓰기 경로가 `save` 하나뿐이라 그 원자성 시험
  (`failed_zitadel_link_leaves_no_user_row`)이 같은 사고를 막는다. 다른 쓰기 경로가 생기면 그때 붙인다.

## Consequences

- `audit_log.before_state/after_state` 의 사용자 스냅숏(`to_jsonb("user".*)`)에 `zitadel_sub` 키가 더 없다.
- `.sqlx` 오프라인 캐시 하나(`find_by_id` 의 `query_as!`)가 런타임 쿼리로 바뀌어 지워졌다.

2026-10-28: 구현 확인 — `products/gongzzang/migrations/20261014100000_external_account_is_the_only_zitadel_subject.sql`, `products/gongzzang/crates/gongzzang-persistence/src/user.rs`
