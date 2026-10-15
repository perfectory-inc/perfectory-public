# ADR 0213: Identity 직원 역할은 `identity.staff_role` 한 곳에만 있다

- Status: Accepted
- Date: 2026-10-14
- Related: ADR-0212 (서비스 권한 원장, 같은 날 별도 PR)

## Context

직원 역할이 두 곳에 있었다.

- `identity.staff.primary_role_code` — 계정 행의 열. 부여자·부여 시각·이벤트가 없다.
- `identity.staff_role` — 부여 행. 부여자·시각이 있고, 부여 API 와 최초 관리자 부트스트랩이
  `identity.staff.role_assigned.v1` 아웃박스 이벤트를 같은 트랜잭션에서 쓴다.

유효 역할 읽기(`PgEffectiveRoleReader`)는 둘을 `UNION ALL` 했고, 부트스트랩은 같은
`MASTER_ADMIN` 을 두 곳에 썼으며, "최초 관리자가 있는가" 검사도 두 곳을 다 봤다. 열에만 있는 역할은
이벤트 소비자에게 보이지 않고, 누가 줬는지도 알 수 없다. 같은 사실이 두 곳에 있으면 어긋날 수 있고,
어긋나도 아무것도 붉어지지 않는다.

## Decision

1. 직원 역할의 정본은 `identity.staff_role` 하나다. 유효 역할 = 그 직원의 `staff_role` 행 전부.
2. 이주 `20261014000002_staff_role_is_the_only_role_source.sql` 은 `primary_role_code` 중 같은
   `staff_role` 행이 없는 것을 부여 행으로 옮기고(부여자 = 본인, 시각 = 계정 생성 시각 — 열이 암시한
   유일한 부여자), 열을 지운다. 옮기기 전과 뒤의 유효 역할은 정의상 같다. 이미 효력이 있던 역할이라
   새 이벤트는 쓰지 않는다.
3. 도메인 `Staff` 에서 `primary_role_code` 를 지운다. 부트스트랩은 계정 행과 `MASTER_ADMIN` 부여 행,
   이벤트만 쓴다.
4. 기계적 차단: 열이 없으므로 다시 쓰는 코드는 컴파일·SQL 단계에서 실패한다. 단위 시험
   `effective_roles_come_only_from_staff_role` 이 읽기 SQL 이 `staff_role` 만 보는지 확인하고,
   `role_grant_postgres` 실 DB 시험은 역할을 부여 행으로만 심는다.

## Consequences

- 운영 DB 이주는 행 수가 직원 수만큼(수 명)이라 즉시 끝난다. 이주 뒤 옛 바이너리는 지워진 열을
  읽다 실패하므로 이주와 새 이미지는 같은 배포에서 나간다(Identity 배포 순서: 마이그레이터 → 런타임).
- Foundation 의 서명 OIDC 스모크(`scripts/smoke/identity-foundation-signed-oidc.sh`)도 직원을 열 없이 심는다.
- 직원 이메일·이름도 Zitadel 에서 한 번 복사된 뒤 갱신되지 않는다. 그 정리는 토큰 클레임을 정본으로
  삼는 별도 결정이며 이 ADR 의 범위가 아니다.

## Alternatives rejected

- **열을 "기본 역할" 표시로 남김**: 기본 역할이라는 개념을 쓰는 코드가 없다. 남기면 다시 두 번째
  정본이 된다.
- **열을 지우지 않고 읽기만 끊음**: 쓰는 쪽(부트스트랩·스모크)이 계속 채우고 아무도 읽지 않는 죽은
  사실이 남는다.
