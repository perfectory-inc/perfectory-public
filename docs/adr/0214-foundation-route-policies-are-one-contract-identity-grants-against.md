# ADR 0214: Foundation 라우트가 묻는 정책은 계약 파일 하나이고, Identity 는 그 파일에 맞춰 권한을 준다

- Status: Accepted
- Date: 2026-10-14

## Context

Foundation API 의 보호 라우트는 Identity 의 `POST /identity/v1/policy/decisions` 에
`(resource, action)` 을 묻는다. 그 이름들이 세 곳에 손으로 적혀 있었다.

1. Foundation `services/foundation-api/src/routes/mod.rs` 의 `IdentityRoutePolicy` 상수 11개.
2. Identity `crates/authorization/authorization-domain/src/policy.rs` 의 `matches!` 역할 행렬.
3. Identity `config/workload-principal-policy.v1.json` 의 서비스 capability.

셋을 잇는 검사가 없었다. Foundation 이 라우트를 더하거나 이름을 바꾸면 그 라우트는 `MASTER_ADMIN`
외에 아무도 못 쓰고, 지운 라우트의 권한은 행렬에 죽은 채 남는다 — 둘 다 아무것도 붉어지지 않는다.
또 Foundation `identity_http_client.rs` 는 결정 요청·응답 타입을 Identity 의 OpenAPI 와 대조 없이
베껴 두었다.

영역 간 Cargo path 의존은 금지다(루트 ADR-0001). 그래서 `identity-contracts` 크레이트를
Foundation 이 직접 가져다 쓰는 길은 없다. 공유는 발행된 파일로 한다.

## Decision

1. **Foundation 이 목록을 소유한다.** `platforms/foundation-platform/config/identity-route-policies.contract.json`
   은 Foundation 라우트가 묻는 정책 전부(`principal_kind`, `resource`, `action`)다. 코드 쪽 정본은
   `routes/mod.rs` 의 `ROUTE_POLICIES` 배열이고, `routes::tests::route_policies` 가 두 가지를 거부한다:
   파일과 배열이 다름, `IdentityRoutePolicy` 상수가 배열에 빠짐.
2. **Identity 는 그 목록에 맞춰 준다.** 역할 행렬은 `matches!` 식이 아니라 데이터 표
   `STAFF_ROLE_GRANTS` 이고 `evaluate_policy` 는 그 표만 읽는다.
   `authorization-domain/tests/route_policy_contract.rs` 가 확인한다.
   - 행렬의 `foundation.*` 항목 집합 = 계약의 staff 정책 집합(도달 못 하는 라우트·죽은 권한 둘 다 거부).
   - 워크로드 정책의 `foundation.*` capability 집합 = 계약의 service 정책 집합.
3. **결정 타입은 Identity OpenAPI 가 정본.** Foundation 의 비공개 요청·응답 타입은
   `identity_http_client::tests` 가 `platforms/identity-platform/docs/openapi/identity.v1.json` 의
   `PolicyDecisionRequest`·`PolicyDecisionResponse`·`ResourceAction` 과 대조한다(보내는 필드 = 발행 필드,
   필수 응답 필드, 결정 값 전부 해석 가능).
4. Identity CI 는 계약 파일이 바뀌면 돈다(`tools/ci/change-scope.json` 의 `identity-ci.yml` 입력).
   Foundation CI 는 범위를 줄이지 않으므로 Identity OpenAPI 변경에도 돈다.

## Consequences

- 라우트를 더하는 Foundation PR 은 계약 파일에 한 줄을 더해야 하고, 그 줄은 Identity 시험을
  붉게 만들어 역할 행렬이나 워크로드 정책에 권한을 함께 주게 한다.
- 정책 이름은 여전히 문자열이다. Rust 타입 공유가 아니라 파일 대조로 막는다.

## Alternatives rejected

- **`identity-contracts` 를 Foundation 이 path 의존**: 루트 ADR-0001 위반, 영역 빌드가 엮인다.
- **Identity 가 Foundation 소스를 텍스트로 파싱**: 형식이 바뀌면 조용히 0개를 읽는다.
- **Identity 가 목록을 소유**: 라우트를 만드는 쪽은 Foundation 이다. 소유자가 바뀌면 라우트를 더할 때
  다른 영역 파일을 먼저 고쳐야 한다.
