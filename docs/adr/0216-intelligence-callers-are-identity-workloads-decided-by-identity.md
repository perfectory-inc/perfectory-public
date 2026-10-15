# ADR 0216: Intelligence 호출자는 Identity workload 이고 권한은 Identity 가 결정한다

- Status: Accepted
- Date: 2026-10-14

## Context

`intelligence-api` 의 인바운드 인증은 공유 토큰 하나였다. 토큰이 맞으면 호출자의 주체·tenant·product·허용
동작을 `INTELLIGENCE_INBOUND_SERVICE_{SUBJECT_ID,TENANT_ID,PRODUCT_ID,ACTIONS}` 환경변수에서 읽었다
(`services/intelligence-api/src/auth.rs`). 이 값은 Identity 의 workload 정책
(`platforms/identity-platform/config/workload-principal-policy.v1.json`)과 연결되지 않았다.
누가 어떤 서비스에 무엇을 할 수 있는지가 Identity 정책과 서버 환경변수 두 곳에 있었고, 두 번째는 어떤 검토도
거치지 않는 손 입력이었다.

Foundation 은 이미 같은 문제를 Identity 에 묻는 방식으로 풀었다: 호출자가 보낸 Identity workload token 을
그대로 `POST /identity/v1/policy/decisions` 에 보내고 허용 여부와 평가된 principal 을 받는다
(`services/foundation-api/src/identity_http_client.rs`). Identity 는 서비스 principal 의 capability 를
`<resource>:<action>` 정확 일치로 평가한다.

측정한 사실: 2026-10-14 현재 `intelligence-api` 를 부르는 운영 코드는 저장소에 없고(gongzzang·dawneer·
foundation 검색 결과 0건) ai-server 에도 떠 있지 않다. 그래서 바꾸는 비용은 지금이 가장 싸다.

## Decision

1. `INTELLIGENCE_INBOUND_AUTH_MODE=identity` 를 둔다. 보호 경로마다 필요한 동작을 Identity 의 published
   `POST /identity/v1/policy/decisions` 에 호출자 Bearer 로 묻는다. 모든 동작이 같은 principal 로 허용될 때만
   통과한다. Identity 가 자격을 거절하면 401, 하나라도 불허면 403, Identity 가 응답하지 않거나 계약 밖 응답이면
   503 이다. 어떤 실패도 허용이 되지 않는다.
2. 동작과 Identity capability 의 대응은 `PermissionAction::identity_capability` 한 곳이다
   (`intelligence.chat:complete`, `intelligence.normalization:{validate,generate,submit}`,
   `intelligence.knowledge:{ingest,retrieve}`, `intelligence.graph:view_context`). `/metrics` 는
   `intelligence.metrics:read` 다. 호출자에게 권한을 주는 일은 Identity workload 정책에 capability 를 적는 것뿐이다.
3. tenant 와 product 는 헤더·환경변수에서 받지 않는다. Identity workload principal 은 tenant 를 갖지 않으므로
   검증된 principal 자신이 그 호출의 범위다(율 제한 버킷도 principal 단위). 다른 범위가 필요해지면 Identity 정책이
   그 사실을 소유해야 하며 Intelligence 가 지어내지 않는다.
4. non-loopback bind 는 `identity` 모드만 허용한다. `shared-token` 은 loopback 전용 로컬 개발로 남는다.
   `INTELLIGENCE_IDENTITY_BASE_URL` 은 HTTPS(loopback 만 HTTP)이고 자격 정보를 담을 수 없다. 둘 다 기동 때 거부한다.
5. 영역 간 Cargo 경로 의존은 없다(루트 ADR-0001). 대신 시험
   `intelligence-normalization-infrastructure/tests/identity_policy_contract.rs` 가 Identity 의 published 파일을 직접
   읽는다: 보내는 요청 필드가 OpenAPI `PolicyDecisionRequest` 와 같은지, 읽는 응답 필드가 필수인지,
   `ResourceAction` 값이 같은지, Identity 정책이 주는 `intelligence.*` capability 가 전부 실재 동작인지.
   두 파일은 `tools/ci/change-scope.json` 의 `intelligence-ci.yml` 입력이라 Identity 쪽 변경도 이 시험을 돌린다
   (루트 ADR-0202 §6).

## Consequences

- Intelligence 를 부를 첫 서비스는 Identity 정책에 principal 과 `intelligence.*` capability 를 받고, Identity 가
  발급한 workload token 을 보낸다. 지금 정책은 `intelligence.*` 를 아무에게도 주지 않으므로 운영 모드는 전부
  거부한다 — 의도한 닫힌 기본값이다.
- 요청마다 Identity 를 동작 수만큼(최대 3회) 부른다. 결정 캐시는 넣지 않았다: 호출량이 없고, 캐시는 철회를
  늦춘다. 호출량이 생겨 지연이 문제가 되면 짧은 TTL 의 결정 캐시를 그때 측정과 함께 정한다.
- LAN 의 Open WebUI 를 공유 토큰으로 붙이던 일시적 개발 방식은 끝난다(loopback 이나 `identity` 모드로).

## Rejected alternatives

- **Identity 정책 파일에 tenant·product 를 추가**: 그 값을 쓰는 소비자가 Intelligence 율 제한 하나뿐이고,
  Identity 의 정책 스키마·발급기(provisioner)를 함께 바꿔야 한다. 쓰임이 생길 때 Identity 가 소유하도록 미룬다.
- **JWT 를 Intelligence 가 직접 검증(JWKS)**: 권한 결정이 Intelligence 에 복제된다. Foundation 과 같은 한 결정점이
  낫다.
- **`identity-contracts` crate 를 경로 의존으로 공유**: 루트 ADR-0001 이 영역 간 Cargo 경로 의존을 금지한다.
  published OpenAPI 를 읽는 시험이 같은 보장을 준다.
