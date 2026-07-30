# ADR-0049: Identity Platform 계약 설계

| Field | Value |
|---|---|
| Date | 2026-07-02 |
| Status | Accepted |
| Decision owner | perfectoryinc |
| Related | ADR-0030, ADR-0031, ADR-0048, foundation implementation ADR-0021, foundation implementation ADR-0023 |

> 이 ADR은 ADR-0048을 구현하는 Identity Platform 계약의 정본이다. 소유권 경계, v1 API/event
> 표면, service identity 단계와 인가 모델을 정의한다. DB migration, 저장소 분리 또는 새
> infrastructure를 승인하는 문서가 아니며, 그 작업은 별도의 승인 게이트를 따른다.

## 배경

ADR-0048은 저장소 간 architecture를 수평 platform으로 재정의하고 공용 identity를
`identity-platform`에 할당했다.

- 직원 identity
- service identity와 service token
- session 검증
- role/permission/policy model
- service 간 authorization contract
- audit principal 해석
- identity 관련 outbox/event

현재 이 책임은 모두 legacy core repository(`foundation-platform`의 전환기 물리 위치)에 구현되어
있다. ADR-0048의 migration strategy는 물리 repository를 나누기 전에 identity 책임을
identity-platform *contract* 뒤로 이동하도록 요구한다. 이 ADR이 그 계약이다.

두 가지 제약이 이 설계를 결정한다.

1. **Product-first / YAGNI**(AGENTS.md 최상위 규칙). 이 platform에는 사용자가 0명이다. 따라서
   v1 contract는 새 engine이 아니라 *현재 존재하는 표면*을 이름과 version만 바꾼 것이다.
   무거운 장치(ReBAC 인가 engine, SPIFFE workload identity infrastructure)는 이름이 정해진
   trigger 기반 미래 단계로만 설계한다.
2. **Infrastructure 품질은 타협하지 않는다**(owner 지시, 2026-07-02). 계약은 공개된 기업
   운영 사례에 맞춰야 하므로 아래 staging target은 Google·Airbnb·Uber·Netflix가 실제로 사용하는
   방식을 참고 문서에 기록한다.

### 현재 상태(2026-07-02 코드 검증)

**Staff identity** — legacy repo의 `crates/workforce/*`에 구현되어 있다.

- 집계: `Staff` (id `StaffId`/UUID, unique `zitadel_subject`, email,
  display_name, primary_role_code, version), `StaffRole` (staff_id,
  role_code matching `[A-Z0-9_]+`, granted_at, granted_by), `StaffSession`
  (session_id, staff_id, unique `jti`, issued_at, expires_at).
- IdP: Zitadel (OIDC). `HttpZitadelClient`는 캐시한 JWKS를 사용해 ID token을 검증하고
  캐시한 JWKS(RS256/384/512, ES256/384)로 검증하고 sub/email/name/jti/iat/exp/roles를
  추출한다. JTI revoke에는 `workforce.revoked_jti` table을 사용한다.
- 유스케이스: `VerifyStaffSession` (token → staff + session + roles),
  `AssignStaffRole`(`can_grant_roles()`를 통해 `MASTER_ADMIN`만 부여 가능),
  `can_grant_roles()`), `BootstrapPlatformAdmin` (idempotent first admin;
  identity-platform 내부 startup routine으로 실행하며 HTTP route나 서비스 간 표면이
  아니다).
- HTTP: `POST /workforce/v1/sessions/verify` (id_token → staff_id,
  session_id, email, display_name, roles[], expires_at)를 반환하며
  `POST /workforce/v1/staff/{id}/roles` (Bearer actor token;
  400/401/403/404/409). OpenAPI `docs/openapi/workforce.v1.yaml`,
  operationIds `verifySession` / `assignRole`.
- 이벤트 (shared-kernel `workforce_v1.rs`, 호환성 corpus):
  `workforce.staff.invited.v1`, `workforce.staff.role_assigned.v1`,
  `workforce.staff.session_revoked.v1` (reason:
  logout|admin_revoke|role_changed|security), published via
  `workforce.outbox_event`.
- DB schema `workforce.*`: staff, staff_role, staff_session, revoked_jti,
  outbox_event.
- 오류 모델: StaffNotFound, DuplicateZitadelSubject, DuplicateRole,
  RoleNotFound, SessionExpired, JtiRevoked, InvalidClaims,
  PermissionDenied, Infrastructure → 400/401/403/404/409/500.

**Service identity** — `services/gongzzang-api/src/routes/service_identity.rs`에 구현되어 있다.

- 현재 소비자: gongzzang (`gongzzang-api` catalog:read,
  `gongzzang-worker` lakehouse:write), dawneer (`dawneer-api`
  catalog:read), intelligence-platform (normalization:propose).
- 방식: static bearer token 또는 요청마다 다시 읽는 workload-identity token *file*이며,
  static token보다 file 방식을 우선한다. Token 비교는 constant-time이다.
  family별 메타데이터 header(policy-id, source, target, allowed-call-id)는 항상 필수이고,
  dawneer와 intelligence lane에서는 scope도 필수다. gongzzang lane에서는 scope가
  선택 사항이지만 있으면 검증한다. route는 `SERVICE_IDENTITY_ROUTES` table
  (필지/건물 catalog 읽기, lakehouse artifact 쓰기, normalization proposal 제출)에
  등록된 경우만 허용하는 deny-by-default 방식이다.
- header family: `x-gongzzang-*`, legacy `x-foundation-platform-*`, 선호
  `x-foundation-platform-*`(2026-07-02 추가). policy-id/target *값*은
  versioned-contract 단계 전까지 의도적으로 고정된 legacy contract ID를 유지한다.
- Gongzzang client(`crates/auth/src/foundation_platform_service.rs`)는 이미 token
  metadata 규율을 강제한다. client에서 16자 이상 길이, scope, issued_at, expires_at
  (RFC 3339, TTL 최대 90일), rotation_owner를 확인하고 환경변수 alias는
  `FOUNDATION_PLATFORM_*`을 우선한다. v1에는 server-side 길이 검사가 없다.
- 정책 registry(JSON): `foundation_platform.traffic_auth_policy_registry.v1`
  (소비자 정책 4개, deny default)와 `gongzzang.traffic_auth_policy_registry.v1`
  (exposure class: public_derived/authenticated_user/privileged/service_to_service).

**Foundation-platform의 identity 의존성** — Catalog normalization command는
`reviewer_staff_id` / `applied_by_staff_id` / `rolled_back_by_staff_id`를 audit principal로
전달한다. Catalog은 ACL adapter(`ActorDto`, `workforce_acl.rs`)를 사용하므로 workforce
domain type을 직접 import하지 않는다. 아래 결정은 이 기존 패턴을 표준 경계 형태로 승인한다.

## 결정

### 1. 소유권 경계

`identity-platform`이 소유한다:

- 직원/관리자 identity 생명주기(초대, role 부여, session, revoke)
- 직원 session 검증과 JTI revoke 상태
- Service identity: service principal, token/검증 규칙, 공유 서비스 간
  traffic-auth policy registry
- role/permission model(현재 role code, 이후 확장)
- 서비스 간 authorization 계약(누가 무엇을 호출할 수 있는지, deny default)
- audit principal 해석(opaque principal id → 사람이 표시할 수 있는 identity)
- identity event와 해당 outbox

`identity-platform`이 **소유하지 않는다**:

- Gongzzang B2C product user, product session, product auth 흐름. 이는
  계속 `gongzzang` 소유이며 이동하려면 별도 ADR이 필요하다(ADR-0048 규칙 재확인).
- Authentication 자체. Zitadel은 IdP(OIDC 발급, JWKS)로 남는다.
  identity-platform은 그 위의 principal/policy/contract 계층이며, 인증은 구매하고
  인가는 소유하는 분리 방식이다(참고 문서의 Zanzibar 계열 사례와 같은 형태).
- product-local exposure policy. `gongzzang.traffic_auth_policy_registry.v1` 같은
  product registry는 product 소유로 남는다. 다만 `service_to_service` class만
  identity-platform 소유 policy ID를 참조해야 한다.
- domain audit *record*. 각 소유 platform이 자신의 audit row를 보관하고,
  identity-platform은 그 안의 principal만 해석한다.

**Principal-reference와 principal-resolution.** Catalog의 `ActorDto` ACL은 모든
platform이 따라야 하는 패턴이다. 소유 platform은 자신의 data 안에 opaque한
`staff_id` UUID(예: `reviewer_staff_id`)인 principal *reference*만 저장하고 identity
domain type을 import하지 않는다. 화면 표시나 검증을 위해 *resolution*(id →
email/display_name/roles)이 필요할 때만 identity-platform을 호출한다. identity-platform
이외의 platform은 `workforce.*`(향후 `identity.*`) table을 읽거나 join할 수 없다.
서비스 간 직접 DB 접근은 계속 금지한다(ADR-0048의 범위 밖 결정).

### 2. v1 계약 표면

v1 계약은 검증된 현재 표면에서 기계적으로 생성한다.
새 capability는 필요한 최소 read(principal lookup) 하나 외에는 추가하지 않는다.
이 read가 ActorDto 패턴의 남은 연결 고리를 닫는다.

#### 2.1 직원 API — `identity-platform.staff.v1`

`workforce.v1`의 후속 계약이다(구현 repo의 OpenAPI 후속 문서는
`docs/openapi/identity.v1.json`).

| Operation | Route | Semantics |
|---|---|---|
| `verifySession` | `POST /identity/v1/sessions/verify` | id_token → staff_id, session_id, email, display_name, roles[], expires_at. 기존과 같은 JWKS 검증, JTI revoke 확인, 오류 매핑. |
| `assignRole` | `POST /identity/v1/staff/{id}/roles` | Bearer actor token; `MASTER_ADMIN`만 부여(`can_grant_roles()`); 400/401/403/404/409 매핑은 동일. |
| `getStaffPrincipal` | `GET /identity/v1/staff/{id}` | **신규 최소 표면.** staff_id → {staff_id, email, display_name, roles[]}. principal reference를 가진 platform이 audit 표시를 할 때 쓰는 읽기 전용 API다. 다른 서비스 간 route와 마찬가지로 deny-by-default route table에 등록한다. scope 이름은 `identity:read`로 예약하며 policy-id와 allowed-call-id는 구현 단계에서 할당한다(직원 PII를 서비스 간 반환하므로 같은 table에 넣는다). |

error model(StaffNotFound … Infrastructure와 HTTP mapping)은 v1 계약의 일부로
변경 없이 계승한다.

호환성 규칙: `POST /workforce/v1/sessions/verify`와
`POST /workforce/v1/staff/{id}/roles`는 모든 consumer가 새 계약으로 compile할
때까지 `/identity/v1/*` route의 **alias**로 계속 허용한다. 이름 변경에서 사용하는
alias+telemetry 규율을 따르며 폐기 전에 alias 사용량을 측정할 수 있어야 한다.

#### 2.2 서비스 Identity 검증 — `identity-platform.service-auth.v1`

검증 의미도 다음과 같이 계약으로 고정한다.
현재 구현 상태는 다음과 같다(code-verified 2026-07-02,
`services/gongzzang-api/src/routes/service_identity.rs:350-384`):

- Credential: static bearer token **또는** workload-identity token file를
  요청마다 다시 읽고, 둘 다 있으면 file을 우선한다. 비교는 constant-time이다.
  v1에는 server-side token 길이 검사가 없으며 강화는 구현 단계 후보다.
- 필수 metadata header(family별): policy-id, source, target, allowed-call-id는
  모든 lane에서 필수다. dawneer와 intelligence lane에서는 scope도 필수이고,
  gongzzang lane에서는 있으면 검증한다. 누락이나 불일치는 deny이며, v1은 이
  비대칭을 그대로 기록하고 균일한 scope enforcement는 구현 단계로 미룬다.
- Deny-by-default: route policy table에 일치하는 policy로 등록된 route만
  서비스 간 호출이 가능하다.
- Token metadata 규율(client 쪽, `crates/auth/src/foundation_platform_service.rs:333-342`):
  token 최소 16자, scope, issued_at, expires_at(RFC 3339, TTL 최대 90일),
  이름 있는 rotation_owner를 요구한다.

Registry ownership 이동: 현재 공유 consumer policy registry인
`foundation_platform.traffic_auth_policy_registry.v1`는 identity-platform 소유로
이동하고, versioned slice에서 후속 ID
`identity-platform.traffic_auth_policy_registry.v1`를 발행한다. 소비 platform
(foundation, gongzzang, dawneer, intelligence)은 이 registry를 소비할 뿐 fork하거나
소유하지 않는다.

**지금은 새 header family를 추가하지 않는다.** 기존 header family는 역사적인
wire prefix다. 일부는 호출 product를 나타내는 source 이름(`x-gongzzang-*`)이고,
일부는 destination API를 나타내는 target 이름(`x-foundation-platform-*`)이다.
전체 family에 통일된 작명 규칙은 없다. `x-identity-platform-*`를 추가하지 않는
이유는 consumer 요구가 없고 policy-id/allowed-call-id 값이 고정 contract ID라
이름을 바꾸면 consumer가 깨지기 때문이다. 네 번째 alias family를 목적 없이
추가하는 것은 형식에 불과하다. policy-id/target *값*도 versioned-contract 단계까지
고정된 legacy contract ID를 유지한다.

#### 2.3 이벤트 — `identity-platform.staff.*.v1`

후속 event 이름을 정의한다. payload는 현재 workforce corpus와 field 단위로 동일하다.

| Successor | Legacy alias | Payload |
|---|---|---|
| `identity-platform.staff.invited.v1` | `workforce.staff.invited.v1` | schema_version, staff_id, email, invited_at, invited_by |
| `identity-platform.staff.role_assigned.v1` | `workforce.staff.role_assigned.v1` | schema_version, staff_id, role_code, assigned_at, assigned_by |
| `identity-platform.staff.session_revoked.v1` | `workforce.staff.session_revoked.v1` | schema_version, staff_id, jti, revoked_at, reason: logout\|admin_revoke\|role_changed\|security |

호환성 규칙: versioned publication 단계 전까지 `workforce.*.v1` 이름을 wire format으로
유지하고, 후속 이름은 이 ADR에서 예약한다. 전환 시 compatibility corpus는 두 이름을
모두 포함하고 consumer도 전환 기간 동안 둘 다 받아야 한다. legacy 이름은 §5 7단계
순서에 따라서만 폐기한다. event는 기존 outbox table을 계속 통과하며, table 이름 변경은
§5 5단계의 DB migration 승인을 받은 경우에만 수행한다.

### 3. 서비스 Identity 스테이징

SPIFFE가 제시한 도입 순서(static secret → platform-issued short-lived identity)에
따라 service identity를 세 단계로 발전시킨다. 1–2단계는 현재 존재하고 3단계는
trigger가 충족될 때만 진행한다.

- **1단계(현재): static token + metadata 규율.** constant-time 비교를 사용하는
  static bearer token, metadata header(항상 필수 4개, scope는 lane별 적용), TTL 최대
  90일과 rotation_owner 요구를 사용한다. consumer가 4개로 적고 rotation 소유자가
  정해져 있으며 TTL이 제한되어 있어 출시 전 단계에는 충분하다.
- **2단계(현재, 부분 적용): workload-identity token file.** 요청마다 file에서 token을
  읽고 static 환경변수 token보다 우선한다. credential 전달을 process 환경과 분리해
  platform 발급 credential로 가는 발판을 만든다. 새 consumer는 1단계가 아니라
  2단계로 onboarding한다.
- **3단계(trigger 조건부): SPIFFE/SPIRE형 workload identity.** 약 한 시간 수명의
  SVID를 자동 교체하고 mTLS를 적용해 공유 secret을 완전히 대체한다. 이는 Uber와
  Netflix가 운영에서 사용하는 CNCF 표준 모델이다(참고 문서).
  **trigger:** Kubernetes 도입(ADR-0046도 자체 trigger 뒤로 미룸) 또는 배포 환경이
  2개를 초과하는 경우다. 어느 조건도 충족하기 전에 SPIRE infrastructure를 만드는
  것은 ADR-0044가 뒤집은 infra-before-users 함정이다.
- **Delegation(trigger 조건부): RFC 8693 token exchange.** service가 경계를 넘어
  staff principal을 *대신해* 행동해야 할 때(예: foundation-platform이
  normalization proposal을 누가 승인했는지 identity-platform 또는 auditor에게
  증명해야 할 때) `act` claim을 사용하는 OAuth 2.0 Token Exchange를 적용한다.
  token 자체에 delegation chain을 보존하므로 service 간에 raw staff token을 보내지
  않는다. **trigger:** 단순 audit reference가 아니라 staff 권한을 전달해야 하는
  첫 서비스 간 호출이다. 그 전까지는 `reviewer_staff_id` 같은 audit-reference
  field가 충분하고 올바르다.

### 4. 인가 모델

**결정: authorization decision은 identity-platform에 중앙화하고, model 자체는
의도적으로 작게 유지한다(deny-by-default route/policy registry + role code).
관계 기반 access control은 이름 있는 trigger 뒤로 미룬다.**

*왜 중앙화하는가:* Google Zanzibar는 가장 큰 공개 운영 규모에서, 서비스별 임의 role
검사가 아니라 전용의 균일한 authorization service가 여러 product(Calendar, Cloud,
Drive, Maps, Photos, YouTube)의 정책을 일관되고 감사 가능하게 만든다는 점을 보였다.
Airbnb의 Himeji와 후속 오픈소스(SpiceDB, OpenFGA, Ory Keto)도 같은 결론을 따른다.
v1에서 “X가 Y를 할 수 있는가”를 한 곳이 답한다는 의미는 identity-platform이
`verifySession`, role model, traffic-auth policy registry를 소유하는 것이다. 다른
platform은 *질의*만 하고 policy를 fork하지 않는다. v1에서는 검증과 policy *data*
(session state, role model, policy registry)를 중앙화하며, 요청별 allow/deny 평가는
target service의 enforcement middleware에서 수행한다. caller가 identity-platform에
policy 평가를 요청해 allow/deny를 받는 완전한 decision-as-a-service는 후속 분리
단계의 범위이지 v1이 아니다.

*지금 ReBAC을 쓰지 않는 이유:* Zanzibar는 수십억 object에서 “이 사진이 viewer가
속한 group과 공유되었는가?” 같은 관계 질문에 답하기 위한 시스템이다. 현재
authorization 범위는 내부 직원이 가진 소수의 role code(`[A-Z0-9_]+`)와 서비스 간
policy 4개뿐이다. 여기에 relationship-tuple engine을 배치하는 것은 engineering이
아니라 Google 흉내이며 product-first 규칙을 위반한다.

*ReBAC 도입 trigger*(SpiceDB/OpenFGA급 engine을 만들지 말고 채택한다): (a) 개별
외부 user에게 listing/site 수준 권한을 주는 세밀한 object 공유 요구, 또는 (b) Dawneer
B2B tenant 관리자가 industrial complex/site별로 자신의 member 권한을 관리하는
multi-tenant delegation 요구다. 어느 하나라도 role code가 조합적으로 폭증하며
model을 바꿔야 한다는 신호가 된다. decision이 이미 identity-platform 계약 뒤에
중앙화되어 있으므로 이 교체는 consumer가 아니라 API 뒤의 engine만 바꾼다.

### 5. 분리 순서

계획의 일곱 단계를 구체화한다. 모든 단계에서 서비스 간 직접 DB 접근은 금지한다.
물리적인 repo 분리는 ADR-0048에 따라 *마지막*에 한다.

1. **Contract ADR** — 이 문서다. 이름, 표면, staging, trigger를 결정했으며 이후
   단계는 이를 구현하고 다시 결정하지 않는다.
2. **읽기 전용 계약을 alias로 발행한다.** legacy repo는 DB와 route를 유지한다.
   `/identity/v1/*` route, `identity.v1` OpenAPI, 후속 event 이름을 workforce 구현의
   alias로 추가하고 alias 사용 telemetry를 수집한다. `workforce.v1`는 완전히 동작시킨다.
3. **Service-identity policy 소유권을 이동한다.** 공유 consumer policy registry를
   versioned slice에서 `identity-platform.traffic_auth_policy_registry.v1`로 재소유하고,
   foundation Catalog policy와 product exposure registry는 shared policy를 복제하지
   않고 이를 참조한다.
4. **Product/staff 분리를 문서화한다.** Gongzzang B2C user와 product session은 명시적으로
   범위 밖이다(§1이 그 문서다). staff/admin account, service principal, 서비스 간
   permission은 identity-platform이 소유한다.
5. **DB/API migration을 별도 gate로 준비한다.** `workforce.*` → `identity.*` schema
   migration plan은 migration을 작성하기 전에 별도 owner 승인이 필요하다. active
   consumer가 강제할 때만 compatibility view나 dual-read를 사용한다.
6. **Consumer를 전환한다.** Catalog admin route는 identity-platform 계약 이름으로
   staff/session을 검증하고, gongzzang·dawneer·intelligence는 발행된 identity API와
   후속 event 이름을 소비한다.
7. **Legacy를 폐기한다.** 모든 consumer가 이동하고, test가 legacy와 최종 이름을 모두
   검증하며, alias telemetry가 legacy traffic 0을 보이고 rollback이 문서화된 뒤에만
   `workforce.v1` route·event 이름·pin을 제거한다.

identity-platform을 별도 repository/deployment로 물리 추출하는 일은 1–7단계가 안정화된
뒤에만, 별도의 repo-local ADR(ADR-0048 재평가 trigger)에 따라 수행한다.

## 범위 밖

- Gongzzang B2C user, product session, product auth 흐름을 이동하지 않는다(별도 ADR 필요).
- 새 IdP를 추가하지 않는다. Zitadel을 유지하며 이 ADR은 authentication technology를
  추가하지 않는다.
- 지금 ReBAC engine을 추가하지 않는다(§4 trigger 조건부). trigger가 발생하면 직접
  만들지 말고 채택한다.
- 지금 SPIFFE/SPIRE infrastructure를 추가하지 않는다(§3 3단계 trigger 조건부).
- 즉시 물리 repo 분리나 deployment 변경을 하지 않는다.
- 이 계약으로 Kafka나 Kubernetes 의무를 추가하지 않는다.
- 새 `x-identity-platform-*` header family를 추가하지 않는다(§2.2).
- 실제 cutover 단계가 배포될 때 필요한 범위를 넘어 새 CI guard나 registry를 추가하지
  않는다(product-first 규칙 3).

## 영향

긍정적 효과:

 - Identity에 명확한 owner와 versioned contract가 생긴 뒤 code를 이동하므로, 향후
   물리 추출은 redesign이 아니라 이미 발행된 API의 re-homing이 된다.
 - consumer(foundation Catalog admin, gongzzang, dawneer, intelligence)가 session
   검증, role 부여, principal lookup, service-auth policy를 제공하는 하나의 안정적인
   identity 표면을 얻는다. 어디서나 deny-by-default다.
 - audit 경계가 명확하다. 각 platform은 opaque principal reference를 보관하고
   identity-platform만 해석한다. `getStaffPrincipal`이 기존 패턴의 유일한 빈틈을 닫는다.
 - 향후 강화 경로(SPIFFE, RFC 8693, ReBAC)를 이름 있는 trigger로 미리 결정했으므로,
   압박 상황에서도 즉흥적으로 만들지 않고 계획적으로 업그레이드한다.

비용과 위험:

- alias 중복(workforce.v1 + identity.v1 route와 event 이름)을 cutover까지 telemetry,
  test와 함께 유지하고 이후 폐기해야 한다.
- 새 `getStaffPrincipal` read는 작지만 새로운 표면이다. deny-by-default route table에
  등록하고 구현 단계에서 test로 검증해야 한다.
- registry 재소유(§5 3단계)는 고정된 policy ID를 건드린다. 부주의하면 기존 consumer
  4개가 깨질 수 있으므로 versioned 단계 전까지 값은 고정한다.
- staff/session 검증을 너무 일찍 이동하면 Catalog admin 승인 경로가 깨질 수 있다.
  §5 순서가 이를 막는다.

## 재평가 조건

- **Kubernetes 도입 또는 배포 환경 2개 초과** → 3단계(SPIFFE/SPIRE workload identity,
  mTLS)를 구현한다(ADR-0046 trigger 참조).
- **staff 권한을 전달하는 첫 서비스 간 호출**(단순 audit reference 아님) → `act` claim을
  사용하는 RFC 8693 token exchange를 구현한다.
- **object별 공유 또는 multi-tenant delegation 요구** → 기존 identity-platform
  decision 계약 뒤에 Zanzibar 계열 engine(SpiceDB/OpenFGA급)을 채택한다.
- **identity-platform을 독립 배포할 수 있게 됨** → repo-local 물리 추출 ADR을 작성한다
  (ADR-0048에 따름).
- **두 번째 product에 staff용 admin UI가 필요함**(Dawneer workbench) → 최소 v1 표면을
  넘어 `identity.v1`에 staff listing/search가 필요한지 재평가한다.

## 참고 문서

- Pang et al., *Zanzibar: Google's Consistent, Global Authorization
  System*, USENIX ATC '19 —
  <https://www.usenix.org/conference/atc19/presentation/pang> ·
  <https://research.google/pubs/zanzibar-googles-consistent-global-authorization-system/>
  (Calendar, Cloud, Drive, Maps, Photos, YouTube를 제공하는 중앙 authorization과
  대규모 ReBAC의 근거).
- Airbnb Engineering, *Himeji: A Scalable Centralized System for
  Authorization at Airbnb* —
  <https://medium.com/airbnb-engineering/himeji-a-scalable-centralized-system-for-authorization-at-airbnb-341664924574>
  (ReBAC을 직접 만들지 말고 채택하라는 결론에 근거가 된 Himeji 중앙 authorization).
- AuthZed, *Google Zanzibar overview and lineage* —
  <https://authzed.com/learn/google-zanzibar> (SpiceDB/OpenFGA/Ory Keto 후속 생태계와
  Zanzibar 설계 계보; 직접 만들지 말고 채택).
- SPIFFE/SPIRE (CNCF graduation 2022) — production adopters (Uber,
  Netflix): <https://github.com/spiffe/spire/blob/main/ADOPTERS.md> ·
  <https://www.cncf.io/announcements/2022/09/20/spiffe-and-spire-projects-graduate-from-cloud-native-computing-foundation-incubator/>
  (static shared secret을 대체하는 단기 자동 교체 workload identity + mTLS; 우리의
  3단계 목표).
- RFC 8693, *OAuth 2.0 Token Exchange* —
  <https://www.rfc-editor.org/info/rfc8693/> ·
  <https://datatracker.ietf.org/doc/html/rfc8693> (`act` claim으로 audit trail을 보존하는
  delegation; trigger 조건부 delegation 해답).
- ADR-0048 — 수평 platform 재정의(이 ADR이 구현하는 소유권 배정).
