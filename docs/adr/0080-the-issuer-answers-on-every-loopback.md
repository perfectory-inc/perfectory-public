# ADR 0080: 발급자는 모든 루프백에서 응답한다 — 신원 운영 기동은 루프백 issuer 로 시작한다

- Status: Accepted
- Date: 2026-09-04

## Context

운영 foundation-api 의 신원 설정 세 칸(`IDENTITY_API_BASE_URL`, `ZITADEL_ISSUER_URL`,
`FOUNDATION_PLATFORM_ZITADEL_AUDIENCE`)은 자리표시자(`https://identity.example.test`)다.
보호 라우트 17개(카탈로그 필지·건물·호 읽기 포함)는 어떤 토큰으로도 통과할 수 없고,
브라우저 데모는 지도까지만 열리고 데이터 패널에서 멈춘다. 실측한 제약:

- foundation-api 는 두 신원 URL 을 `parse_secure_endpoint_url` 로 검증한다
  (`identity_token_verifier.rs:255`): **https 이거나, http 면 host 가 localhost/루프백 IP 여야
  한다.** JWKS 주소는 issuer 와 same-origin 이어야 한다.
- 토큰은 필수 클레임 `principal_kind` 를 실어야 한다. 이 클레임을 만드는 검증된 방법은
  Zitadel Action 뿐이다 (`scripts/smoke/identity-foundation-signed-oidc.sh` 의
  `principalKind` action — machine 사용자는 `service`, 그 외 `staff`).
- audience 는 Zitadel **프로젝트 ID** 다 (스모크 873-874·1060-1061행; client_credentials 에
  `urn:zitadel:iam:org:project:id:<projectId>:aud` 스코프).
- identity-platform 은 compose 한 벌(부트스트랩 5단계 사슬 + identity-api)을 갖고 있으나
  서버 배치 기반이 없고, 서버(ai-server)에는 cloudflared 도 공개 https 통로도 없다.
- 저장소에서 이 배선이 실제로 allow/deny 까지 통과한 유일한 실행은 위 스모크이며, 그
  방식은 "issuer 를 루프백 주소로 두고, 소비자마다 자기 네임스페이스의 루프백에 릴레이를
  둔다"이다.

공개 https(예: Cloudflare 터널 뒤의 실도메인) 는 웹 실배포와 묶이는 별도 결정이고, 지금
그 통로를 만들면 신원 기동이 도메인·터널 결정에 인질로 잡힌다.

## Decision

1. **issuer 는 `http://127.0.0.1:18453` 하나다.** Zitadel 은 ai-server 에서
   `ZITADEL_EXTERNALDOMAIN=127.0.0.1`, `ZITADEL_EXTERNALPORT=18453` 으로 뜨고, 이 문자열이
   토큰의 `iss` 이자 모든 소비자의 설정값이다. 소비자는 각자 자기 루프백 18453 에서 같은
   Zitadel 에 닿는다: 호스트는 포트 공개로, 컨테이너는 socat 사이드카로, 노트북은
   `ssh -L 18453:127.0.0.1:18453` 으로. 이 성질 덕에 같은 설정값이 어느 자리에서나 참이다.
2. **컨테이너 소비자의 루프백은 compose 가 소유한 socat 사이드카가 만든다.**
   사이드카는 `network_mode: "service:<소비자>"` 로 소비자의 네임스페이스를 빌리므로,
   소비자가 재생성되면 compose 가 사이드카도 함께 재생성한다. 릴레이는 인자만으로 도는
   socat(digest 고정)이며 스크립트 파일을 나르지 않는다 — 두 서브트리에 같은 파일을
   복사하는 거울을 만들지 않기 위해서다.
3. **세 compose 프로젝트가 외부 공유망 `identity-shared` 로 만난다.** Zitadel 프로젝트
   (`platforms/identity-platform/infra/zitadel/`), identity 프로젝트(기존 compose +
   `compose.server.yml` 오버레이, identity-api 호스트 공개 127.0.0.1:18082), foundation
   프로젝트(`compose.identity-bridge.yml` 오버레이가 foundation-api 를 공유망에 붙이고
   사이드카 둘 — 18453→zitadel, 18082→identity-api — 을 둔다). 오버레이는
   `foundation-runtime.sh` 의 `-f` 목록에 상시 편입한다: 신원 스택이 꺼져 있으면 보호
   라우트는 지금과 같은 503 으로 수렴할 뿐, 나머지 런타임은 영향이 없다.
4. **`principal_kind` 는 Zitadel Action 으로 주입하고, audience 는 프로젝트 ID 다.**
   기동 절차는 스모크가 아니라 신규 런북
   (`platforms/identity-platform/docs/runbooks/production-bringup-on-a-lan-host.md`)이
   정본으로 소유한다.
5. **identity-policy-worker 는 기동하지 않는다.** 필수 env `IDENTITY_POLICY_EVENT_ENDPOINT`
   가 가리킬 수신자가 아직 없다(ADR-0079 의 구독자 0 과 같은 상태). 수신자가 명명되는
   결정과 함께 기동한다.
6. **공개 https issuer 로의 승격은 후속 결정이다.** 웹 실배포가 공개 도메인을 정할 때
   issuer/base URL 교체 + Zitadel ExternalDomain 변경 + 토큰 재발급으로 이행하며, 이 ADR 은
   그 시점까지의 LAN 단계만 결정한다. 이 구성은 어떤 의미로도 공개 서비스 준비 완료가
   아니다.

## Consequences

- 보호 라우트 17개가 처음으로 실토큰으로 열린다. 데모 검증 경로: 서버에서 기계 JWT 발급
  → `GET /catalog/v1/parcels/by-pnu/{pnu}` 200.
- foundation-api 컨테이너에 네트워크 한 개와 사이드카 둘이 추가된다. 신원 스택 부재 시
  사이드카는 접속 실패만 중계하고, 보호 라우트는 503(오늘과 동일 수렴값)이다.
- identity-api 의 기계 주체는 provisioner 바인딩 파일로 등록해야 한다
  (`identity.service_principal` 조회가 검증 경로에 있다). 파일은 서버
  `/etc/identity-platform/` 에 상주하고 런북이 스키마를 가리킨다.
- 노트북 데모는 로컬 Zitadel 을 서버 Zitadel 터널로 교체한다(포트 18453 은 로컬 데모의
  18443 과 충돌하지 않도록 고른 값이다).
- 남는 부채: 정책 워커 미기동(§5), 공개 issuer 승격(§6), identity 배치의 릴리스 스크립트
  부재(아카이브 + compose 래퍼로 시작).
