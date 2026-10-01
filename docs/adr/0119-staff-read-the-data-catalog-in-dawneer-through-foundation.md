# ADR 0119: 직원은 데이터 카탈로그를 더니어에서 읽고, 더니어는 Foundation 을 거쳐 DataHub 에 닿는다

- Status: Accepted
- Date: 2026-10-01
- Refines: [ADR-0117](./0117-metadata-has-one-contract-per-dataset-and-one-place-to-look.md) §3·§4(사람이 보는 곳 하나)
- Builds on: [ADR-0114](./0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md)(더니어는 틀만, 판단은 플랫폼),
  [ADR-0116](./0116-dawneer-keeps-staff-tokens-on-its-rust-server.md)(더니어 BFF·허용 목록 중계)

## Context

ADR-0117 은 사람이 보는 곳을 DataHub 하나로 정하고 DataHub 화면에 Zitadel 로그인을 붙였다. 2026-10-01 실사용에서 두
문제가 드러났다.

- **로그아웃이 따로 논다.** 더니어에서 로그아웃하면 더니어와 Zitadel 세션은 끝나지만 DataHub 는 자기 세션 쿠키로
  계속 열린다. Zitadel 은 OIDC back-channel logout 을 지원하지만, DataHub 는 그 알림을 받는 기능을 문서에 두지 않았고
  OIDC 로그아웃 문제가 이슈로 남아 있다(datahub-project/datahub#8369).
- **쓸 수 없는 로그인 화면이 보인다.** `AUTH_JAAS_ENABLED=false` 로 아이디·비밀번호 경로는 꺼져 있지만 React 앱의
  `/login` 은 여전히 그 입력칸을 그린다. 숨기는 설정은 없다.
- 사용자 결정(2026-10-01): DataHub 자체 화면 대신 우리 화면(더니어)으로 한다.

## Decision

1. **직원이 보는 곳은 더니어다.** 데이터 카탈로그 메뉴가 검색 → 상세(설명·칸) → 앞뒤 흐름(칸을 누르면 그 칸으로
   옮겨 감)을 보인다. DataHub 의 웹 화면은 직원 메뉴에 없고 플랫폼 관리자만 SSH 터널로 쓴다. 로그인은 더니어
   하나라서 로그아웃도 하나다.

2. **판단은 Foundation 이 한다(ADR-0114 §3).** Foundation API 가 `GET /data-catalog/v1/search` 와
   `GET /data-catalog/v1/entity?urn=` 를 낸다. 둘 다 직원 전용이고 신원 플랫폼의 `foundation.metadata:read` 로
   인가한다. 이 권한은 데이터를 다루는 모든 역할과 새 역할 `DATA_CATALOG_READER` 가 갖는다. Foundation 은 정해진
   질문만 DataHub 에 묻고 자기 타입(`foundation-contracts::data_catalog`)으로 답한다. 화면은 DataHub 의 GraphQL 스키마에
   묶이지 않는다. 계약 문서는 `docs/openapi/data-catalog.v1.json` 이다.

3. **더니어는 중계만 한다.** `/api/data-catalog/<path>` 는 허용 목록(`search`, `entity`, GET 만)에 있는 것만
   `<foundation>/data-catalog/v1/<path>` 로 세션의 토큰과 함께 넘긴다. GraphQL 통과 경로는 없다.

4. **DataHub GMS 는 공유 네트워크 `metadata-shared` 에서 `datahub-gms` 로만 닿는다.** Foundation 런타임과 DataHub
   실행 스크립트가 둘 다 이 네트워크를 만든다(신원의 `identity-shared` 와 같은 방식). DataHub 가 없으면 데이터
   카탈로그 경로만 503 이다.

5. **남은 보호 한 겹.** GMS 는 아직 토큰 인증 없이 돈다. 막는 것은 네트워크(공유 네트워크 + 루프백 포트)뿐이다.
   운영 비밀값 디렉터리는 root 소유라 토큰을 놓으려면 운영자가 한 번 손대야 한다. 그때 `METADATA_SERVICE_AUTH_ENABLED`
   를 켜고, Foundation 이 그 토큰으로 묻게 하며, GMS 의 루프백 포트를 닫는다.

6. **선언 흐름 화면은 걷어 냈다.** 더니어의 "데이터 흐름" 메뉴(한 장에 112칸)는 데이터 카탈로그 메뉴로 바뀌었다.
   같은 선언이 DataHub 에 들어 있기 때문이다(데이터셋 112개, 설명·선언 상태 포함). Foundation 의
   `/catalog/v1/pipeline-graph` 는 ADR-0117 §8 의 조건(실행 기록)대로 남는다.

## Consequences

- 검색·상세·흐름 화면을 우리가 만든다. 칸 단위 흐름·실행 기록·품질 점수 화면은 그 데이터가 들어오는 대로 같은
  자리에 붙인다(ADR-0118).
- DataHub 의 관리 기능(수집 설정 등)은 플랫폼 관리자만 DataHub 화면에서 쓴다.
- 출처: [DataHub OIDC 설정](https://docs.datahub.com/docs/authentication/guides/sso/configure-oidc-react),
  [DataHub #8369](https://github.com/datahub-project/datahub/issues/8369),
  [Zitadel back-channel logout](https://zitadel.com/docs/guides/integrate/back-channel-logout).
