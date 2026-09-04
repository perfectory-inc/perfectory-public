# ADR 0081: 신원 엔드포인트는 계약 하나에서 파생된다

- Status: Accepted
- Date: 2026-09-05

## Context

ADR-0080 기동 직후의 실측: 발급자 포트가 compose 파일 3곳·서버 env 2곳에 리터럴로
살았고, Zitadel 내부 구성(프로젝트·principal_kind 액션·기계 사용자 4)은 손 API 호출로
만든 상태였다. 같은 밤 사이드카 하나가 소비자가 보는 자리에 없다는 이유만으로 readyz
200 뒤에서 판정 전부가 500이었다 — 상수의 사본과 손 구성이 남긴 표면적의 대가다.
기계 사용자 목록은 provisioner 의 정책 산출물(`workload-principal-policy.v1.json`)이
이미 소유하고 있으므로, 어디에도 두 번째 목록이 생겨서는 안 된다.

## Decision

1. **결정 상수의 정본은
   `platforms/identity-platform/config/identity-runtime-endpoints.contract.json` 하나다**
   (발급자 포트·별칭, 신원 API 포트·컨테이너 포트·별칭, 공유망 이름).
2. **compose 는 리터럴을 싣지 않는다.** 신원 쪽 래퍼 둘은 계약에서 포트를 도출해
   compose 에 필수 변수로 넘긴다. foundation 쪽 사이드카 포트는 운영 env 파일의
   `ZITADEL_ISSUER_URL`/`IDENTITY_API_BASE_URL` 에서 **파생**한다 — 청취 위치가 항상
   운영 URL 이 가리키는 곳과 같아지는 성질이 목적이다. foundation 서브트리가 신원
   계약을 실을 수 없어 남는 기본값·별칭 사본은
   `identity-endpoints-match-the-contract` 가드가 계약과 대조한다(위반 4종을 심어
   거부를 증명하는 자기시험 포함).
3. **Zitadel 내부 구성은 멱등 코드다.** `infra/zitadel/configure-zitadel.sh` 가
   프로젝트·액션·플로 연결·기계 사용자를 "없으면 만들고 있으면 통과"로 보장한다.
   기계 사용자 목록은 정책 산출물에서 읽고, 액션 본문은
   `infra/zitadel/actions/principal-kind.js` 한 파일이 소유한다(서명 OIDC 스모크의
   인라인 사본은 제거되어 같은 파일을 읽는다). `--emit-bindings` 는 실측 subject 로
   바인딩 문서를 생성한다 — 손으로 쓰는 subject 는 더 없다.
4. 비밀 발급(클라이언트 시크릿·PAT)은 여전히 운영자 행위다 — 코드가 만들지 않고,
   스크립트는 값을 출력하지 않는다.

## Consequences

- 포트를 바꾸는 길은 계약 수정 + env URL 수정 하나로 좁아지고, 어긋난 사본은 병합
  전(가드) 아니면 접속 거부(런타임)로 시끄럽게 죽는다.
- 빈 서버 재현이 런북의 손 API 절차 대신 스크립트 실행이 된다. 멱등성은 이미 구성된
  운영 서버에 두 번 실행해 전 행 `exists` 로 실증한다.
- 남는 부채: staff(사람) 경로 운영 검증, 공개 https issuer(ADR-0080 §6), 데모
  워크로드 토큰 회전 자동화.
