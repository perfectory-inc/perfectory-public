---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-26
---

# ADR 0058: Foundation이 소유한 환경 이름은 소유자 계약 하나에서 나온다

- Status: Accepted
- Date: 2026-08-26
- Amends: ADR-0001 §11, ADR-0055 §4
- Temporarily narrows: Foundation ADR-0021 이름 규칙

## 배경

루트 ADR-0001은 서비스가 소유한 환경변수에 영역 접두어를 요구하지만 예외와 전환 규칙을
기계가 읽을 수 있는 곳에 두지 않았다. 그 결과 Foundation이 읽는 VWorld 자격증명은
`FOUNDATION_PLATFORM_VWORLD_*` 설정과 `VWORLD_*` 자격증명으로 갈라졌고, 비공개 R2 Gold
프로필 Worker의 R2 binding은 같은 계약 안의 CORS binding과 달리 `LAKEHOUSE`라는 소유자 없는
이름을 썼다. `LAKEHOUSE`만으로는 어느 플랫폼의 어떤 연결인지 알 수 없고 Gongzzang과 Foundation은
서로 다른 lakehouse 버킷을 소유한다.

문제는 철자 하나가 아니라 소유권 규칙, 도구가 강제하는 예외, 구 이름의 제거 조건이 서로 다른
파일에 암묵적으로 흩어진 구조다. 특히 실제 자격증명은 gitignored `.env.local`에 있으므로 추적
파일만 일괄 변경하면 사용자의 로컬 실행을 예고 없이 끊는다. 반대로 alias를 영구 허용하면 두 이름이
계속 같은 의미를 소유한다.

공식 1차 자료는 도구가 직접 읽는 이름은 우리 접두어로 바꿀 수 없음을 확인한다. SQLx CLI는
`DATABASE_URL`을, sccache는 `SCCACHE_*` 설정을, Wrangler는 `CLOUDFLARE_*` 시스템 변수를,
GitHub CLI는 `GH_TOKEN`을 직접 읽는다. sccache가 `SCCACHE_MEMCACHED`를 deprecated alias로
문서화한 사례처럼 이름 전환은 canonical 우선과 한시적 alias를 함께 둘 수 있다. 반면 Worker R2
binding은 애플리케이션이 정하는 식별자이며, Cloudflare의 typed `R2Bucket` binding이 저장소 종류를
이미 표현한다.

## 근본 원인과 불변식

근본 원인은 “우리 런타임이 소유한 이름”과 “외부 도구가 정한 입력 이름”을 구분하는 SSOT가 없었던
것이다. 다음 불변식을 적용한다.

1. 이 결정 범위의 Foundation 소유 이름은 Foundation 소유자를 드러낸다. 다른 영역의 namespace는
   각 영역 계약과 별도 마이그레이션 없이는 이 계약이 선언하지 않는다.
2. 같은 의미의 canonical 이름은 하나뿐이며 alias는 제거 조건을 가진 전환 입력일 뿐이다.
3. typed binding 이름은 소유자와 목적을 담고, 타입이 이미 보장한 저장소 종류를 반복하지 않는다.
4. 외부 도구가 직접 읽는 이름만 근거 URL과 함께 예외가 된다.
5. 자격증명 값은 경고·로그·가드 출력에 나타나지 않는다.

## 결정

1. [`platforms/foundation-platform/config/environment-variable-naming.contract.json`](../../platforms/foundation-platform/config/environment-variable-naming.contract.json)을
   Foundation 소유 namespace, 이 변경에서 확인한 외부 도구 예외, 환경변수 이름 전환의 SSOT로 둔다.
   Identity·Intelligence·Gongzzang namespace는 이 Foundation 소유 계약의 범위 밖이다. typed binding의 실제 이름은
   기존 R2 연결 계약만 소유하고 이 계약의 namespace 규칙을 따른다. 이름 계약은 실제
   소비자와 Foundation Docker build context가 함께 소유하는 Foundation `config/`에 둔다. 새
   라이브러리는 만들지 않는다. Rust는 `include_str!`와 이미 채택한 `serde_json`, Python과 가드는
   표준 라이브러리로 같은 JSON을 직접 소비한다.
2. Foundation 프로필 gateway의 R2 binding은 `FOUNDATION_PLATFORM_LAKEHOUSE`다.
   `r2-connections.contract.json#profile_gateway.r2_binding`만 수정하면 Wrangler 렌더러, Worker,
   Miniflare, Rust deploy contract가 같은 값을 읽는다. `R2_`를 넣지 않는다. `R2Bucket` 타입이 R2를
   이미 보장하고 이름에는 Foundation 소유권과 lakehouse 목적만 남겨야 하기 때문이다.
3. VWorld canonical 자격증명 이름은 `FOUNDATION_PLATFORM_VWORLD_API_KEY`,
   `FOUNDATION_PLATFORM_VWORLD_DOMAIN`, `FOUNDATION_PLATFORM_VWORLD_USERNAME`,
   `FOUNDATION_PLATFORM_VWORLD_PASSWORD` 네 개다. tracked example은 이 이름만 선언한다.
4. 호환 기간에는 `VWORLD_API_KEY`, `VWORLD_DOMAIN`, `VWORLD_USERNAME`, `VWORLD_PASSWORD`와 기존
   dataset 전용 사용자명·비밀번호 이름을 입력 alias로 받는다. canonical 값이 먼저이며, alias가 실제
   값을 공급할 때만 alias와 replacement **이름만** 경고한다. map을 자식 프로세스에 넘기기 전에는
   canonical로 정규화하고 alias를 제거한다. 이는 추적할 수 없는 로컬 비밀 이름을 한 번 옮기기 위한
   Foundation ADR-0021의 한시적 예외다. tracked example과 비공개 운영 프로필이 canonical로 옮겨지고
   실행 경고가 사라진 뒤 별도 변경에서 alias를 제거해 최종 이름만 남긴다.
5. 외부 도구 예외는 계약의 `external_tool_exceptions` 네 항목이다. `DATABASE_URL`은 SQLx,
   `SCCACHE_*`는 sccache, `CLOUDFLARE_*`는 Wrangler, `GH_TOKEN`은 GitHub CLI가 직접 읽으므로
   바꾸지 않는다. 가드는 이 목록을 입력으로 읽고 항목·소비자·공식 근거가 빠지면 실패한다.
6. `environment-variable-naming.sh`는 전체 tracked Rust·Python·shell·TS/JS·JSON/YAML·SQL 입력에서
   이 VWorld 이름의 직접 소비를 찾고 두 호환 어댑터 밖이면 실패한다. 또한 R2 계약과 렌더링된
   Wrangler binding, 두 example, 외부 도구 예외를 검증한다. 모든 제3자 환경변수의 의미를 판정한다고
   주장하지 않는다. 그런 범용 lexical lint는 `DATA_GO_KR_SERVICE_KEY` 같은 공급자 입력까지 우리
   소유 이름으로 오인하며 ADR-0027의 거짓 양성 0건 원칙을 위반한다.
7. 이 Worker는 아직 배포되지 않았다. 따라서 binding 이름을 지금 바꾸면 원격 binding을 재생성하거나
   호환 배포를 운영할 필요가 없다. 첫 배포부터 canonical binding 하나만 만든다.

## 기각한 대안

- **구 이름을 즉시 거부:** `.env.local`이 추적되지 않아 사용자 자격증명을 자동 이전할 수 없고 로컬
  실행을 무경고로 끊는다.
- **구 이름을 영구 허용:** 같은 지식의 두 이름을 영구 계약으로 만들어 SSOT와 제거 가능성을 없앤다.
- **binding 이름에 `R2` segment를 추가:** typed `R2Bucket`이 이미 가진 정보를 반복하고 CORS 같은
  다른 binding과 다른 문법을 만든다.
- **모든 대문자 token을 접두어 lint:** 외부 도구·공급자 입력과 내부 소유 이름을 syntax만으로 구별할
  수 없어 거짓 양성을 만든다.
- **새 환경설정 프레임워크 도입:** 네 자격증명의 alias 전환을 위해 데이터 플레인이나 의존성을 추가할
  이유가 없다. 기존 JSON/serde/logging 경계면이면 충분하다.

## 결과

Worker binding과 VWorld 자격증명은 소유자가 드러나는 이름 하나로 수렴한다. 사용자는 비밀값을
출력하거나 커밋하지 않고 `.env.local`의 왼쪽 이름만 바꿀 수 있으며, 누락한 프로필은 실행 경고로
발견된다. alias·예외·binding은 가드 입력인 단일 계약에서 함께 검토되고, 새로운 직접 소비는 CI 전에
차단된다.

## 참고

- [SQLx CLI `DATABASE_URL`](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md)
- [sccache 환경설정과 deprecated alias](https://github.com/mozilla/sccache/blob/main/docs/Configuration.md)
- [Wrangler 시스템 환경변수](https://developers.cloudflare.com/workers/wrangler/system-environment-variables/)
- [GitHub CLI 환경변수](https://cli.github.com/manual/gh_help_environment)
- [Cloudflare Workers R2 binding](https://developers.cloudflare.com/r2/api/workers/workers-api-usage/)
