---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-11-05
---

# 파운데이션 프로필 게이트웨이

비공개 레이크하우스 R2 에서 정본 산업단지 Gold 프로필 하나만 읽어 주는 Cloudflare 모듈 Worker 다.
저장 위치는 데이터·키 계약을 소유한 영역을 따르며, 제품은 공개 HTTP 주소만 소비한다.

## 계약과 응답

[`r2-connections.contract.json`](../../config/r2-connections.contract.json)의 `profile_gateway`가
Worker·바인딩 이름, 버킷 연결, 키 뿌리·UUID 패턴·접미사, CORS 문법, 콘텐츠 형식과 캐시
정책의 유일한 정의다. `wrangler.jsonc`는 `config:render`가 만드는 투영이므로 손으로 고치지 않는다.

- 허용: `GET`, `HEAD`, `OPTIONS`와 정확한
  `/gold/industrial-complex/profiles/{artifact_id}.json`
- 거부: 질의 문자열, 다른 접두어·경로 조각, 상위 경로 거슬러 오르기는 404; 다른 메서드는 405; 허용되지 않은 출처는 403
- 캐시: 계약의 `cache_control`(불변); 객체는 새로 만들기만 하며 새 판본은 새 산출물 주소
- 조건부 요청: R2·캐시 API 의 ETag 와 `If-None-Match` 판단을 쓰고 일치하면 304
- 권한: R2 바인딩 하나의 `get`만 타입에 드러남; 목록·쓰기와 S3 자격 증명 없음

## 로컬 검증

Node·pnpm 버전은 [`tools/technology-versions.contract.json`](../../../../tools/technology-versions.contract.json)이
정본이고 `package.json`의 `engines`·`packageManager`가 그 값을 따른다. corepack이 `packageManager` 핀을 고른다.

```bash
corepack pnpm install --frozen-lockfile
corepack pnpm run config:check
corepack pnpm run lint
corepack pnpm run typecheck
corepack pnpm test
corepack pnpm run build:check
corepack pnpm run verify:local
```

`verify:local`은 OS 임시 디렉터리의 Wrangler 로컬 R2 에 정본 프로필과 Bronze 모양 객체를
넣고 `wrangler dev --local`을 실행한다. GET/304/404/405/403 을 실제 HTTP 로 검증한 뒤 60초 안에
프로세스 트리와 임시 상태를 지운다. 원격 R2 나 배포 명령을 부르지 않는다.

## Cloudflare 운영 연결

1. **R2 > Overview > foundation-platform-lakehouse-prod > Settings**에서 공개 접근이 꺼져
   있음을 확인한다. R2 사용자 도메인은 연결하지 않는다.
2. **Workers & Pages > Create application > Create Worker**에서 계약에 적힌 이름의 Worker 를 만든다.
3. Worker **Settings > Bindings > Add > R2 bucket**에서 계약에 적힌 이름의 바인딩 하나를 기존
   레이크하우스 버킷에 연결한다.
4. **Settings > Variables and Secrets**에서 계약이 가리키는 CORS 변수에 쉼표로 구분한 앱 출처를
   넣는다. `*`는 금지다. 생성 구성의 `keep_vars: true`가 저장소에 실제 출처를 복제하지 않고
   대시보드 값을 다음 Wrangler 배포에서도 지킨다.
5. 존 **Rules > Settings > URL Normalization**에서 RFC-3986 들어오는 주소 정규화를 켠다.
6. Worker **Settings > Domains & Routes > Add > Custom Domain**에 전용 프로필 호스트 이름을 붙인다.
   Worker 가 원점이 되는 사용자 도메인이며 R2 버킷 사용자 도메인과 다르다. 캐시 API 가
   `workers.dev`에서 보장되지 않으므로 생성 구성은 `workers_dev: false`로 그 경로를 닫는다.
7. 배포 뒤 로컬 소비자 `.env.local`의
   `FOUNDATION_PLATFORM_R2_LAKEHOUSE_PUBLIC_BASE_URL`에 Worker HTTPS 주소와 `/{object_key}` 틀을
   넣는다. `FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS`는 따로 두는 앱 출처 목록이다. 값은 저장소나
   작업 보고에 기록하지 않는다.

배포와 대시보드 변경은 이 코드 작업의 범위 밖이다. 연결이 끝난 뒤의 코드 배포는 데이터 호스트가 main 을 배포한
뒤 스스로 한다(루트 ADR-0175, [런북](../../docs/runbooks/worker-autodeploy.md)).
