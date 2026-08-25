---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-08-25
---

# Foundation Profile Gateway

비공개 lakehouse R2에서 정본 산업단지 Gold 프로필 하나만 읽어 주는 Cloudflare module Worker다.
저장 위치는 데이터/키 계약 소유 영역을 따르며, 제품은 공개 HTTP URL만 소비한다.

## 계약과 응답

[`r2-connections.contract.json`](../../config/r2-connections.contract.json)의 `profile_gateway`가
Worker/binding 이름, bucket 연결, 키 root·UUID pattern·suffix, CORS 문법, content type과 cache
정책의 유일한 정의다. `wrangler.jsonc`는 `config:render`가 만드는 투영이므로 직접 고치지 않는다.

- 허용: `GET`, `HEAD`, `OPTIONS`와 정확한
  `/gold/industrial-complex/profiles/{artifact_id}.json`
- 거부: query, 다른 prefix/segment, traversal은 404; 다른 method는 405; 미허용 Origin은 403
- 캐시: `public, max-age=31536000, immutable`; 객체는 create-only이며 새 판본은 새 artifact URL
- 조건부 요청: R2/Cache API의 ETag와 `If-None-Match` 판단을 사용하고 일치하면 304
- 권한: R2 binding 하나의 `get`만 타입에 노출; list/write와 S3 자격증명 없음

## 로컬 검증

Node 20.19.0과 pnpm 9.12.0이 정본이다.

```bash
corepack pnpm@9.12.0 install --frozen-lockfile
corepack pnpm@9.12.0 run config:check
corepack pnpm@9.12.0 run typecheck
corepack pnpm@9.12.0 test
corepack pnpm@9.12.0 run build:check
corepack pnpm@9.12.0 run verify:local
```

`verify:local`은 OS 임시 디렉터리의 Wrangler local R2에 canonical 프로필과 Bronze 모양 객체를
넣고 `wrangler dev --local`을 실행한다. GET/304/404/405/403을 실제 HTTP로 검증한 뒤 60초 안에
프로세스 트리와 임시 상태를 제거한다. 원격 R2나 deploy 명령을 호출하지 않는다.

## Cloudflare 운영 연결

1. **R2 > Overview > foundation-platform-lakehouse-prod > Settings**에서 Public Access가 꺼져
   있음을 확인한다. R2 custom domain은 연결하지 않는다.
2. **Workers & Pages > Create application > Create Worker**에서 계약 이름의 Worker를 만든다.
3. Worker **Settings > Bindings > Add > R2 bucket**에서 계약 이름의 binding 하나를 기존
   lakehouse bucket에 연결한다.
4. **Settings > Variables and Secrets**에서 계약이 가리키는 CORS 변수에 쉼표 구분 앱 origin을
   넣는다. `*`는 금지다. 생성 구성의 `keep_vars: true`가 저장소에 실제 origin을 복제하지 않고
   Dashboard 값을 다음 Wrangler 배포에서도 보존한다.
5. zone **Rules > Settings > URL Normalization**에서 RFC-3986 incoming normalization을 켠다.
6. Worker **Settings > Domains & Routes > Add > Custom Domain**에 전용 프로필 hostname을 붙인다.
   Worker가 origin이 되는 Custom Domain이며 R2 bucket custom domain과 다르다. Cache API가
   `workers.dev`에서 보장되지 않으므로 생성 구성은 `workers_dev: false`로 그 경로를 닫는다.
7. 배포 후 로컬 소비자 `.env.local`의
   `FOUNDATION_PLATFORM_R2_LAKEHOUSE_PUBLIC_BASE_URL`에 Worker HTTPS URL과 `/{object_key}` template을
   넣는다. `FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS`는 별도의 앱 origin 목록이다. 값은 저장소나
   작업 보고에 기록하지 않는다.

배포와 Dashboard 변경은 이 코드 작업의 범위 밖이다.
