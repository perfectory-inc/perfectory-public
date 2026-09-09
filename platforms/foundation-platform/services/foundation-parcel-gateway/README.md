---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-09-09
---

# Foundation Parcel Gateway

비공개 lakehouse R2의 미리구운 필지 by-PNU JSON 객체를 서빙하는 Cloudflare module Worker다
(루트 ADR-0096). 브라우저는 PNU만 알고, 현재 서빙 세대는 R2 의 serving manifest 가 고정한다.

## 계약과 응답

[`r2-connections.contract.json`](../../config/r2-connections.contract.json)의
`parcel_by_pnu_gateway`가 Worker/binding 이름, bucket 연결, 요청 경로 prefix, 키 root·세대
디렉터리·PNU pattern·suffix, manifest 주소, CORS 문법, content type과 cache 정책의 유일한
정의다. `wrangler.jsonc`는 `config:render`가 만드는 투영이므로 직접 고치지 않는다.

- 허용: `GET`, `HEAD`, `OPTIONS`와 정확한 `/parcels/by-pnu/{pnu}` (PNU 19자리, 11번째
  자리는 대장 구분 `[1289]`)
- 세대 해석: manifest(`serving/parcels/by-pnu/manifest.json`)를 읽어
  `v{current_generation}` 객체를 서빙한다. manifest 해석 결과는 엣지에
  `manifest_edge_cache_seconds` 동안 캐시된다 → 세대 교체 가시화는 그 안에 이뤄진다.
- **manifest 를 신뢰할 수 없으면 503** (부재·비파싱·다른 unit/schema/세대 0): 포인터 장애는
  레인 전체의 장애이지 4천만 필지의 가짜 404가 아니다. 503은 `no-store`로 캐시되지 않는다.
- 거부: query, 원시 객체 키, 다른 prefix, traversal, 비정규 PNU는 404; 다른 method는 405;
  미허용 Origin은 403
- 캐시: 객체는 `public, max-age=3600` — 같은 세대 안에서 델타 재굽기가 제자리 덮어쓰기를
  하므로 immutable이 아니고, 신선도는 1시간으로 유계다
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

`verify:local`은 OS 임시 디렉터리의 Wrangler local R2에 manifest(세대 2)와 v2/v1 객체,
Bronze 모양 객체를 넣고 `wrangler dev --local`을 실행한다. manifest 가 가리키는 세대의
바이트가 서빙되는지, 그리고 GET/304/404/405/403을 실제 HTTP로 검증한 뒤 프로세스 트리와
임시 상태를 제거한다. 원격 R2나 deploy 명령을 호출하지 않는다.

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
6. Worker **Settings > Domains & Routes > Add > Custom Domain**에 전용 catalog hostname을
   붙인다. Worker가 origin이 되는 Custom Domain이며 R2 bucket custom domain과 다르다.
   Cache API가 `workers.dev`에서 보장되지 않으므로 생성 구성은 `workers_dev: false`로 그
   경로를 닫는다.
7. 배포 전에 `export-parcel-by-pnu-serving`과 `publish-parcel-by-pnu-serving-manifest`로
   서빙 세대가 실제로 구워져 있고 manifest 가 그 세대를 가리키는지 확인한다 — manifest 가
   없으면 Worker는 설계대로 503만 낸다.

배포와 Dashboard 변경은 이 코드 작업의 범위 밖이다.
