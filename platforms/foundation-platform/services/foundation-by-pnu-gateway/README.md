---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-10-08
---

# Foundation By-PNU Gateway

비공개 lakehouse R2의 미리 구운 by-PNU 문서(필지 상세, 건물·층·호)를 서빙하는 Cloudflare module
Worker다(루트 ADR-0096, ADR-0100). 브라우저는 PNU만 알고, 현재 서빙 세대는 R2 의 serving manifest 가 고정한다.

**한 소스, 두 레인**(루트 ADR-0160): 필지 Worker(`foundation-parcel-gateway`, `catalog.perfectory.io`)와
건물 Worker(`foundation-building-gateway`, `buildings.perfectory.io`)는 이 소스 하나를 레인만 바꿔 묶는다.
레인은 묶을 때 정해진다(`src/lane.ts`, Wrangler 의 `define`). 배포는 레인의 설정 파일로 한다:
`wrangler deploy -c wrangler.parcel.jsonc` / `-c wrangler.building.jsonc`. 시험은 레인마다 따로 돈다
(`vitest.config.ts` 의 두 project; `parcel-*.test.ts` 가 필지 레인). 레인의 차이는 코드가 아니라 계약
블록(`parcel_by_pnu_gateway`, `building_by_pnu_gateway`)에만 있다.

## 계약과 응답

[`r2-connections.contract.json`](../../config/r2-connections.contract.json)의
`building_by_pnu_gateway`가 Worker/binding 이름, bucket 연결, 요청 경로 prefix, 키 root·세대
디렉터리·PNU pattern·suffix, manifest 주소, CORS 문법, content type과 cache 정책의 유일한
정의다. `wrangler.building.jsonc`·`wrangler.parcel.jsonc`는 `config:render`가 레인마다 만드는 투영이므로 직접 고치지 않는다.

- 허용: `GET`, `HEAD`, `OPTIONS`와 정확한 `/buildings/by-pnu/{pnu}` (PNU 19자리, 11번째
  자리는 대장 구분 `[1289]`)
- 세대 해석(루트 ADR-0141): manifest(`serving/buildings/by-pnu/manifest.json`)는 기본 세대와 패치 목록(최신이
  앞)을 싣는다. 응답은 가장 최신 패치부터 기본 세대까지 처음 찾은 객체다. 패치의 `prefixes`(manifest 가 스스로 적은 `pnu_prefix_length` 자리의 PNU 앞자리)에 없는
  PNU 는 그 패치를 읽지 않는다. 툼스톤(`deleted: true`)을 만나면 더 내려가지 않고 `{"error":"deleted"}` 404
  (`no-store`)를 낸다. v1 manifest(`current_generation`)는 패치 없는 기본 세대로 읽는다. manifest 해석 결과는 엣지에
  `manifest_edge_cache_seconds` 동안 캐시된다. 패치 수는 계약의 고정 상한 `manifest_patch_ceiling` 까지 읽는다 —
  조정값 `max_patches`·`pnu_prefix_length` 는 발행 명령이 새 manifest 를 쓸 때만 본다.
- `GET /buildings/by-pnu/_capabilities` 는 이 Worker 가 읽는 manifest 스키마 목록을 낸다. 발행 명령은 첫 v2
  manifest 를 쓰기 전에 이것을 확인한다 — 게이트웨이를 먼저 배포한다.
- 묶음 파일(루트 ADR-0147, ADR-0151, `src/packs.ts`): v2 manifest 에 `section_packs` 블록(자체 schema 3)이 있으면
  객체 대신 묶음으로 서빙한다. 건물 레인의 항목은 `documents` 하나이고(계약 `section_packs.sections`), 한 PNU 의
  항목은 객체 레인이 쓰던 문서 바이트를 gzip 한 덩어리다. Worker 는 법정동 묶음의 머리에서 이분 탐색해 그 덩어리를
  풀지도 파싱하지도 않고 `Content-Encoding: gzip` 으로 낸다(gzip 을 받지 않는 클라이언트에게만 풀어 준다). 그래서
  CPU 는 객체 경로와 같은 일이다 — 계정이 Workers Free(요청당 10ms)라 항목을 Worker 가 합치던 방식은 오류
  1102(503)를 냈다(ADR-0151). gzip 여부는 엣지가 바꿔 쓴 `Accept-Encoding` 이 아니라 클라이언트 원래 값
  (`cf.clientAcceptEncoding`)으로 정한다. `ETag` 는 묶음의 R2 `etag` 와 덩어리 위치이고, 푼 답은 다른 표현이라
  `-identity` 를 붙인 다른 태그다. CPU 한도는 계약 `cpu_limit_ms` 를 `limits.cpu_ms` 로 투영한다(Workers Paid). 배치는 계약 `placement`(버킷 옆 Tokyo, ADR-0154)를
  운영과 미리보기 둘 다에 투영한다. 패치는 `patch_floor` 위의 것 중
  법정동이 목록에 있는 것만 읽는다. 툼스톤은 typed 404, 없으면 404, 목록의 묶음이 없거나 형식이 어긋나면 503 이다.
  `_capabilities` 는 `[1, 2, 3]`.
  - 읽기(계약 `by_pnu_section_packs.read_path`): 범위 없는 GET 하나가 머리를 찾는다. 묶음이 `whole_pack_max_bytes`
    이하면 그 GET 으로 통째 읽고 문서도 그 바이트에서 꺼낸다(R2 한 번). 크면 머리까지만 읽고 끊은 뒤 문서를
    범위 읽기로 꺼내고, 답의 `etag` 가 머리의 것과 다르면 합치지 않는다(조건 `onlyIf` 는 보내지 않는다: 키가
    create-only 라 아무것도 가르지 않으면서 CPU 를 썼다, ADR-0154). R2 가 범위를 잘라 주기를 기대하지 않는다.
  - 묶음 사본은 isolate 메모리(바이트 예산 LRU)에만 둔다. 엣지 캐시에는 두지 않는다(ADR-0154): Worker 를 버킷
    옆에 두면(계약 `placement`) 한 번 읽힌 묶음의 R2 읽기가 엣지 조회만큼 싸고, 사본 쓰기가 나머지 읽기보다 CPU 를
    더 썼다.
  - PNU 마다의 엣지 응답 사본(ADR-0154): 한 번 답한 문서의 덩어리를 묶음 계획의 지문(`plan.fingerprint`) 아래
    합성 주소에 둔다. 같은 서빙 상태에서 그 PNU 를 다시 읽으면 캐시 조회 하나로 답한다(객체 경로의 PNU 캐시와
    같은 일). 툼스톤과 부재는 두지 않는다. 새 세대나 패치가 발행되면 지문이 바뀌어 옛 사본을 쓰지 않는다.
  - R2 일시 오류는 `r2_attempts` 번까지, `r2_deadline_ms` 안에서 전체 지터 백오프로 다시 묻는다. 시도 하나는
    `r2_attempt_timeout_ms` 를 넘기면 버리고 다시 묻는다(멈춘 읽기 하나가 기한을 다 쓰지 않게). 형식 불일치는
    다시 묻지 않는다. 다시 물을 때와 503 마다 구조화된 로그(`pack_read_retry`, `pack_outage`: 항목·키·단계·오류
    종류·경과)를 남긴다.
  - 미리보기 버전은 `Server-Timing` 을 낸다: `outcome`, `r2;dur=…;desc="gets=… retries=…"`, 항목마다
    `pack-{항목};dur=…;desc="{memory|edge|r2}-{whole|head}[+range]"`, `total`.
  - 바인딩 `FOUNDATION_PLATFORM_BUILDING_PACK_SERVING=off` 인 버전은 manifest 가 묶음을 적어도 객체로 답한다.
    묶음 경로는 이 바인딩만 다른 두 버전 사이의 비율로 올리고 내린다(`scripts/ops/building-gateway-canary.sh`,
    ADR-0151 Revision).
  `?packs=g{n}` 은 미리보기 Worker(바인딩 `FOUNDATION_PLATFORM_BUILDING_PACK_PREVIEW=true`, 계약
  `section_packs.preview_worker`, `wrangler.<레인>.jsonc` 의 `env.preview`)에서만 미발행 세대를 서빙하고, 운영 경로에서는
  manifest 가 그 세대를 가리킬 때만 답한다(전환 관문, 런북 `docs/runbooks/building-section-pack-cutover.md`).
- **manifest 를 신뢰할 수 없으면 503** (부재·비파싱·다른 unit/schema/세대 0): 포인터 장애는
  레인 전체의 장애다. R2 읽기 실패도 503이며 `no-store`로 캐시되지 않는다.
- 모든 답은 자기를 만든 버전 id 를 계약 `version_header`(`Foundation-Worker-Version`)에 적는다. 값은 버전
  메타데이터 바인딩(`version_metadata_binding`)에서 오고, 카나리아 단계가 고정 읽기의 도달을 이것으로 센다(ADR-0157).
- 객체 부재는 CORS 헤더를 포함한 404 `no-store`다. 웹은 이를 `BuildingNotServedError`로 구분한다.
- 거부: query, 원시 객체 키, 다른 prefix, traversal, 비정규 PNU는 404; 다른 method는 405;
  미허용 Origin은 403
- 캐시: 객체는 `public, max-age=3600`. 개별 PNU 응답의 엣지 캐시 키에 서빙 상태의 지문(`v{기본}p{최신 패치}`)이
  들어가므로, 새 패치나 새 기본 세대가 발행되면 엣지는 옛 응답을 즉시 쓰지 않는다(manifest 캐시 1분 이내).
  **브라우저는 다르다**: 같은 URL 의 응답을 `max-age`(계약의 `cache_control`) 동안 다시 묻지 않고 쓸 수 있다
  (루트 ADR-0141 2026-10-04 개정 주석).
- 조건부 요청: R2/Cache API의 ETag와 `If-None-Match` 판단을 사용하고 일치하면 304. 패치의 작은 객체는 먼저
  본문을 읽어 툼스톤인지 정한다 — 조건부 헤더로 삭제된 PNU 의 304 를 받아 낼 수 없다.
- 권한: R2 binding 하나의 `get`만 타입에 노출; list/write와 S3 자격증명 없음

## 로컬 검증

Node와 pnpm 버전은 `package.json`의 `engines`와 `packageManager`를 따른다.

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
6. Worker **Settings > Domains & Routes > Add > Custom Domain**에 계약의 building hostname을
   붙인다. Worker가 origin이 되는 Custom Domain이며 R2 bucket custom domain과 다르다.
   Cache API가 `workers.dev`에서 보장되지 않으므로 생성 구성은 `workers_dev: false`로 그
   경로를 닫는다.
7. 배포 전에 `export-building-by-pnu-serving`과 `publish-building-by-pnu-serving-manifest`로
   서빙 세대가 실제로 구워져 있고 manifest 가 그 세대를 가리키는지 확인한다 — manifest 가
   없으면 Worker는 설계대로 503만 낸다.

배포와 Dashboard 변경은 이 코드 작업의 범위 밖이다.

## Gold와 웹 계약 검증

Gold 잡은 `infra/lakehouse/spark/jobs/building_panel_silver_to_gold.py`다. 다섯 Silver 원천의
단일 스냅숏과 자연키를 검증하고 PNU당 건물·층·호를 묶는다. Iceberg 입력은 필수 인자
`--source-snapshots-path`로 모든 원천 판을 고정한다. 필지와 같은 공통 해석기를 사용하고
실행 요약에 실제 사용한 물리적 판을 별도로 남긴다. `--validate-only`에서도
동일한 원천·샤드·품질 검증을 수행하고 쓰기만 생략한다.

Foundation 디렉터리에서 기존 Python CI 스위트로 순수 계약을 검증한다. `cargo xtask verify
foundation`의 `infra/lakehouse/spark/tests` 디렉터리 커버리지는 필지와 건물 검사를 함께
발견한다. 별도의 Java·PySpark CI 환경은 추가하지 않는다.

```bash
python3 -m unittest discover -s infra/lakehouse/spark/tests -p 'test_building_panel*.py'
```

실제 Spark 컨테이너 검증은 개발 산출물로 실행하고 측정 결과를 PR에 기록한다. 합성
Silver 입력으로 셔플 순서·내용 지문·중복 거부·미연결 호를 검사한 결과가
`tests/fixtures/building_panel_gold_row.json`이다. Rust 문서 조립 검사도 같은 Gold 행을
읽어 Catalog UUID 함수와 공개 DTO 일치를 검증한다. 운영 데이터의 읽기·굽기와 이
합성 식별자 검증은 구분해 보고한다.

웹의 `NEXT_PUBLIC_BUILDING_EDGE_BASE_URL` 기본값은 계약의 공개 호스트이며,
`building-edge.ts`가 문서 버전과 요청 PNU를 검사한다. 건물 패널은 같은 문서의 호를
펼치며, 미연결 호는 별도로 표시한다. 층수 미기재는 `null`을 유지한다.
