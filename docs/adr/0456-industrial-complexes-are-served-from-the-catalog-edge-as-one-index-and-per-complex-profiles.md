# ADR 0456: 산업단지는 필지 엣지에서 목록 한 장과 단지마다의 프로필로 서빙한다

- Status: Accepted
- Date: 2026-11-09
- Builds on: ADR-0036(산업단지 Gold 프로필), ADR-0039(프로필은 lakehouse 버킷 `gold/` 밑), ADR-0296 §5·ADR-0297 §5(필지
  엣지가 같은 버킷의 내용 주소 객체를 읽기만 한다), ADR-0336(스키마 레지스트리), ADR-0426(산업단지 시도·시군구가 법정동 코드 변경을
  따라간다), ADR-0249(읽기 전용 미리보기)

## Context

사장: "산업단지 정보가 안 나온다." 공짱 웹의 산업단지 패널(`components/panels/complex/summary.tsx`)과 `/complexes` 목록은
`apiProxyClient` 로 AWS 의 공짱 API 를 부른다. 그 API 는 Foundation 의 Postgres 사본(`catalog.industrial_complex`)을 읽는다.
미리보기(ADR-0249)에는 그 API 가 없어 `/complexes` 와 `/api/proxy/*` 가 404 다. 데이터는 레이크하우스에 있다.

**측정(운영 Trino 읽기 전용, 2026-11-09).**

| 무엇 | 값 |
|---|---|
| `gold.complex_catalog` 행·단지 | 1,442 · 1,442 |
| 그 표의 스냅숏 | 2026-08-18, 2026-10-01, 2026-11-08(ADR-0426 따라가기의 MERGE) |
| 엣지(`profiles.gongzzang.app`)에 있는 프로필 | 08-18 스냅숏 것만(200) — 10-01·11-08 스냅숏의 프로필은 404 |
| 프로필을 내보내는 일정 | 없음(`export-industrial-complex-gold-profiles` 는 손으로 한 번 돌았다) |
| 단지 id → 현재 프로필을 아는 곳 | Postgres `catalog.industrial_complex_gold_pointer` 뿐 |
| 목록을 아는 곳 | Postgres 뿐 |

즉 프로필 객체·키 규칙·서빙 Worker 는 있는데, ① 내보내기가 Gold 를 따라가지 않고, ② 단지 id 에서 현재 프로필을 찾는 길과 목록이
제품 API 의 Postgres 사본에만 있다. 레이크하우스가 중심이다 — Foundation 데이터는 by-PNU 레인처럼 Foundation 엣지가 서빙해야 한다.

### 실사례

- **작은 목록은 한 장의 정적 문서, 상세는 항목마다.** npm 레지스트리는 패키지마다 문서(packument) 하나, crates.io 의 sparse index 는
  크레이트마다 파일 하나를 CDN 에 두고 클라이언트가 직접 읽는다. 1,442 행(약 0.4 MB, gzip 약 70 KB)은 브라우저가 한 번 받아 검색·필터·
  쪽나누기를 하는 크기다 — Algolia/Pagefind 같은 정적 검색도 같은 판단이다.
- **덮어쓰는 것은 포인터 하나, 나머지는 쓰기 한 번.** Iceberg 의 metadata 포인터, 우리 by-PNU 의 `manifest.json` + `manifest-history/`
  가 같은 모양이다: 바뀌는 객체는 하나이고 그 판들은 지우지 않는다.
- **바뀐 것만 다시 쓴다.** Bazel·Nix 의 내용 주소 캐시처럼 입력의 해시가 같으면 산출물을 다시 만들지 않는다.

## Decision

1. **문서 둘.** 목록 = `gold/industrial-complex/index.json`(레지스트리 주제 `foundation-platform.industrial_complex_index`), 단지마다의
   상세 = 지금의 Gold 프로필 `gold/industrial-complex/profiles/<artifact_id>.json`(주제 `foundation-platform.industrial_complex_gold_profile`,
   이번에 등록; 키·형식의 정본은 `profile_gateway.object_key` 그대로). 목록의 항목은 목록 칸(이름·코드·종류·상태·조성단계·시도·시군구·주소·
   면적)과 그 단지 현재 프로필의 키, 그리고 내용 해시다.
2. **부분 내보내기.** 내용 해시 = 그 단지 Gold 행에서 실행마다 새로 쓰이는 칸(`iceberg_snapshot_id`, `published_at_utc`)을 뺀 것의
   SHA-256. 지금 목록이 같은 해시를 갖는 단지는 지난 프로필을 그대로 가리키고 쓰지 않는다. 바뀐 단지만 새 프로필을 create-only 로 쓴다.
   모든 항목이 같으면 아무것도 쓰지 않는다. Gold 가 법정동 코드 변경을 따라가므로(ADR-0426, `views.changed_codes`) 코드가 바뀐 단지는
   해시가 바뀌어 다시 나간다 — 적재 때 한 번 정하고 멈추지 않는다.
3. **append-only.** `index.json` 이 유일하게 덮어쓰는 객체다. 덮어쓰기 전에 같은 바이트를 `index-history/<generation>.json` 에
   create-only 로 남긴다(같은 세대 같은 바이트 재실행은 그대로, 다른 바이트는 거부). 프로필은 지우지 않는다.
4. **서빙은 필지 엣지.** `catalog.perfectory.io`(parcel lane Worker)가 `parcel_media` 와 같은 방식으로 이미 바인딩한 lakehouse 버킷에서
   두 경로만 읽는다: 목록은 `no-cache` + ETag(바뀌지 않았으면 304), 프로필은 immutable 1년 + 엣지 캐시. CORS 는 by-PNU 경로와 같은
   허용 목록이다. 계약 = `config/r2-connections.contract.json` 의 `industrial_complex_documents`(정의는 `platform.definition.json`).
5. **일정.** 필지 계보 순환(`lineage_stewardship`, 매일 06:50 UTC)이 0a′(Gold 의 코드 따라가기) 바로 뒤에서 매 회차 내보낸다. Gold 를
   손으로 다시 지어도 다음 회차가 따라간다. 첫 회차는 1,442 프로필 + 목록 1세대를 쓰고, 그 뒤는 바뀐 단지 수만큼이다.
6. **웹.** 패널과 `/complexes` 는 레지스트리에서 생성한 클라이언트(`complex-index.generated.ts`, `complex-profile.generated.ts`)로 필지
   엣지를 읽는다. 목록의 검색·시도/상태 필터·시도 집계·정렬·쪽은 브라우저가 목록 한 장 위에서 한다. 미리보기는 `/complexes` 를 서빙한다.
7. **레지스트리의 주인.** by-PNU 가 아닌 문서의 주인은 연결 계약의 최상위 블록(`owner.surface`)이다: 목록은
   `industrial_complex_documents`, 프로필은 `profile_gateway`. 검사기(`scripts/schema_registry.py`)는 lane 과 surface 중 하나만 허용한다.

## 남긴 것

- **제품 API 의 산업단지 경로(`/api/proxy/complexes*`, 공짱 API·Postgres 사본·포인터 발행)는 지우지 않았다.** 그 경로를 지우면
  `traffic-auth-policy-registry` 에서 생성되는 AWS WAF 정책(`aws-wafv2-edge-policy.generated.json`)이 바뀌고, AWS 에 배포돼 있는
  웹 빌드가 아직 그 경로를 부른다. AWS 공짱 배포는 이 작업이 건드리지 않는다. 그 웹이 이 판으로 다시 배포된 뒤 경로를 지우는 것은
  사장의 결정이다.
- **`foundation-profile-gateway`(`profiles.gongzzang.app`)** 는 같은 프로필을 서빙하는 두 번째 호스트가 된다. 소비자는 없다.
  Cloudflare 의 커스텀 도메인·Worker 를 내리는 일은 대시보드 작업이라 따로 한다.
- 조성단계(`development_stage`)는 목록·프로필에 싣지만 패널에 아직 보이지 않는다 — 상태(`status`)와 거의 1:1 이고(운영 1,442 중
  site_completed↔operating 1,069 등) 웹에 그 값의 이름표가 없다.
- 필지 패널의 "OO산업단지 소속" 은 `silver.complex_parcel_memberships`(W38)가 적재된 뒤 이 목록을 써서 붙인다.

## Consequences

- 미리보기와 운영 웹이 제품 API 없이 산업단지를 보인다. 목록은 한 번 받고(이후 304), 패널은 프로필 한 장(엣지 캐시)을 더 받는다.
- 비용(계산): 첫 회차 PUT 약 1,445 회(R2 class A $4.50/백만 → 약 $0.007). 이후 하루 Gold 스캔 1회(수 MB, egress 0)와 바뀐 단지 수만큼의
  PUT. 서빙은 Worker 요청과 R2 GET(class B $0.36/백만)이다.
- 새 문서 모양은 레지스트리 개정으로만 바뀐다(FULL_TRANSITIVE, ADR-0336).

## 개정 기록

- 2026-11-13: §5 의 일정(계보 순환의 한 단계)은 [ADR-0563](./0563-the-model-claims-the-memory-it-takes-not-the-spark-pool-and-a-complex-s-profile-is-exported-when-its-facts-or-its-row-change.md) §3 이 갈았다. 내보내기는 자기 작업 `industrial_complex_edge_export` 이고, `gold.complex_catalog` 나 `gold.complex_document_facts` 가 바뀌면 시작한다. Spark 를 쓰지 않는 명령이 spark 3칸짜리 순환 뒤에서 하루를 기다렸다.
