# ADR 0626: 데이터 이름과 배치 v1 — 이름의 종류마다 집은 하나이고, 다른 철자는 모두 그 집에서 생성하며, 규칙 밖의 이름은 CI·쓰기 경계·조정기가 거부한다

- Status: Accepted (사장 결정 2026-11-28: 제공기관 id 는 오늘 값을 등록부 키로 고정(옵션 K), 표 이름은 한 행을 이르는 단수 명사, 네임스페이스는 `silver`·`gold` 둘, 브레인스토밍 표 E 의 뜻 바꾸기는 '거부' 둘을 빼고 채택, 법원 매각물건명세서 PDF 는 공개 — 그 PDF 는 §6-3 대로 보류. 기술 항목은 코디네이터가 정했다)
- Date: 2026-11-28
- Supersedes: ADR-0032 (제공기관 id 는 도메인 라벨에서 파생하지 않는다. 등록부의 키다)
- 고치는 곳(그 자리에는 이 ADR 을 가리키는 날짜 붙은 한 줄을 남겼다):
  [ADR-0039](./0039-gold-serving-artifacts-live-in-the-lakehouse-bucket-and-tiles-do-not.md) 결정 1(프로필의 자리),
  [ADR-0242](./0242-a-superseded-pack-generation-is-deleted-a-week-after-its-successor-went-live.md)(세대 표시의 철자),
  [ADR-0286](./0286-every-table-contract-declares-how-its-rows-are-versioned.md)(표 이름에 쌓는 방식을 적지 않는다),
  [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)(전환에서 판 쌓는 표가 실체 이름을 갖는다),
  [ADR-0616](./0616-data-jobs-run-as-dagster-assets-on-aws-ecs-and-the-host-job-layer-is-not-ported.md) §3-1(Bronze 키의 시각 도장),
  [ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md)(쓰기 접두는 생성한다),
  [ADR-0624](./0624-the-lakehouse-catalog-is-self-hosted-open-source-lakekeeper-and-it-holds-only-table-pointers-commits-credentials-grants-and-protection.md)(조정기의 표 목록은 계약에서 생성, 네임스페이스는 둘),
  [FP-ADR-0014](../../platforms/foundation-platform/docs/adr/0014-bronze-source-slug-canonical-naming.md) D4·D6.
- 다루는 것: 제공기관·데이터셋·원천 id, 버킷, R2 의 최상위 영역과 경로, 카탈로그의 네임스페이스·표·열의 이름, 이름이 사는 곳과 그것을 지키는 검사, 이름을 바꾸는 순서.
- 다루지 않는 것: Bronze 잎 이름(`page-`, `-part`, `--sha256-`, 제공기관의 파일 정체), 레인 id·엔드포인트 slug·CLI 토큰(FP-ADR-0014 D6 그대로), 계약·정의 파일 이름(코드다), 열 전체의 손질, Iceberg 숨은 파티션, 다른 저장소의 버킷, AWS gongzzang, `perfectory-sccache`(도구 버킷).

## Context

ADR-0624 가 Silver·Gold 를 Lakekeeper 로 옮긴다. 표의 R2 경로는 만들 때 정해진다. Basin 의 rename 은 파일을 옮기지 않고(W108 2b), Lakekeeper
창고의 키 접두 `tables` 는 바꿀 수 없다(ADR-0624 §4-2). 구성 요소마다의 A1 전환 PR 이 표를 새로 만드므로, 지금 정한 이름은 그때 공짜로 선다.
나중에 정하면 두 번째 이전이 든다. 그래서 이름은 첫 전환 전에 정한다.

결함은 철자 하나가 아니다. 한 가지에 손으로 쓴 철자가 여럿이고, 제공기관 id 가 호스트 이름의 함수다(브레인스토밍
`concepts_brief.r2-naming-round3.md`, 일곱 렌즈. 아래 "[m]"은 잰 것, 나머지는 2026-11-28 main `1128cf473` 에서 읽은 것).

| # | 사실 | 근거 |
| --- | --- | --- |
| F1 | 제공기관 id = 승인 도메인에서 점을 뺀 것. 저장된 id 가 없다. `rtmolitkr`→`rtmolitgokr` 고침이 고아 객체 98 개를 남겼다 [m] | `collection-domain/src/source_slug.rs` `APPROVED_PROVIDER_DOMAINS`; W106 §2 |
| F2 | 법원경매 하나가 여섯 철자다: `courtauction.go.kr`, `courtauctiongokr`, `courtauction-go-kr`(계약 파일·R2 루트), `court_auction`(표 접두), `court-auction`(작업), `courtauction_`(Rust 모듈) | 브레인스토밍 구조 렌즈 |
| F3 | 표 하나가 셋 이상의 철자다: 계약, 파이프라인 그래프의 케밥 id, Rust 47 파일의 손 상수 117 개 | 브레인스토밍 코드 렌즈 |
| F4 | `catalog.source_catalog` 는 `ON CONFLICT (slug)` 로 upsert 한다. slug 를 바꾸면 새 행·새 id 가 생기고 중복 제거가 옛 객체를 모두 놓친다 | `collection-infrastructure/src/bronze_repository.rs` |
| F5 | Silver·Gold 가 Bronze 문자열을 데이터로 갖는다: 29 계약 파일에 `*object_key*`·`source_slug`·`source_snapshot_id` 50 열, `source_snapshot_id` 는 183 계약 중 164 에 있고 Gold 30 표의 파티션 열이다 | `lakehouse-domain/src` |
| F6 | 계약 산출물의 표 183 개: silver 124, gold 53, reference 2, intelligence 4. 운영 Lakekeeper 는 `["silver","gold"]` 만 선언하고, 네 목록이 서로 다르다(`LakehouseLayer` 넷, 등록부 enum 셋, DB CHECK 셋, Lakekeeper 둘) | `infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json`, `config/lakekeeper-prod.contract.json` |
| F7 | `_versions` 는 넘겨받기의 비계다(ADR-0564). 평이한 이름의 Gold 표가 죽은 쪽인 일이 많다(`parcel_energy`·`parcel_factories`·`parcel_register_masters`·`parcel_sub_parcels`·`complex_catalog`·`lineage_review_queue` 는 한 번 쓰였고 `_versions` 가 살아 있는 생산자다) | `docs/catalog/pipeline-graph.v1.json` |
| F8 | 조정기의 표 목록은 손 목록이다(`reconciler.tables: []`) | `config/lakekeeper-prod.contract.json` |
| F9 | `__changes` 표는 이름을 지어 만든다. 접미사가 세 곳에 따로 있고, 운영에 약 20 개, 그것을 적은 계약은 없다 [m] | `gold_incremental.py`, `changelog.py`, `platform_contracts.py`, `shape-ledger-check.py` |
| F10 | 운영에 smoke 표 넷과 계약에 없는 표 셋이 있다 | `lakehouse-production-shape.json` |
| F11 | Postgres 가 표 이름을 CHECK·트리거 글자로 갖는다(`silver.parcel_boundaries`) | `migrations/20260719000001…`, `20261002130000…` |
| F12 | 재시도는 새 시각 도장으로 새 키·새 장부 행을 만든다: 24 원천의 3,969 행, 도장이 `dedupe_key`·`source_identity_key` 안에도 있다. 법원 수집기는 키에서 도장을 다시 읽는다 [m] | W108 §1-6, `courtauction_go_kr_collect/detail.rs` |
| F13 | R2 최상위(2026-11-28 [m], 읽기 키): `__r2_data_catalog/ bronze/ collector-state/ control/ courtauction-go-kr/ gold/ onbid/ serving/ silver-handoff/`. `staging/` 와 `gold/staging/` 은 없다 | 이 ADR 을 쓰며 잰 목록 |
| F14 | 매각물건명세서 PDF 는 점유자 이름을 가리지 않는다: 무작위 6 개 중 2 개의 점유자 칸이 흔한 성으로 시작하는 2~4 음절이고, 가림 글자는 6 개 모두 0 이다 [m]. 법원 계약도 "PDF 가 사람(판사, 점유자)을 이름한다"고 적는다 | `courtauction-go-kr.contract.json` `sale_specifications`(ADR-0575) |

실사례(조사 메모 `docs/designs/data-naming-conventions-2026-11-27.md`):

| 출처 | 무엇을 정하나 | 이 ADR 에서 |
| --- | --- | --- |
| DataHub | 플랫폼 이름의 정본 목록(소문자, 15 자 이하), 배치마다 사람이 고른 인스턴스 이름 | 제공기관 id 는 등록부의 키, 도메인은 속성 |
| GitLab Data Team | 적재기 이름을 짧고 안정된 원천 이름으로 대응(`gitlab_dotcom`), 단수(`dim_date`), 온전한 낱말 | 등록부, 단수, 줄임말 없음 |
| OpenLineage | id 꼴을 나중에 바꾸면 계보가 끊긴다 | 바뀌는 것(호스트·판·날짜)을 이름에 넣지 않는다 |
| Glue·S3 Tables·Unity Catalog | 소문자와 `_` 만, 하이픈 없음(버킷 이름만 하이픈) | snake_case, 케밥은 DNS 자리만 |
| Athena·Spark·BigQuery | 소문자 `key=value` 경로 조각 | 세대·사다리·run 표시 |
| Iceberg·Delta·BigQuery | 판은 메타데이터에, 표 이름에 날짜·판을 넣지 않는다 | `_versions`·날짜·`_smoke` 없음 |
| dbt | staging 은 원천 시스템 이름으로, marts 는 실체 이름으로 | Silver 는 원천 시스템으로 시작해도 되고 Gold 는 아니다 |
| Kimball | 같은 것은 어디서나 같은 이름 | 열 짝 고침 |

## Decision
### 1. 이름의 종류마다 집은 하나다

다른 철자는 그 집에서 생성하거나 그 집에 대어 확인한다. 집 밖에서 손으로 쓴 이름은 검사(§7)가 거부한다.

| 종류 | 집 | 생성하거나 대어 보는 철자 | 검사 |
| --- | --- | --- | --- |
| 제공기관 id | `definitions/platform.definition.json` 의 `naming.providers`. 생성기(ADR-0418)가 `config/naming.contract.json` 을 만들고 커밋하지 않는다 | Rust `source_slug.rs`(`include_str!`, 선례 `by_pnu_kind.rs`), Python, ADR-0623 쓰기 접두, 엔드포인트 카탈로그 | G1, G5 |
| 데이터셋 id·원천 id | 공개 원천 엔드포인트 카탈로그의 `bronze.source_slug` | Bronze 키(빌더 `build_bronze_object_key` 하나), `catalog.source_catalog.slug` | G1, G5 |
| 네임스페이스 | `naming.namespaces` = `silver`, `gold` | Lakekeeper 계약, `LakehouseLayer`, 등록부 enum, DB CHECK | G2, G3 |
| 표 | Rust 표 계약(`lakehouse-domain`)과 그 산출물 | 파이프라인 그래프 노드 id, Dagster 키 `iceberg/<ns>/<t>`, ODCS, 조정기의 표 목록 | G2, G3, G4 |
| 역할 접미사 | `naming.role_suffixes` = `changes`(`<t>__changes`). 부모 계약이 짝을 선언한다 | Python 의 세 상수 | G2 |
| 열 | 표 계약 | — | G2(새 열) |
| 버킷 | `naming.buckets` | `r2-connections` 의 `expected_values`, 등록부, 거부 목록, Worker 바인딩 | G4 |
| 영역과 키 틀 | `naming.areas`: 버킷, 키 틀, `canonical`·`derived`·`held`, 만료, 쓰는 레인 | 빌더, Worker `path_pattern`, ADR-0623 쓰기 접두, `r2_inventory_audit` `classify_key` | G4, G6 |
| 이름 바꾸기 | `naming.renames`(옛 → 새). 임시 일 목록이다: 전환 PR 마다 제 항목을 지우고, A9 에서 블록을 지운다 | — | G2·G4 는 이 목록에 있는 옛 이름만 남겨 둔다 |

### 2. 규칙

1. **제공기관 id 는 등록부의 키다(옵션 K).** 오늘 값 열한 개를 그대로 고정한다(§3). 실행 때 호스트 이름에서 계산하지 않는다. 도메인·표시
   이름·한국어 이름은 속성이다. 꼴은 `^[a-z][a-z0-9]*(_[a-z0-9]+)*$`, 16 자 이하(오늘 가장 긴 `courtauctiongokr`·`industrylandorkr` 가 16 자),
   `__` 없음이다. 제공기관은 우리가 부르는 접근 창구(열쇠·할당량·예의)이고, 원천 시스템의 이름은 데이터셋에 든다. 그래서 온비드는
   `datagokr` 의 데이터셋이고 Silver 접두 `onbid_` 가 원천 시스템을 이름한다. 새 제공기관은 사람이 등록부에 새 키를 정해 넣는다. 한 번
   쓴 키는 바꾸지 않는다.
2. **데이터셋 id 는 같은 꼴이고, 원천 id 는 `<provider>__<dataset>` 이다.**
3. **구분자.** 모든 식별자는 소문자 snake_case 다. `_` 는 낱말을, `__` 는 두 식별자(원천 id, 표와 그 역할)를 잇는다. 케밥은 DNS 가 `_` 를
   막는 곳에만 쓴다(버킷 이름, 호스트 이름, Worker·Cognito 이름): `dns(id)` = `_` → `-`. 경로 조각은 id 그대로이거나 `<key>=<value>` 다.
   적힌 예외 하나: Bronze 문법 `bronze/source=<provider>__<dataset>/…` 는 바꾸지 않는다(옵션 K, FP-ADR-0014 D4).
4. **세대·사다리·실행 표시**는 `generation=<n>/patch=<n>`(묶음), `ladder=<n>`(사진 사다리), `run=<id>`(작업 공간)다. 순서와 은퇴는
   manifest 가 말한다(ADR-0242). 그래서 0 채우기가 없다.
5. **최상위에는 영역만 있다**: `bronze/ media/ serving/ tables/ work/ control/`. 모든 경로는
   `<영역>/<주제>/<꼴>/[<key>=<value>/…]<잎>` 이다. 꼴의 낱말은 표 이름처럼 단수다(`profile`, `rendition`, `document`, `release`, `pack`).
   Bronze 는 규칙 3 의 예외이고, 잎에 실행 도장이 없다(F12). 표는 `tables/<ns>/<table>/` 이다(ADR-0624). `__r2_data_catalog/` 는 A9 까지
   Basin 의 것이다.
6. **네임스페이스는 층이다**: `silver` 나 `gold`. `reference`·`intelligence` 는 `silver` 에 든다. 네임스페이스는 보안 경계가 아니다(권한은
   표마다, ADR-0624 §2).
7. **표 이름은 한 행(낟알)을 이르는 단수 머리 명사다**: `<실체>[_<한정어>…]`, 온전한 영어 낱말. Silver 는 원천 시스템으로 시작해도 되고
   (`court_auction_`, `onbid_`), Gold 는 아니다. 저장·판·날짜·환경·시험의 낱말이 없다(`smoke`, `test`, `tmp`, `probe`, `staging`, `old`, `new`,
   `next`, `served`, `versions`, `snapshot`, `v<n>`, 6~8 자리 숫자). 유일한 접미사는 생성되는 `__changes` 다. 행이 쌓이는 방식은 `row_versions`
   만 말한다(ADR-0286): 판을 쌓는 표가 실체 이름을 갖는다. 끝 낱말이 `s` 로 끝나면 단수인 낱말 목록(`naming.singular_words_ending_in_s`:
   `gas`, `statistics`, `loss`)에 있어야 한다. 계약은 `grain`(한 행이 무엇이고 그 키가 무엇인가)을 반드시 적는다. smoke·시험 표는 dev 창고에만
   있다(Iceberg 쓰기-감사-게시는 가지로 한다).
8. **열**은 snake_case 이고 `_id`, `_code`, `_at_utc`(UTC 시각), `_date`, `is_`·`has_` 를 쓴다. 같은 것은 어디서나 같은 이름이다. Iceberg
   파티션은 spec 이지 이름이 아니다. Bronze 경로 키는 같은 것의 Silver 열 이름을 쓴다(`sigungu=` → `sigungu_code=`, Bronze 시기에). 새 열에
   강제하고, 알려진 짝은 그 구성 요소의 전환 PR 에서 고친다: `sido` → `sido_code`, `bjdong_code` → `legal_dong_code`, 평이한 `_at` 일곱
   (`edited_at`, `moved_at`, `erased_at`, `bid_begins_at`, `bid_ends_at`, `provider_modified_at`, `opened_at`; 2026-11-28 계약 산출물) → 값을
   UTC 시각으로 바꾼 `_at_utc`.
9. **데이터 버킷**은 `<소유자>-<목적>-<환경>` 이고 환경은 `prod`·`dev` 접미다. `perfectory-sccache` 는 도구 버킷이라 범위 밖이다.

### 3. 제공기관 등록부

| id(고정) | 도메인 | 표시 이름 | 한국어 이름 | main 의 데이터셋 |
| --- | --- | --- | --- | --- |
| `vworldkr` | `vworld.kr` | VWorld | 브이월드 공간정보 오픈플랫폼 | 27 |
| `datagokr` | `data.go.kr` | Public Data Portal | 공공데이터포털 | 49 |
| `rtmolitgokr` | `rt.molit.go.kr` | MOLIT Real Transaction | 국토교통부 실거래가 공개시스템 | 12 |
| `hubgokr` | `hub.go.kr` | Building HUB | 건축HUB | 59 |
| `jusogokr` | `juso.go.kr` | Road Name Address | 도로명주소 | 12 |
| `moisgokr` | `mois.go.kr` | Ministry of the Interior and Safety | 행정안전부 | 0 |
| `factoryongokr` | `factoryon.go.kr` | Factory On | 팩토리온 | 1 |
| `industrylandorkr` | `industryland.or.kr` | ILIS | 산업입지정보시스템 | 6 |
| `codegokr` | `code.go.kr` | Administrative Standard Code | 행정표준코드관리시스템 | 1 |
| `courtauctiongokr` | `courtauction.go.kr` | Court Auction | 법원경매정보 | 13 |
| `kicoxorkr` | `kicox.or.kr` | KICOX | 한국산업단지공단 | 1 |

합 181 원천(`public-source-endpoint-catalog.v1.json`). `moisgokr` 는 데이터셋이 없지만 사장이 고른 열한 값에 든다. 잔해 id(`rtmolitkr__*` 98
객체, 07-02 의 `vworldkr__boundary_census_*`·`hubgokr__building_energy_yearly_*`)는 등록부에 없고, Bronze 시기(§8 N2)에 선언하거나 지운다.

### 4. 데이터셋 id 고침(Bronze 시기 N2, 별도 작업)

옵션 K 에서도 이 고침과 실행 도장(F12, 3,969 행)은 한 시기에 같이 한다. 장부 정체 규칙을 따른다: `catalog.source_catalog` 는
`ON CONFLICT (slug)` 이므로 slug 는 그 자리에서 `UPDATE` 하고(F4) `bronze_object_id` 를 지킨다. 복사는 `create_only_copy_and_rehash` 다.

| 지금 | 새 | 왜 |
| --- | --- | --- |
| `courtauctiongokr__auction_<x>`(13: `appraisal_report`, `case_appraisal`, `case_detail`, `case_filing`, `case_survey`, `case_survey_report`, `common_code`, `dividend_notice`, `item`, `item_detail`, `sale_notice`, `sale_result`, `sale_specification`) | `courtauctiongokr__<x>` | 제공기관을 되풀이한다 |
| `vworldkr__sandan_<x>`(7: `boundary`, `facility_land_use`, `land_use_zone`, `location`, `parcel`, `permitted_industry`, `profile`) | `vworldkr__industrial_complex_<x>` | 로마자 줄임 |
| `datagokr__lh_land_notice_list`, `datagokr__lh_land_notice_detail` | `datagokr__korea_land_housing_land_notice_list`, `…_detail` | 줄임말 |
| `datagokr__building_register_house_price` | `datagokr__building_register_apartment_price` | 같은 데이터셋(카탈로그 `api_duplicate_of_bulk`: 같은 칸·같은 관리번호) |
| `codegokr__legal_dong_code_table` | `codegokr__legal_dong_code` | `datagokr__legal_dong_code` 와 같은 데이터셋(카탈로그 `file_covers_the_api`) |

`real_transaction_*` 둘(`datagokr`·`rtmolitgokr`)은 이미 같은 데이터셋 id 라 그대로다. 법원 수집기는 무엇이 다음 차례인지 키가 아니라
`collected_at` 에서 읽는다(F12). Bronze 경로 키 `sigungu=` 는 `sigungu_code=` 가 된다. 이 시기는 첫 A1 전환 전, ADR-0616 의 장부 RDS 이전 전이다.

### 5. 표 이름 지도

계약 산출물의 표 183 개(F6)를 규칙 7 과 표 E(브레인스토밍, 사장 채택)로 옮긴다: **64 개는 새 이름, 21 개는 다시 만들지 않음, 98 개는 그대로**.
전환 뒤 카탈로그는 162 표(silver 128, gold 34)와 `__changes` 짝이다. "규칙" 칸: 층 = 네임스페이스가 층이다, 판 표 = 판을 쌓는 표가 실체 이름을
갖는다, 단수, `_served` 없음, 뜻 = 표 E.

| 지금 | 새 | 규칙 |
| --- | --- | --- |
| `gold.administrative_boundary_served` | `gold.administrative_boundary` | `_served` |
| `gold.industrial_complex_boundary_served` | `gold.industrial_complex_boundary` | `_served` |
| `gold.complex_document_facts` | `gold.industrial_complex_document_fact` | 단수·뜻 |
| `gold.complex_catalog_versions` | `gold.industrial_complex_profile` | 판 표·뜻 |
| `gold.complex_spatial_locator` | `gold.industrial_complex_spatial_locator` | 뜻 |
| `gold.lineage_review_queue_versions` | `gold.lineage_review_queue` | 판 표 |
| `gold.parcel_boundary_served_versions` | `gold.parcel_boundary` | 판 표·`_served` |
| `gold.parcel_building_permits_versions` | `gold.parcel_building_permit` | 판 표·단수 |
| `gold.parcel_characteristics` | `gold.parcel_characteristic` | 단수 |
| `gold.parcel_court_auction_versions` | `gold.parcel_court_auction` | 판 표 |
| `gold.parcel_demolitions_versions` | `gold.parcel_demolition` | 판 표·단수 |
| `gold.parcel_energy_versions` | `gold.parcel_energy_use` | 판 표·뜻 |
| `gold.parcel_factories_versions` | `gold.parcel_factory` | 판 표·단수 |
| `gold.parcel_land_rights` | `gold.parcel_land_right` | 단수 |
| `gold.parcel_nearby_versions` | `gold.parcel_nearby_facility` | 판 표·뜻 |
| `gold.parcel_ownership_versions` | `gold.parcel_ownership` | 판 표 |
| `gold.parcel_panel_versions` | `gold.parcel_panel` | 판 표 |
| `gold.parcel_public_auction_versions` | `gold.parcel_public_auction` | 판 표 |
| `gold.parcel_real_transaction_versions` | `gold.parcel_real_transaction` | 판 표 |
| `gold.parcel_register_masters_versions` | `gold.parcel_register_master` | 판 표·단수 |
| `gold.parcel_regulation_zones` | `gold.parcel_regulation_zone` | 단수 |
| `gold.parcel_sub_parcels_versions` | `gold.parcel_sub_parcel` | 판 표·단수 |
| `gold.place_id_bridge_versions` | `gold.place_id_bridge` | 판 표 |
| `gold.place_id_registry_versions` | `gold.place_id_registry` | 판 표 |
| `gold.real_transaction_region_stats_versions` | `gold.real_transaction_region_statistics` | 판 표·단수 |
| `gold.land_use_zone_area_by_tile` | `gold.regulation_zone_tile_index` | 뜻 |
| `gold.map_edit_stale` | `gold.stale_map_edit` | 뜻 |
| `gold.unit_price_versions` | `gold.unit_official_price` | 판 표·뜻 |
| `silver.administrative_boundaries` | `silver.administrative_boundary` | 단수 |
| `silver.household_count` | `silver.administrative_dong_household` | 뜻 |
| `silver.dong_population` | `silver.administrative_dong_population` | 뜻 |
| `silver.adm_dong_code_linkage` | `silver.administrative_dong_to_legal_dong` | 온전한 낱말·뜻 |
| `silver.building_permit_road_ledger` | `silver.building_permit_road_register` | 뜻 |
| `silver.building_permit_structure_ledger` | `silver.building_permit_structure_register` | 뜻 |
| `silver.building_register_apartment_price_versions` | `silver.building_register_apartment_price` | 판 표 |
| `silver.building_register_floors` | `silver.building_register_floor` | 단수 |
| `silver.building_register_master` | `silver.building_register_recap_title` | 뜻 |
| `silver.building_register_titles` | `silver.building_register_title` | 단수 |
| `silver.building_register_units` | `silver.building_register_unit` | 단수 |
| `silver.building_register_unit_areas` | `silver.building_register_unit_area` | 단수 |
| `silver.building_maintenance_regular_inspection_history` | `silver.building_regular_inspection` | 뜻 |
| `silver.court_auction_appraisal_report_read` | `silver.court_auction_appraisal_report_fetch_attempt` | 뜻 |
| `silver.court_auction_case_document_read` | `silver.court_auction_case_document_fetch_attempt` | 뜻 |
| `silver.court_auction_case_object` | `silver.court_auction_case_property` | 뜻 |
| `silver.court_auction_dividend_object` | `silver.court_auction_dividend_property` | 뜻 |
| `silver.court_auction_item_object` | `silver.court_auction_item_property` | 뜻 |
| `silver.court_auction_case_related` | `silver.court_auction_related_case` | 뜻 |
| `silver.court_auction_sale_specification_read` | `silver.court_auction_sale_specification_fetch_attempt` | 뜻 |
| `silver.court_auction_survey_object` | `silver.court_auction_survey_property` | 뜻 |
| `intelligence.document_fact` | `silver.document_fact` | 층 |
| `intelligence.document_fact_outcome` | `silver.document_fact_outcome` | 층 |
| `intelligence.document_text_outcome` | `silver.document_text_outcome` | 층 |
| `intelligence.document_text_page` | `silver.document_text_page` | 층 |
| `silver.industrial_complexes` | `silver.industrial_complex` | 단수 |
| `silver.industrial_complex_boundaries` | `silver.industrial_complex_boundary` | 단수 |
| `silver.complex_parcel_memberships` | `silver.industrial_complex_parcel_membership` | 단수·뜻 |
| `silver.industrial_land_sale_notice_read` | `silver.industrial_land_sale_notice_fetch_attempt` | 뜻 |
| `silver.land_forest_ledger` | `silver.land_forest_register` | 뜻 |
| `silver.land_transfer_history` | `silver.land_transfer` | 뜻 |
| `reference.legal_dong_code_snapshot` | `silver.legal_dong_code` | 층·`snapshot` 없음 |
| `reference.legal_dong_code_change` | `silver.legal_dong_code_change` | 층 |
| `silver.parcel_boundaries` | `silver.parcel_boundary` | 단수 |
| `silver.land_use_zone_area` | `silver.regulation_zone` | 뜻 |
| `silver.unit_official_price_versions` | `silver.unit_official_price` | 판 표 |

**다시 만들지 않는 21 표.** 판을 쌓는 짝이 위 표에서 실체 이름을 갖는다: silver `building_register_apartment_price`, `unit_official_price`; gold
`complex_catalog`, `lineage_review_queue`, `place_id_registry`, `place_id_bridge`, `parcel_public_auction`, `parcel_court_auction`,
`parcel_ownership`, `parcel_energy`, `parcel_nearby`, `parcel_real_transaction`, `parcel_panel`, `unit_price`, `parcel_sub_parcels`,
`parcel_building_permits`, `parcel_demolitions`, `parcel_factories`, `parcel_register_masters`, `real_transaction_region_stats`,
`parcel_boundary_served`. 앞 조건: 그 구성 요소의 넘겨받기 판정(ADR-0564)이 전환 PR 에서나 그 전에 통과한다.

**그대로인 98 표**는 계약 산출물에서 위 64 와 21 을 뺀 나머지다. 표 E 에 없는 이름은 바꾸지 않는다(예: `gold.parcel_forest_ledger`,
`gold.parcel_transfer_history`). 표 E 의 규칙을 다른 이름으로 넓히는 것은 새 결정이다. 표 E 에서 거부된 둘은 하지 않는다: `silver.onbid_*` →
`public_auction_*`(Silver 는 원천 시스템 이름을 따른다), `housing_permit_managed_communal_ancillary_welfare_facility` 줄이기(온전한 낱말).

**계약 밖.**
- `gold.<부모>__changes`(운영에 약 20 [m])는 부모의 최종 이름을 따른다. 부모 계약이 짝으로 선언하고, 조정기가 빈 채로 만들며, 소비자는 전체
  빌드에서 시작한다.
- 지우고 다시 만들지 않는다: `silver.parcel_boundaries_202609_smoke`, `silver.building_register_units_smoke`,
  `gold.industrial_complex_boundary_served_smoke`, `silver.map_edit_ledger_smoke`(사람의 편집이 없음을 확인한 뒤), 모양 원장에만 있는
  `gold.building_resources`·`gold.parcel_public_sale`·`gold.unit_price_panel`(운영에 있으면), 탐침·빈 네임스페이스 `tiles_slice_proof`,
  `sail_probe`, `dist_probe`, `w75b_commit_probe`, `sqlmesh__w81d_sqlmesh_probe`, `staging`, `intermediate`. `gongzzang_silver.court_auction_property`
  는 사장이 2026-11-27 에 지웠다(W111).

### 6. 카탈로그 밖의 경로

#### 6-1. 경로 지도

| 지금 | 새 | 언제 |
| --- | --- | --- |
| `onbid/media/sha256/<h>.<ext>` | `media/display/sha256/<h>.<ext>` | 이 ADR 의 이동(§6-2) |
| `onbid/media/sources/sha256/<h>.<ext>` | `media/received/sha256/<h>.<ext>` | 같음 |
| `onbid/media/renditions/r<n>/<h>/<w>.<fmt>`, `…/r<n>/<h>.json` | `media/rendition/ladder=<n>/<h>/<w>.<fmt>`, `…/ladder=<n>/<h>.json` | 같음. 사다리는 복사한다(같은 바이트, 다시 굽지 않는다) |
| `courtauction-go-kr/sale-specifications/sha256/<h>.pdf` | 보류(§6-3) | 사장 결정 뒤 |
| `gold/industrial-complex/profiles/<uuid>.json` | `serving/industrial_complex/profile/<uuid>.json` | 이 ADR 의 이동 |
| `gold/industrial-complex/index.json`, `…/index-history/<g>.json` | `serving/industrial_complex/index/current.json`, `…/index/<g>.json` | 같음 |
| `silver-handoff/<원천 id>/…` | `work/<레인>/run=<id>/…`(관리 규칙으로 만료) | 같음: 쓰는 쪽·읽는 쪽을 바꾼 뒤 옛것을 지운다. 레인마다 다음 판을 Bronze 에서 한 번 다시 내보낸다 |
| `serving/**/by-pnu/v<n>/…`(필지별 문서) | 지운다(게이트웨이는 묶음만 서빙, ADR-0274) | 이 ADR 의 지움. 레인마다의 `manifest.json`·`manifest-history/` 는 서빙 다시 짓기까지 그 자리에 남는다(게이트웨이가 읽는다) |
| `serving/<lane>/…/packs/<section>/g<n>[/p<n>]/<unit>.pack` | `serving/<product>/pack/<section>/generation=<n>/[patch=<n>/]<unit>.pack` | 서빙 다시 짓기(N-serving). Worker 패턴은 영역의 틀에서 생성한다 |
| `collector-state/<lane>/` | `control/collector_state/<lane>/` | ADR-0623 쓰기 범위를 생성할 때 |
| 타일 버킷 `gold/vector-tiles/releases/…` | `vector_tile/release/<unit>-<uuid>.pmtiles` | 다음 타일 릴리스(타일 버킷은 그대로, ADR-0039 결정 3) |
| `__r2_data_catalog/…` | `tables/<ns>/<table>/`(Lakekeeper) | 구성 요소마다 A1, 옛 뿌리는 A9 |
| `landing/provider=vworldkr/`, `gold/_smoke/`(코드에만 있다) | 선언하거나 지운다 | G6 의 보고가 이름한다 |

#### 6-2. 한 번의 이동

1. **증거 목록 먼저.** 옮기거나 지울 객체마다 옛 키·새 키·크기·ETag·마지막 수정 시각을 gzip JSON Lines 로
   `control/evidence/r2_move/<날짜>-<주제>.jsonl.gz` 에 만들기만으로 쓴다(W111 의 `control/evidence/r2-cleanup/` 꼴). 내용은 담지 않는다.
2. **복사는 서버 쪽, 만들기만.** CopyObject 에 `cf-copy-destination-if-none-match: *` 를 보낸다. R2 는 표준 `If-None-Match` 를 CopyObject 에서
   조용히 무시하고 덮어쓴다(W108 3e).
3. **확인.** 새 키마다 크기와 ETag 가 옛 것과 같다(5 GiB 아래의 단일 CopyObject 는 MD5 ETag 를 지킨다, W108 3c). 내용 주소 키는 표본을 내려받아
   sha256 이 키와 같은지 본다. 개수·바이트가 증거 목록과 같다.
4. **정지는 코드로.** 쓰는 쪽과 읽는 쪽(Worker 패턴, 웹 상수, ADR-0623 쓰기 접두)이 같은 PR 에서 새 틀로 바뀐다. 쓰는 쪽은 옛 접두를 더는 모른다.
   배포 뒤 옛 접두에 새로 생긴 객체만 한 번 더 복사한다(내용 주소라 다시 복사해도 같은 바이트다).
5. **지우기는 관리 규칙(lifecycle)으로**, 접두를 정확히 적는다. 목록이 0 이 되면 규칙을 지운다. Bronze 의 30 일 잠금은 `bronze/` 에만 있다.

#### 6-3. 매각물건명세서 PDF 는 보류한다

사장은 공개로 정했지만 그 전제는 "어차피 공개된 문서"였다. F14 가 점유자 이름을 가리지 않음을 보였으므로, 그 결과가 사장에게 돌아갔다
(코디네이터 2026-11-28). 결정까지 PDF 는 그 자리(`courtauction-go-kr/sale-specifications/`)에 있고, 영역은 `held` 로 선언하며, Worker 는
지금처럼 서빙하지 않는다. 공개로 정하면 `media/document/sha256/<h>.pdf` 로, 비공개로 정하면 Bronze 쪽의 서빙하지 않는 영역으로 간다.

#### 6-4. 데이터로 저장된 키 문자열(옮기기 전의 목록)

2026-11-28 운영 Postgres 를 읽기 전용으로 센 것 [m] 과 W108 §1-5.

| 어디 | 무엇 | 이동 뒤 |
| --- | --- | --- |
| `serving_postgis.parcel_boundary_publication`·`parcel_boundary_mirror`(각 약 39.9M 행) | `source_object_key` = `silver-handoff/vworldkr__parcel/…` | 매달린 계보 문자열이 된다. 그 구성 요소의 전환이 새 Silver 에서 다시 싣는다(출시 전, 코디네이터 수락 2026-11-28) |
| `catalog.source_record` 1 행 | `silver-handoff/…` | 매달린다. append-only 기록이라 고치지 않는다 |
| 산단 색인 `index.json` 과 그 이력 | 단지마다 `profile_object_key` | 옮길 때 새 세대의 색인을 새 키로 쓴다. 옛 이력 파일은 옛 키를 기록으로 갖는다 |
| outbox `gold_pointer.published.v1`, 미디어 | `gold/industrial-complex/`·`onbid/media/` 를 가진 outbox 행 0 [m]. 미디어 키는 데이터에 없다: 문서는 `sha256`·`media_format` 만 싣고 웹이 상수로 URL 을 짓는다 | 웹 상수와 Worker 패턴만 바뀐다 |
| `catalog.bronze_object.object_key`(38,435), outbox 366 행, `serving_postgis` 행정경계 5,067·산단경계 1,343 행, Iceberg 열 `bronze_object_key`(Silver 건축물대장 다섯 표)·`boundary_object_key`(`gold.complex_catalog`) | `bronze/…` | 이 이동은 Bronze 를 건드리지 않는다. N2 가 다룬다 |
| `catalog.file_asset` 28 행 | 타일 버킷 `gold/vector-tiles/…` | 다음 타일 릴리스 |

### 7. 검사

새 이름은 처음부터 fail 이다. 옛 이름은 `naming.renames` 에 있을 때만 남는다: 그 목록이 일 목록이고, 전환 PR 마다 줄어든다. 각 검사가 막는
실제 사고(ADR-0001):

| 검사 | 거부하는 것 | 어디서 | 막는 사고 |
| --- | --- | --- | --- |
| G1 `scripts/catalog/declared_definitions.py check` | 꼴 밖의 제공기관 id, 겹치는 id·도메인, 등록부에 없는 제공기관의 slug, 꼴 밖의 그래프 표 이름 | CI, 훅 | `rtmolitkr` 가 근거 없이 코드에만 생겨 고아 객체 98 개를 남겼다(ADR-0032, W106) |
| G2 `lakehouse-domain/tests/naming.rs` | 꼴 밖의 이름, silver·gold 밖 네임스페이스, 규칙 7 의 금지 낱말, 생성기가 만들지 않은 역할 접미사, `grain` 없음, 목록에 없는 `s` 끝 낱말 | cargo | 운영에 smoke 표 넷(F10), 평이한 이름의 Gold 가 얼어붙은 표를 가리킨다(F7) |
| G3 조정기 `validate()` | G2 를 넘지 못하는 표 만들기(표는 조정기만 만든다) | 매 apply | 시험 표가 운영 네임스페이스에 생겼다(F10) |
| G4 `scripts/guard/names-are-declared-once.sh`(`the-contract-names-where-its-objects-live.sh` 를 넓힌다) | 선언된 표나 `renames` 로 풀리지 않는 `"<ns>.<table>"` 글자(.rs/.py/.sql/.ts), 생성 모듈·빌더 밖의 버킷 이름·영역 뿌리·제공기관 도메인·`bronze/` 접두 | CI | 등록부는 `media/` 를 기대하는데 쓰는 쪽이 `onbid/media/` 에 써서 `allows_object_key_for_asset` 이 실물을 거부했다(round 2 사실 3) |
| G5 Bronze 쓰기 경계 | 등록부 밖 slug, 잎의 실행 도장(`\d{8}t\d{6}z`), 뜻으로 찾은 예약 파티션 키(`collected_*`, `ingest_*`) | 매 쓰기 | 재시도가 새 키·새 장부 행 3,969 개를 만들었다(F12) |
| G6 드리프트 | 선언 밖 표·네임스페이스(조정기 A6, fail), 선언 밖 접두(`r2_inventory_audit` `classify_key` 를 영역으로, 매일, 보고 뒤 fail) | 실행 | 지운 표의 폴더 19.1 GB 와 잔해 접두를 손으로 한 조사에서야 찾았다(W106) |
| G7 semgrep | `rename_table`(이름 바꾸기는 새 표와 복사다), 레인 안의 키 파싱(`bronze-lane-writes.yml` 을 넓힌다) | CI | Basin 의 rename 은 파일을 옮기지 않아 경로가 낡는다(W108 2b). 법원 수집기가 키에서 도장을 읽어 차례를 정했다(F12) |

단수는 정규식만으로 확인할 수 없다. G2 의 `s` 검사와 필수 `grain` 이 기계의 몫이고, 나머지는 검토의 몫이다.

### 8. 순서

| # | 걸음 | 언제 | 증거 | 되돌리기 |
| --- | --- | --- | --- | --- |
| N1 | 등록부 릴리스, 데이터 이동 없음: §1 의 블록과 생성기, `source_slug()` 가 등록부를 읽음, 손으로 지은 `"<id>__"` 자리가 `source_slug()` 를 부름, G1~G7 | 첫 A1 전 | CI; 생성된 쓰기 접두가 오늘 것과 바이트까지 같다 | revert |
| N-move | §6-1 의 "이 ADR 의 이동"과 지움 | N1 과 함께 또는 뒤 | §6-2 의 증거 목록과 확인 | 옛 접두는 확인 뒤에만 지운다 |
| N2 | Bronze 시기(§4, 실행 도장 3,969, 잔해 id), 별도 작업 | 첫 A1 전, ADR-0616 RDS 이전 전 | 모든 레인 재실행이 객체 0·장부 행 0·`source_catalog` 행 0 을 쓴다; 해시로 장부 ↔ 객체 일대일 | 증거 목록에서 역 `UPDATE` |
| N3…n | 구성 요소마다 A1 전환 PR: 최종 이름이 계약과 모든 독자에, 조정기가 `tables/<ns>/<최종>/` 을 만든다, 판 표가 실체 이름을 갖고 `Replaced` 짝은 만들지 않는다, `__changes` 는 빈 채로, 열 짝을 고친다, `serving_postgis` 를 다시 싣는다, `silver.parcel_boundary` 의 Postgres CHECK·트리거를 옮긴다(F11), 제 `renames` 항목을 지운다 | A1 | 행 수·키 유일성·열 해시가 Basin 과 같다; 옛 꼴 값 0 | 설정을 Basin 으로 |
| N-serving | 서빙 다시 짓기와 다음 타일 릴리스 | 서빙 다시 짓기 | 게이트웨이 카나리아 | 게이트웨이 뿌리 되돌림 |
| N-last | A9: Basin 끔, `__r2_data_catalog/`·`renames`·옛 21 표 지움, 모든 검사 fail, 잠금이 지난 옛 Bronze 키 지움 | 모든 전환 뒤 | 선언 밖 접두·네임스페이스·표 0 | 없음 |

### 9. 다른 ADR 에서 바뀌는 것

- ADR-0032: 대체된다. 제공기관 id 는 등록부의 키이고 도메인은 속성이다. 값은 그대로다(옵션 K). D5(`mixed_public_source`)·D6·D7 은 그대로 남는다.
- FP-ADR-0014: D4 의 키 꼴은 그대로이고(규칙 3 의 예외), 잎에 실행 도장이 없다. D6 의 식별자는 범위 밖 그대로다.
- ADR-0039: 결정 1 의 자리는 `serving/industrial_complex/profile/<uuid>.json` 이다. 결정 3(타일 버킷)은 그대로다.
- ADR-0242: 세대 표시는 `generation=<n>/patch=<n>` 이다(서빙 다시 짓기에서). 7 일 규칙은 그대로다.
- ADR-0286: 표 이름은 쌓는 방식을 말하지 않는다. `row_versions` 만 말한다.
- ADR-0564: 전환에서 판을 쌓는 표가 실체 이름을 갖고, `Replaced` 짝은 다시 만들지 않는다. "이름이 날 질의자에게 경고한다"는 이유는 독자
  가드(`current_rows`)가 맡는다.
- ADR-0616 §3-1: Bronze 키의 레인별 모양은 그대로이되, 잎의 실행 도장은 N2 에서 없앤다.
- ADR-0623: 쓰기 접두는 손으로 쓰지 않고 `naming.areas` 에서 생성한다.
- ADR-0624: 조정기의 표 목록은 전환한 구성 요소의 계약에서 생성한다. 네임스페이스는 `silver`·`gold` 그대로다.

## Consequences

- 한 가지 이름은 한 곳에서만 고친다. 제공기관 도메인이 바뀌어도 키는 그대로다.
- 이 ADR 의 이동이 R2 최상위에서 `onbid/`, `gold/`, `silver-handoff/` 를 없앤다. `courtauction-go-kr/` 는 §6-3 의 결정까지, `collector-state/` 는
  ADR-0623 범위 생성까지 남는다.
- 비용(계산): 미디어 복사 약 3.15M CopyObject(Class A 4.50 USD/백만) ≈ 14.2 USD, 확인 HEAD 약 3.2M(Class B 0.36 USD/백만) ≈ 1.2 USD, 지우기는
  관리 규칙이라 요청 비용이 없다. 저장은 복사가 끝나고 옛 접두가 지워질 때까지 겹친다(305 GB × 0.015 USD/GB-월 ≈ 4.6 USD/월, 일할).
  by-PNU 필지별 문서(약 52M 객체)와 silver-handoff(약 238 GB)를 지우면 저장이 월 약 6.2 USD 준다(W106 크기).
- 레인마다 다음 Silver 판을 Bronze 에서 한 번 다시 내보낸다(옛 handoff 가 없다). 전체 다시 처리가 아니라 그 판 하나다.
- 산단 색인은 새 자리에서 세대를 하나 올려 새 키로 쓰고, 옛 이력은 기록으로 옮겨 둔다.
- 확인하지 못한 것: Lakekeeper 가 소프트 삭제한 이름을 같은 경로에 다시 만드는지(U3), 운영에 있는 `__changes` 와 모양 원장만의 셋의 실제
  이름(U4), 제공기관별 장부 행 수(U2).

## 기각한 대안

| 대안 | 왜 |
| --- | --- |
| 호스트에서 파생하는 id 를 두고 "한 번 쓰면 얼린다"만 더함(ADR-0032 유지) | 도메인이 바뀌면 다시 키를 바꿔야 하고, 제공기관마다 철자가 둘 이상 남는다 |
| 읽기 쉬운 새 id(옵션 R: `vworld`, `court_auction` …) | 사장이 K 를 골랐다: Bronze 38,798 객체·1,038 GB 의 키 바꾸기 없이 id 를 데이터로 만든다 |
| 내용 주소 Bronze 키(`bronze/<provider>/sha256/<hex>`) | 레이크 원본 영역에 권하는 출처가 없고, 장부의 유일성을 옮기며, Bronze 를 둘러볼 수 없게 한다 |
| 복수 표 이름(dbt) | 162 기본 이름 중 143 이 이미 단수다. 복수면 약 137 표를 바꾼다. GitLab 은 단수다 |
| `__versions` 짝 표 | 새 카탈로그에서는 비계가 할 일이 없고(F6·F7), 평이한 이름이 지금 독자를 속인다 |
| `reference`·`intelligence` 네임스페이스(ADR-0624 고침) | 네임스페이스는 보안 경계가 아니고, 네 목록 중 셋에 이미 없다(F6) |
| `g<N>/p<N>` 그대로 | 서빙 다시 짓기에서 공짜로 바뀌고, `p` 가 "part" 로 읽힌다 |
| 0 채운 `generation-000012` | 순서는 manifest 가 말한다(ADR-0242). Hive `key=value` 가 표준이다 |
| 모든 표 글자를 semgrep 으로 거부 | 글자 약 2,200 개를 한 번에 바꾼다. G4 의 풀이 검사가 같은 부류를 덜 흔들며 닫는다 |
| 이름 길이 48 자 상한 | 온전한 낱말이 먼저다. 엔진은 255 자까지 받는다 |
| 사다리를 원본에서 다시 굽기 | 같은 바이트를 굽는 데 원본 218 GB 를 다시 읽는다. 복사가 더 싸고 확인도 같다 |
