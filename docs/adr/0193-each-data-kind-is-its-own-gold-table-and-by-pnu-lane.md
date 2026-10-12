# ADR 0193: 자료 종류마다 Gold 표 하나와 by-PNU 레인 하나 — 묶음 패널을 종류별로 푼다

- Status: Proposed
- Date: 2026-10-12
- Builds on: [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md) (법정동 묶음),
  [ADR-0151](./0151-building-packs-serve-each-document-whole.md) (묶음 하나 = 문서 하나),
  [ADR-0163](./0163-a-large-legal-dong-pack-is-split-into-parts-read-whole.md) (큰 동의 조각),
  [ADR-0164](./0164-panel-gold-is-written-in-pnu-order-and-a-shard-reads-only-its-files.md) (PNU 순서 쓰기),
  [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md) (바뀐 PNU 만 다시 만들기)
- Aligns with: ADR-0189 (Silver append 경로, PR #425), ADR-0190 (호실 가격 분리와 공용 첫 발행 관문·레인 계약),
  ADR-0191 (Silver 변경 기록) — 모두 진행 중

## Context

오늘 by-PNU 서빙은 레인 둘이다. 둘 다 여러 종류의 자료를 한 행에 묶는다.

| Gold 표 | 행 | 묶인 종류(Silver 입력) |
|---|---|---|
| `gold.parcel_panel` | PNU 당 1, 39,861,511 행 | 용도지역(`silver.land_use_plan` + `silver.land_use_zone_code`), 공시지가(`silver.land_individual_price`), 토지특성(`silver.land_characteristic`), 임야대장(`silver.land_forest_ledger`), 토지이동이력(`silver.land_transfer_history`), 대지권(`silver.land_right_registration`), 계보 이어받기(`silver.parcel_lineage`) |
| `gold.building_panel` | PNU 당 1, 약 595만 행 | 표제부(`silver.building_register_titles`), 층(`silver.building_register_floors`), 호(`silver.building_register_units`), 호 면적(`silver.building_register_unit_areas`), 호 공시가격(`silver.unit_official_price`) |

묶음의 비용(측정):

- 종류 하나의 월간 릴리스가 묶음 전체를 다시 만든다. 표제부 릴리스 하나가 `gold.building_panel` 전량 39–40분을
  불렀다(ADR-0180). 증분(ADR-0180)도 바뀐 PNU 의 **모든 종류**를 다시 직렬화한다: 새 가격 빈티지 하나가 거의 모든
  아파트 PNU 의 건물·호·가격 JSON 전체를 다시 쓰게 했고, 2026-10-11/12 에 힙 OOM 이 반복됐다
  (`gold_incremental.py` `Stages` 주석, PR #423).
- 묶음 행의 최대 크기는 가장 무거운 종류가 정한다. 건물 행은 최대 1,270만 자였고(2026-10-11) 벡터 Parquet 읽기의
  2 GiB Arrow 버퍼를 넘었다(`building_panel_silver_to_gold.py` `apply_gold_read_properties`).
- 필지 Gold 전량은 20g 상한에서 18.0 GiB 를 썼고(계보 이어받기 없이), 계보를 싣는 경우는 아직 못 쟀다
  (`gold-panel-rebuild.contract.json` `unmeasured_inputs`).
- 필지 묶음은 법정동 묶음 중앙값 1.67 MB, 256 KiB 이하가 3% 뿐이라 첫 읽기 지연 관문(나)을 통과하지 못했고 면제로
  열었다(ADR-0162).
- 화면은 이미 종류별로 나뉘어 있다. 첫 화면(`summary.tsx`)은 지목·용도지역 첫 이름·공시지가 셋만 그린다
  (`lib/api/parcels.ts` `toParcelInfo`). 건물·층은 버튼을 눌러야 열리는 별도 보기이고, 호 목록과 호 가격은 접힌
  `<details>` 안이다. 그런데 응답은 묶음이라 첫 화면이 대지권 200행·토지이동이력 전부까지 내려받고, 건물 보기와 층
  보기는 같은 건물 문서를 두 번 받는다(쿼리 키가 다르다). 토지이동이력·대지권·임야대장은 지금 화면 어디에도 그려지지
  않는다.

소유자 결정(2026-10-12): 자료 종류마다 전체 PNU 에 걸친 자기 Gold 표와 법정동 묶음 by-PNU 레인을 갖는다. 바뀌면 그
종류의 바뀐 행만 다시 만들고 그 종류의 바뀐 동 묶음만 다시 굽는다. 화면은 종류별로 따로 부른다. 새 레인에 PNU 당
객체는 금지(전량 4천만 PUT), 동 묶음만 쓴다.

### 종류별 목록(현황)

크기는 직렬화 JSON(압축 전) 추정이다. 근거: 입력 열 목록(`BUILD_COLUMNS`)과 행 수. 측정은 결정 6항의 첫 PR 이 한다.

| 종류 | Silver 입력 | 갱신 주기(근거) | PNU 당 크기 추정(보통 / 최대) | 첫 화면 |
|---|---|---|---|---|
| 용도지역 판정 | `land_use_plan`(약 2.6억 행 추정, PNU 당 6.5 행) + `land_use_zone_code`(1,270 행) | 월간 빈티지(`vworld-land-use-plan-source-objects.json`) | 0.3 KB / 수 KB (포함코드 1·2 만) | 예(첫 이름) |
| 공시지가 | `land_individual_price`(빈티지당 약 4천만) | 연 1–2 회(ADR-0085: 빈티지 2) | 0.1 KB / 0.1 KB | 예 |
| 토지특성 | `land_characteristic` | 비정기(ADR-0087: 빈티지 3) | 0.2 KB / 0.2 KB | 예(지목) |
| 임야대장 | `land_forest_ledger` | 월간(ADR-0088: 12 빈티지) | 0.15 KB, 임야 PNU 만 | 아니오(미표시) |
| 토지이동이력 | `land_transfer_history`(1.21억 행, PNU 당 약 3) | 월간(ADR-0089: 9 빈티지) | 0.6 KB / 상한 없음(수백 건 PNU 존재) | 아니오(미표시) |
| 대지권 | `land_right_registration`(2,780만 행, 42.8만 PNU) | 월간(ADR-0091: 12 빈티지) | 약 12 KB(평균 65 행) / 200 행 상한 약 36 KB | 아니오(미표시) |
| 건물 표제부 | `building_register_titles`(806만) | 월간(HUB) | 0.4 KB × 동 수 | 아니오(건물 보기) |
| 층 | `building_register_floors` | 월간(HUB) | 동당 층 수 × 50 B | 아니오(층 보기) |
| 호 | `building_register_units`(1,940만) + `building_register_unit_areas`(1.14억) | 월간(HUB) | 0.35 KB × 호 수, 대단지 MB 단위 | 아니오(접힘) |
| 호 공시가격 | `unit_official_price`(빈티지당 약 1.9억) | 월간(HUB) | 호당 약 10 건 × 40 B; 묶음 행 최대 1,270만 자의 주성분 | 아니오(접힘) — ADR-0190 |
| 계보 이어받기 | `parcel_lineage` | 필지 판과 함께 | 섹션별 출처 PNU | 해당 종류에 붙음 |

토지대장은 Silver 표가 없다(오픈 API 전용, `public-source-endpoint-catalog.v1.json`). 이 결정의 대상이 아니다.

## Decision

1. **종류 = Silver 등록부 하나(또는 그 위의 판정 하나)가 PNU 에 붙인 사실의 묶음.** 종류마다 Gold 표 하나, by-PNU
   레인 하나를 둔다. 같은 사실은 정확히 한 종류, 한 레인에만 있다(같은 값을 두 레인에 싣지 않는다).
   - 묶음으로 남기는 예외는 둘이고 이유를 남긴다:
     - **호 = 호 + 호 면적.** 전용면적은 호의 속성이고 같은 목록 줄에 그려진다. 둘 다 HUB 월간이고 같은 등록 키로
       묶인다. 나누면 화면이 호 id 로 다시 합쳐야 한다.
     - **용도지역 판정 = 용도지역 원장 + 코드표.** 판정은 두 입력의 함수다. 코드표만의 레인은 사실이 아니다.
   - 첫 화면용 "요약 레인"(지목·용도지역·공시지가를 한 문서로)은 만들지 않는다. 같은 값이 요약과 원 종류에 두 번
     있게 되고, 주기가 다른 세 종류가 서로를 다시 굽게 만든다. 첫 화면은 세 레인을 병렬로 부른다(4항). 첫 화면
     p95 가 목표(7항)를 넘는 것이 측정되면 다시 연다.
2. **종류 목록과 이름.** 키는 모두 `pnu`, 법정동(PNU 앞 10 자리) 묶음, ADR-0163 조각 규칙 그대로.

   | 레인(unit) | Gold 표 | 문서 본문 칸 | PNU 범위 |
   |---|---|---|---|
   | `parcel-zoning-by-pnu` | `gold.parcel_zoning` | `zonings` | 판정이 있는 PNU |
   | `parcel-price-by-pnu` | `gold.parcel_price` | `price` | 가격이 있는 PNU |
   | `parcel-characteristics-by-pnu` | `gold.parcel_characteristics` | `characteristics` | 특성이 있는 PNU |
   | `parcel-forest-ledger-by-pnu` | `gold.parcel_forest_ledger` | `forest_ledger` | 임야 PNU |
   | `parcel-transfer-history-by-pnu` | `gold.parcel_transfer_history` | `transfer_history` | 이력이 있는 PNU |
   | `parcel-land-rights-by-pnu` | `gold.parcel_land_rights` | `land_rights`, `land_right_total` | 대지권이 있는 PNU |
   | `building-titles-by-pnu` | `gold.building_titles` | `buildings`(층·호 없음) | 표제부가 있는 PNU |
   | `building-floors-by-pnu` | `gold.building_floors` | `floors`(건물 id 별) | 층이 있는 PNU |
   | `building-units-by-pnu` | `gold.building_units` | `units`, `unlinked_units` | 호가 놓인 PNU |
   | (ADR-0190) | (ADR-0190 의 호 가격 표) | ADR-0190 | ADR-0190 |

   - Gold 행 모양은 ADR-0190 의 호 가격 표와 같다: `pnu`, 본문 JSON 열, (필지 종류만) `attached_via`,
     `row_digest`(내용 열만, ADR-0099), `source_snapshot_id`, `published_at_utc`. 정렬 `pnu`, range 분포(ADR-0164).
   - 행은 그 종류의 내용이 있는 PNU 에만 있다. 빈 배열 행은 쓰지 않는다. 필지 종류의 PNU 우주는 지금처럼 서빙 판
     `silver.parcel_boundaries` 이고 계보 이어받기(`attached_via`)는 종류마다 따로 적용한다. 그래서 필지 판과 계보는
     모든 필지 종류의 입력이다.
   - 호가 놓이는 PNU 는 ADR-0190 의 `unit_placement`(`serving_pnu`)를 그대로 쓴다. 호 레인과 호 가격 레인은 같은 PNU
     에서 같은 호 id 로 만난다.
3. **생산자 = 종류마다 작은 모듈, 엔진은 하나.** 각 종류는 `BUILD_COLUMNS`·`build`·`affected_pnus`·`restrict_inputs`
   를 선언하고 `gold_incremental.build_or_merge`(ADR-0180)와, 들어오면 ADR-0191 의 변경 기록을 쓴다. 필지 종류의 변경
   키는 `pnu` 라 영향 PNU = 변경 기록의 키 + 계보 후계다. 사업 논리는 지금 생산자의 함수를 옮겨 쓴다(사본 금지):
   `parcel_panel_silver_to_gold.py` 의 섹션 함수와 `building_panel_silver_to_gold.py` 의 조립을 종류 모듈로 옮기고, 옛
   묶음 생산자는 퇴역 전까지 같은 함수를 부른다. 재생성 계약(`gold-panel-rebuild.contract.json`)에 종류마다 표를 하나씩
   올린다. 계획기(ADR-0139)가 입력이 안 바뀐 종류는 건너뛰므로 **안 바뀐 종류의 비용은 0** 이다.
   - `silver.land_use_zone_code` 의 `WHOLE_TABLE_INPUTS` 전량 재생성은 `gold.parcel_zoning` 에만 걸린다.
4. **서빙: 레인은 계약의 한 줄, Worker 는 호스트당 하나.** 레인 목록과 각 레인의 Gold 표·뿌리 경로·문서 스키마는
   ADR-0190 의 레인 계약 한 곳에 둔다. 굽기(`section_packs::bake`), 첫 발행 관문(ADR-0190, PNU 객체 없이 묶음으로
   첫 발행), 패치(ADR-0141), 감시·카나리아는 레인 이름만 받는다.
   - 경로: 필지 종류는 `catalog.perfectory.io/parcels/{kind}/by-pnu/{pnu}`, 건물 종류는
     `buildings.perfectory.io/buildings/{kind}/by-pnu/{pnu}`. 기존 `/parcels/by-pnu`·`/buildings/by-pnu` 는 퇴역까지 그대로.
     호스트마다 Worker 빌드 하나가 계약의 레인을 경로로 고른다(레인마다 Worker 를 늘리지 않는다).
   - 묶음 뿌리: `serving/parcels/{kind}/packs`, `serving/buildings/{kind}/packs`. 레인마다 자기 manifest·세대·패치.
   - 응답: 문서는 오늘과 같은 `Cache-Control: public, max-age=3600`, ETag, gzip 그대로 전달. **없음(404 absent) 도 같은
     세대 지문으로 캐시한다.** 묶음 색인이 그 동에 그 PNU 가 없음을 증명하므로 없음은 그 세대의 사실이다. 드문 종류
     (임야·대지권)는 대부분의 답이 없음이라, 없음을 `no-store` 로 두면 R2 읽기가 그대로 늘어난다. 지워진 PNU
     (tombstone)는 지금처럼 `no-store`.
   - 문서: `{schema_version: "foundation-platform.<kind>_by_pnu.v1", pnu, source: {table, snapshot}, <본문>}`.
5. **화면 조립(gongzzang web).**
   - 쿼리 키는 `["by-pnu", lane, pnu]` 하나로 통일한다. 보기마다 키를 따로 두지 않으므로 건물 보기와 층 보기가 같은
     문서를 두 번 받지 않는다. `staleTime` 30 분.
   - 첫 화면(`summary`): `parcel-zoning`·`parcel-price`·`parcel-characteristics` 세 요청을 병렬로, 각 줄은 자기 응답이
     오면 그린다. 404 absent 는 "자료 없음" 으로 그리고 오류로 다루지 않는다.
   - 건물 보기: `building-titles`. 동의 호 `<details>` 를 처음 열 때 `building-units`. 호의 가격 `<details>` 를 처음
     열 때 ADR-0190 레인(PNU 하나에 한 번).
   - 층 보기: `building-titles`(지상·지하 층수) + `building-floors`.
   - 토지이동이력·대지권·임야대장 보기는 지금 없다. 레인은 서빙하되 화면 추가는 제품 결정으로 따로 한다.
6. **전환은 종류 하나씩, 옛 묶음과 나란히.** 각 종류는 아래 순서를 다 거친 뒤에만 다음 종류로 간다.
   1. 측정(첫 PR, 읽기 전용): 묶음 Gold 의 섹션마다 PNU 당 직렬화 크기(원문·gzip) p50/p95/max, 비어 있지 않은 PNU 수,
      동 묶음 크기 분포를 내는 작업. 7항 목표의 크기 칸을 측정치로 바꾼다.
   2. Gold 표 + 생산자 + 재생성 계약 항목. 같은 Silver 판에서 새 종류 표의 본문이 묶음 표의 해당 섹션과 **모든 PNU 에서
      같아야** 한다(대등성 검사, 묶음이 아직 정본일 때만 의미가 있다).
   3. 레인 계약 항목 + 굽기 + 첫 발행 관문(ADR-0190) + 미리보기 호스트. 관문 (가)(전수 대등)·(나)(지연) 통과.
   4. 웹이 그 종류를 새 레인에서 읽도록 바꾼다. 옛 레인은 그대로 서빙한다.
   5. 모든 종류가 옮겨진 뒤에만 묶음을 퇴역한다: 재생성 계약에서 `gold.parcel_panel`/`gold.building_panel` 을 빼고,
      굽기를 멈추고, 옛 경로는 한 달 더 서빙한 뒤 Worker 경로와 R2 객체를 지운다(삭제는 별도 승인). 퇴역 전에는 묶음과
      종류 표를 둘 다 만든다 — 그 기간의 계산 비용은 감수한다.
   - 순서: 건물 쪽은 ADR-0190(호 가격)이 먼저다. 그 다음 **호 → 표제부 → 층 → 건물 묶음 퇴역**. 필지 쪽은 Gold 계산이
     무거운 순서로 **용도지역 판정 → 토지이동이력 → 대지권 → 임야대장 → 공시지가 → 토지특성 → 필지 묶음 퇴역**.
     첫 화면 셋(용도지역·공시지가·토지특성)이 다 나온 뒤에 첫 화면을 바꾼다.
7. **종류마다 측정 목표.** 운영 규모로 재고, 못 맞추면 그 종류의 전환을 멈추고 원인을 ADR 로 남긴다.
   - Gold: 월간 릴리스 하나의 증분 ≤ 10 분, 전량 ≤ 20 분(용도지역 판정은 전량 ≤ 30 분), 드라이버 16g 에서 OOM 0.
   - 굽기: 전량 세대 ≤ 30 분, 패치 ≤ 15 분.
   - 묶음: 조각 후 묶음 크기 p95 ≤ 256 KiB(통째 읽기 경로, ADR-0151), 최대 ≤ 256 KiB(조각 규칙, ADR-0163). PNU 문서
     하나가 gzip 256 KiB 를 넘는 종류는 굽기가 거부한다 — 측정(6-1)에서 넘는 PNU 가 나오면 그 종류는 문서 키를
     좁힌다(예: 호를 `pnu` + 건물 id 로) 그리고 이 ADR 을 개정한다.
   - 첫 화면: 세 요청 합 gzip p95 ≤ 2 KB, 첫 읽기 p50 ≤ 오늘 필지 레인의 관문 (나) 기준.
8. **비용 원칙.** 새 레인의 R2 쓰기는 동 묶음뿐이다(전량 세대 = 묶음 수 + 조각 색인 + manifest). 패치는 바뀐 동만.
   안 바뀐 종류는 쓰기 0. 돈이 드는 대량 작업(전량 굽기·관문 읽기)은 운영자가 금액을 보고 승인한 뒤에 돈다. 금액은
   공개 저장소에 적지 않는다.

## Consequences

- 월간 릴리스 하나가 그 종류의 Gold·굽기만 부른다. 가장 무거운 행(호 가격, 대단지 호 목록)이 다른 종류의 재계산을
  끌고 다니지 않는다. Gold 표가 묶음 2개에서 종류 9개(+ ADR-0190)로 늘고, 레인도 그만큼 늘어난다 — manifest·관문·감시는
  계약의 레인 목록을 돌므로 레인당 손 작업은 계약 한 줄이어야 한다(ADR-0190 의 공용 관문이 전제).
- 첫 화면 요청이 하나에서 셋이 된다. HTTP/2 한 연결 위의 병렬 요청이고 각 문서는 수백 바이트다.
- 퇴역 전까지 두 체계를 함께 만든다: Gold 계산·R2 저장이 일시적으로 늘어난다.
- 레인이 늘면 Worker 의 manifest 읽기가 레인 수만큼 는다(각 60 초 엣지 캐시). 측정 대상이다.
- 토지대장은 Silver 가 생길 때 같은 규칙으로 종류 하나가 된다.

### 기각한 대안

- **묶음 유지 + 증분만 개선**(ADR-0180/0191 만): 바뀐 PNU 는 여전히 모든 종류를 다시 직렬화하고, 행 최대 크기는 가장
  무거운 종류가 정한다. 2026-10-11/12 OOM 의 원인이 그대로다.
- **Worker 에서 종류를 합쳐 한 응답으로**: ADR-0151 이 4 섹션 합치기를 측정으로 기각했다(503 276 건, CPU p99 19.8 ms).
- **PNU 당 객체**: 레인 하나 전량에 4천만 PUT. 소유자가 금지했다.
- **첫 화면 요약 레인**: 1항.
- **레인마다 Worker 와 호스트**: 레인 9개면 배포·도메인·감시가 9벌이다. 경로로 고른다.

### 참고한 사례

- Kimball Group, *Dimensional Modeling Techniques* — "grain" 을 먼저 정하고 한 사실 표는 한 업무 과정·한 grain 을
  담는다. 종류 = grain 이 같은 사실의 묶음.
  <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>
- Stripe API `expand` — 기본 객체는 가볍게, 무거운 하위 객체는 요청할 때만. <https://docs.stripe.com/expand>
- PMTiles v3 — 한 파일 안의 색인 + 범위 읽기, 객체 수를 묶음 수로 줄인다(ADR-0147 의 묶음과 같은 발상).
  <https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md>
- RFC 9113 (HTTP/2) §5 — 한 연결 위의 병렬 스트림. 첫 화면 세 요청의 근거. <https://www.rfc-editor.org/rfc/rfc9113>
- Apache Iceberg `MERGE INTO`(copy-on-write) — 종류 표마다 바뀐 행만 합친다(ADR-0180).
  <https://iceberg.apache.org/docs/latest/spark-writes/>
- Cloudflare R2 가격표(Class A 쓰기 단위 과금) — 묶음 수가 비용을 정한다. <https://developers.cloudflare.com/r2/pricing/>
