---
status: current
owner: foundation-platform
doc_type: reference
last_reviewed: 2026-11-11
---

# 카탈로그 전수 처리 현황 (2026-11-11 실측)

| 항목 | 내용 |
|---|---|
| 질문 | "카탈로그에 있는 것 전부 실제로 처리하고 있나?" (W67, 측정만 — 코드 변경 없음) |
| 대상 | [`public-source-endpoint-catalog.v1.json`](./public-source-endpoint-catalog.v1.json) 의 endpoint **159개 전부**(지시서의 143 이후 늘었다) + 카탈로그 밖 원천 |
| 측정 시각 | 2026-11-11 06:40–07:30 UTC |
| 이 표는 | 그 시각의 실측 사진이다. 다시 재면 숫자가 바뀐다. 계약 정본은 위 JSON·`definitions/`·`jobs.v1.json` 이다 |

## 결론

| 단계 | 159개 중 | 비율 | 측정 방법 |
|---|---:|---:|---|
| 수집 중 (켜진 정기 작업 + Bronze 장부에 객체) | **118** | 74% | 운영 `catalog.bronze_object`·`ingestion_run`, 배포본 `jobs.v1.json` |
| Silver 표에 행 있음 | **65** | 41% | Trino `"<표>$snapshots"` 최근 1건의 `total-records` (메타데이터만) |
| Gold/서빙 됨 | **45** | 28% | 서빙 주소 GET 2회(200), by-PNU bake 저널. 이 중 2개는 지도 타일만이라 미측정 |
| 화면(web-preview 패널)에 보임 | **45** | 28% | 배포된 web-preview JS 묶음에 그 레인 읽기 코드가 있음 + 서빙 200 |

안 되는 114개(159−45)는 다섯 갈래다.

| 갈래 | 개수 | 내용 |
|---|---:|---|
| 금지·받을 수 없음 (일부러 안 함) | 25 | data.go.kr 건축물대장 API 10·실거래 API 12(대량 파일과 중복, ADR-0400), VWorld 토지대장 API 1(ADR-0359), 연도별 에너지 2(제공자가 파일을 안 냄) |
| Bronze 만 있고 Silver 변환 없음 (미구현) | 47 | 주택인허가 16·폐쇄대장 10·건축인허가 7·건축물대장 3(기본개요·오수·지역지구)·점검기관 1, VWorld 10(시도·통계경계 3·공인중개사·산단 5) |
| 고장 (작업은 도는데 실패) | 22 | 실거래 12(Gold OOM), 주변시설 입력 7(학교·대학·철도역·철도역 노선·도시철도역·공항·상권 — 철도역 CSV 한 줄이 run 전체를 떨어뜨림), 고속도로 접근점 1(Silver 는 있으나 같은 레인이 실패), 항만·행정동 연계 2 |
| Silver 까지만, 서빙 꺼짐·없음 | 7 | 법원경매 3(Gold 작업 한 번도 안 돎), 인구·세대수 2(contract_only), 법정동 참조 1(모든 레인의 키 해석용), 필지번호 변동연혁 1(계보 입력) |
| 수집 자체가 없음 | 13 | juso 11(작업은 돌지만 파일 0), ILIS 고시 상세·첨부 1(수집기 미배포), mois API 1(**설명 필요**) |

서빙 45개 안에도 주의할 것이 있다: ILIS 목록·고시·상세 3개는 2026-10-01 한 번 적재한 `silver.industrial_complexes` 가 산단 패널로 서빙될 뿐 운영 수집 작업이 없고(Bronze 장부 0), VWorld 토지이용구역은 규제 레인 Gold·bake 작업이 꺼진 채 수동 실행분이 서빙된다.

## 특별 확인 1 — 건축HUB `01 건축인허가`(17) · `02 주택인허가`(16)

| 묶음 | Bronze | Silver | PNU 연결 | 서빙·화면 |
|---|---|---|---|---|
| 01 건축인허가 17종 | **17/17** 수집(각 5객체, 최근 2026-10-20, 월 1판) | **10/17** (기본개요·동별·층별·대수선·공작물·철거멸실·가설·주차장·부설주차장·대지위치) | Silver `pnu` 칸 있음: 기본개요 1% 표본 65,456행 중 **98.5%** 가 PNU | 9종은 `parcel-building-permits` 레인, 철거멸실은 `parcel-demolitions` 레인 — 둘 다 **200**, 화면 "건축인허가"·"멸실" 섹션 |
| 01 중 Silver 없는 7종 | 수집됨 | 없음 | — | 호별개요·오수정화시설·전유공용면적·호별전유공용면적·지역지구구역·도로대장·주택유형 |
| 02 주택인허가 16종 | **16/16** 수집(각 5객체, 2026-10-20) | **0/16** | — | 없음. **주택인허가는 수집만 하고 아무것도 안 한다** |

허가·착공·사용승인 날짜(기본개요, 표본 1% = 65,456행):

| 칸 | 채워진 비율 | Gold 에도 있나 |
|---|---:|---|
| `permit_date` 허가일 | 99.4% | 있음. `gold.parcel_building_permits` 표본 43,107필지 중 착공일 있는 문서 24,766, 사용승인일 있는 문서 23,118 |
| `start_actual_date` 착공일(실제) | 51.5% | 있음(위) |
| `use_approval_date` 사용승인일 | 50.7% | 있음(위) |

착공·사용승인이 절반인 것은 아직 착공·준공 전인 허가가 섞여 있어서로 보이나, 원인은 이번에 확인하지 않았다. 웹 응답 스키마(`building-permits.generated.ts`)도 `permit_date`·`start_*_date`·`use_approval_date` 를 낸다.

## 특별 확인 2 — `mois_standard_code_open_api` ("설명 필요")

| 항목 | 실측 |
|---|---|
| 카탈로그 | `open_api_only`, `national_collection_allowed: false`, **`collection_decision` 칸 없음** — 159개 중 수집도 안 하고 안 하는 이유도 안 적힌 유일한 행 |
| 수집기·작업 | 없음(배포본 `jobs.v1.json` 에 없음, Bronze 장부에 `datagokr__legal_dong_code` 0) |
| 그래프 | `source-mois-standard-code` 가 `reference-legal-dong-code-snapshot` 을 먹인다고 적혀 있고 상태가 `implemented` — 실물과 다르다(입력은 수동, 수집기 없음이라고 노드 설명에 적혀 있음) |
| 같은 사실의 실제 원천 | `code-go-kr-legal_dong_code_table`: `legal_dong_code_changes` 작업이 매일 05:15 받아 Bronze 284객체(최근 10-28), `reference.legal_dong_code_snapshot` 106,790행 |
| 판단 | API 는 code.go.kr 전체 표와 같은 법정동 코드다. 카탈로그에 `collection_decision`(예: `api_duplicate_of_bulk` 또는 `file_covers_the_api`)을 적고, 그래프 노드 상태를 실물대로 고치는 후속 PR 이 필요하다(이 PR 은 측정 문서만) |

## 이번에 찾은 고장 (운영 저널·로그 실측)

| # | 무엇 | 증거 | 영향 |
|---|---|---|---|
| 1 | `data_go_kr_file_data` 11-10 run 실패 | `MALFORMED_CSV_RECORD` — 철도역 파일의 한 행(따옴표 안 쉼표·줄바꿈)에서 Spark 가 멈추고 run 전체 exit 1 | 공항·학교·대학·철도역·도시철도역·상권·항만·연계 Silver **0행**(표만 있고 스냅샷 없음) |
| 2 | `gold_parcel_nearby_rebuild` 실패 | `ValueError: source snapshot IDs must be positive signed-64 integers` — 입력 Silver 가 비어 스냅샷 ID 가 없다 | 주변시설 레인 첫 발행 못 함 → `parcels/nearby/by-pnu` **503** |
| 3 | `gold_parcel_real_transaction_rebuild` 11-10 21:58 실패 | 커널 `oom-killer`(serve-DataFrame 프로세스), 로그가 `collect at real_transaction_silver_to_gold.py:212` 직후 끊김. 원인 수정 PR 은 이번에 확인하지 못했다 | 실거래 Gold 표 없음, 레인 첫 발행 전 → `parcels/real-transaction/by-pnu` **503**. Silver 13,628,593행은 매시 쌓인다 |
| 4 | 자동배포 멈춤 | `foundation-autodeploy`: "the release holds jobs; waiting for the operator's units" (11-11 06:50) | 운영 릴리스는 `b55864aa`(#1270). 그 뒤 병합된 #1255(ILIS 고시 수집기)~#1271 미배포 |
| 5 | juso 전자지도 | 매일 run 은 성공하지만 `{"requests": 120, "files": 0}` | Bronze `jusogokr__*` 0, Silver 없음. 카탈로그 lane 은 아직 `manual_approval_bulk`/`waiting_on_owner_application`(ADR-0475 이전 문구) |
| 6 | 법원경매 Gold | `gold_court_auction_rebuild`·`by_pnu_court_auction_bake` 꺼짐("Never run", #978 미병합) | Silver 187,790 물건·27,665 매각결과·사진 14,905 는 매일 쌓이지만 `parcels/court-auction/by-pnu` **503**. 화면 탭 코드는 배포돼 있으나 데이터가 없다 |

## 카탈로그 밖 원천·표

| 원천/표 | 수집 | Silver/Gold | 서빙·화면 | 비고 |
|---|---|---|---|---|
| 공매 사진(공용 미디어 저장소) | `onbid_public_auction_collect` 매시 | `silver.onbid_real_estate_item_photo` 219,214 · 11-11 | `public-auction` 200(문서 14,898B, 사진 칸 포함) → 공매 탭 갤러리 | ADR-0296·0429 |
| 법원경매 사진 | `court_auction_collect` | `silver.court_auction_item_photo` 14,905 · 11-11 | 없음(위 #6) | ADR-0429 |
| ILIS 고시 첨부(문서) | 수집기 #1255 미배포 | 없음 | 없음 | 읽기·추출은 intelligence platform 이 나중에(ADR-0484). intelligence 쪽 고시 수집 작업은 운영에 없음 |
| 지도 편집 원장 | `map_edit_fold_*` 매시 | `silver.map_edit_ledger` 스냅샷 없음(편집 0건) | 행정·산단 타일에 접힘 | 내부 원천 |
| 공매 옛 Gold `gold.parcel_public_sale` | — | 4,821 · 10-23 이후 멈춤 | 쓰는 레인 없음 | `parcel_public_auction` 개명 전 표(정리 후보) |
| 옛 공짱 Silver `gongzzang_silver.court_auction_property` | — | 47,929 · 07-09 이후 멈춤 | 쓰는 레인 없음 | 정리 후보 |
| `gold.building_resources` | — | 스냅샷 없음 | — | 빈 표(정리 후보) |

## 측정 방법과 한계

- **수집 중**: 원천 묶음의 정기 작업이 배포본 `/opt/foundation-platform/current/orchestration/jobs.v1.json` 에서 `enabled` 이고, 운영 Postgres `catalog.bronze_object` 에 그 `source_slug` 객체가 1개 이상. "최근 성공"은 `ingestion_run.status='succeeded'` 의 최근 `finished_at`. 월·연 단위로 새 판이 나오는 원천은 최근 날짜가 오래돼도 정상이다(예: VWorld 산단·통계경계는 07-02 판이 마지막).
- **Silver**: Trino `r2` 카탈로그 각 표의 `$snapshots` 최근 1건(메타데이터 조회, 데이터 스캔 없음). 행 수는 그 스냅샷의 `total-records` 로, append-only 표는 여러 판 합계다. `query_max_memory=1GB`·`query_max_execution_time` 상한을 걸었고 날짜 표본은 `TABLESAMPLE BERNOULLI(1) … LIMIT` 로만 읽었다.
- **Gold/서빙**: 파이프라인 그래프의 Silver→Gold→서빙 간선으로 레인을 찾고, 각 by-PNU 레인은 Gold 표본 필지 하나로 `catalog.perfectory.io`·`buildings.perfectory.io` 에 GET 두 번(두 번째 응답 기준). 산단 프로필은 `gold/industrial-complex/index.json` 200. 호별 공시가는 bake 저널로만 확인했다. 지도 타일(필지·행정·산단·용도지역)은 이번에 재지 않았다(`미측정`).
- **화면**: 배포된 web-preview 의 JS 묶음에 그 레인의 읽기 경로(`…/by-pnu/`)가 있는지와 서빙 200 을 함께 봤다. 브라우저로 패널을 직접 눌러 보지는 않았다.
- 법정동·필지경계·건물 표제부처럼 거의 모든 레인의 키 해석에 쓰이는 표는 "키로 쓰임"을 서빙으로 세지 않고, 내용으로 보이는 레인만 적었다.
- 공개 저장소이므로 표본 필지 PNU 는 적지 않았다.

## endpoint 별 표 (159행)

열: 수집 중?(정기 작업 · 장부의 최근 성공 run) · Bronze 객체 수 · 최근 객체 날짜 · Silver 표 · 최근 스냅샷 행 수 · 날짜 · 서빙 레인과 실측 상태 · 화면 섹션 · 안 되는 이유.

### `building_register_open_api` — 10개 (수집 0 · Silver 0 · 서빙 0 · 화면 0)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `data-go-kr-building-register-getBrTitleInfo` | 건축물대장 표제부 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-building-register-getBrBasisOulnInfo` | 건축물대장 기본개요 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-building-register-getBrFlrOulnInfo` | 건축물대장 층별개요 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-building-register-getBrExposPubuseAreaInfo` | 건축물대장 전유공용면적 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-building-register-getBrHsprcInfo` | 건축물대장 주택가격 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-building-register-getBrExposInfo` | 건축물대장 전유부 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-building-register-getBrWclfInfo` | 건축물대장 오수정화시설 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-building-register-getBrRecapTitleInfo` | 건축물대장 총괄표제부 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-building-register-getBrAtchJibunInfo` | 건축물대장 부속지번 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-building-register-getBrJijiguInfo` | 건축물대장 지구지역구역 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |

### `building_hub_bulk` — 59개 (수집 57 · Silver 20 · 서빙 20 · 화면 20)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `hub-building-building_register` | 건축물대장 기본 정보 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_register_main` | 건축물대장 표제부 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_register_titles` 8,064,354 · 2026-10-20 | 예 · building-permits 200, 건물 200 | 예 · 건물·층·호, 건축인허가 |  |
| `hub-building-building_register_master` | 건축물대장 총괄표제부 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_register_master` 621,193 · 2026-10-30 | 예 · register-masters 200 | 예 · 총괄표제부 |  |
| `hub-building-building_register_apartment_price` | 건축물대장 공동주택 가격 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_register_apartment_price` 606,258,106 · 2026-10-20 | 예 · 건물 200, 호별 공시가 bake 기록 | 예 · 건물·층·호, 건물·호 공시가 |  |
| `hub-building-building_register_exclusive_unit` | 건축물대장 전유부 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_register_exclusive_unit` 59,530,225 · 2026-10-20<br>`silver.building_register_units` 19,860,453 · 2026-10-20 | 예 · 건물 200, 호별 공시가 bake 기록 | 예 · 건물·층·호, 건물·호 공시가 |  |
| `hub-building-building_register_floor_overview` | 건축물대장 층별개요 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_register_floors` 42,380,380 · 2026-10-20 | 예 · 건물 200, 호별 공시가 bake 기록 | 예 · 건물·층·호, 건물·호 공시가 |  |
| `hub-building-building_register_sewage_facility` | 건축물대장 오수정화시설 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_register_sub_parcel` | 건축물대장 부속지번 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_register_sub_parcel` 5,492,042 · 2026-11-04 | 예 · sub-parcels 200 | 예 · 부속지번 |  |
| `hub-building-building_register_district_zone` | 건축물대장 지역지구구역 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_register_exclusive_common_area` | 건축물대장 전유공용면적 파일 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_register_unit_areas` 114,461,830 · 2026-11-07 | 예 · 건물 200, 호별 공시가 bake 기록 | 예 · 건물·층·호, 건물·호 공시가 |  |
| `hub-building-building_gas_usage` | 건물 가스 사용량 파일 | 예 · source_sweep 매일 · 최근 성공 2026-11-11 | 7 · 2026-11-11 | `silver.building_energy_monthly_gas` 2,255,289 · 2026-11-06 | 예 · energy 200 | 예 · 에너지 |  |
| `hub-building-building_electricity_usage` | 건물 전기 사용량 파일 | 예 · source_sweep 매일 · 최근 성공 2026-11-11 | 7 · 2026-11-11 | `silver.building_energy_monthly_electricity` 7,857,034 · 2026-11-06 | 예 · energy 200 | 예 · 에너지 |  |
| `hub-building-building_permit_basis_outline` | 기본개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_basis_outline` 6,527,521 · 2026-10-30 | 예 · building-permits 200 | 예 · 건축인허가 |  |
| `hub-building-building_permit_building_outline` | 동별개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_building_outline` 8,546,999 · 2026-11-09 | 예 · building-permits 200 | 예 · 건축인허가 |  |
| `hub-building-building_permit_floor_overview` | 층별개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_floor_overview` 15,041,597 · 2026-11-10 | 예 · building-permits 200 | 예 · 건축인허가 |  |
| `hub-building-building_permit_unit_outline` | 호별개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_permit_major_repair` | 대수선 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_major_repair` 113,817 · 2026-11-09 | 예 · building-permits 200 | 예 · 건축인허가 |  |
| `hub-building-building_permit_structure_ledger` | 공작물관리대장 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_structure_ledger` 559,438 · 2026-11-09 | 예 · building-permits 200 | 예 · 건축인허가 |  |
| `hub-building-building_permit_demolition_loss` | 철거멸실관리대장 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_demolition_loss` 462,774 · 2026-11-04 | 예 · demolitions 200 | 예 · 멸실 |  |
| `hub-building-building_permit_temporary_building` | 가설건축물 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_temporary_building` 2,308,148 · 2026-11-09 | 예 · building-permits 200 | 예 · 건축인허가 |  |
| `hub-building-building_permit_sewage_facility` | 오수정화시설 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_permit_parking` | 주차장 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_parking` 2,835,072 · 2026-11-05 | 예 · building-permits 200 | 예 · 건축인허가 |  |
| `hub-building-building_permit_annex_parking` | 부설주차장 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_annex_parking` 58,823 · 2026-11-09 | 예 · building-permits 200 | 예 · 건축인허가 |  |
| `hub-building-building_permit_exclusive_common_area` | 전유공용면적 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_permit_unit_exclusive_common_area` | 호별전유공용면적 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_permit_district_zone` | 지역지구구역 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_permit_road_ledger` | 도로대장 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_permit_site_location` | 대지위치 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_permit_site_location` 13,639,544 · 2026-11-09 | 예 · building-permits 200 | 예 · 건축인허가 |  |
| `hub-building-building_permit_housing_type` | 주택유형 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_basis_outline` | 기본개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_building_outline` | 동별개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_floor_overview` | 층별개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_unit_outline` | 호별개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_ancillary_facility` | 부대시설 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_sewage_facility` | 오수정화시설 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_parking` | 주차장 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_annex_parking` | 부설주차장 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_exclusive_common_area` | 전유공용면적 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_act_unit_exclusive_common_area` | 행위호전유공용면적 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_act_outline` | 행위개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_managed_communal_type_outline` | 관리공동형별개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_managed_communal_ancillary_welfare_facility` | 관리공동부대복리시설 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_district_zone` | 지역지구구역 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_site_location` | 대지위치 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-housing_permit_welfare_sale_facility` | 복리분양시설 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 주택인허가 Silver 변환 없음(Bronze 만) |
| `hub-building-building_register_closed_basis_outline` | 기본개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_register_closed_master` | 총괄표제부 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_register_closed_main` | 표제부 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_register_closed_floor_overview` | 층별개요 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_register_closed_sub_parcel` | 부속지번 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_register_closed_exclusive_common_area` | 전유공용면적 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_register_closed_sewage_facility` | 오수정화시설 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_register_closed_exclusive_unit` | 전유부 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_register_closed_apartment_price` | 공동주택가격 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_register_closed_district_zone` | 지역지구구역 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: 폐쇄(말소) 대장 Silver 없음(Bronze 만) |
| `hub-building-building_energy_yearly_electricity` | 지번별에너지(전기,연도별) | 아니오 | 0 | 없음 | 아니오 | 아니오 | 받을 수 없음: 제공자 목록에 파일이 없다(연도값=월별 파일 합, ADR-0355) |
| `hub-building-building_energy_yearly_gas` | 지번별에너지(가스,연도별) | 아니오 | 0 | 없음 | 아니오 | 아니오 | 받을 수 없음: 제공자 목록에 파일이 없다(연도값=월별 파일 합, ADR-0355) |
| `hub-building-building_maintenance_inspection_agency` | 점검기관 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `hub-building-building_maintenance_regular_inspection_history` | 정기점검이력 | 예 · source_sweep 매일 · 최근 성공 2026-10-20 | 5 · 2026-10-20 | `silver.building_maintenance_regular_inspection_history` 64,328 · 2026-11-09 | 예 · building-permits 200 | 예 · 건축인허가 |  |

### `real_transaction_open_api` — 12개 (수집 0 · Silver 0 · 서빙 0 · 화면 0)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `data-go-kr-real-transaction-getRTMSDataSvcRHTrade` | 연립다세대 매매 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcRHRent` | 연립다세대 전월세 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcInduTrade` | 공장창고 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcLandTrade` | 토지 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcNrgTrade` | 상업업무용 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcOffiTrade` | 오피스텔 매매 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcOffiRent` | 오피스텔 전월세 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcAptTradeDev` | 아파트 매매 상세 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcAptRent` | 아파트 전월세 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcSilvTrade` | 아파트 분양권전매 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcSHTrade` | 단독다가구 매매 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |
| `data-go-kr-real-transaction-getRTMSDataSvcSHRent` | 단독다가구 전월세 실거래가 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): 같은 사실을 대량 파일로 받는다(ADR-0400) |

### `real_transaction_bulk` — 12개 (수집 12 · Silver 12 · 서빙 0 · 화면 0)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `rt-molit-go-kr-real-transaction-export-H1` | 공장창고 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 288 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-G1` | 토지 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 277 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-F1` | 상업업무용 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 130 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-A1` | 아파트 매매 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 32 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-A2` | 아파트 전월세 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 32 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-E1` | 아파트 분양권전매 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 30 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-D1` | 오피스텔 매매 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 30 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-D2` | 오피스텔 전월세 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 31 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-B1` | 연립다세대 매매 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 30 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-B2` | 연립다세대 전월세 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 30 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-C1` | 단독다가구 매매 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 31 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |
| `rt-molit-go-kr-real-transaction-export-C2` | 단독다가구 전월세 실거래가 | 예 · rt_molit 매시 · 최근 성공 2026-11-10 | 30 · 2026-11-10 | `silver.real_transaction` 13,628,593 · 2026-11-11<br>`silver.real_transaction_rejected` 4,255 · 2026-10-31 | 아니오 · real-transaction 503 | 아니오 | 고장: 실거래 Gold 11-10 21:58 커널 OOM kill + by-PNU 레인 첫 발행 전(ADR-0190) → 503 |

### `juso_electronic_map_bulk` — 11개 (수집 0 · Silver 0 · 서빙 0 · 화면 0)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `juso-electronic-map-juso_building` | JUSO 건물 도형 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_base_interval` | JUSO 기초구간 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_basic_area` | JUSO 국가기초구역 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_building_group` | JUSO 건물군 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_building_entrance` | JUSO 건물 출입구 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_legal_emd` | JUSO 법정구역 읍면동 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_legal_ri` | JUSO 법정구역 리 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_legal_sido` | JUSO 법정구역 시도 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_legal_sigungu` | JUSO 법정구역 시군구 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_road_section` | JUSO 도로구간 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |
| `juso-electronic-map-juso_road_width` | JUSO 실폭도로 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 미구현: 작업은 매일 돈다(11-10 run 요청 120·파일 0) — Bronze 0, Silver 없음(ADR-0475). 카탈로그 lane 은 아직 manual_approval/waiting_on_owner_application 이라 낡음 |

### `vworld_dataset` — 25개 (수집 25 · Silver 15 · 서빙 14 · 화면 14)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `vworld-dataset-boundary_sido` | VWorld 시도 경계 | 예 · source_sweep 매일 · 최근 성공 2026-10-31 | 3 · 2026-10-31 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `vworld-dataset-boundary_sigungu` | VWorld 시군구 경계 | 예 · source_sweep 매일 · 최근 성공 2026-10-31 | 3 · 2026-10-31 | `silver.industrial_complexes` 4,326 · 2026-10-01 | 예 · 산단 타일 미측정, 산단 프로필 200 | 예 · 산단 패널, 지도 |  |
| `vworld-dataset-boundary_emd` | VWorld 읍면동 경계 | 예 · source_sweep 매일 · 최근 성공 2026-10-15 | 34 · 2026-10-15 | `silver.administrative_boundaries` 15,201 · 2026-10-16 | 예 · 행정 타일 미측정 | 예 · 지도 |  |
| `vworld-dataset-boundary_census_emd` | VWorld 통계 읍면동 경계 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 3 · 2026-07-02 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `vworld-dataset-boundary_census_sido` | VWorld 통계 시도 경계 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 3 · 2026-07-02 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `vworld-dataset-boundary_census_sigungu` | VWorld 통계 시군구 경계 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 3 · 2026-07-02 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `vworld-dataset-parcel` | VWorld 필지 | 예 · source_sweep 매일 · 최근 성공 2026-11-04 | 812 · 2026-11-04 | `silver.parcel_boundaries` 79,738,057 · 2026-10-30 | 예 · 필지 문서 200, 필지 타일 미측정 | 예 · 지도, 필지정보·토지 |  |
| `vworld-dataset-parcel_number_change_history` | VWorld 필지고유번호변동연혁 | 예 · source_sweep 매일 · 최근 성공 2026-10-14 | 35 · 2026-10-14 | `silver.parcel_number_change_history` 1,370,351 · 2026-10-20 | 아니오 | 아니오 | 계보 입력(필지 계보·place_id), 직접 서빙 안 함 |
| `vworld-dataset-land_characteristic` | VWorld 토지특성 | 예 · source_sweep 매일 · 최근 성공 2026-10-10 | 1,076 · 2026-10-10 | `silver.land_characteristic` 35,888,121 · 2026-10-15 | 예 · real-transaction 503, 필지 문서 200 | 예 · 필지정보·토지 |  |
| `vworld-dataset-land_individual_price` | VWorld 개별공시지가 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 68 · 2026-07-02 | `silver.land_individual_price` 35,887,377 · 2026-10-15 | 예 · 필지 문서 200 | 예 · 필지정보·토지 |  |
| `vworld-dataset-land_forest` | VWorld 임야 | 예 · source_sweep 매일 · 최근 성공 2026-11-11 | 752 · 2026-11-11 | `silver.land_forest_ledger` 39,917,568 · 2026-10-26 | 예 · 필지 문서 200 | 예 · 필지정보·토지 |  |
| `vworld-dataset-land_ownership` | VWorld 토지소유 | 예 · source_sweep 매일 · 최근 성공 2026-11-01 | 62 · 2026-11-01 | `silver.land_ownership` 123,103,469 · 2026-11-10 | 예 · ownership 200, real-transaction 503 | 예 · 토지(소유) |  |
| `vworld-dataset-land_use_plan` | VWorld 토지이용계획 | 예 · source_sweep 매일 · 최근 성공 2026-11-11 | 335 · 2026-11-11 | `silver.land_use_plan` 247,584,668 · 2026-10-15 | 예 · 필지 문서 200 | 예 · 필지정보·토지 |  |
| `vworld-dataset-land_transfer_history` | VWorld 토지이동연혁 | 예 · source_sweep 매일 · 최근 성공 2026-11-11 | 820 · 2026-11-11 | `silver.land_transfer_history` 131,850,695 · 2026-11-04 | 예 · real-transaction 503, 필지 문서 200 | 예 · 필지정보·토지 |  |
| `vworld-dataset-land_right_registration` | VWorld 토지권리등록 | 예 · source_sweep 매일 · 최근 성공 2026-11-11 | 710 · 2026-11-11 | `silver.land_right_registration` 27,967,962 · 2026-10-16 | 예 · 필지 문서 200 | 예 · 필지정보·토지 |  |
| `vworld-dataset-land_use_zone` | VWorld 토지이용구역 | 예 · source_sweep 매일 · 최근 성공 2026-11-11 | 11,928 · 2026-11-11 | `silver.land_use_zone_area` 880,424 · 2026-11-01 | 예 · regulation-zones 200, 용도지역 타일 미측정 | 예 · 규제, 지도 | 규제 레인 Gold·bake 작업은 꺼짐(첫 실행 측정 대기, ADR-0375) — 지금 서빙은 수동 실행분 |
| `vworld-dataset-land_use_zone_code` | VWorld 토지이용구역 코드 | 예 · source_sweep 매일 · 최근 성공 2026-10-14 | 4 · 2026-10-14 | `silver.land_use_zone_code` 1,270 · 2026-10-14 | 예 · 필지 문서 200 | 예 · 필지정보·토지 |  |
| `vworld-dataset-real_estate_broker` | VWorld 공인중개사 | 예 · source_sweep 매일 · 최근 성공 2026-11-10 | 33 · 2026-11-10 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `vworld-dataset-sandan_location` | VWorld 산업단지 위치 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 2 · 2026-07-02 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `vworld-dataset-sandan_boundary` | VWorld 산업단지 경계 | 예 · source_sweep 매일 · 최근 성공 2026-09-04 | 2 · 2026-07-02 | `silver.industrial_complex_boundaries` 1,343 · 2026-09-01 | 예 · 산단 타일 미측정 | 예 · 지도 |  |
| `vworld-dataset-sandan_profile` | VWorld 산업단지 개요 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 2 · 2026-07-02 | `silver.industrial_complexes` 4,326 · 2026-10-01 | 예 · 산단 타일 미측정, 산단 프로필 200 | 예 · 산단 패널, 지도 |  |
| `vworld-dataset-sandan_facility_land_use` | VWorld 산업단지 시설용지 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 2 · 2026-07-02 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `vworld-dataset-sandan_land_use_zone` | VWorld 산업단지 용도지역 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 2 · 2026-07-02 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `vworld-dataset-sandan_permitted_industry` | VWorld 산업단지 유치업종 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 2 · 2026-07-02 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |
| `vworld-dataset-sandan_parcel` | VWorld 산업단지 필지 | 예 · source_sweep 매일 · 최근 성공 2026-09-05 | 2 · 2026-07-02 | 없음 | 아니오 | 아니오 | 미구현: Silver 변환 없음(Bronze 만) |

### `vworld_ned_open_api` — 1개 (수집 0 · Silver 0 · 서빙 0 · 화면 0)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `vworld-dataset-land_register` | VWorld 토지대장 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 금지(중복): PNU 단건 API, 토지특성·토지소유 파일이 대신(ADR-0359) |

### `data_go_kr_file_data` — 13개 (수집 13 · Silver 4 · 서빙 1 · 화면 1)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `public-bulk-highway_access_point` | 고속도로 접근점 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.highway_access_point` 844 · 2026-11-10 | 아니오 · nearby 503 | 아니오 | 주변시설 Gold 가 다른 입력(위 6종) 고장으로 실패 → 레인 첫 발행 전 503 |
| `public-bulk-school_location` | 학교 위치 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.school_location` **0행** | 아니오 | 아니오 | 고장: 11-10 run 이 철도역 CSV 깨진 행(MALFORMED_CSV_RECORD)으로 통째 실패 → 이 Silver 0행 → 주변시설 Gold 실패("source snapshot IDs must be positive") → 503 |
| `data-go-kr-file-registered_factory` | 한국산업단지공단 전국등록공장현황 | 예 · registered_factory 매일 · 최근 성공 2026-10-19 | 1 · 2026-10-19 | `silver.registered_factory` 220,705 · 2026-10-20 | 예 · factories 200 | 예 · 등록공장 |  |
| `public-bulk-university` | 대학교 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.university` **0행** | 아니오 | 아니오 | 고장: 11-10 run 이 철도역 CSV 깨진 행(MALFORMED_CSV_RECORD)으로 통째 실패 → 이 Silver 0행 → 주변시설 Gold 실패("source snapshot IDs must be positive") → 503 |
| `public-bulk-household_count` | 세대수 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.household_count` 3,620 · 2026-11-09 | 아니오 | 아니오 | 미구현: Silver 까지만, 읽는 Gold 없음(contract_only) |
| `public-bulk-dong_population` | 동 인구 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.dong_population` 3,620 · 2026-11-09 | 아니오 | 아니오 | 미구현: Silver 까지만, 읽는 Gold 없음(contract_only) |
| `public-bulk-railway_station` | 철도역 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.railway_station` **0행** | 아니오 | 아니오 | 고장: 11-10 run 이 철도역 CSV 깨진 행(MALFORMED_CSV_RECORD)으로 통째 실패 → 이 Silver 0행 → 주변시설 Gold 실패("source snapshot IDs must be positive") → 503 |
| `public-bulk-railway_station_line` | 철도역 노선 매핑 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.railway_station` **0행** | 아니오 | 아니오 | 고장: 11-10 run 이 철도역 CSV 깨진 행(MALFORMED_CSV_RECORD)으로 통째 실패 → 이 Silver 0행 → 주변시설 Gold 실패("source snapshot IDs must be positive") → 503 |
| `public-bulk-subway_station` | 도시철도역 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.subway_station` **0행** | 아니오 | 아니오 | 고장: 11-10 run 이 철도역 CSV 깨진 행(MALFORMED_CSV_RECORD)으로 통째 실패 → 이 Silver 0행 → 주변시설 Gold 실패("source snapshot IDs must be positive") → 503 |
| `public-bulk-port` | 항만 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.port` **0행** | 아니오 | 아니오 | 고장(위 run 실패로 Silver 0행) + 읽는 Gold 없음(contract_only) |
| `public-bulk-airport` | 공항 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.airport` **0행** | 아니오 | 아니오 | 고장: 11-10 run 이 철도역 CSV 깨진 행(MALFORMED_CSV_RECORD)으로 통째 실패 → 이 Silver 0행 → 주변시설 Gold 실패("source snapshot IDs must be positive") → 503 |
| `public-bulk-commercial_district` | 상권 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.commercial_district` **0행** | 아니오 | 아니오 | 고장: 11-10 run 이 철도역 CSV 깨진 행(MALFORMED_CSV_RECORD)으로 통째 실패 → 이 Silver 0행 → 주변시설 Gold 실패("source snapshot IDs must be positive") → 503 |
| `public-bulk-adm_dong_code_linkage` | 행정동 법정동 연계 | 예 · data_go_kr_file_data 매일 · 최근 성공 2026-11-09 | 1 · 2026-11-09 | `silver.adm_dong_code_linkage` **0행** | 아니오 | 아니오 | 고장(위 run 실패로 Silver 0행) + 읽는 Gold 없음(contract_only) |

### `ilis` — 4개 (수집 0 · Silver 3 · 서빙 3 · 화면 3)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `ilis-industrial_complex_list` | ILIS 산업단지 목록 | 아니오 | 0 | `silver.industrial_complexes` 4,326 · 2026-10-01 | 예 · 산단 타일 미측정, 산단 프로필 200 | 예 · 산단 패널, 지도 | 운영 작업 없음: 고시·첨부 수집기(#1255, ADR-0484)는 main 병합됐으나 자동배포가 11-11 "운영자 유닛 대기"로 멈춰 미배포. 목록·상세는 10-01 1회 적재 뒤 작업 없음, Bronze 장부 0 |
| `ilis-industrial_complex_notice` | ILIS 산업단지 고시 목록 | 아니오 | 0 | `silver.industrial_complexes` 4,326 · 2026-10-01 | 예 · 산단 타일 미측정, 산단 프로필 200 | 예 · 산단 패널, 지도 | 운영 작업 없음: 고시·첨부 수집기(#1255, ADR-0484)는 main 병합됐으나 자동배포가 11-11 "운영자 유닛 대기"로 멈춰 미배포. 목록·상세는 10-01 1회 적재 뒤 작업 없음, Bronze 장부 0 |
| `ilis-industrial_complex_detail` | ILIS 산업단지 상세 | 아니오 | 0 | `silver.industrial_complexes` 4,326 · 2026-10-01 | 예 · 산단 타일 미측정, 산단 프로필 200 | 예 · 산단 패널, 지도 | 운영 작업 없음: 고시·첨부 수집기(#1255, ADR-0484)는 main 병합됐으나 자동배포가 11-11 "운영자 유닛 대기"로 멈춰 미배포. 목록·상세는 10-01 1회 적재 뒤 작업 없음, Bronze 장부 0 |
| `ilis-industrial_complex_notice_detail` | ILIS 산업단지 고시 상세·첨부 | 아니오 | 0 | 없음 | 아니오 | 아니오 | 운영 작업 없음: 고시·첨부 수집기(#1255, ADR-0484)는 main 병합됐으나 자동배포가 11-11 "운영자 유닛 대기"로 멈춰 미배포. 목록·상세는 10-01 1회 적재 뒤 작업 없음, Bronze 장부 0 |

### `mois_standard_code_open_api` — 1개 (수집 0 · Silver 0 · 서빙 0 · 화면 0)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `mois-standard-code-getStanReginCdList` | 행정표준코드 법정동코드 목록 | 아니오 | 0 | 없음 | 아니오 | 아니오 | **설명 필요**: 수집기·작업 없음, collection_decision 칸도 없음. 같은 법정동 전체 표를 code.go.kr 로 매일 받으므로(다음 행) "중복" 결정을 카탈로그에 적어야 한다 |

### `code_go_kr` — 1개 (수집 1 · Silver 1 · 서빙 0 · 화면 0)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `code-go-kr-legal_dong_code_table` | 행정표준코드 법정동 전체 표 | 예 · legal_dong_code_changes 매일 · 최근 성공 2026-10-28 | 284 · 2026-10-28 | `reference.legal_dong_code_snapshot` 106,790 · 2026-10-04<br>`reference.legal_dong_code_snapshot` 106,790 · 2026-10-04 | 아니오 | 아니오 | 참조 표: 모든 레인이 법정동 변경 추적(ADR-0329 views.changed_codes)에 쓴다 — 직접 서빙 대상 아님 |

### `onbid_open_api` — 7개 (수집 7 · Silver 7 · 서빙 7 · 화면 7)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `data-go-kr-onbid-getRlstCltrList2` | 온비드 부동산 물건목록 | 예 · onbid 매시 · 최근 성공 2026-11-11 | 130 · 2026-11-11 | `silver.onbid_real_estate_item` 425,635 · 2026-11-11 | 예 · public-auction 200 | 예 · 공매 탭 |  |
| `data-go-kr-onbid-getRlstDtlInf2` | 온비드 부동산 물건상세 | 예 · onbid 매시 · 최근 성공 2026-11-11 | 180 · 2026-11-11 | `silver.onbid_real_estate_item_photo` 219,214 · 2026-11-11<br>`silver.onbid_real_estate_item_detail` 31,093 · 2026-11-11 | 예 · public-auction 200 | 예 · 공매 탭 |  |
| `data-go-kr-onbid-getCltrBidInf2` | 온비드 물건상세 입찰정보 | 예 · onbid 매시 · 최근 성공 2026-11-11 | 117 · 2026-11-11 | `silver.onbid_item_bid_info` 10,822 · 2026-11-11 | 예 · public-auction 200 | 예 · 공매 탭 |  |
| `data-go-kr-onbid-getPbancDtlInf2` | 온비드 공고상세 | 예 · onbid 매시 · 최근 성공 2026-11-11 | 127 · 2026-11-11 | `silver.onbid_announcement_detail` 11,861 · 2026-11-11 | 예 · public-auction 200 | 예 · 공매 탭 |  |
| `data-go-kr-onbid-getPbancList2` | 온비드 공고목록 | 예 · onbid 매시 · 최근 성공 2026-11-11 | 450 · 2026-11-11 | `silver.onbid_announcement` 10,376 · 2026-11-11 | 예 · public-auction 200 | 예 · 공매 탭 |  |
| `data-go-kr-onbid-getPbancBidRsltList2` | 온비드 공고 입찰결과목록 | 예 · onbid 매시 · 최근 성공 2026-11-11 | 450 · 2026-11-11 | `silver.onbid_announcement_bid_result` 6,787 · 2026-11-11 | 예 · public-auction 200 | 예 · 공매 탭 |  |
| `data-go-kr-onbid-getCltrBidRsltDtl2` | 온비드 물건 입찰결과상세 | 예 · onbid 매시 · 최근 성공 2026-11-11 | 157 · 2026-11-11 | `silver.onbid_item_bid_result` 23,792 · 2026-11-11 | 예 · public-auction 200 | 예 · 공매 탭 |  |

### `court_auction` — 3개 (수집 3 · Silver 3 · 서빙 0 · 화면 0)

| endpoint_slug | 한글 이름 | 수집 중? | Bronze 객체 · 최근 | Silver 표 · 행 · 최근 스냅샷 | Gold/서빙 | 화면 | 안 되는 이유 |
|---|---|---|---|---|---|---|---|
| `courtauction-go-kr-auction_item` | 법원경매 물건 검색 | 예 · court_auction 매일 · 최근 성공 2026-11-10 | 6 · 2026-11-10 | `silver.court_auction_item` 187,790 · 2026-11-11 | 아니오 · court-auction 503 | 아니오 | 꺼짐: 법원경매 Gold·bake 작업 한 번도 안 돎(#978 미병합) → 503. 화면 탭 코드는 있음 |
| `courtauction-go-kr-auction_sale_result` | 법원경매 매각결과 검색 | 예 · court_auction 매일 · 최근 성공 2026-11-10 | 4 · 2026-11-10 | `silver.court_auction_sale_result` 27,665 · 2026-11-11 | 아니오 · court-auction 503 | 아니오 | 꺼짐: 법원경매 Gold·bake 작업 한 번도 안 돎(#978 미병합) → 503. 화면 탭 코드는 있음 |
| `courtauction-go-kr-auction_item_detail` | 법원경매 물건상세 | 예 · court_auction 매일 · 최근 성공 2026-11-10 | 1 · 2026-11-10 | `silver.court_auction_item_detail` 1,800 · 2026-11-11<br>`silver.court_auction_item_photo` 14,905 · 2026-11-11 | 아니오 · court-auction 503 | 아니오 | 꺼짐: 법원경매 Gold·bake 작업 한 번도 안 돎(#978 미병합) → 503. 화면 탭 코드는 있음 |
