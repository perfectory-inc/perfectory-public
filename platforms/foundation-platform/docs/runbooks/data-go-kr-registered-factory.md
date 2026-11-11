---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-09
---

# data.go.kr 파일 데이터 — 등록공장 수집·주소로 PNU 찾기·적재, 기타 공공 일괄 파일 수집 런북

루트 ADR-0280 §4(원천이 PNU 를 주지 않으면 주소로 찾되, 어느 단계가 찾았는지 남긴다)의 운영 절차다. 받는 곳·멤버 형식·행 수
한계는 `infra/lakehouse/contracts/data-go-kr-file-data.contract.json` 의 `files.registered_factory` 하나에 있다. 주소 첫 낱말의
시도 코드는 적재마다 법정동 표와 기록된 코드 쌍에서 계산한다(`address_pnu.sido_prefixes`, 루트 ADR-0403).
루트 ADR-0401 이 같은 계약에 기타 공공 일괄 파일 8개를 더했다(아래 「기타 공공 일괄 파일」).

## 흐름

작업 `registered_factory`(하루 한 번, spark 3칸, 가중치 1) → `foundation-registered-factory.service` →
`scripts/ops/registered-factory-cycle.sh`:

1. `collect-data-go-kr-file-data`(`..._DATASETS=registered_factory`): 포털 메타데이터를 한 번 묻는다. 파일 id(`atchFileId`)가
   `collector/registered_factory/accepted.json` 의 것과 같으면 받지 않는다. 다르면 zip 을 받아 Bronze 에 남긴다.
   ADR-0401 전의 `collector/accepted.json` 은 스크립트가 처음 한 번 그 자리로 옮긴다.
2. `plan-silver-load-objects`: R2 만 보고 쌓을 판을 정한다. 이 원천의 Bronze zip 가운데 `silver.registered_factory`
   의 커밋 요약(`foundation.ingest-batch-objects`)이 아직 기록하지 않은 것, 계약의 `silver_load` 범위(최근 7일,
   한 번에 하나) 안에서 오래된 것부터. 없으면 `unchanged` 로 끝난다(Spark 없음).
3. `data_go_kr_registered_factory.py load`: 멤버를 검사하고(행 수 한계는 표가 가진 최신 판과 비교), `address_pnu.py`
   로 PNU 를 찾고, `silver.registered_factory` 에 판 전체를 한 번 쌓는다.

대기열은 없다. 적재가 실패하면 그 zip 은 표에 기록이 없으니 다음 실행이 다시 받는다. 7일 넘게 실패가 이어지면
계획이 그 zip 을 쌓지 않고 `unrecorded_outside_window` 로 알린다(스냅숏 보존 30일보다 짧게 둔 범위).

상태: `/var/lib/foundation-platform/registered-factory/`(`collector/registered_factory/accepted.json`, `journal.log`, 실행마다
`runs/<실행>/run.log`).

## PNU 찾기 (`pnu_basis`, `pnu_unresolved_reason`)

| 결과 | 뜻 |
|---|---|
| `jibun_address` | 지번 주소: 법정동 이름 → 코드(+산·본번·부번), 그 PNU 가 `silver.parcel_boundaries` 현재 행에 있다 |
| `road_address` | 도로명 주소: `silver.building_register_titles` 의 같은 (시도, 시군구, 도로명, 건물번호) 표제부가 모두 같은 PNU |
| `no_address` / `unparsed` | 주소가 없다 / 어느 읽기로도 읽히지 않는다 |
| `no_legal_dong` | 지번 주소의 동 이름이 현재 법정동 코드표에 없다(개칭·폐지 전 이름 등) |
| `parcel_absent` | 만든 PNU 가 현재 필지에 없다 |
| `road_not_found` / `road_ambiguous` | 같은 도로명 주소의 표제부가 없다 / 여러 PNU 에 걸친다 |

도형 포함관계(좌표가 필지 안)로는 정하지 않는다. 일치율은 실행 요약 줄 `registered-factory-summary-json` 의 `match_rate`
와 `by_basis`·`unresolved_reasons` 다.

## 법정동 코드 변경 따라가기

적재가 끝나면 같은 실행이 `reference.legal_dong_code_change` 를 본다(루트 ADR-0329 §3, ADR-0416). 표의 커밋 기록
`follow:reference.legal_dong_code_change@<스냅숏>@<UTC>` 이후 변경 표가 움직였으면, `views.changed_codes` 가 이름 대는 코드
아래의 현재 행과 PNU 가 없는 행만 다시 잇는다. 이 표는 판을 통째로 읽으므로, 연결이 바뀐 줄이 있으면 그 판을
이번 시각으로 한 번 더 싣는다(바뀐 줄은 새 링크, 나머지는 그대로). 따라가기 기록이 아직 없는 표는 처음 한 번 현재
행 전부를 다시 잇는다. 요약 줄의 `follow`/`follows` 가 `checked`·`appended` 를 센다.

## 처음 켜기

1. 감독 아래 단위를 직접 한 번 시작한다(꺼진 작업은 `start-scheduled-job.sh` 가 거부한다):
   `sudo systemctl start foundation-registered-factory.service`, 그다음 `journalctl -u foundation-registered-factory.service`.
2. 요약 줄의 `match_rate`·`by_basis` 를 perfectory-ops 의 비용·실행 기록에 남긴다.
3. 작업의 `enabled` 를 켜는 PR(ADR-0122 §4).

## 거부

- 데이터 멤버가 하나가 아니거나, 머리글이 계약과 다르거나, CP949 로 읽히지 않는 바이트가 있다: 공급자가 형식을 바꿨다. 계약을 고치는 PR.
- 행이 바닥보다 적거나, 직전에 쌓은 판보다 한계 넘게 줄었다: 잘린 파일일 수 있다. 포털에서 내려받아 확인한다.

## 기타 공공 일괄 파일 (루트 ADR-0401)

작업 `data_go_kr_file_data`(하루 한 번 11:40 UTC, spark 3칸, 90분) → `foundation-data-go-kr-file-data.service` →
`scripts/ops/data-go-kr-file-data-cycle.sh`:

1. `collect-data-go-kr-file-data`: 계약의 파일 가운데 등록공장을 뺀 전부. 파일마다 메타데이터를 한 번 묻고 새 파일 id 일
   때만 받아 그 원천의 Bronze(`bronze/source=datagokr__<dataset_slug>/`)에 남긴다.
2. `plan-silver-load-objects`: 표(`silver.<dataset_slug>`)마다, 그 표의 커밋 요약이 기록하지 않은 Bronze 파일을 하나.
3. `data_go_kr_file_data.py load`(매일, 쌓을 판이 없어도): 판마다 인코딩·머리줄·칸 수·행 수 한계를 검사하고 UTF-8
   사본을 만든 뒤, 행 키(제공자 키 또는 행 SHA-256)로 현재 행과 비교해 새 키·바뀐 키와 사라진 키의 삭제 표지만 쌓는다.
   원문 칸은 `provider_fields_json` 에 머리줄 이름 그대로 있다. 그다음 코드 변경 따라가기: 표가 마지막으로 이은
   `reference.legal_dong_code_change` 스냅숏(커밋 기록 `follow:reference.legal_dong_code_change@<스냅숏>@<시각>`)과 지금이
   다르면, `views.changed_codes` 가 말하는 코드 아래 행과 잇지 못한 행만 다시 잇고, 연결이 바뀐 행만 쌓는다. 옛 스냅숏이
   만료됐으면 모든 현재 행을 다시 잇는다(쌓는 것은 여전히 바뀐 행만). 요약 줄은 `data-go-kr-file-data-summary-json`.

| `place_link` | 표 | 잇는 법 | 2026-11-04 실측(표본·전수) |
|---|---|---|---|
| `legal_dong_by_names` | highway_access_point, land_price_change_rate | 도·시군구·읍면동 → 현재 법정동 코드 | 844 중 634(75.1%); 지가변동률은 첫 적재 때 잼 |
| `adm_dong_via_linkage` | household_count, dong_population | 행정동 코드를 두고, 읽을 때 `silver.adm_dong_code_linkage` 현재 행으로 | 3,620 중 3,547(98.0%)이 2026-10-01 연계에 있음 |
| `address_pnu` | railway_station, airport, port, elevator_installation, knowledge_industry_center | 지번 → 도로명 | 역 215 중 116(54.0%), 공항 14 중 6, 항만 72 중 0(위치가 시도·시군구뿐); 승강기 표본 3,000 중 2,910 이 도로명으로 읽히고 그 (시도, 시군구, 도로명, 건물번호) 2,829 가운데 2,551(90.2%)이 PNU 하나(2026-11-11) |
| `place_identity` | commercial_district, standard_land_price | 지번코드·토지대장번호(PNU) → place-identity 해석기 → 현재 필지 | 상가 표본 2,000 중 1,980(99.0%); 표준지 19자리 표본 2,994 중 2,990(2026-11-11), 19자리가 아닌 사업지구 블록 1,902행(0.32%)은 못 찾음 |
| `is_the_bridge` | adm_dong_code_linkage | 행정동코드·법정동코드 그대로 | — |
| `legal_dong_code` | urban_plan_notice, land_use_regulation_by_zone | 제공자의 시군구(5자리)·법정동(10자리) 코드 칸 → 10자리로 채워 코드 대응표(`legal_dong_code_change` 의 유효 쌍)로 현재 코드 → 현재 코드표에 있으면 연결, `nationwide_codes`(00000 = 법률)는 일부러 법정동 없이 `nationwide` | 고시 408,054 중 407,640 이 그대로 현재, 412 는 폐지 코드(군위 47720 등)라 대응표로; 규제 23,427 중 17,627 그대로, 5,748 은 2026-07-01 통합 전 전남 코드(46xxx → 12xxx), 52 는 전국(2026-11-11) |

| 계약 `files` | data.go.kr | 형식 | 2026-11-04 실측 |
|---|---|---|---|
| `highway_access_point` | 15116370 한국도로공사_출입시설 준공년도 | CSV CP949 | 49 KB, 844행 |
| `household_count` | 15097974 행정안전부_행정동 세대원수별 세대수 | CSV CP949, 매월 | 316 KB, 3,620행 |
| `dong_population` | 15097972 행정안전부_행정동 성별 연령별 인구수 | CSV CP949, 매월 | 2.6 MB, 3,620행, 230칸 |
| `railway_station` | 15067652 국가철도공단_철도역 정보 | CSV CP949 | 293 KB, 215행 |
| `port` | 15088273 해양수산부_항만정보 | CSV CP949 | 40 KB, 72행 |
| `airport` | 15002851 한국공항공사_공항 위치정보 | CSV CP949 | 1 KB, 14행 |
| `commercial_district` | 15083033 소상공인시장진흥공단_상가(상권)정보 | ZIP(시도별 CSV 16개, UTF-8), 분기 | 349 MB, 2,753,778행, 59초 |
| `adm_dong_code_linkage` | 15136368 국가데이터처_법정동 연계정보 | CSV UTF-8 | 12 MB, 130,193행 |
| `standard_land_price` | 15004246 국토교통부_표준지공시지가 | CSV CP949, 해마다(가격기준일 1월 1일) | 139.5 MB, 600,000행, 43칸(2026-11-11) |
| `elevator_installation` | 15112638 한국승강기안전공단_승강기 설치 현황 | ZIP(CSV 2개, UTF-8), 수시 | 33 MB, 893,715행, 29칸(2026-11-11) |
| `knowledge_industry_center` | 15117154 한국산업단지공단_전국지식산업센터현황 | ZIP(CSV 1개 CP949 + 데이터 사전), 수시 | 0.1 MB, 1,549행, 22칸(2026-11-11) |
| `urban_plan_notice` | 15130432 국토교통부_(도시계획)고시정보 | ZIP(TN_NTFC.csv CP949 + 코드표), 포털 반기 | 35.5 MB(풀면 202 MB), 408,054행, 15칸(2026-11-11) |
| `land_use_regulation_by_zone` | 15122715 국토교통부_지역지구별 토지이용규제정보 | CSV UTF-8, 수시 | 3.3 MB, 23,427행, 11칸(2026-11-11) |
| `land_price_change_rate` | 15063473 국토교통부_읍면동 지가변동률(실제 단위 시군구) | CSV CP949, 매월 | 0.66 MB, 5,834행, 16칸(2026-11-11) |

카탈로그의 `public-bulk-railway_station_line`(역↔노선) 은 따로 받는 파일이 아니라 철도역 파일의 `관련노선` 칸이다. 그래서
두 endpoint 가 같은 Bronze 원천(`datagokr__railway_station`)을 가리킨다.

- run 당 상한은 파일 자체다: 8개가 모두 새 판이면 약 365 MB, 요청 간격(5초)을 넣어 3분 안팎. 2026-11-11 에 더한 중개 설명
  자료 3개(표준지·승강기·지식산업센터)가 모두 새 판이면 약 173 MB 가 더해진다(표준지는 해마다, 나머지는 수시라 보통 run 은 하나도
  받지 않는다). 상가(상권) zip(900)·표준지 CSV(900)·승강기 zip(600)이 계약에 자기 시간 한도(`request_timeout_seconds`)를 둔다. 상가 판의 UTF-8 사본(약 1.5 GB)은 실행 폴더에 잠시 놓였다가 지워진다.
- 한 파일이 실패해도(HTTP 오류, 로그인 화면) 다른 파일은 받는다. run 은 끝에 실패한 파일을 모두 적고 실패한다. 상태:
  `/var/lib/foundation-platform/data-go-kr-file-data/`(`collector/<dataset_slug>/accepted.json`, `journal.log`).
- 받지 않을 파일이 생기면(제공 중단) 계약에서 지우는 PR. 같은 파일을 다시 받게 하려면 그 파일의 `accepted.json` 을 지운다
  (Bronze 는 쓰기 한 번이라 같은 바이트면 그대로다).
- 거부: 머리줄·칸 수가 계약과 다르거나 인코딩으로 읽히지 않는 바이트(제공자가 형식을 바꿈 → 계약을 고치는 PR), 제공자 키가
  비었거나 겹침, 행이 바닥보다 적거나 현재 행보다 한계 넘게 줄어듦(잘린 파일일 수 있음 → 포털에서 받아 확인).

### 토지이음 공개자료 3개 — 포털 항목이 잇는 곳에서 받는다

토지이용규제정보(행위제한, 15047873)·토지이용규제법령정보(15047872)·개발행위허가정보(15021109)는 포털 항목에 파일이 없고
토지이음(`www.eum.go.kr/web/op/sv/svItemDet.jsp?dataCd=<코드>`) 공개자료 페이지로 잇는다(메타데이터의 `dataUrl`). 같은 수집기가
그 페이지의 스크립트(`svItemDet.js`)와 같은 세 단계를 키 없이 보낸다(`format: eum_open_data`, 계약 `eum_open_data`).

1. 목록(`list_path`): 파일마다 `dataDownload('<fileId>')` 가 있고 가장 큰 id 가 최신이다. `EUM_<dataCd>_<fileId>` 가 판이고, 마지막으로
   받은 판과 같으면 여기서 끝난다(요청 1개).
2. 파일 정보(`file_info_path`, `function=selectFileInfo` 폼): 목록 페이지를 referer 로 보내야 답한다(없으면 오류 화면, 2026-11-11).
   일회용 `key`·`fileNm`·`fileSize` 를 준다.
3. 내려받기(`download_base_url` `:8002` + `download_path?key=&filename=`): zip 이 `fileSize` 와 다르거나 zip 이 아니면 거부.

| 계약 `files` | 토지이음 `dataCd` | 2026-11-11 실측 | Silver |
|---|---|---|---|
| `land_use_activity_restriction` | 007 | zip 55 MB(XLSX 72 MB, 시도 시트 16 + 법령 1, 2,177,899행) | 아직 없음: 지역지구를 이름으로만 적는다(코드 없음) → 이름↔`silver.land_use_plan.zone_code` 대응표가 먼저(이름 383 중 250 일치, 행의 80.7%) |
| `land_use_regulation_law` | 006 | zip 7 MB(XLSX 7.5 MB) | 아직 없음 |
| `development_activity_permit` | 001 | zip 18.8 MB(CSV CP949 141 MB, 858,488줄, PNU 가 모든 줄에) | 아직 없음: 따옴표 없는 `개발행위목적` 안의 줄바꿈이 약 380줄을 가른다 → 잇는 규칙이 먼저 |

`silver_load` 가 없는 파일은 수집만 한다: Silver 잡(`data_go_kr_file_data.py`)의 `files()` 가 그 파일을 고르지 않는다.

### 표준데이터 3개 — 데이터셋 페이지의 키 없는 내려받기 (루트 ADR-0453)

학교 위치·대학·도시철도역은 포털의 표준데이터라 파일 두 단계로 받을 수 없다(메타데이터가 파일 id 를 주지 않는다). 대신
데이터셋 페이지(`data.go.kr/data/<번호>/standard.do`)의 CSV·JSON 내려받기 단추가 쓰는 것과 같은 두 요청을 같은 수집기가
보낸다(`format: standard_pages`, 계약 `standard_download`). 서비스 키도, 사람 손(활용신청)도 필요 없다.

1. `columns_path`(`/download/columList.json?pk=<번호>&ext=CSV`)가 지금의 표(`svcTableNm`), 칸 목록(`colNmList`), 행 수
   (`totalCount`)를 말한다. run 마다 묻는다: 제공자가 표나 칸을 바꾸면 다음 run 이 그대로 본다(상태 파일에 표 이름을 적지 않는다).
2. `rows_path`(`/download/standard.json`)를 쪽마다(`perPage` 10,000) 묻는다. 답은 칸 코드를 키로 한 행의 JSON 배열이다.

| 계약 `files` | data.go.kr · 작업 | 행 키 | 2026-11-09 실측 | 필지 연결(2026-11-04 표본) |
|---|---|---|---|---|
| `school_location` | 15021148 `stdData15021148` | `SCHOOL_ID` | 12,004행, 2쪽, JSON 7.5 MB(gzip 0.84 MB) | 표본 2,000 중 1,939(97.0%) |
| `university` | 15107736 `stdData15107736` | (`SCHL_NM`, `MAINBRANCH_NM`) | 1,995행, 1쪽, 1.2 MB | 1,995 중 1,356(68.0%), 599 는 road_ambiguous |
| `subway_station` | 15013205 `stdData15013205` | 행 SHA-256 | 898행, 1쪽, 0.45 MB | 898 중 205(22.8%), 511 은 `지하` 건물번호라 unparsed |

- 판: 행(정렬한 행의 SHA-256)이 마지막으로 받은 판과 같으면 아무것도 남기지 않는다. 다르면 칸 목록 답과 모든 쪽 답의 원문 줄
  (`kind: columns|rows`)을 gzip JSON lines 객체 하나로 그 원천의 Bronze 에 남긴다. 행 수가 칸 목록의 `totalCount` 와 다르면 거부.
- run 당 상한은 계약의 `max_pages`(3·1·1쪽)다. `totalCount` 가 그보다 많은 쪽을 요구하면 행을 묻기 전에 거부한다. 세 개 합쳐
  요청 7개, 답 2초 안팎, 간격(5초)을 넣어 약 35초.
- 머리줄은 내려받기의 칸 코드(`SCHOOL_ID`…, 마지막 두 칸 `INSTT_CODE`·`INSTT_NM`)다. 계약에 없는 칸이 오면 Silver 적재가 그 판을
  거부하고 오류가 칸 이름을 말한다 → 계약을 고치는 PR.
- OpenAPI(`api.data.go.kr/openapi/tn_pubr_*`)는 교차 확인일 뿐이다: `DATA_GO_KR_SERVICE_KEY` 가 있으면 새 판을 남긴 run 이 한 행만
  물어 그 `totalCount` 가 판의 행 수와 같은지 manifest 의 `openapi_check`(`matches`·`differs`·`skipped`)에 적는다. 키가 없거나
  게이트웨이가 거부해도(2026-11-04 세 작업 모두 `SERVICE_KEY_IS_NOT_REGISTERED_ERROR`, HTTP 403) 파일은 실패하지 않는다.
- 다른 data.go.kr 원천(2026-11-09 저장소 전수 36개): 파일 데이터는 이미 키 없이 받고, OpenAPI 만 있는 것(온비드 13개·건축HUB 2개·
  국세청 15081808·한전 15101404·산단공 15152884)은 공개 파일이 없다. 활용신청에 막혀 있으면서 공개 파일이 있는 것은 이 셋뿐이었다.
  표준데이터가 더 생기면 계약 `files` 에 `standard_pages` 항목 하나를 더한다.
