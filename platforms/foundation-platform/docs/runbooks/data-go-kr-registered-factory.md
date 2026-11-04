---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-04
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

## 처음 켜기

1. 감독 아래 단위를 직접 한 번 시작한다(꺼진 작업은 `start-scheduled-job.sh` 가 거부한다):
   `sudo systemctl start foundation-registered-factory.service`, 그다음 `journalctl -u foundation-registered-factory.service`.
2. 요약 줄의 `match_rate`·`by_basis` 를 perfectory-ops 의 비용·실행 기록에 남긴다.
3. 작업의 `enabled` 를 켜는 PR(ADR-0122 §4).

## 거부

- 데이터 멤버가 하나가 아니거나, 머리글이 계약과 다르거나, CP949 로 읽히지 않는 바이트가 있다: 공급자가 형식을 바꿨다. 계약을 고치는 PR.
- 행이 바닥보다 적거나, 직전에 쌓은 판보다 한계 넘게 줄었다: 잘린 파일일 수 있다. 포털에서 내려받아 확인한다.

## 기타 공공 일괄 파일 (루트 ADR-0401)

작업 `data_go_kr_file_data`(하루 한 번 11:40 UTC, default_pool, 30분) → `foundation-data-go-kr-file-data.service` →
`scripts/ops/data-go-kr-file-data-cycle.sh` → `collect-data-go-kr-file-data`. 계약의 파일 가운데 등록공장을 뺀 전부를
맡는다. 파일마다 메타데이터를 한 번 묻고 새 파일 id 일 때만 받아 그 원천의 Bronze(`bronze/source=datagokr__<dataset_slug>/`)에
남긴다. Spark 는 없다. Silver 적재(바뀐 행만, ADR-0401 §3–4)는 다음 PR 이다.

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

카탈로그의 `public-bulk-railway_station_line`(역↔노선) 은 따로 받는 파일이 아니라 철도역 파일의 `관련노선` 칸이다. 그래서
두 endpoint 가 같은 Bronze 원천(`datagokr__railway_station`)을 가리킨다.

- run 당 상한은 파일 자체다: 8개가 모두 새 판이면 약 365 MB, 요청 간격(5초)을 넣어 3분 안팎. 상가(상권) zip 만 계약에
  자기 시간 한도(`request_timeout_seconds` 900)를 둔다.
- 한 파일이 실패해도(HTTP 오류, 로그인 화면) 다른 파일은 받는다. run 은 끝에 실패한 파일을 모두 적고 실패한다. 상태:
  `/var/lib/foundation-platform/data-go-kr-file-data/`(`collector/<dataset_slug>/accepted.json`, `journal.log`).
- 받지 않을 파일이 생기면(제공 중단) 계약에서 지우는 PR. 같은 파일을 다시 받게 하려면 그 파일의 `accepted.json` 을 지운다
  (Bronze 는 쓰기 한 번이라 같은 바이트면 그대로다).
