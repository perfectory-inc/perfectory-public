# ADR 0402: 건축HUB 의 "나중 연결" 파일 9개를 지금 하나씩 잇는다 — 같은 엔진, 레인 하나에 PR 하나

- Status: Accepted
- Date: 2026-11-04
- Builds on: ADR-0355(82개 원천 판정), ADR-0356·0357·0395·0396·0397(HUB Silver 레인), ADR-0169(장부가 판을 고른다),
  ADR-0318(바뀐 키만 쌓는다), ADR-0329 §3(`views.changed_codes`), ADR-0400(수집 완전성 판정표)
- Supersedes(부분): ADR-0355 원천별 판정의 "나중 연결" 9행의 순서 — 판정(가치 있음)은 그대로다

## Context

ADR-0355 는 건축HUB 59개를 판정했다. "지금 연결" 5개는 모두 레인이 있거나 PR 이 열려 있다
(기본개요 ADR-0356, 총괄표제부 ADR-0357, 부속지번 #1016·ADR-0395, 월별 전기·가스 #1019·ADR-0396, 철거멸실 ADR-0397).
"나중 연결" 9개는 가치가 있다고 판정했지만 "다른 레인이 먼저 서야 한다" 는 이유로 미뤘다. 앞 순위 다섯이 이제 섰다.

사장 결정은 두 번이다. 2026-10-30 "나는 모든 데이터 수집을 승인할게", 2026-11-03 "비용은 상관없고, 데이터를
보여 주는 것이 항상 이긴다". 미룰 이유가 남지 않았다.

**측정 (2026-11-04, 운영 `catalog.bronze_object` 읽기 전용).** 9개 모두 2026-06 ~ 2026-10 다섯 판을 이미 받는다.

| source_slug | 이름 | Bronze 5판 | ADR-0355 가치 |
|---|---|---:|---|
| `hubgokr__building_permit_building_outline` | 건축인허가 동별개요 | 1.580 GB | M |
| `hubgokr__building_permit_floor_overview` | 건축인허가 층별개요 | 2.146 GB | L |
| `hubgokr__building_permit_major_repair` | 건축인허가 대수선 | 0.014 GB | M |
| `hubgokr__building_permit_structure_ledger` | 공작물관리대장 | 0.072 GB | M |
| `hubgokr__building_permit_temporary_building` | 가설건축물 | 0.272 GB | M |
| `hubgokr__building_permit_parking` | 건축인허가 주차장 | 0.297 GB | M |
| `hubgokr__building_permit_annex_parking` | 건축인허가 부설주차장 | 0.005 GB | M |
| `hubgokr__building_permit_site_location` | 건축인허가 대지위치 | 1.154 GB | M |
| `hubgokr__building_maintenance_regular_inspection_history` | 정기점검이력 | 0.008 GB | M |

### 실사례

- **원천 하나에 파이프라인 하나, 엔진은 하나.** Airbyte·Fivetran 커넥터는 스트림마다 같은 동기화 엔진을 쓰고
  스트림의 스키마·키·커서만 선언한다. 새 스트림은 선언을 더하는 것이지 엔진을 새로 짓는 것이 아니다.
  우리의 HUB 엔진(`hub_register_silver_export`)과 레인 계약(`hub-*-source-objects.json`)이 그 구조다.
- **키가 없는 행은 내용이 키다.** Delta Lake·Iceberg 의 CDC 비교(Debezium 의 키 없는 표, dbt snapshot 의
  `check` 전략)는 키가 없으면 행 전체의 해시로 같고 다름을 판정한다. ADR-0397 의 `row_key: "sha256_of_row"` 가 같은 규칙이다.

## Decision

1. **9개를 지금 연결한다.** 판정을 "나중 연결" 에서 "지금 연결" 로 올린다. 순서는 레인끼리 의존이 없으므로
   정하지 않는다. 각 레인은 따로 draft PR 이다(#1016·#1018·#1019 와 같은 틀). 한 PR 에 두 레인을 싣지 않는다.
2. **새 엔진은 없다.** 모두 `hub_register_silver_export` 와 Silver 갱신(ADR-0169), 키 비교(ADR-0318)를 쓴다.
   레인마다 바뀌는 것은 계약 하나(`infra/lakehouse/contracts/hub-<이름>-source-objects.json`), Silver 표 계약,
   작업(`orchestration/jobs.v1.json`, `enabled: true`, `scope`), 그래프 엣지, ODCS 계약이다.
3. **칸 배치는 실측한다.** 각 PR 이 최신 판 ZIP 전체를 R2 읽기 전용 키로 읽어 행 수·칸 수·키 칸의 채움·
   서로 다른 키 수·PNU 입력 유효율을 계약의 `how_measured` 에 적는다. 칸 뜻의 근거는 같은 데이터의
   data.go.kr 건축인허가 OpenAPI 속성 목록(`field_layout_authority`)과 실측의 교차 확인이다. 뜻을 확인하지
   못한 칸은 이름을 붙이지 않고 `raw_columns` 의 원래 index 로만 둔다.
4. **키.** 제공자 키 칸(관리허가대장PK 등)이 모든 행에 채워져 있고 실측에서 행을 가르면 그 칸이 `mgmt_key` 다.
   비어 있거나 행을 가르지 못하면 ADR-0397 의 `row_key: "sha256_of_row"` 를 쓴다. 어느 쪽인지와 그 근거 숫자를
   계약 `$comment` 와 PR 에 적는다. 정기점검이력처럼 지번 칸이 없을 수 있는 파일은 PNU 입력 칸을 실측으로
   확인하고, 없으면 그 PR 이 멈추고 이유를 보고한다(엔진은 PNU 입력 다섯 칸을 요구한다).
5. **PNU 는 해석기, 코드 변경은 따라간다.** PNU 는 엔진이 HUB 코드로 짓고(ADR-0145, FP-ADR-0023) 그래프 엣지의
   `pnu_resolution` 에 그렇게 적는다. 합병·분할·법정동 코드 변경으로 바뀐 필지는 소비자(by-PNU 레인)가
   place-identity 해석기(ADR-0288)와 `views.changed_codes`(ADR-0329 §3)로 현재 PNU 에 옮긴다. 적재 때 한 번
   풀고 멈추지 않는다. 각 PR 은 표본 2,000 행의 PNU 를 `silver.parcel_boundaries` 현재 행과 맞춰 본 연결률을 적는다.
6. **run 당 상한.** 판 하나가 run 하나다(장부가 가장 새 완전한 판을 고른다). 1 GB 를 넘는 판(동별개요·층별개요·
   대지위치)은 기본개요(ADR-0356)처럼 관리번호 앞 두 자리 묶음으로 나눠 비교한다(`bucket_prefix_length: 2`).
   PR 이 첫 판의 실행 시간을 계산해 작업의 단위 시간 안에 드는지 적는다.
7. **화면은 다음 PR.** Silver 표는 소비자가 서기 전까지 `contract_only` 섬이다(그래프 규칙). 각 PR 은 다음 화면
   (필지 패널의 어느 칸)을 PR 본문에 적는다.

## Consequences

- 9개 레인이 같은 파일(엔진의 레인 목록 `lane.rs`, `jobs.v1.json`, 그래프, 지도)을 건드린다. 서로 독립 PR 이므로
  먼저 병합되는 쪽 뒤의 PR 은 rebase 한다. 생성 문서는 다시 생성한다.
- **비용(계산).** 9개 Bronze 는 이미 받고 있다(5판 합 5.55 GB). Silver 첫 적재는 판당 원문의 수분의 일(Parquet)이고,
  이후는 바뀐 키만 쌓는다. R2 저장 단가 $0.015/GB·월, 1,450원/$ 로 9개 합쳐 월 100원 아래다. 계산 비용은
  ai-server 에서 돌아 추가 지출이 없다.
- 남는 HUB 파일은 ADR-0355 의 "Bronze 보관만" 과 "받을 수 없음" 뿐이다. 판정표는 ADR-0400 에 있다.
