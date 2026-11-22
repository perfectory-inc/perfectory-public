# ADR 0604: 사업장 명부 둘(국민연금 가입 사업장·고용·산재보험 가입 현황)은 data.go.kr 파일 쪽에서 포털이 아직 주는 지난 판까지 받아 Bronze 에 그대로 쌓는다

- Status: Accepted
- Date: 2026-11-22
- Builds on: [ADR-0401](./0401-other-public-bulk-files-are-collected-by-the-data-go-kr-file-collector-and-land-as-changed-rows.md)(data.go.kr 파일 수집기),
  [ADR-0453](./0453-standard-data-is-collected-through-the-dataset-pages-keyless-download-not-an-openapi-key-a-person-must-register.md)(키 없는 데이터셋 쪽),
  [ADR-0406](./0406-the-factory-operating-signal-comes-from-pension-workplaces-and-kicox-complex-statistics-not-electricity.md)(공장 가동 신호 원천 조사, 국민연금 사업장),
  [ADR-0574](./0574-every-limit-is-set-from-measurement-and-a-unit-near-its-limit-is-named-before-it-fails.md) §8(작업이 메모리를 적는다),
  [ADR-0583](./0583-jobs-move-to-dagster-in-four-groups-one-switch-each-and-the-per-job-dual-run-gate-retires-with-the-first.md)(collect 묶음)

## Context

사장은 2026-10-30 아직 받지 않은 원천을 모두 받기로 했다(실거래가·경매·HUB 나머지·…·기타). 코디네이터가 2026-11-22 그 가운데
회사 정보 원천 둘을 정했다: 키도 로그인도 없이 받는 파일이고, 이용허락범위는 둘 다 "제한 없음"이다.

| 원천 | 쪽 | 지금 판 (2026-11-22 잼) | 지난 판 |
| --- | --- | --- | --- |
| 국민연금공단 국민연금 가입 사업장 내역 | data.go.kr `15083277`, 매월(다음 등록 2026-11-25) | CP949 CSV 하나, 약 115 MB, 589,413 사업장. 사업자등록번호는 **앞 6 자리만**, 주소는 동·리나 도로명까지(번지 없음), 10 자리 법정동 코드, 법인/개인, 국세청 업종 코드, 가입자 수·당월 고지금액·신규·상실. 사업자번호가 없는 사업장은 업종 `999999 BIZ_NO미존재사업장` | 130 판(자료 2015-12 ~ 2026-09, 2019-08-05 ~ 2026-09-23 등록), 81 MB ~ 121 MB, 합계 12,984 MB |
| 근로복지공단 고용·산재보험 가입 현황 | data.go.kr `15002150`, 매년 | zip 134,017,856 바이트, CSV 셋(앞 둘 UTF-8, 셋째 CP949) 2,607,699 사업장. 사업자등록번호 **10 자리 전부**, 건물번호까지 주소, 상시근로자수, 업종, 성립일자 | 16 판(2019-06-07 ~ 2026-03-04 등록): zip 14·CSV 2, 14 MB ~ 526 MB, 합계 2,756 MB. 둘은 같은 쪽에 올린 다른 자료다(고용보험 5인 이상 법인사업장 보험료 고지정보 2018-06, 가입 사업장 상세정보 조회 2019-06-25) |

잰 것(2026-11-22, 요청 사이 5 초):

- 데이터셋 쪽의 **주기성 과거 데이터** 탭은 `POST /tcs/dss/selectHistAndCsvData.do`(지금 판의 `publicDataPk`·`publicDataDetailPk`)로
  채워진다. 답은 지난 판마다 한 줄: 그 판 고유의 `publicDataDetailPk`(`data-public-pk`)와 등록일.
- 수집기가 이미 묻는 메타데이터(`/tcs/dss/selectFileDataDownload.do`)에 지난 판의 `publicDataDetailPk` 를 넣으면 그 판의 파일
  (`atchFileId`)을 이름 짓는다(예: 2026-09-23 판 `FILE_000000007692952`, 지금 판은 `FILE_000000007740322`). 146 판 모두 답했다.
- 지금 판의 `publicDataDetailPk` 는 달이 바뀌어도 그대로다(세대수 파일의 계약 값이 11-04 이후 그대로 지금 판이다).
- 지난 판은 지금 판과 형식이 다를 수 있다(고용·산재: zip 사이에 CSV 둘).

## Decision

1. **두 파일은 data.go.kr 파일 계약의 파일이다.** `data-go-kr-file-data.contract.json` 의 `files.pension_workplace`·
   `files.employment_insurance_workplace` 가 메타데이터 질의, 형식, 요청 시간 한도(900 초)를 적고, 수집기
   `collect-data-go-kr-file-data` 가 다른 파일과 같은 두 단계(메타데이터 → `atchFileId` → 내려받기)로 받는다. 새 수집기는 없다.
2. **`own_job` 이 파일의 주인을 적는다.** 자기 작업으로 받는 파일은 `own_job` 에 그 작업을 적는다(`registered_factory`,
   `pension_workplace_collect`, `employment_insurance_workplace_collect`). 매일의 cycle(`data-go-kr-file-data-cycle.sh`)과 그
   Silver 적재(`FileDataSource`)는 `own_job` 을 적지 않은 파일만 받는다 — 지금까지 세 곳에 박혀 있던 `registered_factory`
   제외가 이 칸 하나가 됐다.
3. **지난 판을 모두 한 번 받는다.** 파일이 `past_editions` 를 적으면 실행마다 지금 판 다음에 지난 판 목록을 한 번 묻고,
   아직 묻지 않은 판을 **등록일이 오래된 것부터** `max_per_run` 개까지(국민연금 20, 고용·산재 4) 그 판의 키로 메타데이터를
   묻고 받는다. 오래된 것부터인 것은 포털이 먼저 내릴 것이 오래된 판이기 때문이다. 물은 판은 묻는 대로 상태
   (`<dataset_slug>/past.json`: 판 키 → 파일 id)에 적어, 중간에 멈춘 실행도 물은 것을 잃지 않는다. 이미 받은 파일(지금 판이나
   다른 판의 파일 id)을 이름 짓는 판은 내려받지 않고 적기만 한다: 지난달의 지금 판이 목록에 새 키로 돌아오는 경우다. 전체
   이력은 국민연금 일곱 실행, 고용·산재 네 실행에 들어오고, 그 뒤 실행은 메타데이터와 목록 두 요청이다.
4. **받은 그대로, 한 번.** 판마다 Bronze 객체 하나: `bronze/source=datagokr__<dataset_slug>/<dataset_slug>-<atchFileId>.<zip|csv>`.
   확장자와 내용 형식은 도착한 바이트가 정한다(zip 머리면 zip, 아니면 표지가 아닌 CSV 여야 한다; 로그인·안내 쪽은 거부).
   키는 포털의 파일 id 이고 포털은 올릴 때마다 새 id 를 준다([ADR-0152](./0152-bronze-keys-of-reused-provider-file-numbers-name-their-bytes.md) 의 번호 재사용이 없다): 같은 키에 같은 바이트는
   회복 경로로 성공하고, 다른 바이트는 거부된다. 원장(`catalog.bronze_object`)이 판마다 SHA-256 과 크기를 적는다. 덮어쓰는 것은
   없다. Silver 표는 아직 없다(다음 ADR).
5. **작업 둘, 유닛 하나.** `pension_workplace_collect`(매일 12:50 UTC)와 `employment_insurance_workplace_collect`(매일 13:20 UTC)가
   템플릿 `foundation-data-go-kr-file-collect@<dataset-slug>.service`(`scripts/ops/data-go-kr-file-collect.sh`; 인스턴스 이름에는 `_` 가 못 들어가 `-` 로 쓴다)로 돈다.
   스크립트는 `own_job` 을 적고 `silver_load` 가 없는 파일만 받는다. 둘 다 `dagster_group: collect`, `dagster_stage: shadow`
   (ADR-0583 의 collect 전환을 따른다), 메모리 2G(가장 큰 판 526 MB 를 메모리에 하나, 커밋하며 한 벌 더: 약 1.1 GB), 시간 1 시간.
6. **이름과 번호는 Bronze 에 그대로 둔다.** 두 파일은 사업장명(개인 사업장 포함)과 사업자등록번호(고용·산재는 10 자리)를 싣는다.
   Bronze 는 비공개 버킷에 받은 그대로 둔다. 무엇을 화면에 보일지는 Silver 를 정할 때 정한다(데이터 지형 §8: 원문은 원천에만,
   화면에는 법인 번호만이 안전한 기본값). 공개 저장소의 시험 고정물은 모두 합성이다.

## Consequences

- **용량과 비용**: 첫 채우기 약 16.0 GB: 국민연금 13,100 MB(지난 판 12,984 + 지금 판 115) + 고용·산재 2,890 MB(지난 판 2,756 + 지금 판 134), 그 뒤 매달 약 115 MB 와 매년
  약 134 MB. R2 저장 비용은 달에 약 0.24 달러다(GB 당 0.015 달러).
- **제공자 부담**: 첫 채우기 동안 하루에 국민연금 목록 1 + 메타데이터 21 + 파일 21(많아야 약 2.5 GB), 고용·산재 목록 1 + 메타데이터 5 + 파일
  5(많아야 약 1.75 GB), 요청 사이 5 초. 그 뒤 하루 네 요청과 새 판.
- **알려진 한계**: 판은 파일 id 로만 알아본다. 포털이 같은 바이트를 새 id 로 다시 올리면 한 번 더 받는다 — 고용·산재 목록의
  2020-12-31 판 둘(2021-04-19·2022-08-09 등록)이 크기가 같다(357,493,430 바이트). 지난달의 지금 판이 목록으로 옮겨 갈 때 id 가
  바뀌는지는 첫 달에 본다(바뀌면 체크섬으로 알아보게 고친다).
- **남은 일**: (1) 두 표의 Silver(국민연금 월 파일을 사업장×월로, 고용·산재를 사업자번호로; 법정동 코드·주소로 필지·단지에 잇기,
  ADR-0406 §2); (2) 첫 실행들의 저널로 `measured_run_seconds` 를 적고 `first_run` 을 지우는 PR; (3) Open DART·국세청 상태조회·
  금융위 기업정보는 사장의 계정 일이라 여기 없다.

## Alternatives

| 대안 | 왜 아닌가 |
| --- | --- |
| 국민연금 OpenAPI(3046071, 자동 승인) | 활용신청과 키가 필요하고, 지금 판만 준다. 파일 쪽은 키 없이 지난 판까지 준다 |
| 지금 판만 받기 | 130 달·16 해의 이력(사업장의 생멸·가입자 수 추이)을 잃는다. 포털은 오래된 판부터 내린다 |
| 한 실행에 전체 이력 | 국민연금만 약 13 GB 를 한 번에 — 유닛 시간과 제공자 부담이 크다. 상한으로 나눠도 며칠이면 끝난다 |
| 매일의 cycle 에 두 파일을 더하기 | cycle 은 파일마다 Silver 를 쌓는다; 두 파일은 Silver 가 없고, 큰 이력 채우기가 다른 파일의 매일 적재를 늦춘다 |
| 새 계약·새 수집기 | 포털·경로·간격·형식 검사가 data.go.kr 파일 계약과 같다; 한 곳에 둔다 |
