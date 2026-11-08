# ADR 0452: 표준데이터는 사람이 등록해야 하는 OpenAPI 키가 아니라 데이터셋 페이지의 키 없는 내려받기로 받는다

- Status: Accepted
- Date: 2026-11-09
- Amends: ADR-0401 — 표준데이터 3개(학교 위치·대학·도시철도역)의 수집 경로. ADR-0401 의 행 키·장소 연결·바뀐 행만 쌓기·
  법정동 코드 변경 따라가기(ADR-0329 §3)는 그대로다.
- Builds on: ADR-0280 §4(data.go.kr 파일 데이터 수집기), ADR-0318(바뀐 키만 쌓는다), ADR-0386(run 당 상한)

## Context

ADR-0401 은 표준데이터 3개를 공공데이터 OpenAPI(`api.data.go.kr/openapi/tn_pubr_*`)로 받게 했다. 2026-11-04 게이트웨이는
세 작업 모두 우리 키에 `SERVICE_KEY_IS_NOT_REGISTERED_ERROR`(HTTP 403)로 답했다. 풀려면 사장이 data.go.kr 에 로그인해 데이터셋
페이지 세 곳에서 「활용신청」을 눌러야 했다. 그때까지 세 파일은 매 run 실패하고 run 도 실패로 끝나게 되어 있었다. 사장 원칙은
"사람이 할 일을 두지 않는다"(2026-11-07)다.

**측정 (2026-11-09, 키 없이).** 데이터셋 페이지(`data.go.kr/data/<번호>/standard.do`)의 CSV·JSON 내려받기 단추는 페이지
스크립트(`std-download-manager.js`)가 두 요청으로 만든다.

1. `/download/columList.json?pk=<번호>&ext=CSV` — 지금의 표(`svcTableNm`), 칸 목록(`colNmList`), 행 수(`totalCount`).
2. `/download/standard.json?publicDataPk=…&svcTableNm=…&colNmList=…&perPage=10000&page=N` — 칸 코드를 키로 한 행의 JSON 배열.

| 데이터셋 | 행(`totalCount`) | 쪽 | JSON | gzip | 답 시간 | 행 키 |
|---|---:|---:|---:|---:|---:|---|
| 15021148 전국초중등학교위치 | 12,004 | 2 | 7,467,170 B | 842,677 B | 0.9 s + 0.5 s | `SCHOOL_ID` 12,004 모두 다름 |
| 15107736 전국대학및전문대학정보 | 1,995 | 1 | 1,196,308 B | 96,834 B | 0.3 s | (`SCHL_NM`, `MAINBRANCH_NM`) 1,995 모두 다름 |
| 15013205 전국도시철도역사정보 | 898 | 1 | 446,116 B | 51,705 B | 0.1 s | 없음(행 SHA-256), 898 행 모두 다름 |

행 수는 2026-11-04 포털 내려받기로 잰 값(12,004·1,995·898)과 같다. 모든 행이 칸 목록과 `INSTT_CODE`·`INSTT_NM` 을 갖는다.
Silver 적재의 `parse` 가 세 판을 모두 읽었다(12,004·1,995·898행).

다른 data.go.kr 원천(저장소에 나오는 번호 36개, 2026-11-09 페이지를 하나씩 확인): 파일 데이터(`fileData.do`)는 이미 키 없이
받는다. OpenAPI 만 있는 것 — 온비드 13개(사장이 2026-10-30 활용신청 끝냄), 건축HUB 2개(15134735·15135963), 국세청 15081808,
한전 15101404, 산단공 15152884 — 은 공개 파일이 없다. 활용신청에 막혀 있으면서 공개 파일이 있는 원천은 이 셋뿐이다.

## Decision

1. **표준데이터는 데이터셋 페이지의 키 없는 내려받기로 받는다.** 계약 `data-go-kr-file-data.contract.json` 에
   `standard_download`(두 경로, `perPage` 10,000)를 두고, 세 파일은 `format: standard_pages`, `standard.public_data_pk`,
   `standard.max_pages`(3·1·1)를 갖는다. 작업 이름은 `stdData<번호>` 다(`fileData<번호>` 와 같은 꼴). 카탈로그
   (`public-source-endpoint-catalog.v1.json`)의 세 endpoint 는 `bulk_file`·`public_web_page`·`auth_kind: none` 이다.
2. **지금의 표는 run 마다 페이지에게 묻는다.** 수집기는 칸 목록 답에서 표 이름과 칸 목록을 읽어 쪽을 묻는다. 상태 파일이나 계약에
   표 이름을 적지 않으므로, 제공자가 표·칸을 바꾸면 다음 run 이 그대로 본다. 판의 id 는 ADR-0401 그대로 정렬한 행의 SHA-256 이라,
   행이 같으면 아무것도 남기지 않고 다르면 칸 목록 답과 모든 쪽 답의 원문 줄(`kind: columns|rows`)을 gzip JSON lines 한 객체로
   남긴다.
3. **상한과 거부.** `totalCount` 가 `max_pages` 보다 많은 쪽을 요구하면 행을 묻기 전에 거부한다(ADR-0386). 받은 행 수가
   `totalCount` 와 다르거나 쪽이 행 배열이 아니면 거부한다. Silver 적재는 계약 머리줄(칸 코드)에 없는 칸이 오면 판을 거부한다.
4. **OpenAPI 는 교차 확인일 뿐이다.** `DATA_GO_KR_SERVICE_KEY` 가 있으면 새 판을 남긴 run 이 작업마다 한 행만 물어 그
   `totalCount` 가 판의 행 수와 같은지 manifest `openapi_check` 에 적는다(`matches`·`differs`·`skipped`). 키가 없거나 거부돼도
   파일은 실패하지 않는다. 사람이 키를 등록하기를 기다리는 길은 없다.
5. **일반화.** 활용신청에 막힌 data.go.kr 원천에 공개 파일이나 키 없는 내려받기가 있으면 그것을 정본 경로로 하고 OpenAPI 는
   교차 확인으로 둔다. 표준데이터가 더 생기면 계약 `files` 에 `standard_pages` 항목 하나를 더한다(새 코드 없음).

## Consequences

- 세 파일은 키 없이 하루 한 번 확인된다. 한 run 의 요청은 7개(칸 목록 3·쪽 4), 답 2초 안팎, 간격 5초를 넣어 약 35초다.
  ADR-0401 의 OpenAPI 쪽 수(1,000행 쪽 16개, 약 80초)보다 적다.
- 머리줄이 OpenAPI 필드 이름(`schoolId` 등)에서 내려받기 칸 코드(`SCHOOL_ID` 등)로 바뀐다. 세 표는 아직 한 판도 쌓이지 않았으므로
  (2026-11-09 `r2.silver` 에 표가 없다) 옮길 행은 없다.
- 작업 `data_go_kr_file_data` 는 여전히 꺼져 있다(`enabled: false`, ADR-0386 §3: 켜기 전에 운영자 run 으로 잰다). 이 결정은 그
  run 이 세 파일 때문에 실패하지 않게 할 뿐, 켜는 일은 따로다.
- 위험: 내려받기는 문서화된 API 가 아니라 페이지 스크립트의 경로다. 포털이 경로를 바꾸면 칸 목록 요청이 HTTP 오류나 JSON 아님으로
  실패하고, 그 파일만 실패한다(다른 파일은 받는다). 그때 키가 등록돼 있으면 OpenAPI 교차 확인 기록이 남아 있다.
