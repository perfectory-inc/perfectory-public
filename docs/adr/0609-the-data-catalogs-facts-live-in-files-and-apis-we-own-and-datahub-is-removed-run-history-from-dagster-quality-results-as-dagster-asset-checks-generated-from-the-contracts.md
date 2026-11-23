# ADR 0609: 데이터 카탈로그의 사실은 우리가 가진 파일과 API 에 있고 DataHub 는 걷어 낸다 — 실행 기록은 Dagster, 품질 결과는 계약에서 생성한 Dagster 자산 검사, 칸·계보는 계약·Iceberg REST·파이프라인 그래프

- Status: Accepted (2026-11-23, 소유자 결정 "DataHub 를 걷어 내라, 빠르게"; 코디네이터가 네 PR 계획을 승인)
- Date: 2026-11-23
- Supersedes: [ADR-0117](./0117-metadata-has-one-contract-per-dataset-and-one-place-to-look.md) §3(사람이 보는 곳은 DataHub 하나), §4(DataHub 의 Zitadel 로그인), §6(품질 결과는 DataHub);
  [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) §2(실행 기록은 OpenLineage 로 카탈로그에), §4(품질 결과는 카탈로그에);
  [ADR-0119](./0119-staff-read-the-data-catalog-in-dawneer-through-foundation.md) §4·§5(Foundation 이 GMS 에 닿는 길과 그 보호);
  [ADR-0165](./0165-dawneer-shows-operations-status-from-the-data-catalog-and-the-job-list.md) §1·§2 의 출처 칸(실행은 DataHub 의 OpenLineage 기록, 품질은 DataHub `odcs` 데이터셋)
- Builds on: [ADR-0432](./0432-a-published-dataset-passes-blocking-dagster-asset-checks-generated-from-its-contract.md) §5(계약 `quality` 에서 검사 종류를 생성하는 일은 다음 조각),
  ADR-0123(계약은 두 정본에서 생성), ADR-0583(Dagster 가 정한다), [ADR-0605](./0605-scheduled-job-pools-are-cut-by-the-resource-they-guard-and-counted-by-dagsters-own-pools-and-a-jobs-wait-is-measured-not-computed.md)(같은 표 형식)
- 설계 비교: [docs/designs/data-catalog-datahub-vs-dagster.md](../designs/data-catalog-datahub-vs-dagster.md)(2026-11-19, 선택지 B)

## Context

DataHub 는 지금 세 가지 일을 한다. 셋 모두 사실을 만들지 않고 옮겨 담기만 한다.

| 일 | 읽는 곳 | 사실이 실제로 나오는 곳 |
|---|---|---|
| 실행 기록 | 운영 현황 예약 작업 칸과 Slack 경보(`operations.rs` `FLOWS_QUERY`, 30초마다) | Airflow·Dagster 의 실행 |
| 품질 규칙의 결과 | 품질 작업(`data-quality-check.py`)과 운영 현황 품질 칸(`CONTRACTS_QUERY`) | 계약 파일(`contracts/data`)과 품질 작업 |
| 검색·칸·한 걸음 계보 | 더니어 데이터 카탈로그 화면(`data_catalog.rs` `SEARCH_QUERY`·`ENTITY_QUERY`) | 파이프라인 그래프, 계약, Iceberg REST 카탈로그 |

11-19 에 ai-server 에서 읽기 전용으로 잰 값(설계 비교의 "잰 사실"):

- 컨테이너 일곱 개가 **3.79 GiB** 를 쓰고 상한 합은 **7.06 GiB** 다. 그때 스왑은 4,095 MiB 중 3,979 MiB 였다.
- 경보만으로 GMS 를 하루 약 **5,800번** 부른다. GMS 로그 24시간에 느린 GraphQL 118줄(p90 5.5초)이 있었고, foundation-api 는 40분에 `data catalog unreachable` 을 세 번 남겼다.
- 표·칸 수집은 10-01 이 마지막이다(누구도 일정을 걸지 않았다). 계약 143개 중 35개만 등록됐고, 용어집·도메인·태그는 0개다.

2026-11-23 의 main:

- 사용 중인 작업 117개 가운데 113개가 이미 Dagster run 으로 돈다(`dagster` 107, `proxied` 6). Airflow 가 돌리는 것은 `shadow` 넷이고, 11-25 W82b 의 전환 뒤에는 없다.
- 품질 작업은 11-19 부터 규칙을 계약 파일에서 읽는다(W82b). 하지만 결과는 여전히 DataHub 의 assertion 에 적는다.
  그래서 DataHub 가 assertion 을 가진 35개만 기록이 남고, 나머지는 출력만 된다.
- 계약은 177개다. 모두 파이프라인 그래프의 노드 하나와 `table_name` 으로 맞는다(겹침 0, 빠짐 0). 177개 모두 칸을 적고,
  허용 값 규칙(`invalidValues`)은 한 계약의 네 칸에만 있다.

## Decision

1. **사실의 자리.** 어떤 사실도 카탈로그에만 있지 않다. DataHub 가 담던 것은 아래 자리에서 읽는다.

   | 사실 | 자리 | 읽는 것 |
   |---|---|---|
   | 실행 기록 | Dagster 의 run 저장소. `shadow` 넷은 11-25 까지 Airflow `dagRuns` | 운영 현황 예약 작업 칸, Slack 경보 (PR 2) |
   | 품질 규칙 | 계약 파일 `contracts/data/*.odcs.yaml` | 품질 작업, Dagster 코드 위치 |
   | 품질 결과 | Dagster 자산 검사의 평가(event log) | 운영 현황 품질 칸, 품질 작업의 "바뀌지 않은 표" 판단 |
   | 칸(이름·형·설명) | Iceberg REST `loadTable` + 계약의 칸 설명 | 데이터 카탈로그 상세 (PR 3) |
   | 표 단위 계보 | `docs/catalog/pipeline-graph.v1.json`(Dagster 자산 그래프와 같은 간선) | 상세의 앞뒤 흐름, 흐름 지도 (PR 3) |
   | 검색 | 그래프 노드와 계약의 이름·설명·칸 이름을 Foundation 이 메모리에서 거른다 | 데이터 카탈로그 검색 (PR 3) |
   | 담당 | 계약의 `team` | 상세 |

2. **품질 검사는 계약에서 생성한 Dagster 자산 검사다(ADR-0432 §5 의 다음 조각).**
   - `orchestration/dagster/contract_checks.py` 가 계약 파일마다 검사를 선언한다. 대상 자산은 그 계약의 `physicalName` 을
     `table_name` 으로 가진 그래프 노드다. 자산 키·표 이름은 코드에 적지 않는다.
     - `contract_columns_present`: 계약이 칸을 적은 표마다 하나. 계약의 칸이 모두 물리 표에 있는가.
     - `contract_allowed_values_<칸>`: 허용 값 규칙 하나마다 하나. 값이 있는 행이 모두 허용 값 안에 있는가.
   - 평가는 품질 작업의 유닛이 낸다. 유닛은 칸마다 결과를 Pipes `report_asset_check` 줄로 저널에 쓰고, 그 유닛을 돌린 op 가
     평가로 기록한다(실버 적재의 결과 검사와 같은 길, ADR-0559). REST `report_asset_check` 쓰기도, 인증 없는 웹서버 쓰기도 없다.
   - **op 의 규칙 하나가 넓어진다.** 지금까지 run 은 자기가 쓰는 데이터셋만 판정했다. 품질 작업은 남이 쓰는 표를 판정하는 것이
     일이다. 그래서 `contract_checks` 가 그 검사를 낼 작업으로 `data_quality` 를 선언하고, op 는 그 작업에만 계약 검사를 받는다.
     다른 작업이 계약 검사를 내면, 또 품질 작업이 다른 검사를 내면 지금처럼 run 이 실패한다.
   - **심각도는 유닛이 정한다.** 처음 판정되는 계약은 7일 동안 보고만 한다(`warned`, WARN). 그 뒤와, 전에 판정된 계약은 ERROR 다.
     검사는 막지 않는다(`blocking: false`). 막을 물질화가 이 run 에는 없다. 집행 규칙이 실패하면 run 이 실패하고(종료 1)
     알림이 간다. 지금과 같다.
   - 바뀌지 않은 표(스냅숏과 규칙 지문이 직전 평가와 같은 표)는 다시 세지 않는다. 직전 평가는 Dagster 의 읽기 전용 웹서버에서
     읽는다(`jobs.v1.json` `observers.dagster.ui`, 조정자와 같은 길).
   - **옮겨 갈 때 한 번.** 첫 판정 시각을 적은 상태 파일(`first-checked.json`)에 DataHub 가 결과를 가진 35개가 없다.
     DataHub 가 판정 기록을 가진 계약 가운데 이 파일에 없는 것만, 첫 run 에서 "전에 판정됨"으로 적는다. 그 뒤로는 묻지 않는다.
     이 읽기는 PR 4 가 지운다.
   - 운영 현황 품질 칸(`GET /data-catalog/v1/quality-checks`)은 Dagster 에서 계약 검사의 목록과 마지막 평가를 읽는다.
     식별자는 DataHub URN 대신 자산 키와 검사 이름이다(`asset_key`, `check_name`).

3. **Dawneer 의 두 화면도 같은 원칙이다.** 실행 기록(PR 2)과 검색·상세(PR 3)는 이 ADR 의 §1 표를 따른다. 응답 형식은 그대로
   두고 출처만 바꾼다. "DataHub 계보" 링크는 흐름 지도의 노드 링크로 바뀐다.

4. **DataHub 는 배포가 걷어 낸다(PR 4, 11-25 Airflow 의 OpenLineage 송신이 끝난 뒤, W82b 와 함께).**
   - 사람의 손 단계 없이 배포가 `datahub` compose 프로젝트를 멈추고 지운다.
   - 같이 지우는 것: 배포의 DataHub 단계(`apply_datahub`), `compose.metadata-bridge.yml`, 런타임 비밀·호스트 배치·플랫폼 정의의
     `datahub` 항목, Zitadel 앱, 이미지 핀 다섯, 메모리 상한 7.06 GiB.
   - `infra/datahub/dataset_names.py` 는 `orchestration/` 으로 옮긴다. Airflow 자산, Dagster 자산 키, DataHub URN 이 함께 쓰는
     이름 규칙이다. 수집 레시피와 compose 파일은 보관 폴더로 옮긴다. Dagster 의 OpenLineage 송신 코드(`lineage.py`)는 송신처
     없음으로 남긴다.
   - 볼륨은 30일 남긴다. 지우는 것도 배포가 하는 다른 PR 이다(되돌릴 수 없는 유일한 걸음).

5. **다시 들이는 길.** 계약·그래프·Iceberg·Dagster 가 사실을 가지므로, DataHub 를 다시 쓰려면 수집을 다시 켜면 된다
   (레시피 둘, Dagster 플러그인 또는 OpenLineage). 아래 가운데 하나가 참이 되면 다시 본다. 팀 밖의 사람 다섯 명 넘게 스스로
   데이터를 찾는다. Dagster 밖의 원천을 함께 검색해야 한다. 칸 단위 분류·접근 신청이 의무가 된다. 칸 단위 계보가 필요하다.
   데이터를 밖에 판다. 용어집을 실제로 쓴다. 노드가 수천 개를 넘는다. 근거는 설계 비교의 "계기" 다.

6. **Dagster 기본 기능 / 우리 것(계속) / 우리 것(한시).**

   | 것 | 구분 | 언제까지 |
   |---|---|---|
   | 자산 검사와 그 평가의 기록·화면, run 저장소, 자산 그래프, Pipes `report_asset_check` | **Dagster 기본 기능** | 계속 |
   | 계약 파일(ODCS v3.1.0)과 그 생성기, 파이프라인 그래프 | 우리 것, 계속(표준 형식) | 계속 |
   | 계약에서 검사를 생성하는 `contract_checks.py`, 품질 작업의 규칙 실행 | 우리 것, 계속 | 계약의 `quality` 를 직접 읽는 OSS 검사기로 바꿀 때 |
   | op 가 `data_quality` 에만 남의 표의 계약 검사를 받는 규칙 | 우리 것, 계속 | 같은 |
   | 첫 판정 7일 보고만(WARN) | 우리 것, 계속 | 계약이 심각도를 적을 때 |
   | Foundation 의 고정 질문(품질 칸, 실행 기록, 검색·상세)과 메모리 거름 검색 | 우리 것, 계속 | §5 의 조건 하나가 참일 때 |
   | 품질 작업이 DataHub 에 한 번 묻는 "전에 판정됨" | 우리 것, **한시** | PR 4 |
   | DataHub 컨테이너와 그 배포 단계 | 걷어 낼 것, **한시** | PR 4(11-25 뒤) |

## Consequences

- **판정이 35 → 177 계약이 된다.** 새로 판정되는 142개는 7일 동안 WARN 만 낸다. 첫 판정이 11-20~22 였던 것은 11-27~29 부터 집행된다.
- **Dagster 에 검사가 181개 생긴다.** `contract_columns_present` 177, `contract_allowed_values_*` 4. 코드 위치를 불러올 때 계약 파일
  177개를 읽는다. 릴리스에 이미 있는 파일이고, 이미지에는 싣지 않는다.
- 운영 현황 품질 칸의 응답에서 `dataset_urn`·`assertion_urn` 이 `asset_key`·`check_name` 으로 바뀐다. 더니어는 목록 키만 바꾼다.
  검사 종류는 `DATA_SCHEMA`·`FIELD` 에서 검사 이름으로 바뀐다.
- PR 4 까지 DataHub 는 돈다. 그 사이 품질 결과는 DataHub 에 새로 쌓이지 않는다. DataHub 의 assertion 결과는 11-23 에서 멈춘 기록이다.
- 증명. Python 은 CI(`dagster-observe` 레인, 코드 위치 이미지 안)가 판정한다. Rust 는 CI 가 판정한다. 배포 뒤에는 DataHub 가 아직
  돌므로, 계약마다 DataHub 의 마지막 결과와 Dagster 의 첫 평가를 호스트에서 읽기 전용으로 맞대 본다.
- 되돌리기: 이 PR 의 revert. Dagster 에 남은 평가는 해가 없다(검사 선언이 사라지면 화면에도 없다).

## 기각한 대안

- **REST `POST /report_asset_check/` 로 쓰기**(설계 비교의 "계획" 2): OSS 웹서버의 REST 에는 인증이 없고, 호스트가 닿는 웹서버는
  읽기 전용이다. 쓰기 웹서버를 열거나 쓰기 관문에 REST 를 더해야 한다. Pipes 줄은 이미 있는 길이다.
- **DataHub 와 Dagster 에 두 주 동안 같이 쓰기**(설계 비교의 "계획" 2): 소유자가 "빠르게"를 골랐다. DataHub 는 PR 4 까지 돌고
  마지막 결과가 남으므로, 배포 뒤 한 번 맞대 보는 것으로 같은 것을 증명한다.
- **계약마다 검사 하나(칸 결과는 메타데이터)**: 칸마다 "바뀌지 않음"과 결과를 따로 보이던 화면이 칸 단위를 잃는다.
- **DataHub 를 Airflow 가 없어질 때까지 두기(선택지 C)**: 3.8 GiB 와 경보 경로의 느린 GMS 를 그만큼 더 지고, 얻는 사실은 없다.
