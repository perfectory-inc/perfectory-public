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
- 품질 작업은 11-19 부터 모든 계약의 규칙을 계약 파일에서 읽어 돌린다(W82b). 처음 판정한 계약은 7일 동안 보고만 한다.
  하지만 결과는 여전히 DataHub 의 assertion 에 적는다. 그래서 결과 기록이 남는 것은 DataHub 가 assertion 을 가진 계약뿐이고
  (11-19 의 등록 35개), 나머지는 저널에 출력만 된다.
- 계약은 11-19 의 143개에서 지금 177개다(그 사이 새 표의 계약이 생성됐다). 모두 파이프라인 그래프의 노드 하나와
  `table_name` 으로 맞는다(겹침 0, 빠짐 0). 177개 모두 칸을 적고, 허용 값 규칙(`invalidValues`)은 한 계약의 네 칸에만 있다.

## Decision

1. **사실의 자리.** 어떤 사실도 카탈로그에만 있지 않다. DataHub 가 담던 것은 아래 자리에서 읽는다.

   | 사실 | 자리 | 읽는 것 |
   |---|---|---|
   | 실행 기록 | Dagster 의 run 저장소. `shadow` 넷은 11-25 까지 Airflow `dagRuns` | 운영 현황 예약 작업 칸, Slack 경보 (PR 2) |
   | 품질 규칙 | 계약 파일 `contracts/data/*.odcs.yaml` | 품질 작업, Dagster 코드 위치 |
   | 품질 결과 | Dagster 자산 검사의 평가(event log) | 운영 현황 품질 칸, 품질 작업의 "바뀌지 않은 표" 판단 |
   | 계약마다 집행이 시작되는 때 | 품질 유닛의 상태 디렉터리 `first-checked.json`(처음 판정한 시각, 전에 판정된 것은 0) | 품질 작업 |
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
   - **심각도는 계약이 집행되는지를 말한다.** 유닛이 정한다. 처음 판정되는 계약의 규칙은 7일 동안 WARN 이다(실패는 `warned`).
     그 뒤와, 전에 판정된 계약의 규칙은 통과든 실패든 ERROR 다. 검사는 막지 않는다(`blocking: false`). 막을 물질화가 이 run
     에는 없다.
   - **집행 규칙이 실패하면 run 이 실패하고(종료 1) 알림이 간다.** 그 run 의 평가도 Dagster 에 남는다. 실패한 op 가 기록한
     사건은 Dagster 가 버리므로(1.13.27: op 이 값을 내거나 돌아올 때만 넘긴다), 품질 작업의 실패한 run 에서만 op 가 평가를
     인스턴스에 run 없는 평가로 넣는다. Dagster 의 `report_asset_check` 가 쓰는 같은 길이고, 각 평가는 그 run id 를 적는다.
     다른 작업의 실패한 run 은 아무 평가도 남기지 않는다(실패한 그림자 빌드의 판정은 판정이 아니다).
   - 바뀌지 않은 표(스냅숏과 규칙 지문이 직전 평가와 같은 표)는 다시 세지 않는다. 직전 결과를 이 run 의 평가로, 오늘의
     심각도로 다시 낸다. 직전 평가는 Dagster 의 읽기 전용 웹서버에서 읽는다(`jobs.v1.json` `observers.dagster.ui`). 웹서버가
     답하지 않으면 건너뛰기만 없어지고 모든 표를 센다(조정자가 같은 오류를 넘기듯이).
   - **옮겨 갈 때.** `first-checked.json` 에는 DataHub 가 결과를 가진 계약이 없다(그것들은 DataHub 로 "전에 판정됨"을 알았다).
     이 파일에 없는 계약을 만나면 DataHub 에 한 번 묻고, 판정한 적이 있으면 0 을, 없으면 지금을 적는다. 첫 run 이 이미
     판정된 계약을 모두 적은 뒤에는 새 계약에만 묻는다. DataHub 가 답하지 않으면 아무것도 적지 않고 그 run 에서만 WARN 으로
     내며, 다음 run 에 다시 묻는다. PR 4 는 그 전에 호스트에서 읽기 전용으로, 판정되는 계약이 모두 이 파일에 있는지 본다.
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
   | 자산 검사와 그 평가의 기록·화면(`AssetCheckEvaluation`, 인스턴스의 run 없는 평가), run 저장소, 자산 그래프, Pipes 메시지 형식 | **Dagster 기본 기능** | 계속 |
   | 계약 파일(ODCS v3.1.0)과 그 생성기, 파이프라인 그래프 | 우리 것, 계속(표준 형식) | 계속 |
   | 계약에서 검사를 생성하는 `contract_checks.py`, 품질 작업의 규칙 실행(`data-quality-check.py`) | 우리 것, 계속 | 계약의 `quality` 를 직접 읽어 Dagster 검사를 만드는 OSS 도구를 쓸 때. 지금 후보: `datacontract-cli`(검사는 하지만 Dagster 검사를 만들지 않는다) |
   | 저널의 Pipes 줄을 읽는 것(`systemd_pipes.JournalMessageReader`)과 op 의 받는 규칙(`check_evaluations`: `data_quality` 만 남의 표의 계약 검사, 심각도는 유닛), 실패한 run 의 평가 넣기 | 우리 것, 계속 | 유닛이 Dagster 의 Pipes 클라이언트로 직접 보고할 때(systemd 유닛을 Dagster 가 띄울 때) |
   | 검사를 혼자 돌릴 때의 다시 말하기(`restate`), 건너뛰기의 GraphQL 읽기 | 우리 것, 계속 | 위와 같은 때 |
   | 첫 판정 7일 보고만(WARN), `first-checked.json` | 우리 것, 계속 | 끝 조건 없음. ODCS 의 `quality[].severity` 는 고정 값이라 "첫 판정 뒤 7일"을 적지 못한다 |
   | Foundation 의 고정 질문(품질 칸, 실행 기록, 검색·상세)과 메모리 거름 검색 | 우리 것, 계속 | §5 의 조건 하나가 참일 때 |
   | 품질 작업이 DataHub 에 묻는 "전에 판정됨" | 우리 것, **한시** | 판정되는 계약이 모두 `first-checked.json` 에 있을 때. 지우는 것은 PR 4 |

   DataHub 컨테이너와 그 배포 단계는 이 표의 셋 중 어느 것도 아니다. PR 4(11-25 뒤)가 걷어 낸다.

## Consequences

- **결과 기록이 DataHub 가 아는 계약에서 177 계약으로 넓어진다.** 판정 자체는 11-19 부터 모든 계약에 돌았다. 보고만 하는 7일은
  다시 시작하지 않는다(`first-checked.json` 이 그대로다). 레이크하우스에 아직 표가 없는 계약(11-19 에 잰 것으로 6개)은 평가가
  없고, 품질 칸에 "결과 없음"으로 보인다.
- **Dagster 에 검사가 181개 생긴다.** `contract_columns_present` 177, `contract_allowed_values_*` 4. 코드 위치를 불러올 때 계약 파일
  177개를 읽는다(프로세스마다 한 번). 릴리스에 이미 있는 파일이고, 이미지에는 싣지 않는다. 계약이 그래프와 어긋나면 불러오기가
  실패하므로, CI `dagster-observe` 레인이 실제 계약 전부로 같은 선언을 짓는다.
- 품질 칸은 검사 목록을 Dagster 에서 읽는다. 코드 위치가 불러지지 않아 계약 검사가 하나도 없으면, 빈 목록이 아니라 503 이다.
- 운영 현황 품질 칸의 응답에서 `dataset_urn`·`assertion_urn` 이 `asset_key`·`check_name` 으로 바뀐다. 더니어는 목록 키만 바꾼다.
  검사 종류는 `DATA_SCHEMA`·`FIELD` 에서 검사 이름으로 바뀐다.
- PR 4 까지 DataHub 는 돈다. 그 사이 품질 결과는 DataHub 에 새로 쌓이지 않는다. DataHub 의 assertion 결과는 11-23 에서 멈춘 기록이다.
- 증명. Python 은 CI(`dagster-observe` 레인, 코드 위치 이미지 안)가 판정한다. Rust 는 CI 가 판정한다. 배포 뒤에는 DataHub 가 아직
  돌므로, DataHub 가 결과를 가진 계약마다 그 마지막 결과와 Dagster 의 첫 평가를 호스트에서 읽기 전용으로 맞대 본다. 몇 개인지는
  그때 DataHub 에서 센다. 나머지 계약은 Dagster 의 평가가 177 계약의 판정 줄(저널)과 같은지 본다.
- 되돌리기: 이 PR 의 revert. Dagster 에 남은 평가는 해가 없다(검사 선언이 사라지면 화면에도 없다).

## 기각한 대안

- **REST `POST /report_asset_check/` 로 쓰기**(설계 비교의 "계획" 2): OSS 웹서버의 REST 에는 인증이 없고, 호스트가 닿는 웹서버는
  읽기 전용이다. 쓰기 웹서버를 열거나 쓰기 관문에 REST 를 더해야 한다. Pipes 줄은 이미 있는 길이다.
- **DataHub 와 Dagster 에 두 주 동안 같이 쓰기**(설계 비교의 "계획" 2): 소유자가 "빠르게"를 골랐다. DataHub 는 PR 4 까지 돌고
  마지막 결과가 남으므로, DataHub 가 아는 계약은 배포 뒤 한 번 맞대 보는 것으로 같은 것을 증명한다.
- **계약마다 검사 하나(칸 결과는 메타데이터)**: 칸마다 "바뀌지 않음"과 결과를 따로 보이던 화면이 칸 단위를 잃는다.
- **DataHub 를 Airflow 가 없어질 때까지 두기(선택지 C)**: 3.8 GiB 와 경보 경로의 느린 GMS 를 그만큼 더 지고, 얻는 사실은 없다.

## 개정 각주 — 2026-11-24, §1·§3 의 조각(PR 3): 검색·상세는 그래프 노드와 Iceberg 의 현재 스키마다

이 각주는 결정을 바꾸지 않는다. §1 표의 칸·계보·검색 줄을 구현한 모양과, 표와 다른 한 가지를 적는다.

- **식별자.** 응답 형식은 그대로다(§3). `urn` 의 값이 DataHub URN 에서 파이프라인 그래프의 노드 `id` 가 된다. 이름은 노드의 `title`,
  `platform` 은 노드의 `type`, 설명은 표 이름(있으면)과 노드의 `description` 이다. 작업(`dataJob`) 엔티티는 없다: 계보는 표 단위 간선이다.
- **검색.** Foundation 이 릴리스의 그래프 파일(이미지에 실린 `pipeline-graph.v1.json`)을 메모리에서 거른다. 물음의 낱말이 모두 노드의
  `id`·`title`·`table_name`·`description` 에 있어야 하고, 대소문자는 가리지 않으며, 순서는 그래프의 순서다. 노드는 291개다.
- **표와 다른 것: 칸 이름으로는 찾지 않는다.** §1 은 "계약의 칸 이름"도 거른다고 적었다. 계약 파일은 YAML 이고 Foundation 에는 YAML 을 읽는
  의존성이 없으며, 계약의 이름·설명은 그래프에서 생성된 것이라 그래프가 이미 가진다. 칸 이름 검색 하나를 위해 의존성이나 색인 파일을
  더하지 않았다. 칸은 상세에서 본다. 칸 이름 검색이 필요해지면 계약 생성기가 칸 이름을 그래프 노드에 싣는 것이 길이다.
- **칸.** `table_name` 이 있는 노드는 Iceberg REST `loadTable` 의 현재 스키마(`current-schema-id`)를 읽는다(읽기 전용 토큰, 운영 지도와 같은
  설정). 형은 Iceberg 의 형 이름이고 설명은 스키마의 `doc` 이다. 계약의 칸에는 설명이 없어(177 계약 모두) 계약에서 읽을 것이 없다.
  레이크하우스에 아직 없는 표는 칸 없이 답하고, 카탈로그가 답하지 않으면 503 이다.
- **계보.** 그래프의 간선에서 한 걸음 앞·뒤의 노드를 각각 한 번씩 낸다.
- **더니어.** 데이터 흐름의 상세는 노드 `id` 로 상세를 연다(`catalog_urn` 을 거치지 않는다). "DataHub 계보" 링크를 지웠다: 같은 화면의
  앞뒤 흐름 그림이 그 일을 한다. 작업 목록 응답의 `catalog_urn`·`catalog_url` 은 PR 4 가 이름 규칙과 함께 지운다.
- **지운 것.** Foundation 의 DataHub GraphQL 호출(`SEARCH_QUERY`·`ENTITY_QUERY`)과 `FOUNDATION_PLATFORM_DATAHUB_GMS_URL` 읽기. 이 PR 뒤
  Foundation 은 DataHub 를 부르지 않는다(품질 작업의 "전에 판정됨" 한 번 묻기만 남고, PR 4 가 지운다).
- **시험.** `entities.rs`(노드→엔티티, 검색·쪽, 이웃), `iceberg_rest_catalog_adapter.rs`(현재 스키마의 칸, 없는 표). OpenAPI 문서는 두 경로의 태그만 바뀐다(핸들러가 `entities` 모듈로 가서, 다른 하위 모듈의 경로처럼 모듈 이름이 붙는다).
