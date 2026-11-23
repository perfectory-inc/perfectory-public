---
status: current
owner: foundation-platform
doc_type: reference
last_reviewed: 2026-11-23
---

# 데이터 카탈로그: DataHub 를 둘 것인가, Dagster + Iceberg REST + ODCS 로 바꿀 것인가

상태: 소유자의 결정을 위한 설계 비교다. ADR 이 아니다. 2026-11-19 에 로컬 `main` 의 `d28af46c`(#1459)와
같은 날 ai-server 에서 읽기 전용으로 잰 값을 기준으로 썼다.

아래 경로는 `platforms/foundation-platform/` 기준이다(`docs/catalog/` 와 `docs/runbooks/` 포함).
단 `consoles/`, `tools/`, `scripts/ci/`, `scripts/guard/` 로 시작하는 경로는 저장소 루트 기준이다.
ADR 은 `docs/adr/` 의 루트 ADR 이다.

## 개요

질문: DataHub 를 그대로 둘 것인가, 아니면 Dagster 자체 카탈로그(자산, 계보, 자산 검사), Iceberg REST
카탈로그, 저장소의 ODCS 계약 파일로 바꾸고, 필요하면 나중에 DataHub 를 다시 들일 것인가?

짧은 답: **단계별로 바꾼다. DataHub 가 읽는 것은 모두 우리가 가진 파일과 API 로 둔다. 그래야 나중에
다시 들이는 일이 "수집을 다시 켠다" 로 끝난다.**

이유를 한 문단으로 쓴다. 지금 DataHub 는 우리에게 세 가지 일을 한다. (1) 운영 지도와 그 Slack 경보가
읽는 실행 기록을 저장한다. (2) 데이터 품질 규칙과 그 최신 결과를 저장한다. (3) 더니어 화면 하나에 검색,
칸, 한 걸음 계보 질문의 답을 준다. (1)과 (2)에서 DataHub 는 거쳐 가는 저장소일 뿐이다. 사실은
Airflow/Dagster 와 ODCS 파일에서 나온다. (3)의 내용은 낡았다. 마지막 스키마·계약 수집은 2026-10-01 이고,
계약 143개 중 35개만 등록됐으며, 용어집·도메인·태그·그룹이 하나도 없다. 수집에 일정을 건 적이 없어서
아무도 채우지 않는다. 스왑을 97% 쓰는 호스트에서 RAM 약 3.8 GiB 를 쓰고, AWS 에서는 백엔드만으로 한 달에
대략 $120 에서 $230 이 든다(OpenSearch, Kafka, MySQL 이 필요하다). 어차피 옮겨 가는 Dagster 가 이미 자산,
자산 그래프, 자산 검사 기록을 가진다. 저장소는 이미 선언된 계보(`docs/catalog/pipeline-graph.v1.json`)와
계약을 가진다. Iceberg REST 카탈로그는 이미 실제 스키마를 가진다.

잃는 것: 설명과 칸을 가로지르는 전문 검색, 칸 단위 계보, 용어집, 소유 화면, 팀 밖 사람을 위한 기성 UI 다.
지금 이 중 쓰는 것은 없다. DataHub 를 다시 들일 계기는 아래에 적는다.

## 잰 사실

모두 ai-server 에서 2026-11-19 ~02:30 UTC 에 읽기 전용으로 쟀다(`docker stats --no-stream`,
`docker inspect`, `docker logs --since`, `journalctl`, 그리고 127.0.0.1:18095 의 GMS 에 `count: 0` 이나 작은
페이지로 보낸 GraphQL `searchAcrossEntities`). 예외가 하나 있다. 줄 수를 세려고 24 h GMS 로그를 호스트의
`/tmp` 에 복사했고 바로 지웠다.

### DataHub 컨테이너(프로젝트 `datahub`)

| 컨테이너 | CPU | 메모리 사용 / 상한 |
|---|---|---|
| datahub-gms-quickstart | 2.16% | 1.477 GiB / 2 GiB |
| opensearch | 2.08% | 1011 MiB / 1.5 GiB |
| kafka-broker | 0.69% | 631 MiB / 1 GiB |
| mysql | 2.36% | 420 MiB / 1 GiB |
| frontend-quickstart | 0.07% | 223 MiB / 1 GiB |
| datahub-actions-quickstart | 0.03% | 81 MiB / 512 MiB |
| zitadel-loopback | 0.00% | 1 MiB / 64 MiB |
| **합계** | **코어 하나의 ~7.4%** | **~3.79 GiB 사용 / 상한 합 7.06 GiB** |

같은 순간의 호스트: 전체 64,113 MiB, 가용 40,735 MiB, **스왑 4,095 MiB 중 3,979 MiB 사용**.
비교하면 Dagster 컨테이너 세 개는 212 + 188 + 295 MiB 를 쓴다(daemon, webserver, code server).
foundation-api 는 상한 512 MiB 중 504 MiB 였다.

### DataHub 가 가진 것

| 엔터티 | 개수 | 비고 |
|---|---|---|
| 데이터셋 | 300 | `iceberg` 에 153, `perfectory` 에 112(선언 그래프 시드), `odcs` 에 35 |
| 스키마를 수집한 데이터셋(`lastIngested` 있음) | 80 | iceberg 45 + odcs 35; **모두 마지막 수집이 2026-10-01**(01:20 부터 13:55 UTC 까지) |
| 데이터 작업 / 데이터 흐름 | 260 / 161 | Airflow 의 OpenLineage 이벤트와 시드에서 왔다 |
| Assertion | 183 | 등록된 계약 35개에서 만들었다 |
| 소유자가 있는 데이터셋 | 36 | 계약 35개와 하나 더 |
| 사용자 / 그룹 | 2 / 0 | |
| 용어집 용어, 도메인, 데이터 제품, 태그 | 0, 0, 0, 0 | ADR-0117 §5 가 용어집을 약속했지만 쓰인 적이 없다 |

그래서 iceberg 엔터티 153개 중 108개는 계보 이벤트가 만든, 칸 없는 이름뿐인 껍데기다.
레시피 `infra/datahub/recipes/lakehouse-iceberg.yml` 과 `data-contracts.yml` 은 손으로만 돌린다
(`scripts/deploy/datahub-runtime.sh ingest`, 94번째 줄). ADR-0117 §3 은 "CI 가 병합 때 계약을 등록한다",
"Iceberg 수집기가 매일 돈다" 고 말하지만, 어떤 작업·타이머·CI 단계도 이 레시피를 돌리지 않는다.

### 품질 작업

`foundation-data-quality.service` 의 최근 실행(journal, 최근 2일):
`contracts=143 passed=35 failed=0 absent_tables=2 unregistered=110 unchanged_tables=1`.
**계약 143개 중 110개는 한 번도 검사되지 않는다.** 작업이 규칙 목록을 DataHub 에서 가져오는데, DataHub 는
계약 35개만 알기 때문이다. 실행 시간은 p50 56 s, 최대 159 s 다(`definitions/data-quality.definition.json`).

### DataHub 를 얼마나 자주 부르는가

- foundation-api 는 요청을 로그에 남기지 않는다. 그래서 화면별 직원 사용량을 로그로 셀 수 없다.
- 꾸준한 부하는 직원과 상관없다. Prometheus 가 foundation-api 를 15 s 마다 긁는다
  (`infra/observability/prometheus/prometheus.yml:2,15`). `/metrics` 는 `jobs_status()` 를 부르고
  (`services/foundation-api/src/routes/data_catalog/alerts.rs:47`), 이 함수는 30 s 보다 오래되면 새로 고치는
  보관된 읽기 결과를 내준다(`facts.rs:34`). 새로 고칠 때마다 페이지로 나눈 `FLOWS_QUERY` 를 돌린다
  (`operations.rs:549`). 흐름 161개에 2페이지다. 경보만으로 하루 약 **GraphQL 요청 5,800건**이다.
- 열린 더니어 페이지는 60 s 마다 `scheduled-jobs`(같은 보관된 읽기에서 내준다)와 300 s 마다
  `quality-checks` 를 더한다. `quality-checks` 는 캐시되지 않고 `CONTRACTS_QUERY` 전체를 페이지로 읽는다
  (`consoles/dawneer/web/src/components/OperationsTables.tsx:108,243`).
- GMS 로그, 최근 24 h: "slow operation graphql" 줄 118개, p50 1.5 s, p90 5.5 s, 최대 56 s;
  OpenLineage `dataProcessInstanceRunEvent` 쓰기 1,418건(계보 로그 줄은 모두 67,008개).
- foundation-api 는 40분 동안 `data catalog unreachable` 을 3번 남겼다(01:59, 02:02, 02:11 UTC).
  GMS 는 4주째 떠 있었으므로 이것은 GMS 재시작이 아니라 실패한 요청이다. 더 조사하지 않았다.
- DataHub UI 로그인 수는 셀 수 없었다. frontend 컨테이너가 stdout 에 로그를 쓰지 않는다.

## 필요 목록 표

DataHub 를 읽거나 쓰는 모든 곳과, 각 곳이 DataHub 에서 실제로 필요로 하는 것이다.

| # | 위치(파일:줄) | 읽기 / 쓰기 | DataHub 에서 필요한 것 | 지금 쓰는가? |
|---|---|---|---|---|
| 1 | `scripts/ops/data-quality-check.py:78-87` `assertions_of()` | 읽기 | 계약별 **규칙 목록**(assertion)과 각 규칙의 **최신 결과**(`runEvents(limit: 1)`) | 예, 매일 07:20 |
| 2 | `scripts/ops/data-quality-check.py:90-104` `report()` | 쓰기 | **Assertion 결과 저장소**(`reportAssertionResult`), 스냅샷 id 와 규칙 지문을 속성으로 붙인다 | 예 |
| 3 | `scripts/ops/data-quality-check.py:128-140` `unchanged_results()` | 읽기 | 바뀌지 않은 표를 건너뛰기 위한 **Assertion 기록**(마지막 결과) | 예 |
| 4 | `services/foundation-api/src/routes/data_catalog.rs:225-262` `search` + `SEARCH_QUERY` (322) | 읽기 | 데이터셋 **전문 검색**(`searchAcrossEntities`) | 더니어 카탈로그 검색창 |
| 5 | `data_catalog.rs:277-317` `entity` + `ENTITY_QUERY` (326) | 읽기 | **스키마**(칸, 타입, 설명)와 위아래 **한 걸음 계보** | 더니어 엔터티 패널 |
| 6 | `data_catalog/operations.rs:541-555` `catalog_flows` / `FLOWS_QUERY` | 읽기 | 모든 작업의 **실행 기록**(OpenLineage DataFlow → DataJob → runs → state) | 예, 30 s 마다(경보) |
| 7 | `data_catalog/facts.rs:148`, `alerts.rs:47`, `health.rs` | 읽기(6 경유) | 작업 건강, 놓친 시작, Slack 경보를 위한 실행 기록 | 예 |
| 8 | `data_catalog/operations.rs:216-219` `quality_checks` / `CONTRACTS_QUERY` (557) | 읽기 | 계약별 **Assertion 과 그 최신 결과** | 더니어 품질 표 |
| 9 | `data_catalog/operations.rs:41,196-204` `links()` | 환경 변수만 | 직원 링크용 DataHub **UI 주소** | 링크 |
| 10 | `consoles/dawneer/web/src/api/client.ts:232-278` | foundation-api 경유 | `search`, `entity`, `scheduled-jobs`, `quality-checks` | 예 |
| 11 | `consoles/dawneer/web/src/components/CatalogSearch.tsx:67,142` | 4, 5 경유 | 검색 결과, 칸, 이웃 | 예 |
| 12 | `consoles/dawneer/web/src/components/FlowDetails.tsx:78,301`, `lib/dataFlow.ts:537` | 링크 | DataHub 의 계보 페이지로 가는 "DataHub 계보" 링크 | 링크만 |
| 13 | `compose.orchestration.yml:35-37` | 쓰기 | 모든 Airflow 실행의 **OpenLineage 끝점** | 예, 하루 ~700회 실행 |
| 14 | PR #1432(열림, draft) `orchestration/dagster/lineage.py` | 쓰기 | Dagster 가 정하는 작업에 대한 같은 OpenLineage 보고 | 병합 안 됨 |
| 15 | `infra/datahub/seed_declared_lineage.py` | 쓰기 | 선언된 파이프라인 그래프를 데이터셋 + "build" 작업으로 쓴다; 스스로를 임시 시드라 부른다(3번째 줄) | 손으로 |
| 16 | `infra/datahub/recipes/data-contracts.yml` | 쓰기 | ODCS → 논리 데이터셋, 소유자, assertion | 손으로, 마지막 10-01 |
| 17 | `infra/datahub/recipes/lakehouse-iceberg.yml` | 쓰기 | Iceberg 표 → 스키마, 스냅샷 | 손으로, 마지막 10-01 |
| 18 | `infra/datahub/dataset_names.py` | 이름 짓기만 | URN 규칙; `orchestration/dags/job_specs.py:123-124,924` 와 `orchestration/dagster/observe_assets.py:32-33` 이 가져다 쓴다 | 예(이름 짓기, DataHub 호출 아님) |
| 19 | `infra/datahub/compose.yml` | 실행 | 서비스 7개, 상한 2g/1.5g/1g/1g/1g/512m/64m | 예 |
| 20 | `scripts/deploy/datahub-runtime.sh` (init 40, compose 63, verify 77, ingest 94, provision 125) | 실행 | 비밀, Zitadel 로그인과 함께 up/verify, 수집, 사용자 프로비저닝 | 배포 때와 손으로 |
| 21 | `scripts/deploy/datahub_changes.py`, `datahub_state.py` | 배포 | 다시 만들 서비스를 정한다; 상태 디렉터리는 `config/host-layout.contract.json:17-20` 에서 온다 | 배포마다 |
| 22 | `scripts/deploy/foundation-release.sh:1134-1160,1246-1248` `apply_datahub`; `foundation-deploy.sh:429-432` 9단계 | 배포 | 바뀐 DataHub 서비스를 다시 만든다; DataHub 가 실패하면 배포가 실패한다 | 배포마다 |
| 23 | `compose.metadata-bridge.yml:2-11` | 네트워크 | `FOUNDATION_PLATFORM_DATAHUB_GMS_URL` 과 함께 `metadata-shared` 에 붙은 foundation-api | 예 |
| 24 | `config/runtime-secrets.contract.json:143,150`, `config/host-layout.contract.json:36` | 설정 | UI URL 비밀, 건강 검사 | 예 |
| 25 | `definitions/platform.definition.json:1023-1027,1529` | 설정 | Compose 프로젝트와 loopback 중요도 | 예 |
| 26 | `../identity-platform/config/staff-console-applications.v1.json:14-15` | 설정 | Zitadel OIDC 앱 "DataHub metadata catalog (trial)" | 예 |
| 27 | `tools/technology-versions.contract.json:34-38` | 핀 | DataHub 이미지 digest 5개 | 예 |
| 28 | `scripts/catalog/render-data-contracts.py:26` | 제약 | DataHub 1.7 이 3.1 까지만 읽으므로 ODCS 를 v3.1.0 에 묶어 둔다 | 예 |

필요별로 묶으면:

| 필요 | 누가 필요로 하는가 | 얼마나 쓰는가 |
|---|---|---|
| 실행 기록(실행, 상태, 결과) | 운영 지도, 경보(6, 7) | 끊임없이; 가장 중요한 쓰임 |
| 품질 규칙 목록 + 결과 저장소 + 마지막 결과 | 품질 작업, 품질 표(1-3, 8) | 매일 + 화면; 하지만 규칙 목록의 77% 가 빠져 있다 |
| 전문 검색 | 카탈로그 검색(4) | 모른다, 직원만, 엔터티 ~300개 |
| 스키마(칸) | 엔터티 패널(5) | 모든 표가 10-01 부터 낡았다 |
| 한 걸음 계보 | 엔터티 패널(5), 링크(12) | 파이프라인 그래프로 그린 흐름 지도와 겹친다 |
| 소유, 용어집, 도메인 | 없음 | 소유자 36, 용어 0 |
| 칸 단위 계보 | 없음 | 수집하지 않는다 |

## 선택지 비교 표

선택지 A: DataHub 를 둔다.
선택지 B: Dagster OSS 1.13.27(`tools/technology-versions.contract.json:10-13`) + Iceberg REST + ODCS 파일.
선택지 C: 지금 B 로 가되, Airflow 가 없어질 때까지 DataHub 를 손대지 않고 돌린다.

| 필요 | A: DataHub | B: 무엇이 대신하는가 | B 의 빈틈 |
|---|---|---|---|
| 실행 기록, Airflow 가 정하는 작업(지금 `shadow` 88, `proxied` 6) | Airflow 의 OpenLineage | Airflow REST API(`dagRuns`, task instance). foundation-api 가 이미 여기에 로그인한다(`facts.rs:37-40`) | 이행 중 두 번째로 읽는 쪽이 생긴다; Airflow 와 함께 사라진다 |
| 실행 기록, Dagster 가 정하는 작업 | PR #1432 OpenLineage → DataHub | 내부 네트워크의 Dagster webserver GraphQL(`runsOrError`, 자산 이벤트) | Dagster 는 자기 GraphQL 이 "아직 발전 중이며 호환을 깨는 변경이 있을 수 있다" 고 말한다([문서](https://docs.dagster.io/api/graphql)); 지금 `data_catalog.rs` 가 하듯 쿼리를 고정하고 시험한다 |
| 품질 규칙 목록 | ODCS 수집이 만든 assertion | ODCS 파일을 바로 읽는다(작업이 이미 파싱한다, `data-quality-check.py:148-167`) | 없음; 검사 범위가 계약 35개에서 143개로 늘어난다 |
| 품질 결과 저장소와 기록 | `reportAssertionResult`, 실행 이벤트 | Dagster 자산 검사: 계약 규칙마다 검사 하나를 선언하고(`orchestration/dagster/asset_checks.py` 가 definitions 에 하듯) `POST /report_asset_check/`([REST API](https://docs.dagster.io/api/rest-api))로 보고하거나 Dagster 안에서 돌린다. 기록은 event log 로 읽는다. `place_identity_checks.py:89` 가 이미 쓰는 방식이다 | OSS REST 끝점에는 인증이 없다: webserver 를 내부 네트워크에만 둔다(지금도 공개하지 않는다) |
| 스키마 변경 감지 | 쓰지 않는다 | `dagster/column_schema` 메타데이터 위의 `build_column_schema_change_checks`([문서](https://docs.dagster.io/api/dagster/asset-checks)) | 자산에 스키마 메타데이터를 붙여야 한다 |
| 표의 칸 | 낡은 수집 | Iceberg REST `loadTable`(살아 있는 스키마) + ODCS 칸 설명 | `lakehouse-infrastructure` 의 `IcebergRestCatalog` 에는 아직 스키마 읽기가 없다; 메서드 하나를 더하면 된다 |
| 표 단위 계보 | 시드 + 실행 이벤트 | `docs/catalog/pipeline-graph.v1.json`(선언됨, 이미 `/catalog/v1/pipeline-graph` 로 내주고 더니어의 흐름 지도가 그린다)과 Dagster 의 자산 그래프(같은 간선, `observe_assets.py:160-193`) | 관찰된 계보(실행이 실제로 읽은 것)는 따로 저장하지 않는다; 지금은 둘 다 `job_specs` 에서 나오므로 선언된 계보와 같다 |
| 검색 | OpenSearch 전문 검색 | foundation-api 메모리에서 파이프라인 그래프 노드 ~300개 + 계약 이름/설명/칸을 거르는 필터; Dagster OSS 자산 카탈로그는 key, group, kind, owner, tag 로 거른다 | **원천을 가로지르는 순위 매긴 전문 검색이 없다.** Dagster 의 더 풍부한 카탈로그 검색과 칸/표 이름 필터는 Dagster+ Pro 에 있다([문서](https://docs.dagster.io/guides/operate/webserver)) |
| 칸 단위 계보 | 가능하지만 수집하지 않는다 | `dagster/column_lineage` 메타데이터를 내보낼 수 있다 | **UI 에서 보는 것은 Dagster+ 전용이다**([문서](https://docs.dagster.io/guides/build/assets/metadata-and-tags/column-level-lineage)) |
| 소유 | ODCS `team`, 데이터셋 36개에 보인다 | ODCS `team` 이 계속 원천이다; `AssetSpec` 의 Dagster `owners=` | 소유 절차(요청, 승인)가 없다 |
| 용어집 / 업무 용어 | 지원하지만 비어 있다 | 없음 | Dagster 에는 **용어집이 없다**; 언젠가 쓰면 용어를 파일로 둔다 |
| 로그인이 있는 직원 UI | Zitadel OIDC, 미리 만든 계정 | 더니어(이미 Zitadel 뒤에 있다)가 UI 다; Dagster UI 는 내부에 둔다 | **Dagster OSS 에는 로그인도 역할도 없다**; 직원에게 UI 를 열려면 앞에 OIDC 프록시가 필요하다 |
| 팀 밖 사람을 위한 UI | 준비돼 있다 | 없음 | 그런 사람이 생길 때만 실제 빈틈이다 |
| 운영 비용(AWS) | 비용 절 참고 | 추가 ~$0; Dagster 와 Postgres 는 어차피 옮긴다 | — |
| 운영 | 컨테이너 7개, 상태 저장소 3개, 이미지 핀 5개, 배포 9단계, OIDC 앱, 손 수집 | 추가 없음 | — |

선택지 C 는 꼬리가 더 긴 B 다. DataHub 가 OpenLineage 이벤트를 계속 받으므로 마음을 바꿔도 잃는 것이 없고,
Airflow 와 함께 걷어 낸다. 그때까지 3.8 GiB 가 든다.

## 다시 들이는 길

**어떤 사실도 카탈로그에만 있지 않는 한**, 수집을 켜는 것으로 DataHub 를 다시 들일 수 있다.
규칙:

1. **계약은 파일로 남는다.** `scripts/catalog/render-data-contracts.py` 가 생성하는 `contracts/data/*.odcs.yaml`
   이 이름, 소유자(`team`), 칸, 품질 규칙, 상태의 원천으로 남는다. 다시 들일 때: `datahub-runtime.sh ingest data-contracts`
   를 돌린다(레시피는 남겨 둔다). 그 사이 ODCS 가 v3.1.0 을 넘어가면 DataHub 의 `odcs` 원천이 그것을 읽는지
   확인한다(26번째 줄의 제약).
2. **선언된 계보는 파이프라인 그래프로 남는다.** `docs/catalog/pipeline-graph.v1.json` 과
   `orchestration/jobs.v1.json` 은 계속 `definitions/*.definition.json` 에서 생성한다.
   `infra/datahub/dataset_names.py` 는 남긴다(`orchestration/` 으로 옮기고 URN 규칙은 지킨다). 이것은 Airflow
   자산, Dagster 자산 키, DataHub URN 이 함께 쓰는 이름 규칙이다(`observe_assets.py:36-42`).
3. **스키마는 Iceberg REST 카탈로그에 남는다.** 다시 들일 때: `ingest lakehouse-iceberg`(레시피는 남겨 둔다).
4. **실행 기록은 오케스트레이터에 남는다.** Postgres 에 있는 Dagster 의 run·event-log 저장소다(Airflow 가
   사라지기 전까지는 Airflow 의 데이터베이스도). 다시 들일 때: DataHub 의 Dagster 연동
   (`acryl-datahub-dagster-plugin` sensor)이나 OpenLineage. #1432 의 `lineage.py` 송신기는 transport 를 none
   으로 둔 채 코드로 남긴다. 그러면 다시 GMS 로 향하게 하는 일이 설정 하나 바꾸기로 끝난다.
5. **품질 결과는 Dagster 자산 검사에 남는다.** 키는 계약 id 와 규칙이다. 다시 들일 때: DataHub 가 Dagster
   플러그인으로 Dagster 검사 결과를 읽거나, 품질 작업이 양쪽에 보고한다.
6. **서빙 표는 각자의 마이그레이션과 계약에 기술된 채로 남는다.** 다시 들일 때: 관리형 Postgres 에 DataHub
   `postgres` 원천을 붙인다.
7. **레시피를 낡은 채로 두지 않는다.** `infra/datahub/recipes/` 와 `compose.yml` 을 이미지 핀을 적어 둔 채
   `docs/` 나 보관 위치에 두거나, 제자리에 두되 배포가 참조하지 않게 한다. 핀은 어차피 다시 들일 때 새로
   고쳐야 한다.

카탈로그(DataHub 나 Dagster UI)에만 두어서는 **안 되는** 것: 소유자, 업무 용어 정의, 품질 규칙, 허용 값,
상태(planned / implemented), 데이터셋 설명, 인증 등급, 그리고 이것들에 대한 모든 손 편집. 지금 DataHub 는
이 목록 가운데 파일에 없는 것을 하나도 갖고 있지 않다(용어집 용어 0개, 설명은 시드에서 온다). 그래서 걷어
내기 전에 내보낼 것이 없다. 볼륨을 지우기 전에 하나는 확인한다. UI 에서 편집한 `editableDatasetProperties`
와 `ownership` aspect 를 내보낸다(비어 있을 것으로 본다; 확인한다).

## 비용

### AWS 서울(ap-northeast-2), 온디맨드, 730 h/month

가격 출처: 2026-11-19 에 읽은 AWS Price List API 벌크 파일. `AmazonECS`(2026-11-11 게시),
`AmazonES`(2026-11-16), `AmazonMSK`(2026-11-17), `AmazonRDS`(2026-11-17).

| 단가 | USD |
|---|---|
| Fargate ARM vCPU-hour | 0.03725 |
| Fargate ARM GB-hour | 0.00409 |
| OpenSearch t3.small.search / t3.medium.search / m7g.large.search, 시간당 | 0.056 / 0.112 / 0.166 |
| OpenSearch gp3 저장소, GB-month | 0.1391 |
| MSK kafka.t3.small / kafka.m7g.large broker-hour | 0.0569 / 0.2508 |
| MSK 저장소, GB-month | 0.114 |
| RDS MySQL 또는 PostgreSQL db.t4g.small / db.t4g.medium, Single-AZ, 시간당 | 0.051 / 0.102 |
| RDS gp3 저장소, GB-month | 0.131 |

선택지 A, AWS 위의 DataHub(태스크 크기는 오늘 잰 사용량에 여유분을 더했다):

| 부분 | 크기 | 월 |
|---|---|---|
| Fargate ARM 의 GMS | 1 vCPU, 3 GB | $36.15 |
| Fargate ARM 의 Frontend | 0.5 vCPU, 1 GB | $16.58 |
| Fargate ARM 의 Actions | 0.25 vCPU, 0.5 GB | $8.29 |
| **앱 태스크 소계** | | **$61.02** |
| A1, 관리형 백엔드, 가장 작은 것: OpenSearch t3.small + 20 GB; MSK 2 × t3.small + 2 × 10 GB; RDS MySQL t4g.small + 20 GB | | $43.66 + $85.35 + $39.85 = $168.86 |
| **A1 합계** | | **~$230/month** |
| A2, 가볍게: 메타데이터는 공유 관리형 Postgres 에 둔다(DataHub 는 Postgres 를 지원한다); OpenSearch(1 vCPU, 2 GB)와 Kafka(0.5 vCPU, 2 GB)는 ECS 관리 EBS 를 단 Fargate 태스크로 | | $2.62 + $33.16 + $19.57 + 작은 EBS = ~$56 |
| **A2 합계** | | **~$120/month** |
| A3, 운영 규모 관리형: OpenSearch m7g.large; MSK 2 × m7g.large; RDS t4g.medium | | $123.96 + $368.45 + $77.08 = $569.49 |
| **A3 합계** | | **~$630/month** |

넣지 않은 것: UI 용 로드 밸런서 몫, NAT/데이터 전송, CloudWatch 로그, 백업. MSK 는 브로커가 적어도 둘
필요하다(AZ 마다 하나). OpenSearch t3 인스턴스는 x86 버스트형이다. 파일에서 가장 작은 Graviton 검색
인스턴스는 m6g.large($0.156/h)다. A2 는 상태 저장소 둘을 Fargate 에서 돌린다. Fargate 에서는 ECS 관리 EBS
볼륨이 태스크 하나에 속하므로, 재시작하면 검색 색인을 데이터베이스에서 다시 짓고 Kafka 의 밀린 메시지를
잃는다. 할 수는 있지만, 그것이 우리가 떠안게 될 운영이다.

나중에 Foundation 이벤트가 어차피 Kafka 를 필요로 하면(ADR-0118 §3), Kafka 몫은 DataHub 의 것이 아니라
공유된다. 지금은 DataHub 의 브로커가 운영의 유일한 Kafka 다.

AWS 위의 선택지 B: **추가 ~$0.** Dagster 의 webserver, daemon, code server 와 그 Postgres 는 어떤 경우든
이전에 들어간다. foundation-api 는 이미 Iceberg REST 카탈로그를 읽는다. 추가되는 것은 엔지니어링 시간이다
(계획 절).

### 지금 호스트

DataHub 를 걷어 내면 지금 **상주 메모리 ~3.79 GiB** 가 풀리고, 호스트 예산
(`docs/runbooks/host-memory-budget.md`)에서 **메모리 상한 7.06 GiB** 가 빠지며, 코어 하나의 약 7% 도 풀린다.
스왑이 97% 이므로 풀린 메모리는 이전 뒤만이 아니라 지금 중요하다. MySQL, OpenSearch, Kafka 볼륨이 쓰는
디스크는 재지 않았다.

## 계기

아래 가운데 하나라도 참이 되면 DataHub(또는 다른 완전한 카탈로그)를 다시 들인다:

1. **사람.** 데이터 팀 밖의 약 5명보다 많은 사람(분석가, 영업, 협력사)이 스스로 데이터를 찾고 이해해야
   하고, 더니어의 직원 화면이 알맞은 자리가 아니다.
2. **원천.** Dagster 가 돌리지 않는 시스템(BI 도구, 두 번째 웨어하우스, SaaS 원천, AWS Glue, Gongzzang
   데이터베이스)에서 메타데이터를 합쳐야 해서, 검색이 원천을 가로질러야 한다.
3. **거버넌스.** 어떤 의무(juso 보안 각서, 개인정보, 고객 감사)가 카탈로그 안에서 감사 기록이 남는 칸 단위
   분류, 접근 요청, 승인 절차를 요구한다. ADR-0569 의 감사 이벤트는 데이터 접근이 아니라 플랫폼 변경을
   다룬다.
4. **칸 계보.** 영향 분석이나 규정 준수를 위해 누군가 "어느 서빙 필드가 어느 원천 칸에서 오는가" 에 답해야
   하고, Dagster+ 를 고르지 않았다.
5. **외부 데이터 제품.** 계약, 소유자, SLA 가 있는 둘러볼 수 있는 카탈로그가 필요한 외부 소비자에게
   데이터셋을 팔거나 공개한다.
6. **용어집.** 업무 용어가 실제로 쓰이고(ADR-0117 §5) 그것을 관리할 UI 가 필요하다.
7. **규모.** 자산 수가 파이프라인 그래프 위 필터로 잘 감당할 수 있는 수(대략 엔터티 수천 개)를 넘거나,
   순위 매긴 텍스트 검색 요청이 가끔보다 잦아진다.

각 계기는 다시 짓는 이유가 아니라 레시피를 다시 돌릴 이유다. 사실은 파일과 Dagster 에 남는다.

## 권고

**DataHub 를 Dagster + Iceberg REST + ODCS(선택지 B)로 바꾼다. 아래의 되돌릴 수 있는 단계로 하고, 운영
지도와 품질 작업이 더는 DataHub 를 읽지 않게 되면 지금 호스트에서 멈춘다.** PR #1432 는 지금 모양대로
병합하지 않는다. 그 Dagster 쪽 작업은 "운영 지도가 Dagster run 을 읽는다" 가 된다.

이유:

- 중요한 두 쓰임(실행 기록, 품질 결과)은 저장소만 있으면 되고, Dagster 가 이미 실행과 검사의 저장소다.
  DataHub 는 사실을 더하지 않으면서 경보 경로에 끼어 있다(하루 5,800 요청, p90 5.5 s 의 느린 쿼리, 40분에
  도달 불가 오류 3번).
- 사람이 보는 카탈로그 부분은 관리되지 않는다. 7주째 낡았고, 계약 143개 중 35개이며, 용어집이 없다.
  DataHub 가 규칙의 원천이라 품질 작업은 계약의 23% 만 검사한다. 파일을 읽으면 이것이 고쳐진다.
- 스왑 97% 인 호스트에서 3.8 GiB 를 풀고, 배포를 실패시킬 수 있는 배포 단계 하나, 이미지 핀 5개, OIDC 앱
  하나, 상태 저장소 셋을 없애며, AWS 에서 한 달에 $120 에서 $230+ 를 아낀다.
- 어떤 사실도 DataHub 에만 있지 않으므로 모든 것을 다시 들일 수 있다.

**가장 강한 반론.** ADR-0117 은 바로 우리가 카탈로그 화면을 직접 만들지 않으려고 DataHub 를 "사람이 보는
한 곳" 으로 골랐고, 소유자의 규칙은 "재발명 금지, OSS 를 쓴다" 다. 바꾸면 foundation-api 와 더니어가 우리가
직접 관리하는 검색, 칸, 계보 화면을 계속 가지게 되고, Dagster 의 GraphQL 은 공식적으로 불안정하다. 팀이나
보는 사람이 늘면, 작은 카탈로그에 힘을 쓰고도 결국 DataHub 가 필요해진다. 답: 그 화면들은 이미 있고 이미
고정된 질문 뒤에 DataHub 를 숨긴다(`data_catalog.rs:1-6`). 그래서 이 변경은 화면의 코드가 아니라 데이터
원천을 바꾼다. 원천은 파일과 OSS Dagster 다. 그리고 위의 계기가 작은 카탈로그로 모자라게 되는 때를 말해
준다. 소유자가 "표준 카탈로그 하나" 를 비용과 호스트 메모리보다 무겁게 본다면 선택지 C(Airflow 가 사라질
때까지 DataHub 에 OpenLineage 를 계속 먹인다)가 대비책이다. 선택지 A 는 두 수집에 일정도 걸 때만 타당한데,
7주 동안 아무도 그렇게 하지 않았다.

**가장 큰 불확실성.** Airflow 가 아직 정하는 작업 88개의 실패 실행 기록을, 운영 지도가 지금 OpenLineage 에서
얻는 것과 같은 충실도(결과 종류, 시도 상태, 걸린 시간)로 Airflow 의 REST API 에서 얻을 수 있는가, 그것도
30 s 새로 고침을 늦추지 않고. dagster-airlift 의 observe 단계는 성공한 발행을 materialization 으로 기록하지만
실패한 실행의 완전한 기록은 아니다. 그래서 작업이 `dagster` 로 옮겨 가기 전까지는 유일한 원천이 될 수 없다.

## 계획

순서대로 한다. 각 단계는 그 PR 을 되돌려서 무를 수 있고, DataHub 는 6단계까지 계속 돈다.

1. **결정 기록.** ADR-0117 §3, §4, §6(보는 곳으로서의 DataHub, 그 로그인, DataHub 의 품질 결과),
   ADR-0119 의 저장소, ADR-0165 의 실행 원천, ADR-0118 §2 와 §4(실행 기록과 품질 결과가 가는 곳)를 대체하는
   새 ADR. 무르기: 다시 대체한다.
2. **품질 작업이 규칙을 파일에서 읽고 Dagster 에 쓴다.** `contracts/data` 에서 계약 규칙마다 Dagster 자산
   검사 하나를 생성한다(`asset_checks.py` 옆에, 손으로 쓴 키 없이). `data-quality-check.py` 를 바꿔, 이미
   파싱하는 ODCS 파일에서 규칙 목록을 가져오고, 각 결과를 `snapshot_id` 와 `rules` 를 메타데이터로 붙여 내부
   webserver 에 `POST /report_asset_check/` 로 보고하게 한다. 바뀌지 않은 스냅샷 건너뛰기에 쓸 마지막 결과는
   Dagster 에서 읽는다. 2주 동안은 DataHub 와 Dagster 양쪽에 보고하고 차이를 로그에 남긴다. 무르기: Dagster
   쓰기를 뺀다. 효과: 35개 대신 계약 143개를 검사한다.
3. **품질 표가 Dagster 를 읽는다.** `operations.rs` 의 `quality_checks` 는 검사 결과를 Dagster GraphQL 에서
   읽고(고정 쿼리, 기존 "every document closes" 시험 아래에 둔다), 계약 목록은 파일에서 읽는다. 무르기: 쿼리를
   되돌린다(환경 변수 플래그는 필요 없다; 되돌린다).
4. **운영 지도와 경보가 실행 기록을 오케스트레이터에서 읽는다.** `dagster_stage` 가 `dagster` 인 작업은
   Dagster run 을 읽고, 나머지는 Airflow `dagRuns` 와 task instance 를 읽는다. `catalog_flows()` 를 바꾸기
   전에 일주일 동안 둘 다 DataHub 의 답과 나란히 돌리고 작업별로 비교한다. #1432 는 닫거나 transport 를 끈
   송신기로 줄인다. 무르기: `FLOWS_QUERY` 로 되돌린다.
5. **카탈로그 검색과 엔터티 패널이 우리 원천을 읽는다.** 검색은 메모리에서 파이프라인 그래프 노드와 계약
   이름, 설명, 칸 이름을 거른다. 엔터티 패널은 칸을 Iceberg REST `loadTable` 과 ODCS 설명에서, 이웃을
   파이프라인 그래프에서 가져온다. "DataHub 계보" 링크를 흐름 지도 자체의 노드 링크로 바꾼다. 응답 타입
   (`DataCatalogSearchPage`, `DataCatalogNeighbourhood`)은 그대로 두어 더니어는 링크 글자만 바꾼다.
   무르기: 되돌린다.
6. **DataHub 로 보내기를 멈추고, 그다음 DataHub 를 멈춘다.** Airflow 의 OpenLineage transport 를 none 으로
   둔다(`compose.orchestration.yml:37`). UI 에서 편집한 aspect 가 있으면 내보낸다(없을 것으로 본다).
   `datahub-runtime.sh stop` 을 하고 볼륨은 30일 동안 남긴다. 무르기: 다시 띄우고 두 레시피를 다시 돌린다.
7. **배포 단계와 설정을 없앤다.** `foundation-deploy.sh:429-432` 의 9단계와 `foundation-release.sh` 의
   `apply_datahub` 를 지운다. `compose.metadata-bridge.yml`, `host-layout.contract.json`,
   `runtime-secrets.contract.json`, `platform.definition.json` 의 `datahub` 항목, Zitadel 앱, 이미지 핀 5개를
   없앤다. `dataset_names.py` 를 `orchestration/` 으로 옮기고 그것을 가져다 쓰는 곳을 고친다
   (`job_specs.py:123`, `observe_assets.py:32`, `tools/ci/change-scope.json:791,812`,
   `scripts/ci/foundation_ci_scope.py:152`, `scripts/guard/declared-definitions-reproduce-artifacts-self-test.sh:17`).
   레시피와 compose 파일은 보관 폴더에 둔다. 무르기: PR 을 되돌린다.
8. **ADR-0118 §3 을 고친다.** 공유 Kafka 는 두 번째 입주자를 잃는다. Kafka 는 Foundation 이벤트가 필요로 할
   때 그 자체의 이유로 들이고, DataHub 는 더는 이유가 아니다. 이벤트 부분에 대해서는 ADR-0118 §3 의
   "보류(deferred)" 상태를 그대로 둔다.
9. **30일 뒤 DataHub 볼륨을 지운다.** 무를 수 없는 유일한 단계이고, 그때쯤에는 아무것도 그것을 읽지 않는다.

## 검토한 대안

- **DataHub 를 그대로 둔다(A).** 지금 엔지니어링으로는 가장 싸다. 하지만 경보 경로에 낡은 카탈로그를
  남기고, AWS 에서 한 달에 $120 에서 $630 이 든다. 수집에 일정도 걸 때만 타당하다.
- **Airflow 를 은퇴시킬 때까지 DataHub 를 두고, 그다음 걷어 낸다(C).** Airflow 의 OpenLineage 가 계속
  흐르므로 실행 기록 위험이 가장 낮다. 스왑 중인 호스트에서 몇 달 동안 3.8 GiB 가 들고, #1432 는 걷어 내기
  위해서만 병합하게 된다.
- **DataHub Cloud(Acryl SaaS).** 운영은 없애지만 비용은 없애지 못한다. 가격이 공개되지 않았다. 메타데이터가
  우리 계정 밖으로 나간다. 여기서는 가격을 매기지 않았다.
- **OpenMetadata.** DataHub 보다 가볍고(Kafka 없음; MySQL 또는 Postgres 와 OpenSearch), Dagster 커넥터와
  데이터 계약이 있다. 그래도 검색 클러스터를 가진 두 번째 시스템이다. 아무것도 카탈로그를 필요로 하지 않는
  동안에는 바꿀 이유가 없다.
- **Dagster+(유료).** 카탈로그 검색, 칸 계보 보기, RBAC, SSO 를 더하고, B 의 빈틈 대부분을 메운다. 직접
  호스팅·자기 클라우드라는 방향과 맞지 않는다. 계기 1 이나 4 가 일어나면 가격을 알아볼 만하다.
- **Marquez(OpenLineage 참조 서버).** Postgres 만 쓰는 작은 서비스로 OpenLineage 를 실행 저장소로 둔다.
  실행 전용의 더 작은 DataHub 다. Dagster 가 이미 같은 실행을 저장한다.

## 위험

| 위험 | 영향 | 완화 |
|---|---|---|
| 업그레이드에서 Dagster GraphQL 이 바뀐다 | Dagster 를 올린 뒤 운영 지도나 품질 표가 깨진다 | 고정 쿼리, 쿼리마다 시험 하나, Dagster 핀은 파일 하나에 둔다; UI 가 직접 쓰는 필드만 읽는다 |
| Airflow REST 실행 기록이 OpenLineage 기록과 다르다 | 이전 중 작업 건강이 틀리거나 경보를 놓친다 | 4단계가 바꾸기 전에 작업별로 일주일 동안 둘을 비교한다 |
| Dagster webserver 의 REST 와 GraphQL 에 인증이 없다 | 네트워크의 프로세스가 검사 결과를 쓸 수 있다 | webserver 는 공개하지 않고 내부 네트워크에만 둔다(지금처럼); foundation-api 는 읽고, 품질 작업만 쓴다 |
| 검색이 나빠진다 | 직원이 설명 속 낱말로 데이터셋을 찾지 못한다 | 이름, 제목, 설명, 칸을 거르는 메모리 필터가 엔터티 ~300개를 감당한다; 모자라면 계기 7 |
| "관찰된" 계보를 잃는다 | 선언되지 않은 표를 읽는 실행이 보이지 않는다 | 지금은 관찰된 것과 선언된 것이 같다(둘 다 `job_specs` 에서 온다); OpenLineage 송신기는 코드로 남는다 |
| 다시 들일 때의 어긋남 | 레시피와 핀이 썩는다 | 메모를 달아 보관한다; 다시 들일 때는 `compose.yml:3-5` 가 이미 말하듯 상류 quickstart 에서 다시 만든다 |
| 소유자의 "대기업 수준, 재발명 금지" 지시를 "카탈로그를 둔다" 로 읽는다 | 나중에 결정이 뒤집힌다 | 계기가 명시돼 있다; 사실이 파일에 남으므로 뒤집는 일은 이전이 아니라 수집이다 |
| 품질 검사 범위가 뛰면서(35 → 143) 실패가 드러난다 | 첫날부터 새 빨간 검사와 경보가 뜬다 | 2단계는 처음 2주 동안 새로 검사하는 계약에 대해 보고만 한다(WARN) |
