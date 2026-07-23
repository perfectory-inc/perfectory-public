# hub.go.kr 벌크 Bronze 수집 실행 Runbook

## 목적

`hub.go.kr`에서 공식 배포하는 건축물 계열 벌크 파일을 API 페이지 반복 호출 대신 원본 파일 그대로 Bronze에 저장한다.

이 경로는 API fallback이 아니다. 파일 벌크가 공식 수집 경로로 확정된 데이터는 이 명령만 사용한다.

## 실행 명령

### 전체 목록 수집 계획 생성

```bash
cargo run -p foundation-outbox-publisher -- plan-building-hub-bulk-collection
```

이 명령은 `hub.go.kr` 공개 벌크 목록 전체 페이지를 읽고
`target/audit/building-hub-bulk-collection-plan.json`에 다운로드 job을 만든다.
파일 다운로드나 R2 저장은 하지 않는다.

계획 파일의 job은 두 종류다.

| 값 | 의미 |
|---|---|
| `cataloged_endpoint` | `docs/catalog/public-source-endpoint-catalog.v1.json`에 의미론적 endpoint로 등록된 파일이다. |
| `provider_inventory_only` | 공식 Hub 목록에는 있으나 아직 Silver 의미 계약은 없는 파일이다. Bronze raw mirror 대상으로 보존한다. |

Provider inventory의 현재 크기와 분류별 실행 건수는 계획을 생성할 때마다 달라집니다. 공개
runbook은 그 측정값을 고정하지 않으며, 생성된 plan과 실행 evidence가 해당 run의 SSOT입니다.
Planner는 승인된 inventory item마다 정확히 하나의 job을 만들고 위 두 분류 중 하나를 부여해야
합니다.

### 단일 파일 Bronze 저장

```bash
cargo run -p foundation-outbox-publisher -- ingest-building-hub-bulk-file
```

### 계획 기반 batch Bronze 수집

```bash
cargo run -p foundation-outbox-publisher -- ingest-building-hub-bulk-collection
```

이 명령은 `plan-building-hub-bulk-collection`이 만든 plan JSON을 읽고 각 job의 파일을 받는다.
기본값은 명시적으로 승인하지 않은 전체 inventory 다운로드를 거부한다.

| 환경 변수 | 의미 |
|---|---|
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_PLAN_PATH` | 입력 plan. 기본값은 `target/audit/building-hub-bulk-collection-plan.json` |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_EVIDENCE_PATH` | 실행 증거 JSON. 기본값은 `target/audit/building-hub-bulk-collection-ingest-evidence.json` |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_MAX_JOBS` | 앞에서부터 일부 job만 실행한다. 파일 다운로드 smoke에 사용한다. |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_MAX_IN_FLIGHT` | 동시에 처리할 벌크 파일 job 수. 기본값은 `4`이고 `0`은 거부한다. |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_CONFIRM_FULL_DOWNLOAD` | 전체 plan 다운로드는 정확히 `1`일 때만 허용한다. |

실제 R2/DB 저장은 단일 파일 명령과 동일하게
`FOUNDATION_PLATFORM_BUILDING_HUB_BULK_LIVE_WRITE=1`일 때만 수행한다.
`LIVE_WRITE`가 없으면 파일은 받아서 hash/key 계획까지 만들지만 Bronze 저장은 하지 않는다.

## 필수 환경 변수

| 이름 | 의미 |
|---|---|
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_SOURCE_SLUG` | Source catalog slug |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_DATASET_NAME` | Source catalog dataset name |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_OPERATION` | 내부 operation 이름 |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_PROVIDER_FILE_PERIOD` | 공급자 제공월, 예: `2099-12` |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_PROVIDER_FILE_ID` | `hub.go.kr`의 `OPN...` 파일 id |

Live write에는 기존 Bronze 저장 설정도 필요하다.

| 이름 | 의미 |
|---|---|
| `DATABASE_URL` | Bronze metadata DB |
| `FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER` | `r2` 또는 `local` |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_LIVE_WRITE` | 실제 저장은 정확히 `1`일 때만 수행 |

## 선택 환경 변수

| 이름 | 기본값 |
|---|---|
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_SOURCE_NAME` | `hub.go.kr bulk file` |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_PROVIDER` | `hub.go.kr` |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_BASE_URI` | `https://www.hub.go.kr` |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_TERMS_URL` | 공개 벌크 목록 페이지 |
| `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_USER_AGENT` | `foundation-platform-building-hub-bulk-ingestor/1.0` |

## 동작 방식

### 전체 계획 생성

1. 공개 목록 페이지 `pageIndex=1`을 읽는다.
2. pagination에서 최대 pageIndex를 찾는다.
3. 2페이지부터 마지막 페이지까지 순회한다.
4. 각 목록 항목의 `fnDownloadPop(task_group_code, task_code, provider_file_id)`를 공식 파일 identity로 사용한다.
5. endpoint catalog의 `provider_inventory_selector`와 매칭되는 파일은 `cataloged_endpoint` job이 된다.
6. catalog에 아직 없는 파일도 누락하지 않고 `provider_inventory_only` raw mirror job으로 남긴다.
7. 생성된 계획은 completion/cutover/rollout 완료를 주장하지 않는다.

### 단일 파일 저장

1. `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_PROVIDER_FILE_ID`를 `srvrFileNm` form field로 POST한다.
2. 응답이 HTML이면 로그인/권한 페이지로 판단하고 Bronze 파일로 저장하지 않는다.
3. `Content-Disposition`의 파일명을 Bronze object 확장자와 provider metadata에 사용한다.
4. 원본 bytes는 수정하지 않고 그대로 object storage에 저장한다.
5. DB에는 ingestion run, Bronze object metadata, SHA-256, provider file identity를 기록한다.
6. 벌크 zip 내부 레코드는 아직 해석하지 않으므로 `logical_record_count`는 `None`으로 둔다.

### 계획 기반 batch 저장

1. plan JSON의 job을 읽는다.
2. `MAX_JOBS`가 있으면 앞에서부터 지정 개수만 선택한다.
3. 전체 plan을 실행하려면 `CONFIRM_FULL_DOWNLOAD=1`을 요구한다.
4. 각 job을 단일 파일 수집 config로 변환한다.
5. 선택된 job은 `MAX_IN_FLIGHT` 개수만큼 동시에 실행한다.
6. 파일 다운로드, Bronze key/checksum 계획, 선택적 live write를 수행한다.
7. evidence JSON에는 `max_in_flight`와 각 job의 성공/실패를 남긴다.
8. evidence의 job 순서는 병렬 완료 순서가 아니라 plan 순서로 고정한다.
9. 실패가 하나라도 있으면 전체 evidence 상태는 `blocked`다.
