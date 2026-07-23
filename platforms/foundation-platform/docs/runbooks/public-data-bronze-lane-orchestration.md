# Public Data Bronze Lane Orchestration Runbook

## 목적

`foundation-platform`의 공공 데이터 Bronze 수집을 provider/source 성격별 lane으로 나누어 병렬 실행한다.
endpoint catalog는 "무엇을 수집할 수 있는지"의 SSOT이고, lane registry는 "어떤 실행 lane으로 수집할지"의 SSOT다.

## 핵심 원칙

- 진입점은 `foundation-outbox-publisher` Rust subcommand다(별도 PowerShell wrapper는 없다).
- 실행 정책, registry 검증, 병렬 실행, evidence 작성, gate 판단은 Rust command가 소유한다.
- lane registry의 `command_args`는 `cargo run ...` 형식이 아니라 outbox-publisher subcommand 인자만 가진다.
- runner는 기본적으로 dry-run만 수행하며 provider 호출을 시작하지 않는다.
- 실제 실행은 `FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTE=1`과 `FOUNDATION_PLATFORM_CONFIRM_PUBLIC_DATA_BRONZE_LANE_EXECUTION=1`이 함께 있어야 시작된다.
- `non_executable_source_acquisition_lanes`에 등록된 방식은 실행 lane으로 등록할 수 없다.
  예를 들어 `disabled_api_duplicate`인 공공데이터포털 건축물대장 API는 endpoint catalog에는
  원천 목록/진단 기록으로 남지만, 전국 Bronze 수집 lane에는 들어갈 수 없다.

허용 예:

```json
{
  "command": "ingest-vworld-dataset-files",
  "command_args": ["ingest-vworld-dataset-files"]
}
```

금지 예:

```json
{
  "command_args": ["run", "--quiet", "-p", "foundation-outbox-publisher", "--", "ingest-vworld-dataset-files"]
}
```

## Lane 구조

| lane | provider | 수집 방식 | 내부 병렬 설정 |
|---|---|---|---|
| `building-hub-bulk` | `hub.go.kr` | bulk file | `FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_MAX_IN_FLIGHT` |
| `vworld-dataset-file` | `VWorld` | provider dataset file | `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_IN_FLIGHT` |
| `data-go-kr-api` | `data.go.kr` | API parity/fallback only | `FOUNDATION_PLATFORM_NATIONAL_ASYNC_MAX_IN_FLIGHT` |
| `vworld-ned-open-api` | `VWorld` | Open API | `FOUNDATION_PLATFORM_VWORLD_NED_ATTRIBUTE_MAX_IN_FLIGHT` |

현재 기본 실행 대상은 `status=enabled`이고 `include_by_default=true`인 lane이다.
`planned` lane은 endpoint coverage를 설명하기 위한 계획 상태이며 기본 실행되지 않는다.
건축물대장 계열의 전국 Bronze 원천은 `building-hub-bulk`이며,
`building_register_open_api`는 `disabled_api_duplicate`로 고정되어 API 폴백 수집 대상이 아니다.

## Dry Run

기본 실행은 provider 호출을 시작하지 않고 계획 evidence만 쓴다.

```bash
cargo run -p foundation-outbox-publisher -- run-public-data-bronze-collection-lanes
```

결과:

- `target/audit/public-data-bronze-lane-orchestration-evidence.json`
- `status=planned`
- `executed=false`
- `completion_claim_allowed=false`
- `national_rollout_allowed=false`

## 실제 실행

실제 lane 실행은 명시 승인 없이 시작되지 않는다.

```bash
FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTE=1 \
FOUNDATION_PLATFORM_CONFIRM_PUBLIC_DATA_BRONZE_LANE_EXECUTION=1 \
FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_MAX_CONCURRENT_LANES=0 \
cargo run -p foundation-outbox-publisher -- run-public-data-bronze-collection-lanes
```

`FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_MAX_CONCURRENT_LANES=0`은 선택된 lane 수만큼 자동 병렬 실행한다.
명시 값을 주면 그 값을 상한으로 사용한다.

예를 들어 선택된 lane이 3개이고 `FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_MAX_CONCURRENT_LANES=2`이면 한 번에 최대 2개 lane만 실행된다.
각 lane 내부의 요청 병렬성은 해당 Rust 수집기의 환경 변수로 별도 제어한다.

## Lane Executor

테스트나 운영 배포에서 lane 실행 파일을 명시하려면 `FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTOR_EXE`를 사용한다.

```bash
FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTOR_EXE=target/debug/foundation-outbox-publisher \
cargo run -p foundation-outbox-publisher -- run-public-data-bronze-collection-lanes
```

명시하지 않으면 Rust orchestrator는 다음 순서로 실행 파일을 선택한다.

1. `FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTOR_EXE`
2. `FOUNDATION_PLATFORM_OUTBOX_PUBLISHER_EXE`
3. 현재 실행 중인 outbox-publisher binary

## 안전 Gate

- endpoint catalog / lane registry 검증은 `run-public-data-bronze-collection-lanes` 실행 시
  orchestrator가 registry 로드 단계에서 직접 수행한다. 위반이 있으면 provider 호출 없이
  `status=blocked` evidence를 남기고 실행을 거부한다. (별도 gate 커맨드였던
  `check-public-source-endpoint-catalog`·`check-public-data-bronze-lane-registry`는
  2026-06-22 self-verifying evidence-gate ceremony 정리에서 삭제되었다.)
- `FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTE`가 없으면 provider 호출은 시작되지 않는다.
- execute flag가 있어도 `FOUNDATION_PLATFORM_CONFIRM_PUBLIC_DATA_BRONZE_LANE_EXECUTION`이 없으면 차단된다.
- bulk 전체 다운로드는 lane별 `*_CONFIRM_FULL_DOWNLOAD=1`이 필요하다.
- R2 live write는 lane별 `*_LIVE_WRITE=1`이 필요하다.
- orchestration evidence의 `completion_claim_allowed`와 `national_rollout_allowed`는 항상 `false`다.
- async lane(`national_data_collection_async`)은 page-window **shard fragment**만 수집한다. pagination guard는 이 경로에서 전국 coverage를 단언하지 않으며(`ASYNC_SHARD_WINDOW_DEFERS_TO_COVERAGE_MANIFEST`), **"전국 누락 없음" 완전성은 오직 national coverage manifest(`check-national-bronze-object-manifest`)만 단언한다.** 개별 async run의 evidence를 완전성 주장으로 읽어선 안 된다.

## 선택 실행

특정 lane만 포함하거나 제외할 수 있다.

```bash
FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_INCLUDE_LANE_IDS=building-hub-bulk \
FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTE=1 \
FOUNDATION_PLATFORM_CONFIRM_PUBLIC_DATA_BRONZE_LANE_EXECUTION=1 \
cargo run -p foundation-outbox-publisher -- run-public-data-bronze-collection-lanes
```

```bash
FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_SKIP_LANE_IDS=vworld-dataset-file \
FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTE=1 \
FOUNDATION_PLATFORM_CONFIRM_PUBLIC_DATA_BRONZE_LANE_EXECUTION=1 \
cargo run -p foundation-outbox-publisher -- run-public-data-bronze-collection-lanes
```

## 실패 처리

하나의 lane이라도 실패하면 전체 orchestration evidence는 `status=blocked`가 된다.
runner는 lane id, command, exit code, 시작/종료 시각, duration, output을 evidence에 남긴다.
병렬 실행 중이어도 evidence는 registry sequence 순서로 정렬된다.
## Bulk 파일 스트리밍 저장 경계

HUB(`hub.go.kr`) bulk 파일과 VWorld provider dataset 파일의 live write 경계는
end-to-end streaming 경로를 사용한다.

- provider client는 `open_file_stream()`으로 HTTP response body를 byte stream으로 연다.
- live write는 단일 Bronze committer(`BronzeCommitter::commit_streaming_bulk`, ADR 0016)를 통해
  `BronzeStreamingObjectStorageWriter`로 같은 chunk를 R2/local object storage로 보내면서 SHA-256과
  byte count를 동시에 계산한다. write는 write-once(`CreateOnly` / `If-None-Match: *`)이고, 412
  충돌은 streaming recovery(행이 있으면 idempotent skip, 없으면 GET-rehash 후 복구)로 self-heal 한다.
- Bronze object metadata는 streaming write가 성공한 뒤 committer가 in-flight checksum + size로 기록한다.
- bulk ingest 파일에서 `fetch_file()`, `plan_public_data_bulk_file()`,
  `PublicDataBulkFilePlanInput`, `plan.raw_payload.clone()`, 일반
  `put_object(PutObjectRequest)`를 재도입하면 CI 의 bulk streaming storage boundary 검사가 차단한다.
- `*_MAX_IN_FLIGHT`는 동시에 처리하는 provider 파일 수다. 메모리 envelope는
  `provider chunk size * concurrent files + runtime overhead`에 가깝고, provider 파일
  전체 크기에 비례하면 안 된다.
- 현재 single-pass streaming upload는 provider가 `Content-Length`를 제공해야 한다.
  `Content-Length`가 없는 대형 파일까지 처리하려면 multipart/resumable upload 경로를
  별도 hardening 단계로 추가한다.
- 작은 API page Bronze 수집은 기존 `PutObjectRequest` 경로를 계속 사용할 수 있다.
