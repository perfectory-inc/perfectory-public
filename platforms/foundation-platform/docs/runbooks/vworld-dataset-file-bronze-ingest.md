---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-08-26
---

# VWorld 데이터 파일 Bronze 수집 런북

## 목적

VWorld 제공기관 데이터 파일은 변경 불가 Bronze 객체로 수집한다. 데이터셋에 공식 파일 다운로드
경로가 있으면 같은 국가 원자료 snapshot을 위해 WFS/OpenAPI 수집으로 대체하지 않는다.

## 증거 경계

수집 계획·제공기관 inventory·객체 수·바이트 합계·checksum·실제 쓰기 결과는 `target/audit/` 아래에
생성하고 비공개 운영 증거 저장소에 보관한다. 공개 저장소에 커밋하지 않는다. 현재 상태나 완료를
주장하기 전에 대상 환경에서 아래 명령을 다시 실행한다.

## 명령

데이터셋 수집 계획 생성:

```bash
cargo run -p foundation-outbox-publisher -- plan-vworld-dataset-collection
```

제공기관 파일 inventory 생성:

```bash
cargo run -p foundation-outbox-publisher -- inventory-vworld-dataset-files
```

자동 로그인으로 파일 하나 dry-run smoke:

```bash
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH="target/audit/vworld-dataset-file-ingest-auto-login-dry-run-evidence.json"
unset FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE
cargo run -p foundation-outbox-publisher -- ingest-vworld-dataset-files
```

파일 하나 R2/DB 실제 쓰기 smoke:

```bash
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE="1"
cargo run -p foundation-outbox-publisher -- ingest-vworld-dataset-files
```

smoke 증거가 준비된 뒤에만 전국 파일 수집 실행:

```bash
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE="1"
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES
cargo run -p foundation-outbox-publisher -- ingest-vworld-dataset-files
```

현재 자동화할 수 있는 모든 제공기관 파일을 실행하되 RAON/KUpload 선택 archive는 보류:

```bash
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_EXCLUDE_SELECTION_ARCHIVES="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_DEFER_PROVIDER_ACQUISITION_BLOCKED="1"
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES
cargo run -p foundation-outbox-publisher -- ingest-vworld-dataset-files
```

`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_EXCLUDE_SELECTION_ARCHIVES=1`은 선택 다운로드 집합에서
`SelectionArchive` inventory 항목을 제외한다. 이 파일은 provider acquisition plane(RAON/KUpload
agent 또는 공식 대안)이 필요하므로 일반 dataset-file lane의 성공 수집으로 세면 안 된다. 남은 대상
파일에는 full-download 확인 gate가 계속 적용된다.

`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_DEFER_PROVIDER_ACQUISITION_BLOCKED=1`은 선택된 provider file이
RAON/KUpload 수집을 요구해도 일반 lane을 성공 상태로 유지한다. 해당 파일은 evidence에
`status=provider_acquisition_blocked`로 기록하고 실행 상태는 `ready_with_provider_acquisition_deferred`가
된다. 실제 파일 실패는 여전히 실행을 막는다.

## 병렬 실행

`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS`와 `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES`가
선택 항목 수를 정한다. `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_IN_FLIGHT`는 동시에 다운로드할
선택 파일 수를 정한다.

| 변수 | 기본값 | 의미 |
|---|---:|---|
| `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_IN_FLIGHT` | `4` | 동시 선택 파일 다운로드 수. `0`은 거부 |

evidence JSON에는 `max_in_flight`를 기록한다. 파일 report는 완료 순서가 아니라 inventory 순서로
다시 써서 다운로드 완료 순서가 달라도 audit diff가 안정적이다.

## 필수 환경

실제 쓰기:

| 변수 | 목적 |
|---|---|
| `DATABASE_URL` | Bronze metadata database |
| `FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER` | `r2` (developer/staging/production); `local` is bounded-test-only for local/CI |
| `FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET`, `FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT`, `FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION`, `FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID`, `FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY` | R2 object storage |

VWorld 파일 다운로드:

| Variable | Purpose |
|---|---|
| `FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER` | Optional pre-authenticated provider Cookie header |
| `FOUNDATION_PLATFORM_VWORLD_USERNAME` | Cookie header가 없을 때 쓰는 공급자 로그인 사용자명 |
| `FOUNDATION_PLATFORM_VWORLD_PASSWORD` | Cookie header가 없을 때 쓰는 공급자 로그인 비밀번호 |

호환 기간에는 `VWORLD_API_KEY`, `VWORLD_DOMAIN`, `VWORLD_USERNAME`, `VWORLD_PASSWORD`와
기존 dataset 전용 사용자명·비밀번호 이름도 읽지만, 그 이름이 실제 값을 공급하면 이름만 포함한
폐기 예정 경고를 남긴다. 운영자는 `.env.local`에서 값을 출력하지 말고 왼쪽 이름만 위 canonical
이름으로 옮긴다. canonical 이름과 구 이름이 함께 있으면 canonical 값이 우선한다.

Cookie header가 없으면 ingestor는 실행마다 한 번 로그인하고 반환된 session Cookie를 선택 파일마다
재사용한다. credential은 log·evidence·shell 출력에 남기면 안 된다.

## 안전 게이트

- Full national download is blocked unless
  `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD=1`.
- Live writes are disabled unless `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1`.
- provider file inventory는 `status=ready`여야 하며 파일 수가 collection plan과 일치해야
  한다.
- 비어 있거나 HTML인 다운로드 응답은 거부하며 Bronze에 저장하지 않는다.
