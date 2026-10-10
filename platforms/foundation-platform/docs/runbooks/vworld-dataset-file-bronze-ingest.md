---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-09
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
- 이미 받은 파일 건너뛰기는 파일 번호(`provider_file_id`)와 제공자 갱신일이 모두 같을 때만이다. VWorld 는 판마다 같은
  파일 번호를 다시 쓴다(연속지적도 30563, 루트 ADR-0148). 목록에 갱신일이 없으면 파일 번호가 정한다.
  연속지적도 판의 매일 확인·수집은 [VWorld 연속지적도 판 런북](./vworld-parcel-editions.md)이다.
- 건너뛰기는 키 모양을 보지 않는다(루트 ADR-0168): 같은 파일 번호·같은 제공자 갱신일·원장에 체크섬이 있으면 번호 키
  (`<ds>-<no>.zip`)로 받은 것도 가진 것이다. 키 모양이 필요한 소비자(30527 넘김)는 키를 스스로 검사하고
  `FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1` 로 받는다.
- 바이트 예산은 없다(루트 ADR-0172). 가지지 않은 파일은 전부 받고, 파일마다 따로 커밋한다. 멈춘 실행(시간 초과·OOM·
  배포)은 다음 실행이 이어 받는다 — 이미 커밋된 파일은 `skipped_existing` 이다.

## 매일 훑기의 VWorld 레인 (루트 ADR-0168, ADR-0172)

`foundation-source-sweep.service`(작업 `source_sweep`, `scripts/ops/daily-source-sweep.sh`)가 hub 레인 다음에 이 런북의 세
명령을 돈다. 대상은 엔드포인트 카탈로그(`docs/catalog/public-source-endpoint-catalog.v1.json`)에서
`daily_collection` 이 `source_sweep` 인 `vworld_dataset` 엔드포인트이고, 목록은 그 카탈로그 하나뿐이다 — 데이터셋을 더하거나
빼려면 그 필드를 고친다. 받는 양에는 상한이 없다. 같은 카탈로그의 `daily_collections.source_sweep.landed_bytes_notice`
는 안내 기준일 뿐이다: 한 실행이 그보다 많이 받으면 슬랙에 ℹ️ 한 줄이 가고, 실행은 막히지도 실패하지도 않는다.

- 계획: `FOUNDATION_PLATFORM_VWORLD_DATASET_DAILY_COLLECTION=source_sweep`, 요약 CSV 없이. 예상 파일 수가 없으므로 목록 수
  어긋남 경고도 없다.
- 수집: 내용 해시 키(`content_addressed`, ADR-0152), RAON 선택 묶음(`SelectionArchive`) 제외, 강제 재수집 없음.
- 기록: `/var/lib/foundation-platform/source-sweep/journal.log` 의 한 줄에 `hub ... | vworld planned= new= skipped= failed=
  pending= pending_bytes= landed_bytes= status=` 가 남고(`pending` 은 이번 실행이 받아야 했던 파일 — 가지지 않았던 것 — 과 그
  목록 크기, `landed_bytes` 는 실제로 커밋한 바이트), 증거는 같은 디렉터리의 `vworld-evidence.json` 이다(같은 줄과 실패 이유가 `journalctl -u foundation-source-sweep.service` 에도 간다, 루트 ADR-0174). 신규·실패는 hub 와
  같은 슬랙 메시지에 실린다.
- Silver 반영은 하지 않는다. 슬랙 알림을 받은 사람이 각 데이터셋의 적재 런북으로 반영한다(ADR-0077 §5).
- RAON 선택 묶음(루트 ADR-0170): 수집은 그 파일들을 받지 않되, 원장이 가진 것인지는 같은 확인으로 묻는다. 가진 것은
  `skipped_existing`, 갖지 않은 것은 `deferred_selection_archive` 로 증거의 `selection_archives` 에 따로 적고(`files`
  에는 들어가지 않는다) 그 수를 `deferred_selection_archive_file_count` 로 센다. 0 이 아니면 매일 훑기가 raon
  레인(`scripts/ops/raon-large-files.sh`)을 부른다 — [제공기관 수집 런북](./provider-acquisition-fargate.md)의
  "데이터 호스트의 RAON 대용량 레인". journal 줄의 `| raon ...` 다음에 `| members ...` 가 온다.
- ZIP 안 이름(루트 ADR-0169 §1): 레인들 뒤에 매일 훑기가 `scripts/ops/bronze-object-members.sh` 를 인자 없이 불러
  아직 잰 적 없는 Bronze ZIP 만 범위 읽기로 잰다(읽기 전용 키, 단위가 `lakehouse-reader.env` 를 싣는다). 한 실행
  2000 개까지다. journal 줄 끝은 `| members measured= failed= selected= members=` 이고, 읽지 못한 객체가 있거나
  명령이 요약 없이 끝나면 레인 실패처럼 🔴 이며 결과 줄을 내지 않는다(받은 파일은 같은 줄의 `new=` 에 남는다). 못 잰
  객체는 다음 실행이 다시 잰다. 자세한 것은 [Silver 갱신 런북](./silver-refresh.md) 5 절.
- 실패 파일: 실패한 레인이 증거에 실패 파일을 적었으면 `journalctl -u foundation-source-sweep.service` 에 레인마다
  20 줄까지 `failed <원천>:<파일> <이유>` 가 나온다(이유는 가린 뒤 200 자, 루트 ADR-0174 개정 기록). 증거 파일을
  `sudo` 로 열 필요가 없다.

### 밀린 파일과 실행 시간 (루트 ADR-0172)

밀린 파일을 받는 별도 운영자 절차는 없다. 매일 실행이 가지지 않은 것을 전부 받는다. 단위의 상한
(`TimeoutStartSec=8h`, Airflow `timeout_minutes` 490)에 걸려 멈춘 실행은 실패로 알려지지만, 그때까지 커밋한 파일은 남고
다음 실행이 나머지를 받는다. 바로 이어 받으려면 `airflow-runtime.sh trigger source_sweep`. 매일 단위와 겹쳐 손으로 같은
스크립트를 돌리지 않는다 — 시작할 때 스풀의 남은 파일을 지운다.

### 스풀 (루트 ADR-0168 §9)

내용 해시 키(`content_addressed`)는 본문을 `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR` 의 임시 파일
(`.provider-*.part`)에 받으며 해시를 재고, 그 파일에서 올린 뒤 지운다. 크기 상한은 없고, 본문을 읽기 전에 선언된 길이 +
진행 중인 파일들 + 2GiB 가 스풀 파일시스템의 여유 공간 안에 들어야 한다(아니면 그 파일만 `failed`). 매일 훑기의 스풀은
데이터 디스크의 `/data/foundation-platform/source-sweep/spool` 이 기본값이다(`FOUNDATION_SOURCE_SWEEP_SPOOL_DIR`로 바꾼다).
목록 크기가 500MB 를 넘는 파일은 제공자가 선택 묶음(RAON)으로만 내주므로 이 레인이 받지 않는다 — 그런 파일의 새 판은
evidence 에 오르지 않고, 이 런북의 수동 수집 몫이다.
