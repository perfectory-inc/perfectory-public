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
- `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_NEW_BYTES_BUDGET` 이 있으면 live 실행은 다운로드 전에 가지지 않은 파일의 목록
  크기(`size_kib`) 합을 그 값과 비교하고, 넘으면 아무것도 받지 않는다. evidence 상태는 `blocked_new_bytes_budget`, 그 파일들은
  `deferred_new_bytes_budget` 이다.

## 매일 훑기의 VWorld 레인 (루트 ADR-0168)

`foundation-source-sweep.service`(작업 `source_sweep`, `scripts/ops/daily-source-sweep.sh`)가 hub 레인 다음에 이 런북의 세
명령을 돈다. 대상은 엔드포인트 카탈로그(`docs/catalog/public-source-endpoint-catalog.v1.json`)에서
`daily_collection` 이 `source_sweep` 인 `vworld_dataset` 엔드포인트이고, 목록은 그 카탈로그 하나뿐이다 — 데이터셋을 더하거나
빼려면 그 필드를 고친다. 하루 예산은 같은 카탈로그의 `daily_collections.source_sweep.new_bytes_budget` 이다.

- 계획: `FOUNDATION_PLATFORM_VWORLD_DATASET_DAILY_COLLECTION=source_sweep`, 요약 CSV 없이. 예상 파일 수가 없으므로 목록 수
  어긋남 경고도 없다.
- 수집: 내용 해시 키(`content_addressed`, ADR-0152), RAON 선택 묶음(`SelectionArchive`) 제외, 강제 재수집 없음.
- 기록: `/var/lib/foundation-platform/source-sweep/journal.log` 의 한 줄에 `hub ... | vworld planned= new= skipped= failed=
  deferred= pending_bytes= budget= status=` 가 남고, 증거는 같은 디렉터리의 `vworld-evidence.json` 이다. 신규·실패는 hub 와
  같은 슬랙 메시지에 실린다.
- Silver 반영은 하지 않는다. 슬랙 알림을 받은 사람이 각 데이터셋의 적재 런북으로 반영한다(ADR-0077 §5).

### VWorld 밀린 파일 받기 (운영자)

매일 실행이 예산 초과로 멈추면(슬랙 🔴, journal `status=blocked_new_bytes_budget`) 가지지 않은 파일이 하루치보다 많다는
뜻이다 — 첫 실행, 오래 멈췄던 뒤, 제공자가 과거 판을 다시 올린 날. `vworld-evidence.json` 의 `new_bytes_budget`
(`pending_file_count`, `pending_listed_bytes`)과 `deferred_new_bytes_budget` 파일 목록을 보고, 받을 만하면 그 실행 하나만
예산을 올려 같은 스크립트를 돌린다. 환경 파일은 계약이 정한다(`config/runtime-secrets.contract.json` 의 run
`source-sweep-vworld-backlog`, 루트 ADR-0153).

```bash
sudo systemd-run --wait --collect --pipe -p User=foundation-platform \
  $(python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py properties source-sweep-vworld-backlog) \
  -E FOUNDATION_SOURCE_SWEEP_VWORLD_NEW_BYTES_BUDGET=<pending_listed_bytes 이상의 바이트 수> \
  /opt/foundation-platform/current/scripts/ops/daily-source-sweep.sh
```

journal 줄에 `budget_override=1` 이 붙는다. 받은 뒤의 매일 실행은 다시 카탈로그 예산으로 돈다. 매일 단위가 도는 동안에는
돌리지 않는다 — 시작할 때 스풀의 남은 파일을 지운다.

### 스풀 (루트 ADR-0168 §9)

내용 해시 키(`content_addressed`)는 본문을 `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR` 의 임시 파일
(`.provider-*.part`)에 받으며 해시를 재고, 그 파일에서 올린 뒤 지운다. 크기 상한은 없고, 본문을 읽기 전에 선언된 길이 +
진행 중인 파일들 + 2GiB 가 스풀 파일시스템의 여유 공간 안에 들어야 한다(아니면 그 파일만 `failed`). 매일 훑기의 스풀은
데이터 디스크의 `/data/foundation-platform/source-sweep/spool` 이 기본값이다(`FOUNDATION_SOURCE_SWEEP_SPOOL_DIR`로 바꾼다).
목록 크기가 500MB 를 넘는 파일은 제공자가 선택 묶음(RAON)으로만 내주므로 이 레인이 받지 않는다 — 그런 파일의 새 판은
evidence 에 오르지 않고, 이 런북의 수동 수집 몫이다.
