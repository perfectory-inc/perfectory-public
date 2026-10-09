---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-09
---

# 제공기관 수집 헤드리스 재생 런북

상태: 수집 계약 참고 문서. 런타임은 ADR이 고른다 — VWorld RAON 대용량 파일은 데이터 호스트의 매일 훑기가
받는다([ADR-0170](../../../../docs/adr/0170-large-vworld-files-come-through-the-raon-agent-on-the-data-host.md),
아래 "데이터 호스트의 RAON 대용량 레인").
소유자: foundation-platform

이 파일은 과거 경로 이름을 유지하지만 수집 계약은 runtime 중립이다. 브라우저를 이용한 제공기관
수집의 보안·소유권 경계를 정의하며 실행 로그가 아니다. 계정 생성이나 라이브 실행 증거도 기록하지
않는다. [ADR 0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md)에 따라
실행 ID, 제공기관 파일 식별자, object key, checksum, 바이트 수, 날짜별 결과는 private operations
evidence 시스템에 보관한다.

## 목적

일반 Rust HTTP로 제공기관 파일을 수집할 수 없어 V-World RAON/KUpload 같은 제공기관 관리 브라우저
페이지를 거쳐야 하는 경우를 다룬다. 선택한 adapter와 runtime도 같은 Foundation Platform commit
경계를 보존해야 한다.

## 명령 흐름

```text
browser acquisition adapter
  -> provider-controlled download page
  -> provider-approved raw file acquisition
  -> private task-local artifact or replay request
  -> foundation-outbox-publisher import-provider-acquisition-landing
  -> Rust local staging
  -> Rust validation
  -> BronzeCommitter commit
  -> R2 Bronze CreateOnly + Postgres bronze_object
```

Python/browser 코드는 수집 adapter일 뿐이다. 검증·checksum·저장·계보·최종 commit은 Rust가 소유한다.
진단용 R2 landing은 제한된 조사에 필요할 때만 선택적으로 사용한다. 기본 운영 모드는
`FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE=1`로 같은 대용량 파일을 두 번 쓰지 않는다.

## 필수 런타임 능력

선택한 런타임은 다음을 제공해야 한다.

- 브라우저 adapter를 쓰면 Python 3.11+와 provider-acquisition worker 의존성
- 선택한 adapter가 요구하는 browser·provider-agent 의존성
- Bronze commit용 Rust `foundation-outbox-publisher` binary
- private replay 파일과 staged 응답 바이트를 위한 쓰기 가능한 임시 저장소
- runtime 전용 secret 주입
- 제공기관과 R2 endpoint로의 outbound network

운영 수집 경로가 Windows desktop agent에 의존하면 안 된다. 정확한 bulk 파일에 필요한 provider agent는
먼저 저장소에 고정한 Linux container 계약으로 증명해야 한다. provider package binary와 credential은
build 또는 task runtime에 주입하며 저장소에 커밋하지 않는다. VWorld는 Windows RAON 에이전트만 게시한다.
Linux 패키지는 RAON K 제조사 주소에서 받고, 그 주소와 바이트의 sha256 은
`config/provider-agent-packages.contract.json` 한 곳에만 적는다(ADR-0170).

## 필수 환경

Provider acquisition:

| variable | purpose |
|---|---|
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_STAGING_DIR` | task-local staging directory for private bytes |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_IMPORT_OUTPUT_PATH` | redacted import report path |
| provider login/cookie variables | authenticated browser session, only when required |

Bronze commit:

| variable | purpose |
|---|---|
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_COMMIT_BRONZE` | set to `1` to commit staged bytes to Bronze |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE` | set to `1` for the default no-landing path |
| `DATABASE_URL` | Bronze catalog database |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_SOURCE_SLUG` | canonical source slug |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_ID` | provider identity used by the Bronze compiler |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_NAME` | provider file label |

R2 Bronze write:

| variable | purpose |
|---|---|
| `FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER` | set to `r2` |
| `FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET` | environment-specific Bronze bucket binding |
| `FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID` | Cloudflare account ID |
| `FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT` | R2 S3 endpoint; optional when derived from the account ID |
| `FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID` | runtime access key |
| `FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY` | runtime secret key |

live-write 사전 점검이 실패하면 제공기관 다운로드 전에 task를 실패시킨다. 활성 account·bucket·secret
binding은 공개 런북 사실이 아니라 private 배포 상태다.

## 1단계 - 제한된 재생 요청 캡처

```bash
python -m foundation_platform_provider_acquisition.raon \
  --download-ds-id "$DOWNLOAD_DS_ID" \
  --file-no "$FILE_NO" \
  --output "$PUBLIC_PROOF_PATH" \
  --prove-raon-replay \
  --private-replay-request-output "$PRIVATE_REPLAY_REQUEST_PATH" \
  --landing-object-key "$LANDING_OBJECT_KEY"
```

`DOWNLOAD_DS_ID`와 `FILE_NO`는 승인된 private 선택에서 설정한다. 공개 증거에는 provider secret을
남기지 않는다. private replay request는 runtime 동안만 존재한다.

## 2단계 - Rust로 검증·커밋

```bash
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_REPLAY_REQUEST_PATH="$PRIVATE_REPLAY_REQUEST_PATH" \
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_IMPORT_OUTPUT_PATH="$IMPORT_REPORT_PATH" \
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_COMMIT_BRONZE=1 \
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE=1 \
foundation-outbox-publisher import-provider-acquisition-landing
```

Bronze commit이 켜져 있을 때 `DATABASE_URL` 또는 필요한 Bronze identity가 없으면 importer는 provider
request를 replay하기 전에 실패해야 한다.

## 일괄 모드

차단된 파일을 반복 수집할 때는 커밋된 batch runner를 사용한다.

```bash
python -m foundation_platform_provider_acquisition.raon_batch \
  --selection "$PROVIDER_ACQUISITION_SELECTION_JSON" \
  --batch-id "$BATCH_ID" \
  --output-root "$PROVIDER_ACQUISITION_OUTPUT_ROOT" \
  --source-slug "$SOURCE_SLUG" \
  --rust-binary foundation-outbox-publisher
```

batch runner는 orchestration만 담당한다.

- 운영자가 승인한 provider 선택을 읽는다.
- browser adapter에 private replay request를 요청한다.
- `foundation-outbox-publisher import-provider-acquisition-landing`을 호출한다.
- 기본으로 direct-to-Bronze 경로를 켠다.
- 각 job 뒤 private replay·staging 파일을 삭제한다.
- 상세 증거는 private에 두고 파일별 결과만 redacted 형태로 낸다.

병렬 실행은 `--shard-index`·`--shard-count` 또는 명시적 `--provider-file-id` filter로 나눈다. 모든
shard는 같은 Rust importer와 `BronzeCommitter`를 사용해야 한다.
브라우저 코드는 독립적인 저장소 writer가 되어서는 안 된다.

선택값을 환경변수로 전달하는 runtime은 `PROVIDER_ACQUISITION_SELECTION_JSON_BASE64`로
base64 인코딩한 UTF-8 JSON을 제공한다. `PROVIDER_ACQUISITION_SELECTION_JSON`으로 mount한
`PROVIDER_ACQUISITION_SELECTION_JSON` is also supported.
`PROVIDER_ACQUISITION_SELECTION_JSON_INLINE` is limited to controlled local debugging where the
caller owns shell quoting. The entrypoint materializes the selection only in task-local ephemeral
storage.

runtime은 batch 전에 필요한 Linux provider agent와 browser 지원을 시작해야 한다. 저장소에
checked-in container definition은 서로 다른 책임을 가진다.

- `services/foundation-provider-acquisition-worker/Dockerfile.raon-agent-proof`는 제한된 replay
  request 하나만 기록하고 Rust importer와 R2/Postgres write path는 의도적으로 제외한다.
- `services/foundation-provider-acquisition-worker/Dockerfile.raon-batch`는 Linux agent와
  provider-acquisition worker를 포함한다. Rust는 빌드하지 않는다 — 실행이 승인된 릴리스의
  `foundation-outbox-publisher`를 `/usr/local/bin/foundation-outbox-publisher`에 읽기 전용으로 넣는다.
  secret은 task runtime에 이름으로만 주입하고 image에 복사하지 않는다.

어느 image든 `RAON_DEB_URL`·`RAON_DEB_SHA256` build arg(기본값 없음)로 build하고, 받은 바이트를 설치 전에
`sha256sum -c`로 확인한다. 값을 손으로 옮기지 않는다: `scripts/ops/raon-large-files.sh build`가
`config/provider-agent-packages.contract.json`에서 읽어 넘기고, 작업자 소스와 그 고정값의 해시를 이름에 단
image(`foundation-platform/raon-batch-<내용 해시>:local`)를 없을 때만 build한다.

## 3단계 - 정리

Remove private runtime files before the task exits:

```bash
rm -f "$PRIVATE_REPLAY_REQUEST_PATH"
rm -f "$PROVIDER_BROWSER_LOG_PATH"
```

정리가 실패하면 경고를 내고 작업이 끝날 때까지 작업 저장소를 민감한 데이터로 취급한다.

## 재생 식별자 형태

```text
landing/provider=<provider>/acquisition=<adapter>/job_id=<job-id>/download_ds_id=<dataset-id>/file_no=<file-number>/download.zip
```

adapter는 호환성과 추적을 위해 private replay request에 이 landing 형태 identity를 담을 수 있다.
Direct-to-Bronze 모드에서는 이를 R2에 쓰지 않는다. browser 코드는 절대 `bronze/`에 쓰지 않는다.

## 안전 게이트

adapter 또는 실행 runtime을 바꿀 때마다 다음 gate를 순서대로 적용한다.

1. 운영자가 승인한 파일 하나를 private replay request로 캡처한다.
2. Rust importer로 응답 본문 전체를 검증한다. prefix만 보는 검사는 부족하다.
3. 진단 landing이 필요하면 제한된 객체 하나만 쓰고 retry 전에 reconcile한다.
4. provider identity를 Foundation 정본 source metadata에 묶는다.
5. 제한된 파일 하나를 `BronzeCommitter`로 commit한다.
6. 승인된 소규모 batch를 실행하고 catalog·object store 결과를 reconcile한다.
7. 명시적 owner 승인과 private 검토 가능 실행 증거가 있을 때만 범위를 넓힌다.

다음 불변식은 항상 적용한다.

- 공개 증거에는 cookie, agent token, signed URL, request body가 없어야 한다.
- Rust 검증은 빈 본문·HTML 본문·잘못된 archive 바이트·provider HTML/error page가 들어간 archive를 거부한다.
- R2 쓰기는 CreateOnly여야 한다.
- Bronze commit은 `BronzeCommitter`를 거쳐야 한다.
- commit 증거에는 private evidence 시스템에 object identity·크기·checksum을 남기되 이 공개 런북에 복사하지 않는다.

## 실패 처리

| failure | handling |
|---|---|
| browser adapter cannot start | mark job blocked; no R2 write |
| provider page yields no replay request | mark job blocked; keep only a redacted public proof |
| replay proof shows only an archive prefix | continue only to Rust validation |
| replay request returns non-2xx | fail job; retry only when provider policy allows |
| replay body is HTML, empty, or an invalid archive | reject; no Bronze write |
| archive contains provider HTML | reject; no Bronze write |
| replay identity already exists in diagnostic landing mode | reconcile before retry |
| Bronze database is unavailable | fail before provider replay when commit is enabled |
| staging disk is full | fail task; reduce batch size or increase ephemeral storage |
| private cleanup fails | warn; task storage remains sensitive until task exits |

## 런타임 선택

runtime 선택은 이 런북이 아니라 ADR이 한다. 선택한 runtime은 위 소유권 chain을 그대로 보존해야 한다.
VWorld RAON 대용량 파일은 ADR-0170이 데이터 호스트를 골랐다: 매일 훑기가 그 호스트에서 돌고, 원장이 그
곁에 있으며, 큰 본문을 받을 데이터 디스크가 있다.
Fargate는 관리형 후보로 깔끔하지만 이 런북에서 선택하지 않는다. 데이터 모델은 runtime 선택에 종속되지 않으므로
다른 실행 plane으로 바꿔도 커밋된 R2 Bronze 객체와 Postgres catalog row를 버릴 필요가 없다.

## 데이터 호스트의 RAON 대용량 레인 (루트 ADR-0170)

제공자가 RAON 에이전트로만 주는 파일(`SelectionArchive`, 목록 크기 약 500MB 초과)은 매일 훑기의 VWorld 레인이
받지 않는다. 그 레인의 증거(`/var/lib/foundation-platform/source-sweep/vworld-evidence.json`)는 원장이 갖지 않은
선택 묶음을 `selection_archives`에 `deferred_selection_archive`로 적는다. 그런 파일이 있을 때 매일 훑기는 한 실행
상한(아래 1)을 본다.

- 상한 0 = 레인 꺼짐: `raon-large-files.sh`를 부르지 않는다. journal에
  `| raon deferred=N listed_bytes=… budget=0 status=lane-off files=…`(기다리는 파일 id)를 적고, 슬랙에 안내 한 줄
  (ℹ️, 🔴 아님)을 보낸다. 이 레인 때문에 매일 훑기가 실패하지 않는다.
- 상한 양수: `scripts/ops/raon-large-files.sh run <증거>`를 부른다. 상한 초과·전제 부족·실패는 그 레인의 실패다.
- 상한이 정수 바이트가 아니면 레인의 실패다(`status=invalid-budget`).

1. 계획: `plan-provider-acquisition-jobs`가 증거와 증거가 읽은 목록(`file_inventory_path`)으로 작업을 만든다. 한
   실행의 상한은 엔드포인트 카탈로그의 `daily_collections.source_sweep.selection_archive_new_bytes_budget`이고, 작업
   목록 크기 합이 넘으면 하나도 받지 않고 실패한다(슬랙 🔴, journal `| raon ... status=blocked_new_bytes_budget`).
2. image: 위 "일괄 모드"의 내용 해시 image. 없을 때만 build한다.
3. 수집: 그 image가 브라우저로 RAON 페이지를 열어 replay request를 잡고, 이 릴리스의 publisher가 내용 주소
   키(`FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_BRONZE_KEY=content_addressed`)로 Bronze에 바로 쓴다. 본문은
   `/data/foundation-platform/source-sweep/raon/runs/<run>/batch/` 아래에 잠시 머물고 파일마다 지워진다.
4. 기록: 실행마다 `/data/foundation-platform/source-sweep/raon/runs/<run>/`(plan, run log, batch 요약)와
   `/data/foundation-platform/source-sweep/raon/journal.log` 한 줄. 매일 훑기는 그 줄을 자기 journal에 `| raon ...`
   으로 싣는다.

전제(VWorld 로그인, Bronze 쓰기 설정, 패키지 고정값, 도커와 그 데몬, 증거)가 하나라도 없으면 스크립트는
무엇도 쓰기 전에 78로 끝나고, 매일 훑기의 raon 레인은 실패한다. 조용히 건너뛰지 않는다. image build가 실패하면
(주소가 답하지 않거나 바이트가 고정값과 다르면) 아무것도 받기 전에 레인이 실패한다.

### 첫 감독 실행 (운영자)

카탈로그의 상한은 0(레인 꺼짐)에서 시작한다: 매일 실행은 원장이 갖지 않은 대용량 파일을 찾으면 받지 않고 몇 개·
얼마·어느 파일인지 journal에 적고 슬랙에 안내 한 줄로 알린다(실패 아님). 운영자가 아래 순서로 한 번 지켜본 뒤 측정한
양수 상한을 PR로 카탈로그에 적어야 매일 실행이 받는다.
환경 파일은 계약이 정한다(`config/runtime-secrets.contract.json`의 run `raon-large-files`, 루트 ADR-0153).
매일 단위(`foundation-source-sweep.service`)가 도는 동안에는 돌리지 않는다.

```bash
raon() {
  sudo systemd-run --wait --collect --pipe -p User=foundation-platform \
    $(python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py properties raon-large-files) \
    "$@"
}
evidence=/var/lib/foundation-platform/source-sweep/vworld-evidence.json

# 1. image: 고정 주소에서 패키지를 받아 sha256을 확인하고 내용 해시 image를 만든다(있으면 그대로 쓴다).
raon /opt/foundation-platform/current/scripts/ops/raon-large-files.sh build

# 2. 계획만: 아무것도 받지 않는다. 상한이 0이면 계획은 거부되지만 plan.json에 받을 파일과 크기가 남는다.
raon /opt/foundation-platform/current/scripts/ops/raon-large-files.sh plan "$evidence"
ls -t /data/foundation-platform/source-sweep/raon/runs/ | head -1   # 그 실행의 plan.json 을 읽는다

# 3. 한 파일만, 상한을 그 파일 크기 이상으로 올려 지켜본다.
raon -E FOUNDATION_RAON_LARGE_FILES_MAX_FILES=1 \
  -E FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET=<plan.json 첫 작업의 listed_bytes 이상> \
  /opt/foundation-platform/current/scripts/ops/raon-large-files.sh run "$evidence"
```

3의 실행 디렉터리에서 `batch/<batch>/summary.json`이 `committed`이고, 원장(`catalog.bronze_object`)에 그 파일의
내용 주소 키(`...--sha256-<checksum>.zip`) 행이 생겼는지 확인한다. 그 다음 밀린 것을 같은 명령으로
`FOUNDATION_RAON_LARGE_FILES_MAX_FILES` 없이 받고(상한은 plan의 `listed_bytes_total` 이상), 측정한 하루 상한을
카탈로그에 적는 PR을 연다. journal 줄에는 `budget_override=1`이 붙는다. 실행 ID·파일 식별자·크기·checksum은 이
문서가 아니라 ADR 0007의 private operations evidence에 남긴다.

## Linux RAON 에이전트 컨테이너 증명

정확한 provider bulk 파일에 cloud runtime을 선택하기 전에 proof image를 사용한다. Xvfb 아래에서
Linux provider agent를 시작하고 browser adapter로 provider 페이지를 연 뒤 redacted 공개 증거와
private task-local replay request만 쓴다. R2에 쓰거나 `DATABASE_URL`에 접속하거나
`foundation-outbox-publisher`를 호출하면 안 된다.

패키지 주소와 sha256은 `config/provider-agent-packages.contract.json`의 값을 그대로 준다(ADR-0170). 패키지는
커밋하지 않는다.

```bash
docker build \
  -f services/foundation-provider-acquisition-worker/Dockerfile.raon-agent-proof \
  --build-arg RAON_DEB_URL="<config/provider-agent-packages.contract.json 의 url>" \
  --build-arg RAON_DEB_SHA256="<config/provider-agent-packages.contract.json 의 sha256>" \
  -t foundation-platform/raon-agent-proof:local \
  .
```

승인된 파일 하나를 identity가 command history에 남지 않게 실행한다.

```bash
docker run --rm \
  --env-file "$PROVIDER_ACQUISITION_RUNTIME_ENV" \
  -v "$PWD/target/provider-acquisition-proof:/work/staging" \
  foundation-platform/raon-agent-proof:local
```

통과 기준:

- 공개 출력이 존재하고 redacted protocol 증거만 포함한다.
- private replay request가 task-local storage에만 존재한다.
- 공개 출력에 provider token·cookie·signed URL·replay body가 없다.
- setup executable과 HTML-wrapper archive를 provider 데이터로 분류하지 않는다.
- Bronze 쓰기 전에 private replay 응답이 Rust 검증을 통과한다.

결과 execution ID, 선택한 provider identity, object key, 크기, checksum, reconcile 결과는 ADR 0007의
private operations evidence 시스템에 저장한다. 과거 결과는 새 provider 파일·image·runtime·account
binding의 유효성을 증명하지 않는다.
