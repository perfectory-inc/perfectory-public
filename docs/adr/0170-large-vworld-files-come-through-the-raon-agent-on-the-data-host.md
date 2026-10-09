# ADR 0170: VWorld 대용량 파일은 데이터 호스트의 RAON 에이전트로 받는다

- Status: Accepted
- Date: 2026-10-09
- Amends: [ADR-0168](./0168-the-daily-sweep-collects-the-vworld-land-datasets-within-a-byte-budget.md) §6(매일 훑기의
  레인)과 Consequences("선택 묶음은 이 레인에서 보류되고 수동 수집 몫")
- Related: [ADR-0152](./0152-bronze-keys-of-reused-provider-file-numbers-name-their-bytes.md)(내용 주소 키),
  [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(환경 파일 계약),
  [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)(승인된 릴리스의 실행 파일),
  [ADR-0137](./0137-the-release-build-is-sized-from-a-measurement-and-runs-alone.md)(릴리스 빌드 크기),
  [ADR-0007](./0007-public-code-private-operations-boundary.md)(공개 코드·비공개 운영 증거)

## Context

VWorld 는 목록 크기가 약 500MB 를 넘는 파일(`LARGE_FILE_THRESHOLD_KIB`, `collection-infrastructure`
`vworld_dataset_file.rs`)을 일반 HTTP 로 주지 않는다. 그 파일은 `SelectionArchive` 이고 RAON K 에이전트로만
내려받힌다. ADR-0168 의 매일 훑기는 그 파일을 빼고 받고 수만 센다(`selection_archives_not_swept`). 운영 원장
(`catalog.bronze_object`, 읽기 전용 실측)에는 훑는 토지 데이터셋의 그런 파일이 116개 있다 — 토지이용계획 96개(최대
1,524MiB), 개별공시지가 16개(최대 1,369MiB), 토지이동이력 4개. 2026년 7월 데이터 호스트에서 RAON Linux 에이전트로
받은 것이다. 제공자가 이 데이터셋들의 새 판을 올리면 대용량 시도 파일은 아무도 모르게 빠진다 — ADR-0077 이 막으려던
침묵이다.

저장소에는 경로가 이미 있었지만 이어지지 않았다. 코드에서 다섯 가지를 확인했다.

1. **넘겨줄 증거가 없다.** `plan-provider-acquisition-jobs` 는 수집 증거의 `files` 중
   `provider_acquisition_blocked` 행만 읽는다. 매일 훑기는 선택 묶음을 선택 단계에서 빼므로 그 증거에 선택 묶음은
   한 행도 없다. 원장이 이미 가진 파일인지도 아무도 묻지 않는다.
2. **계획이 일괄 실행의 입력이 아니다.** `raon_batch.load_selection` 은 작업마다 `operation`·`dataset_name` 을
   요구하는데 계획은 그것을 적지 않았다. 제공자 기준월·갱신일도 없어 수입기가 원장에 판을 적지 못한다.
3. **수입기의 키는 파일 번호다.** `import-provider-acquisition-landing` 은 `<ds>-<no>.zip` 에 CreateOnly 로 쓴다.
   VWorld 는 판마다 같은 파일 번호를 다시 쓰므로(ADR-0148) 다음 판은 그 키에 들어가지 못한다. ADR-0168 의 레인은
   내용 주소 키를 쓴다(ADR-0152).
4. **패키지 주소가 고정되지 않았다.** `Dockerfile.raon-batch` 는 `RAON_DEB_URL`·`RAON_DEB_SHA256` 을 build 때 손으로
   받는다. VWorld 는 Linux 패키지를 게시하지 않는다(2026-10-09 추적): 대용량 내려받기 페이지
   (`dtmk/downloadDtnaResourceFile.do`)는 `raonkupload.js` 를 `InitXml=raonkupload.config.txt` 로 띄우고, 그 설정은
   암호화 핸들러(`dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp`)가 `k00` 요청으로 준다. 그 응답을 RAON
   자체의 `makeDecryptReponseMessage` 로 풀면 설치 주소(`agent_install_file_url`)도 허용 OS(`apply_agent_os`)도 없어
   `applyAgentOs` 는 JS 기본값 `,windows,` 로 남는다 — Linux 브라우저에는 설치 안내가 뜨지 않는다. VWorld 의 기본
   폴더 경로 `raonkupload/agent/raonk-2018_amd64.deb` 는 HTML 페이지(soft 404)를 주고, 게시된 것은 Windows
   `raonkSetup.exe` 뿐이다. 같은 이름의 패키지는 RAON K 제조사 사이트(raonk.com)에 있다: `raonk-2018` 2018.2.8 amd64,
   그 안의 파일 전부가 7월에 데이터 호스트에 설치된 에이전트와 바이트까지 같다(확인함).
5. **이미지가 Rust 를 빌드한다.** 이미지 안에서 `cargo build --release -p foundation-outbox-publisher` 를 한다.
   그 빌드는 호스트에서 22g 를 쓰고 40분쯤 걸리며 다른 작업과 함께 돌지 않는다(ADR-0137). 매일 훑기가 부를 수 없다.

## Decision

1. **넘겨주는 것은 매일 훑기의 증거다.** `ingest-vworld-dataset-files` 가 선택 묶음을 뺄 때
   (`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_EXCLUDE_SELECTION_ARCHIVES=1`) 뺀 파일마다 원장이 가졌는지 내려받기
   경로와 같은 확인(`existing_file_report`: 같은 파일 번호, 목록의 갱신일, 아는 체크섬)으로 묻는다. 가진 것은
   `skipped_existing`(그 키와 함께), 갖지 않은 것은 `deferred_selection_archive`(목록 크기와 함께)로 증거의
   `selection_archives` 에 따로 적고 수를 `deferred_selection_archive_file_count` 로 센다. 이 행들은 `files`,
   `selected_file_count`, 하루 예산에 들어가지 않는다 — 그 레인의 결과는 예전과 같다. 이 확인은 원장을 늘
   읽는다(`FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH` 는 그 레인의 내려받기에 대한 선택이지 대용량 파일 전부를 다시
   받으라는 뜻이 아니다).
2. **계획은 일괄 실행의 입력 그대로다.** `plan-provider-acquisition-jobs`(스키마 v2)는 `files` 의
   `provider_acquisition_blocked` 와 `selection_archives` 의 `deferred_selection_archive` 를 받아, 증거가 읽은
   목록(`file_inventory_path`)에서 각 파일의 작업(`operation`, `source_name`, `dataset_name`)과 기준월·갱신일·목록 크기를
   찾아 적는다. 목록에 없는 행은 계획을 실패시킨다. `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_MAX_FILES` 는 앞에서 N개만,
   `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_NEW_BYTES_BUDGET` 은 그 작업들의 목록 크기 합의 상한이다. 넘으면 계획은
   `blocked_new_bytes_budget` 으로 쓰이고 0 이 아닌 값으로 끝난다 — ADR-0168 §4 와 같은 규칙(일부만 받지 않는다).
   두 쪽 시험이 같은 파일(`foundation-provider-acquisition-worker/tests/fixtures/provider-acquisition-plan.v2.json`)을
   본다: Rust 는 자기 출력이 그 파일과 같은지, Python 은 그 파일을 `load_selection` 으로 읽어 수입기 환경을 만드는지.
3. **수입기는 내용 주소 키로 쓴다.** `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_BRONZE_KEY=content_addressed` 이면 키는
   파일 번호 키에 준비된 본문의 SHA-256 을 붙인 것이다(`build_bronze_content_object_key`, ADR-0152 와 같은 모양,
   같은 `source_partition_key`). 같은 키에 같은 바이트가 있으면 쓰지 않고 원장만 맞춘다. 다른 바이트면 거부한다.
   `raon_batch` 는 늘 이 값을 준다. 기본값(설정 없음)은 예전의 파일 번호 키다.
4. **패키지는 제조사 주소와 sha256 으로 한 곳에 고정한다.** VWorld 가 Linux 패키지를 주지 않으므로(Context 4)
   제조사 주소가 정본이다. 주소·크기·sha256 은 `platforms/foundation-platform/config/provider-agent-packages.contract.json`
   한 곳에만 적는다. 루트 `tools/technology-versions.contract.json` 이 아닌 까닭: 릴리스는
   `platforms/foundation-platform` 하위 트리만 나르고 스크립트는 고정값을 실행 시점에 읽는다. 두 Dockerfile 의
   `RAON_DEB_URL`·`RAON_DEB_SHA256` 은 기본값이 없는 build arg 이고, 받은 바이트를 설치 전에 `sha256sum -c` 로
   확인한다. 시험이 그 주소와 체크섬을 적은 파일이 계약 하나뿐인지 본다. 호스트에서 다시 묶은 패키지는 쓰지 않는다.
5. **이미지는 내용으로 이름 붙고 Rust 를 담지 않는다.** `scripts/ops/raon-large-files.sh build` 가 작업자 소스·
   Dockerfile·이름 계약으로 build context 를 만들고, 그 내용과 패키지 고정값의 해시 16자를 단
   `foundation-platform/raon-batch-<해시>:local` 이 없을 때만 build 한다(컨테이너 정책 검사가 요구하는 대로 맥락은
   글자 그대로 `.` 하나). 수입기는 이 릴리스의 publisher 를 `/usr/local/bin/foundation-outbox-publisher` 에 읽기
   전용으로 넣어 쓴다 — 릴리스 빌드도 bookworm 이고, 승인된 실행 파일만 쓴다는 ADR-0134 의 묶음이 이미지에도
   이어진다. 넣지 않으면 진입점이 브라우저를 띄우기 전에 끝난다.
6. **실행.** `docker run --network host`(DB 는 호스트 루프백), `--memory 4g`, `--shm-size=1g`, 부른 사용자의 uid.
   설정은 이름만 적은 env 파일로 넘긴다(값은 어디에도 쓰지 않는다): `DATABASE_URL`, 모든 `FOUNDATION_PLATFORM_*`
   (매일 훑기가 publisher 를 직접 돌릴 때와 같은 설정), 이름 계약이 적은 VWorld 로그인 이름들. 본문은
   `/data/foundation-platform/source-sweep/raon/runs/<run>/` 아래에서 파일마다 지워지고(단위의 `ReadWritePaths`
   안), 실행 시작에 죽은 실행이 남긴 본문을 지운다.
7. **매일 훑기의 raon 레인.** vworld 레인 다음, 증거의 `deferred_selection_archive_file_count` 가 0 보다 클 때만
   `raon-large-files.sh run <증거>` 를 부른다(0 이면 부르지 않고 journal 에 `raon deferred=0`). vworld 레인이 예산으로
   거부돼도 증거는 선택 묶음을 적으므로 레인은 서로 독립이다. journal 줄은 `| raon ... status=` 로 끝나고, 실패
   (전제 부족의 78 포함)는 그 레인의 실패로 슬랙 🔴 에 실린다. 실행 시작에 어제의 요약을 지운다.
8. **부작용 전에 확인한다.** 스크립트는 환경(DB 비밀번호, Bronze 쓰기 설정), VWorld 로그인(이름 계약으로 —
   `scripts/ops/vworld-login.sh` 를 매일 훑기와 함께 쓴다), 패키지 고정값(https 주소, 소문자 64자 sha256), 도커와 데몬
   응답, 증거 파일을 확인하고 하나라도 없으면 상태 디렉터리도 만들기 전에 78 로 끝난다. 이미지 build 가 실패하면
   (주소가 답하지 않거나 바이트가 고정값과 다르면) 아무것도 받기 전에 레인이 실패한다. 런타임 비밀 계약에 run
   `raon-large-files` 를 더한다. 매일 훑기는 이 스크립트를 실행할 뿐 source 하지 않으므로 계약 검사가 따라가지
   않는다 — 시험이 이 스크립트의 요구가 매일 훑기 단위가 받는 것의 부분집합인지 본다.
9. **상한은 0 에서 시작한다.** 엔드포인트 카탈로그의 `daily_collections.source_sweep.selection_archive_new_bytes_budget`
   이 한 실행의 상한이다. 첫 값은 0 이다: 매일 실행은 원장이 갖지 않은 대용량 파일을 찾으면 하나도 받지 않고 몇 개·
   얼마인지 알린다(그 수치가 첫 측정이다). 7월에 받은 행이 목록의 갱신일과 체크섬을 갖고 있으면 가진 것으로 읽혀
   레인은 조용하다. 운영자가 아래 첫 감독 실행을 한 뒤, 측정한 하루 상한을 적는 PR 이 이 레인을 켠다. 운영자는 그
   실행 하나의 값을 `FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET`, `FOUNDATION_RAON_LARGE_FILES_MAX_FILES` 로 바꾼다
   (journal 에 `budget_override=1`).

### 첫 감독 실행 (운영자, 배포 뒤)

환경 파일은 계약이 정한다(run `raon-large-files`). 매일 단위가 도는 동안에는 돌리지 않는다. 증거는 배포 뒤 첫 매일
실행이 쓴 것이다(그 전의 증거에는 `selection_archives` 가 없다).

```bash
raon() {
  sudo systemd-run --wait --collect --pipe -p User=foundation-platform \
    $(python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py properties raon-large-files) "$@"
}
evidence=/var/lib/foundation-platform/source-sweep/vworld-evidence.json
raon /opt/foundation-platform/current/scripts/ops/raon-large-files.sh build            # 1. 이미지
raon /opt/foundation-platform/current/scripts/ops/raon-large-files.sh plan "$evidence" # 2. 계획만(받지 않는다)
raon -E FOUNDATION_RAON_LARGE_FILES_MAX_FILES=1 \
  -E FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET=<plan.json 첫 작업의 listed_bytes 이상> \
  /opt/foundation-platform/current/scripts/ops/raon-large-files.sh run "$evidence"   # 3. 한 파일
```

2 의 `plan.json`(`/data/foundation-platform/source-sweep/raon/runs/<run>/`)에서 받을 파일과 합계를 보고, 3 뒤에
`batch/<batch>/summary.json` 이 `committed` 이고 원장에 내용 주소 키 행이 생겼는지 확인한다. 그 다음 밀린 것을
`MAX_FILES` 없이 받고, 측정한 하루 상한을 카탈로그에 적는 PR 을 연다. 실행 ID·파일 식별자·크기·checksum 은 ADR-0007 의
비공개 운영 증거에 남긴다. 절차의 정본은
[제공기관 수집 런북](../../platforms/foundation-platform/docs/runbooks/provider-acquisition-fargate.md)의 "첫 감독
실행"이다.

## Consequences

- 받는 것: 목록에 선택 묶음으로 오르고, 같은 파일 번호의 최신 원장 행이 목록의 갱신일과 체크섬을 갖지 않은 파일.
  7월의 RAON 수입이 갱신일을 적지 않았거나 다른 `operation`·출처로 적었다면 116개 전부가 여기에 들고, 상한 0 의
  매일 실행이 그 수와 크기를 알린다. 저장소에서는 어느 쪽인지 알 수 없다(운영 원장을 조회하지 않았다).
- 같은 판을 다시 받아도 내용 주소 키가 같아 쓰지 않는다. 판이 바뀌면 새 키로 곁에 쌓인다(덧붙이기만).
- 이미지 build 는 제조사 주소에 의존한다. 그 주소가 사라지거나 바이트가 바뀌면 build 가 실패해 레인이 빨갛다 —
  조용히 다른 바이트를 설치하지 않는다. 새 판을 쓰려면 계약의 주소·sha256 을 PR 로 바꾼다(이미지 이름도 바뀐다).
- 이미지는 호스트 루트 디스크의 도커 저장소에 남는다(파이썬·브라우저·에이전트, Rust 없음). 옛 이미지 정리는 이
  결정의 범위가 아니다.
- 위험: 진입점은 지금까지 이미지의 `app` 사용자로만 증명됐다 — 부른 사용자의 uid 로 RAON 에이전트가 뜨는지는 첫 감독
  실행이 처음 본다. 브라우저 자동화는 제공자 페이지가 바뀌면 깨진다(그 파일은 실패로 보고되고 레인은 빨갛다).
  매일 단위의 시간 상한(4시간) 안에 세 레인이 끝나야 한다 — 상한을 정할 때 한 파일의 소요 시간을 함께 잰다.
- 시험(Rust): 뺀 선택 묶음만 따로 보고되고 원장이 가진 것은 키와 함께 건너뜀, 증거의 수·직렬화; 계획이 증거와 목록을
  잇고 목록에 없는 행을 거부함, 앞 N개·상한 경계·거부된 계획도 작업을 적음, 계획 출력이 넘겨주는 파일과 같음; 수입기의
  키 모양 해석·내용 주소 키·같은 바이트 재실행·다른 바이트 거부. (Python) 작업자: 넘겨주는 파일이 그대로 일괄 입력이
  되고 내용 주소 키를 요청함, 두 Dockerfile 이 기본값 없는 인자로 받은 바이트를 확인 뒤 설치하고 주소·Rust 를 적지
  않음, 진입점이 publisher 가 없으면 브라우저 전에 끝남. 스크립트(`orchestration/tests/test_raon_large_files_command.py`):
  전제 하나씩 빼기(78, 아무것도 안 함), 고정값 누락·변형, 데몬 무응답, 이미지 한 번만 빌드·소스나 고정값이 바뀌면 새
  이름, 고정값이 build 인자로 감, publisher 읽기 전용, 이름만 넘김, 계획만, 상한 초과, 운영자 덮어쓰기, 실패 파일,
  죽은 실행의 본문 정리, 주소·체크섬이 저장소에 한 번만 있음. 매일 훑기(`test_daily_source_sweep_command.py`): 넘길
  것이 없으면 부르지 않음, 오늘 증거로 부름, 78·상한 초과·실패가 빨강, vworld 레인 예산 초과와 독립, 어제 요약을 읽지
  않음, 요구가 단위의 부분집합.
