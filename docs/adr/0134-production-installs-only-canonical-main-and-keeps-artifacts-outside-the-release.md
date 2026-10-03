---
status: accepted
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-10-02
---

# ADR-0134 — 운영은 GitHub main 커밋만 설치하고, 산출물·설정은 불변 릴리스 밖에 둔다

이 결정은 ADR-0007(운영 쓰기 경계), ADR-0128(`floor-config` 설치 위치), ADR-0122(작업 진입점 검사)를 보완한다.

## 원인과 불변식

기존 배포기는 호출자가 준 SHA 모양과 압축파일 해시를 기록했다. 두 값이 서로 같아도 그 파일이
정본 `main`에 병합된 코드라는 증거는 아니다. 설치된 트리는 `root:root 0755`로 풀려 그 뒤에도
바뀔 수 있었고, `publisher`·`floor-config` 명령은 실행 파일(`bin/foundation-outbox-publisher`)과
설정(`.foundation-floor.env`)을 **릴리스 안에** 써 넣었다. 따라서 "릴리스 = 병합된 소스"라는
등식이 설치 직후부터 성립하지 않았다. 등록 작업 다섯 개 중 넷은 어느 배포도 갱신하지 않는
`/var/lib/foundation-platform/bin/foundation-outbox-publisher`를 실행했다.

sudo 허용 규칙 `/opt/foundation-platform/*/scripts/deploy/foundation-release.sh`는
`/opt/foundation-platform` 아래 모든 디렉터리의 스크립트에 맞는다. 인증되지 않은 트리의
배포기도 같은 권한으로 실행된다.

Trino의 catalog mount는 짧은 문법(`./infra/lakehouse/trino/catalog:/etc/trino/catalog:ro`)이었다.
host 경로가 없으면 Docker가 빈 디렉터리를 만들고 Trino는 `r2` catalog 없이 정상 기동한다
(2026-10-01 실제로 일어났다).

불변식:

1. 운영에 설치되는 소스는 독립적으로 가져온 정본 `main`의 조상 커밋이고, 설치된 바이트는 그
   커밋의 `platforms/foundation-platform` 트리와 정확히 같다. 파일 하나 더 있어도 거부한다.
2. 실행 파일·JAR은 같은 커밋에서 관리자 빌드가 만든 산출물이며 sha256이 봉인 기록과 같다.
3. 운영자가 고르는 값(설정)은 릴리스 밖에 있고, 릴리스 id로 찾는다.
4. 등록 작업은 위 셋을 실행 직전에 다시 확인하고, 환경변수로 다른 소스·바이너리를 고를 수 없다.

## 결정

### 1. 독립 제어 코드가 소스를 인증한다

관리자가 따로 설치한 `/opt/perfectory-control/current`는 root 소유의 전체 모노레포 제어
체크아웃이다. 운영자·에이전트·작업 계정은 이 경로와 부모를 변경할 수 없다. 후보 압축파일
안의 검증기를 관리자 권한으로 실행하지 않는다.

`scripts/deploy/foundation-release-admission.py`는 기존 `tools/github/repository-identity.json`,
`show-public-repository-identity.sh`, `safe-git-transport.sh`를 그대로 사용한다. 공개 정본의
live identity를 대조하고 literal HTTPS URL의 `refs/heads/main`을 root 소유
`/var/lib/perfectory/foundation-release-source.git`에 독립 fetch한다. 호출자의 `origin/main`,
Git 설정, 객체 치환, Python 검색 경로를 신뢰하지 않는다.

`git merge-base --is-ancestor`로 요청한 SHA가 정본 main에 포함되는지 검사하고, 그 Git 객체의
`git archive <sha>:platforms/foundation-platform`과 후보 tar의 파일 집합·바이트·실행 비트를
비교한다. 설치할 바이트는 후보 tar에서 풀지 않고 독립 fetch한 Git에서 쓴다. squash 전 feature
SHA는 비슷한 변경이 나중에 main에 들어가도 병합 SHA가 아니므로 거부한다.

제어 체크아웃은 자기가 풀린 정본 커밋을 `.perfectory-control-commit`에 기록한다(설치·갱신 명령은
런북 "릴리스 인증 전환" 2단계가 정본이다). `prepare`는 이 커밋이 설치할 릴리스의 조상이거나 같은 커밋이
아니면 거부한다. 그래서 다른 역사에서 온 제어 코드나, 설치할 릴리스보다 새로운 제어 코드(그 릴리스가 모르는
규칙으로 판정하는 경우)는 설치 전에 드러난다.

### 2. 설치 후에도 실제 바이트를 검사하고, sudo는 제어 경로 하나만 허용한다

`foundation-release.sh`의 `prepare`·`install`·`activate`·`rollback`·`migrate`·`verify`·`timers`가
고정 경로의 검증기를 호출한다. 검증기 위치를 환경변수로 바꿀 수 없다. 설치 파일은
`0444`/`0555`, 디렉터리는 `0555`이며 다른 주체가 부모를 바꿀 수 없어야 한다. 활성화·롤백·실행
전에는 root 소유 Git cache로 main 조상 여부와 설치 바이트를 다시 비교한다. GitHub 일시 장애에도
이미 인증한 릴리스의 실행·롤백은 동작한다. 기존 두 marker는 이름이며 인증 근거가 아니다.

`foundation-release.sh deployer-access <account>`는 `/etc/sudoers.d/foundation-release`에
`<account> ALL=(root) NOPASSWD: /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh`
한 줄만 설치하고 `visudo`로 검증한다. 순서는 **새 규칙을 먼저, 옛 줄 삭제는 마지막**이다.
`deployer-access <account>`는 새 규칙을 설치하고, `/etc/sudoers`·`/etc/sudoers.d`에 이 스크립트를 다른
경로로 허용하는 줄이 남아 있으면 그 줄을 보여 주며 `deployer-access-installed`로 끝난다(전환이 끝날
때까지 기존 경로가 남아 있어야 한다). 전환 마지막에 운영자가 옛 줄을 `visudo`로 지우고
`deployer-access <account> --exclusive`를 실행한다. 이 단계는 다른 허용 줄이 하나라도 남아 있으면
실패한다. 옛 줄은 명령이 지우지 않는다(남이 소유한 파일이다).

### 3. 산출물과 설정은 릴리스 밖에 두고, 작업은 같은 릴리스 id로만 찾는다

```text
/opt/foundation-platform/
├── releases/<sha>/                 정본 소스, 읽기 전용 (admission이 파일 집합까지 대조)
├── artifacts/<sha>/                관리자 빌드 산출물, 읽기 전용
│   ├── foundation-outbox-publisher
│   ├── jars/*.jar
│   └── build.json                  source·publisher image ID·태그·파일별 sha256
├── config/<sha>/building-register-floor.env   FLOOR 비밀 없는 설정, root 0644
├── current -> releases/<sha>
└── previous -> releases/<sha>
```

- `install`·`prepare`는 소스를 쓴 뒤 같은 제어 코드의 `build`를 호출한다. 기존
  `Dockerfile.lakehouse-control`과 Cargo lockfile로 publisher를 빌드하고 image ID로 고른 컨테이너에서
  바이너리를 꺼낸다. Spark 이미지 digest는 `compose.lakehouse.yml`, JAR 좌표는
  `lakehouse_engine.iceberg_packages()`가 정본이다. 자격증명 없는 일회용 Spark 컨테이너가 빈 Ivy
  디렉터리에 받은 JAR 전체를 동결한다. Buildx `docker-container` driver(이미지 digest 고정,
  2 CPU·`4g`, swap 추가 없음)만 쓰며, 없으면 제한 없는 `docker build`로 우회하지 않고 거부한다.
  `ldd`로 host loader 호환을 확인한다. compose 설정은 `--profile lakehouse-batch`로 읽는다(Spark는 그
  profile에만 있어 profile 없이 읽으면 서비스 목록이 비고, 읽지 못하면 거부한다).
- publisher image는 `foundation-outbox-publisher:<sha>`로 태그해 `build.json`의 `publisher_tag`에
  기록한다. 태그 없는 `--load` 결과는 `docker image prune`이 지운다. `verify`·`verify-current`는 그 태그가
  `publisher_image`와 같은 image ID로 남아 있는지 확인하고, 없으면 거부한다.
- 호출자의 바이너리를 받는 명령은 없다. `publisher <sha> <binary> <sha256>` 명령은 삭제했다.
  sha256을 같이 받아도 그 값은 호출자가 고른 것이라 인증이 아니다.
- `floor-config <sha> <file>`은 `config/<sha>/building-register-floor.env`에 create-only로 쓴다.
  키 목록·경로 검사(ADR-0128)는 그대로다. `FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE`는 같은
  릴리스 `build.json`의 `publisher_image`와 같아야 한다 — 모양만 digest인 다른 image는 설치 때
  `floor-config`가, 실행 때 `admitted-writer-runtime.sh`가 거부한다. `foundation-building-register-floor.service`는
  `current`가 가리키는 릴리스 id로 이 파일을 찾고, 링크는 읽지 않는다.
- `scripts/ops/admitted-writer-runtime.sh`는 등록 작업 다섯(`map-edit-fold.sh`·
  `daily-source-sweep.sh`·`publish-outbox.sh`·`lineage-stewardship-cycle.sh`·
  `building-register-floor-cycle.sh`)과 `lakehouse-migrate.sh`가 source한다. 자기 실제 위치에서
  `<base>/releases/<sha>`를 얻고, `<base>/current`가 그것을 가리키는지(FLOOR 정리 단계는
  `--installed`로 설치 여부만), `artifacts/<sha>/build.json`의 source와 publisher sha256이 맞는지
  확인한 뒤 `PUBLISHER_BIN`·`SPARK_RELEASE_JARS`를 정한다. `*_PUBLISHER_BIN`·`*_RELEASE_ROOT`
  override와 `/var/lib/foundation-platform/bin`은 없어졌다. Spark는 `--packages` 대신 동결 JAR을
  `--jars`로 받고 그 폴더를 읽기 전용으로 mount한다.
- `timers`는 `orchestration/jobs.v1.json`의 각 service와 `foundation-lakehouse-migrate.service`에
  `foundation-release-admission.conf`를 설치한다. 위치는 systemd가 실제로 읽는 단위다. 템플릿
  인스턴스(`foundation-map-edit-fold@complex.service`)는 템플릿 `foundation-map-edit-fold@.service.d`에
  둔다. 그래서 등록되지 않은 인스턴스를 손으로 시작해도 `ExecStartPre=+…admission.py verify-current`가
  돈다. 이 검사는 소스와 산출물의 모든 바이트를 root로 다시 대조한다.

### 4. Trino catalog는 릴리스 밖 파일 하나이고, 없으면 기동하지 않는다

`compose.lakehouse.yml`의 Trino는 `${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}/r2.properties`
파일 하나를 긴 문법 bind(`read_only: true`, `bind.create_host_path: false`)로 mount한다.
파일이 없으면 Docker engine이 `bind source path does not exist`로 컨테이너 생성을 거부한다
(2026-10-02 Linux 서버에서 확인; Docker Desktop for Windows는 경로를 만들어 버리므로 판정은
Linux에서만 한다). 기본값은 개발용 `./infra/lakehouse/trino/catalog`이고, 운영 릴리스 안에는
이 파일이 있을 수 없으므로(1항) 운영은 반드시 외부 디렉터리를 지정한다.

### 5. 호스트 선행 조건과 비상 절차

첫 배포 전에 다음이 없으면 설치는 그 이름을 대며 실패한다. 우회 스위치는 없다.

| 조건 | 왜 | 확인 |
| --- | --- | --- |
| root의 `gh` 인증 | 공개 저장소 identity를 `gh api`로 대조한다 | `sudo gh auth status` |
| root의 HTTPS fetch | 정본 main을 literal URL로 가져온다 | `sudo git ls-remote https://github.com/<정본>.git main` |
| Docker Buildx `docker-container` driver | 제한 있는 publisher 빌드 | `sudo docker buildx version` |
| `/opt/perfectory-control/current` | 신뢰의 시작점 | root 소유·쓰기 불가, `.perfectory-control-commit` |
| `/opt/foundation-platform{,/releases}`, `/var/lib/perfectory` | 부모를 바꿀 수 있으면 검사가 무의미 | `root:root`, group/other 쓰기 없음 |

런북 1단계의 확인 명령 하나가 위 조건을 모두 `ok`/`FAIL`로 보여 준다.

`gh` 대신 같은 identity를 내는 공개 조회로 바꾸려면 `show-public-repository-identity.sh`를
바꾸는 별도 결정이 필요하다. 지금은 root `gh` 인증이 조건이다.

비상 절차(GitHub 장애·Buildx 고장으로 새 릴리스를 인증할 수 없을 때):

1. 먼저 이미 인증된 릴리스로 `rollback`/`activate`한다. 보호된 Git cache로 검사하므로 네트워크가
   필요 없다.
2. 그것도 불가능할 때만 host 관리자(root)가 런북의 "비상 복귀" 명령을 그대로 실행한다: DAG 정지,
   `current`를 옛 릴리스로 원자 교체, 인스턴스·템플릿의 `10-release-admission.conf` 전부 제거, 옛
   릴리스의 unit 재설치, `systemctl daemon-reload`, 작업별 `Result=success` 확인. 사고 기록에
   시각·이유·SHA를 남긴다. 이 절차는 root 권한 그 자체이며 스크립트에 플래그·환경변수 우회를 만들지
   않는다. 우회가 코드에 있으면 sudo 받은 누구나 쓴다.

### 6. 현재 운영 배치에서의 1회 전환

현재 운영 릴리스(a70e832d)는 tar로 풀렸고 `bin/`·`.foundation-floor.env`를 안에 가진다. 이
릴리스는 1항 검사로 활성화·롤백 대상이 될 수 없다. 전환은 이 ADR을 포함한 병합 커밋을 새로
설치하는 것으로 한다. 기존 디렉터리는 지우지 않는다. 절차는
[lakehouse-compute-engines 런북](../../platforms/foundation-platform/docs/runbooks/lakehouse-compute-engines.md)의
"릴리스 인증 전환" 절이 정본이다. 순서: 선행 조건 확인 → 제어 체크아웃 설치 → `deployer-access`(새 규칙
먼저) → 새 SHA `prepare`(빌드 포함) → 기존 `.foundation-floor.env`를 새 ROOT·`publisher_image`로 고쳐
`floor-config` → Trino catalog **복사**(옮기지 않는다 — 옛 사본의 짧은 bind가 빈 디렉터리를 만든다)와 같은
compose 프로젝트로 재기동 → `activate` → `migrate` → `timers` → 옛 와일드카드 sudo 줄 삭제와
`deployer-access --exclusive`(마지막).
전환 뒤 `rollback`으로 a70e832d에 돌아갈 수 없다. 돌아가야 하면 5항 비상 절차(런북 "비상 복귀")다.

### 7. 막지 못한 경로를 완료라고 부르지 않는다

- 카탈로그 쓰기 토큰 분리는 하지 않았다. 토큰 소지자·Docker 접근자·root는 소스 검증기로 막을 수
  없다. 등록 작업 경로만 닫았다.
- Spark 작업 몇 개(`industrial_complex_boundary_served_gold.py` 등)는 세션 빌더에
  `spark.jars.packages`를 여전히 설정한다. `spark-submit`이 띄운 JVM에서는 해석되지 않는 값이지만
  코드에서 지우는 일은 해당 작업 소유자에게 남긴다.
- `scripts/load/*-handoff-export.sh` 수동 적재 스크립트는 기본값으로 실행 위치 release 안의
  `bin/` publisher를 찾는다. 새 배치에는 그 파일이 없으므로 `FOUNDATION_PLATFORM_PUBLISHER_BIN`으로
  `artifacts/<sha>/foundation-outbox-publisher`를 명시해야 한다. 등록 작업이 아니며, 등록하려면 이
  helper를 거쳐야 한다.
- 개발용 원격 job(`run-remote-lakehouse-job`)은 개발 체크아웃에 Trino catalog를 렌더한다. 운영
  경로가 아니다.

## 재사용과 기각한 대안

- 표준 Git 객체·archive 비교와 기존 정본 identity/transport를 재사용한다. 별도 서명 형식이나
  provenance registry를 만들지 않는다.
- GitHub/Sigstore artifact attestation(`gh attestation verify`)은 성숙한 대안이다. 지금 공개 CI
  정책은 `attestations:write`·`id-token:write`를 금지하고 소스 subtree를 직접 배포하므로, 새 서명
  파이프라인 대신 독립 fetch와 관리자 호스트 빌드를 택한다.
- **릴리스 안에 산출물을 두고 검증 목록에 예외로 넣기**: 기각. "릴리스 = 커밋 트리"가 깨지고, 예외
  목록이 또 하나의 정본이 된다.
- **호출자 바이너리 + sha256 인자(#307의 `publisher`)**: 기각. sha256을 호출자가 고르면 무결성일 뿐
  출처 증명이 아니다.
- **drop-in을 인스턴스마다 설치**: 기각. 등록 목록에 없는 인스턴스(`@parcel` 등)를 손으로 시작하면
  검사 없이 돈다.
- **sudo 규칙을 `current/` 경로로 고정**: 기각. 첫 전환 때 `current`는 인증 전 릴리스이고, 배포기가
  자기 자신을 인증하게 된다.
- Docker `:ro`만으로는 host 쪽 변경을 막지 못하므로 root 소유권·읽기 전용 소스·파일 대조를 함께
  요구한다. 실행 시작 후 바이트를 바꿀 수 있는 root는 명시적인 신뢰 경계다.

## 검증

- `platforms/foundation-platform/scripts/deploy/foundation-release-test.sh`(비특권, real Git
  fixture): 미병합 SHA, 병합 SHA에 다른 바이트, 추가 파일(`bin/foundation-outbox-publisher`와
  `.foundation-floor.env` — 지금 운영 배치 그대로), 쓰기 가능·변조된 릴리스의 활성화·롤백,
  sha256이 다른 publisher 산출물, 없어진 `publisher` 명령, 릴리스 안 설정 쓰기, 템플릿이 아닌
  drop-in 위치, 와일드카드 sudo 줄을 심어 거부를 확인한다. `foundation-api/tests/deploy_contract.rs`가
  실행한다.
- `platforms/foundation-platform/tests/release_admission`(xtask Python 스위트): admission의 Git·
  산출물·빌더 연결, 명명된 선행 조건 실패, helper의 경로·sha256 결박, map-edit-fold가 릴리스 밖
  바이너리를 실행하지 않음, Trino mount의 짧은 문법·`create_host_path: true`·쓰기 가능 mount 거부,
  Linux engine에서 catalog 파일이 없을 때 기동 거부를 확인한다.
- `orchestration/tests/test_job_specs.py`: FLOOR 유닛이 `config/<sha>`만 읽고 릴리스 안 파일·링크를
  읽지 않음, 정리 단계까지 산출물 sha256 결박.
- 합성 검증은 실제 운영 빌드·권한 전환·운영 job 성공의 증거가 아니다. 6항 전환을 관측하기 전에는
  운영 경계가 활성화됐다고 보고하지 않는다.

## 1차 자료

- [Git archive](https://git-scm.com/docs/git-archive)
- [GitHub artifact verification](https://cli.github.com/manual/gh_attestation_verify)
- [Compose file reference — volumes long syntax `create_host_path`](https://docs.docker.com/reference/compose-file/services/#volumes)
- [Docker bind mounts](https://docs.docker.com/engine/storage/bind-mounts/)
- [systemd.unit drop-ins for template units](https://www.freedesktop.org/software/systemd/man/latest/systemd.unit.html)
- [sudoers command matching](https://www.sudo.ws/docs/man/sudoers.man/)
- [Docker container build driver resource limits](https://docs.docker.com/build/builders/drivers/docker-container/)

## 개정 기록

- 2026-10-03: §5 표의 "root의 `gh` 인증" 조건은
  [ADR-0136](./0136-release-admission-reads-the-public-repository-identity-without-a-github-login.md)이
  대체했다. identity는 공개 REST API에서 자격증명 없이 읽고, 인증 fetch는 credential helper 없이 돈다.
  위 결정 본문은 고치지 않았다.
