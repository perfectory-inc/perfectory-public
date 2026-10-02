---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-10-02
---

# ADR 0126: 운영 Iceberg 쓰기는 정본 main에서 인증한 소스로 실행한다

- Status: Accepted
- Date: 2026-10-02
- Amends: ADR-0007

## 원인과 불변식

기존 배포기는 호출자가 준 SHA 모양과 압축파일 해시를 기록했다. 두 값이 서로 같아도
그 파일이 정본 `main`에 병합된 코드라는 증거는 아니다. 공유된 운영 카탈로그 쓰기 토큰은
개발 트리의 Spark 코드도 운영 표를 바꿀 수 있게 했다.

운영 쓰기에 필요한 조건은 **정본 저장소의 병합된 커밋 + 그 커밋의 실제 실행 파일 +
그 파일만 실행하는 주체가 가진 쓰기 권한**이다. 환경변수, 호출자가 준 SHA, 같은 디렉터리의
인증 JSON, Iceberg snapshot property는 이 조건을 대신하지 못한다.

## 결정

### 1. 독립 제어 코드가 소스를 인증한다

관리자가 따로 설치한 `/opt/perfectory-control/current`는 root 소유의 전체 모노레포 제어
체크아웃이다. 운영자·에이전트·작업 계정은 이 경로와 부모를 변경할 수 없다. 후보 압축파일
안의 검증기를 관리자 권한으로 실행하지 않는다. 초기 제어 코드 설치는 관리자가 검토한
정본 커밋을 신뢰의 시작점으로 삼으며, 후보 자신이 시작점을 선언할 수 없다.

`scripts/deploy/foundation-release-admission.py`는 기존
`tools/github/repository-identity.json`, `show-public-repository-identity.sh`,
`safe-git-transport.sh`를 그대로 사용한다. 공개 정본의 live immutable identity를 대조하고
literal HTTPS URL의 `refs/heads/main`을 root 소유
`/var/lib/perfectory/foundation-release-source.git`에 독립 fetch한다. 호출자의 `origin/main`,
Git 설정, 객체 치환, Python 검색 경로를 신뢰하지 않는다.

`git merge-base --is-ancestor`는 요청한 전체 SHA가 관측한 정본 main에 포함되는지 검사한다.
이어 그 Git 객체에서 `git archive <sha>:platforms/foundation-platform`을 만들고, 후보 tar의
전체 파일 집합·바이트·실행 비트를 비교한다. 링크·특수 파일·경로 탈출·중복 파일·추가 파일은
거부한다. 설치할 바이트는 후보 tar에서 풀지 않고 독립 fetch한 Git에서 읽는다.

병합 여부만 검사하면 정본 SHA를 붙인 사설 tar가 통과한다. 파일 해시만 검사하면 사설 코드가
자기 해시를 선언한다. 두 조건을 함께 검사해야 한다. squash merge 전 feature SHA는 나중에
비슷한 변경이 main에 들어가더라도 병합 SHA가 아니므로 거부한다.

### 2. 설치 후에도 실제 바이트를 검사한다

기존 `foundation-release.sh`의 `install`·`activate`·`rollback`·`migrate`·`verify`·`timers`가
고정 경로의 제어 검증기를 호출한다. 검증기 위치를 환경변수로 바꿀 수 없다. 설치 파일은
root 소유 `0444`/`0555`, 디렉터리는 `0555`이며, 다른 주체가 부모를 바꿀 수 없어야 한다.
현재 소스를 바꾸려면 새 병합 커밋을 설치한다.

활성화·롤백·실행 전 검사에서는 root 소유 Git cache의 main 조상 여부와 실제 설치 바이트를
다시 비교한다. 온라인 조회 실패 때 사설 증명으로 우회하지 않는다. 이미 인증한 릴리스의
실행·롤백은 보호된 cache를 사용하므로 GitHub 일시 장애에 의존하지 않는다. 기존 두 marker는
이름과 재설치 충돌 검사용 기록이며 인증 권한이 아니다.

영역 릴리스는 계속 Foundation subtree만 포함한다. root 정책을 영역에 복제하지 않는다.
기존 릴리스 안에 만들어 둔 mutable 파일도 추가 파일로 거부한다.

`install`은 인증한 소스를 설치한 뒤, 활성화 전에 같은 제어 코드의 `build`를 호출한다.
기존 `Dockerfile.lakehouse-control`과 Cargo lockfile로 publisher를 빌드하고 image ID로
선택한 컨테이너에서 바이너리를 꺼낸다. Docker build는 호출자 환경·캐시·runtime env 파일을
받지 않는다. Spark 이미지 digest는 `compose.lakehouse.yml`, JAR 좌표는 기존
`lakehouse_engine.iceberg_packages()`에서 읽는다. 자격증명 없는 일회용 Spark 컨테이너가
빈 Ivy 디렉터리에 받은 JAR 전체를 동결한다. 새 패키지 목록이나 이미지 pin을 복제하지 않는다.

빌드는 빈 `DOCKER_CONFIG`의 일회용 표준 Buildx `docker-container` driver를 사용한다.
BuildKit 이미지도 digest로 고정한다. driver의 CPU quota는 2 CPU, memory와 memory-swap은
각 `4g`이며 종료 시 전용 builder를 제거한다. JAR resolver에도 같은 container 상한을 적용한다.
Cargo `--jobs 2`는 동시성 힌트이고 이 자원 상한을 대신하지 않는다. 상한은 builder/resolver
cgroup에 적용되며 host Docker daemon의 image 전송·저장 I/O 전체에 대한 4 GiB 보장은 아니다.
필요한 Buildx 기능이 없으면 제한 없는 `docker build`로 우회하지 않고 설치를 거부한다.

산출물은 `/opt/foundation-platform/artifacts/<sha>`에 root 소유 읽기 전용으로 둔다.
관리자 빌드가 생성한 `build.json`은 source SHA·publisher image ID·파일별 SHA-256을 기록하며,
실행 전 검사에서는 파일 집합·바이트·소유권·쓰기 비트·링크까지 대조한다. 후보가 제출한
빌드 기록이나 바이너리를 받아들이는 명령은 없다. 이 기록의 권위는 관리자만 생성·교체할 수
있는 경로와 빌드 절차에서 오며, 자체 서명 형식이나 외부 증명 registry가 아니다.
native publisher가 사용하는 host loader/library와 Docker daemon은 관리자 신뢰 경계다.
빌드는 `ldd`로 host runtime library 호환성을 확인하고 누락된 library가 있으면 활성화를 거부한다.

### 3. 작업 진입점과 권한 경계를 함께 닫는다

`orchestration/jobs.v1.json`은 계속 작업 목록의 단일 정본이다. `timers`는 각 등록 작업의
systemd service에 `foundation-release-admission.conf`를 설치한다. 고정 제어 코드의
`verify-current`를 관리자 권한의 `ExecStartPre`로 실행하므로 SSH dispatch뿐 아니라 직접
`systemctl start`도 변조된 릴리스나 빌드 산출물을 실행하지 못한다.
Airflow 예약 목록에 없는 `foundation-lakehouse-migrate.service`도 배포의 `migrate`가
동일한 drop-in 설치 함수를 호출하므로 수동 service 시작에 같은 검사를 적용한다.

네 publisher 사용 스크립트는 `admitted-writer-runtime.sh`를 통해 자기 실제 디렉터리가
고정된 current 릴리스인지 확인한다. `RELEASE_ROOT`·`PUBLISHER_BIN` 호출자 override는 제거한다.
publisher는 같은 SHA의 보호된 산출물만 실행한다. 등록된 Spark 작업은 `--packages` 대신
동결한 JAR 목록을 `--jars`로 전달하며, mount는 읽기 전용이다. 해당 Python 작업의
`spark.jars.packages` 재설정도 제거하여 session 생성이 가변 Ivy 해석을 다시 켜지 못한다.

`render-trino-catalog.sh`는 `FOUNDATION_PLATFORM_TRINO_CATALOG_DIR` 또는 기존 lakehouse
state root 아래 `trino/catalog`에 설정을 생성하며 릴리스 내부 경로는 거부한다. host 디렉터리는
`0700`으로 두고 Compose는 생성 파일 하나만 읽기 전용으로 mount한다. Trino container UID가
host의 비공개 상위 디렉터리를 직접 순회할 필요가 없다.
속성 목록은 기존 `r2-iceberg.properties.template`을 재사용한다. Trino는 별도
`FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN`과 기존 R2 reader key를 요구하며
writer 환경값으로 fallback하지 않는다. 실제 read-only scope는 provider와 private 전환 검증이
강제하며, 변수 이름을 권한 증명으로 신뢰하지 않는다.

`foundation-lakehouse-writer.conf.example`은 실제 호스트 전환 전에 검토할 설정이다.
비로그인 writer 계정만 카탈로그 write token과 Docker 실행 권한을 가지며, 운영자·에이전트는
기존 강제 SSH 명령으로 등록된 job ID만 시작한다. root 소유 `0600` writer 환경파일은 systemd가
읽어 주입한다. 개발자 토큰과 Trino 조회 토큰은 catalog read-only여야 한다. 기존 공유 write
token은 private 운영 절차로 회전하고, 모든 운영자·에이전트의 Docker socket·docker group·
무제한 sudo 접근을 제거한 뒤에만 경계가 활성화됐다고 말한다.

카탈로그 토큰만 회수해서는 충분하지 않다. 같은 bucket의 넓은 S3 object write 권한도 표의
data/metadata 파일을 직접 덮어쓸 수 있으므로 운영자에게 남기지 않는다. 개발자 Bronze 반입은
별도 ingress bucket 또는 provider가 강제하는 `bronze/` 범위 임시 권한으로 분리한다.
Trino 역시 read-only catalog token을 사용해야 조회 SQL 경로가 쓰기 우회로가 되지 않는다.

Docker 접근은 host root에 준하는 권한이다. root 관리자, write token 소지자, Docker 접근자가
다른 코드를 직접 실행하는 것은 소스 검증기로 막을 수 없다. 권한 전환 설정은 자동 적용하지
않으며 실제 host inventory·키·증거는 ADR-0007에 따라 private 영역에 둔다.

### 4. 막지 못한 경로를 완료라고 부르지 않는다

`lakehouse_ingest.append_batch_once`만 바꾸면 SQL overwrite·DDL·WAP·유지보수가 빠진다.
`lakehouse_maintenance.py`는 공통 catalog builder도 호출하지 않는다. 따라서 Python 호출부
목록이나 snapshot annotation을 인가 장치로 만들지 않는다.

`remote_lakehouse_job.rs`의 임의 SSH root/env file, `lakehouse-batch-load.sh`와 직접
`spark-submit`은 write token을 가진 호출자를 막는 보안 경계가 아니다. 권한 분리 후에는
개발자 read token으로 쓰기가 거부되며, 운영 쓰기는 등록된 고정 service에서 실행한다.
새 backfill 작업도 실행 코드 경로와 data 인자를 검토한 service를 먼저 등록한다.
기존 임의 SSH shell에 writer credential을 다시 부여하지 않는다. 원격 job의 handoff·summary
`target/` 작업 공간은 개발/반입용 경로이며, 등록되지 않은 원격 경로를 immutable 운영
릴리스에 붙이는 것만으로 운영 launcher로 승격하지 않는다.

이 변경의 합성 검증은 소스 인증·빌드 명령 연결·실행 파일 결박·실행 전 거부를 증명한다.
실제 release build, private 권한 전환, 기존 credential 회수, 운영 job 성공을 실제 관측하기
전에는 전체 운영 쓰기 경계가 활성화됐다고 주장하지 않는다.

## 재사용과 기각한 대안

- 표준 Git 객체·archive 비교와 기존 정본 identity/transport를 재사용한다. 별도 서명 형식이나
  provenance registry를 만들지 않는다.
- GitHub/Sigstore artifact attestation은 성숙한 대안이다. 현재 공개 CI 정책은
  `attestations:write`·`id-token:write`를 금지하고 source subtree를 직접 배포한다. 이번에는
  새 signing pipeline 대신 독립 fetch와 관리자 호스트의 기존 Docker/Cargo 빌드를 택한다.
  외부 CI 바이너리를 배포하게 되면 signer workflow, source ref/digest를 고정하는
  `gh attestation verify`를 검토한다.
- Docker `:ro`만으로는 호스트 소스 변경을 막지 못하므로 root 소유권·읽기 전용 소스·파일 대조를
  함께 요구한다. 실행 시작 후 바이트를 바꿀 수 있는 root는 명시적인 신뢰 경계다.

## 검증

기존 `foundation-release-test.sh`가 real Git fixture로 미병합 소스, 정본 SHA/다른 archive,
변조·쓰기 가능한 release의 활성화·롤백을 공격한다. 이 shell rehearsal은 기존
`foundation-api/tests/deploy_contract.rs`가 실행하고, `tests/release_admission`의 Python 회귀는
같은 `cargo xtask verify foundation`의 선언된 Python suite가 실행한다.
네트워크는 fixture로 대체하며 실제 Git ancestry와 바이트 비교는 대체하지 않는다.
합성 Docker connector는 실제 빌드인 척하지 않는다. 명령의 인증된 context·깨끗한 환경·image ID
연결을 검사하고 만들어 준 fixture bytes를 실제 파일 해시/권한 검증에 통과시킨 뒤, binary/JAR
치환·추가 JAR·가변 metadata를 공격한다. runtime helper는 사설 source/binary/cache 환경값을
주입해 검사하고, Trino renderer는 실제 임시 디렉터리에서 외부 쓰기와 내부 경로 거부를 검사한다.

## 1차 자료

- [Git archive](https://git-scm.com/docs/git-archive)
- [GitHub artifact verification](https://cli.github.com/manual/gh_attestation_verify)
- [Cloudflare catalog read/write permissions](https://developers.cloudflare.com/r2/api/tokens/)
- [Docker bind mounts](https://docs.docker.com/engine/storage/bind-mounts/)
- [Docker daemon security](https://docs.docker.com/engine/security/)
- [Docker container build driver resource limits](https://docs.docker.com/build/builders/drivers/docker-container/)
- [BuildKit release](https://github.com/moby/buildkit/releases/tag/v0.33.0)
