# ADR 0276: CI 가 릴리스를 한 번 빌드해 서명하고, 서버는 확인만 하고 돌린다

- Status: Proposed — 사장 결정 넷을 반영: 서버 밖 빌드, §7 CI 장애 때 서버 빌드 없음(2026-10-18), §5 릴리스 산출물은 GitHub 이 보관(2026-10-19, 같은 날의 R2 릴리스 전용 버킷 결정을 대신함). 설계 검토 대기. 방향의 근거: [ADR-0270 부록 A](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md)). 이 설계는 코디네이터 검토 대기
- Date: 2026-10-18
- Would change: [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md) §1·§3(서버가 정본 main 을 빌드),
  [ADR-0137](./0137-the-release-build-is-sized-from-a-measurement-and-runs-alone.md)(서버 빌드의 상한, 빌드는 혼자 돈다)
- Keeps: ADR-0134 §2(설치 뒤 바이트 재검사, sudo 는 제어 경로 하나), [ADR-0184](./0184-one-answer-for-running-jobs-and-a-release-the-kernel-keeps-immutable.md)(봉인)

## Context

배포가 돌던 작업을 기다리는 가장 큰 이유는 서버 빌드다. publisher 빌드는 서버에서 2,590초, 익명 메모리 16.7 GiB(상한 22g)를 쓰고
(ADR-0137), 메모리 예산 때문에 등록된 작업이 하나도 돌지 않을 때만 돈다. 2026-10-17 하루 배포 14번의 0단계(DAG 멈춤 + 작업
끝 기다림)가 합쳐 664분이었다(ADR-0270 단계 4 측정). ADR-0270 D1~D5 는 이 기다림을 작업 쪽에서 줄이지만, 22g 빌드와 무거운 작업은
여전히 함께 돌 수 없다. 서버에서 빌드를 없애면 그 마지막 이유가 사라진다.

서버가 지금 만들거나 쓰는 실행물(2026-10-18, 릴리스 766914cb 실측):

| 무엇 | 크기 | 어디서 만드나 | 릴리스마다 바뀌나 |
|---|---:|---|---|
| publisher 실행 파일(`artifacts/<sha>/foundation-outbox-publisher`) | 79 MB | 승인 빌드(publisher 이미지에서 꺼냄) | 예 |
| Spark jar 5개(`artifacts/<sha>/jars`) | 376 MB | 승인 빌드(의존성 해석기가 Maven 에서) | 드묾 |
| publisher 이미지 | 224 MB | 승인 빌드 | 예 |
| tippecanoe 이미지 | 207 MB | 승인 빌드 | 드묾 |
| `foundation-platform-runtime:local`(foundation-api) | 139 MB | **compose `build:`(승인 밖)** | 예 |
| `foundation-postgres-recovery:local` | 751 MB | **compose `build:`(승인 밖)** | 드묾 |
| 릴리스 소스 트리(`releases/<sha>`) | 24 MB | 정본 Git 의 `git archive` | 예 |

마지막 줄은 실행물이 아니라 스크립트·계약이고, 지금처럼 서버가 정본 Git 에서 직접 꺼낸다. 위 두 compose 이미지는 지금도
승인을 거치지 않고 서버에서 빌드된다 — 이 결정은 그 빈틈도 닫는다.

## Decision

### 1. 불변식

**검증하지 않은 실행물은 서버에서 돌지 않는다.** 서버에서 도는 모든 실행 파일·이미지는 (a) 정본 main 의 커밋에 대해 (b) 이
저장소의 정해진 워크플로가 만들고 (c) 서명한 릴리스 목록(manifest)에 다이제스트로 적혀 있어야 하며, 서버는 그 서명과 다이제스트를
확인한 뒤에만 설치·실행한다. 서버는 빌드하지 않는다(`docker build`·`compose build`·`cargo` 없음).

### 2. 산출물과 형식

CI 는 정본 main 의 커밋 하나마다 **릴리스 목록 하나**와 그것이 가리키는 **내용 주소 블롭**들을 만든다.

- 블롭: 각 이미지의 OCI 블롭(manifest·config·layer). publisher 실행 파일과 jar 는 `FROM scratch` 이미지 하나(`files`)에 담는다.
  그래서 레지스트리는 이것들도 이미지처럼 보관하고, 목록은 파일 하나하나의 sha256 도 적는다. 같은 바이트는 레지스트리에 한 번만 있다
  (바뀌지 않은 jar·tippecanoe·기반 레이어는 다시 올라가지 않는다).
- 릴리스 목록 `foundation-platform.release_manifest.v1` (JSON):

  | 칸 | 뜻 |
  |---|---|
  | `source_commit` | 40자 커밋. 서버가 설치하려는 릴리스 id 와 같아야 한다 |
  | `workflow`, `run_url` | 만든 워크플로 경로와 실행 |
  | `toolchain` | `rust-toolchain.toml` 의 판, 기반 이미지 다이제스트 |
  | `files` | `{설치 경로: {sha256, size, mode}}` — `foundation-outbox-publisher`(0555), `jars/<이름>`(0444) |
  | `images` | `{publisher, tippecanoe, foundation-platform-runtime, foundation-postgres-recovery, files}` 각각 `{oci_manifest, config}` 다이제스트. **이미지 ID = `config` 다이제스트** |

### 3. 서명

- CI 는 출처 증명(SLSA provenance) 하나를 GitHub artifact attestation(Sigstore, 키 없는 서명)으로 만든다. 그 대상(subject)은
  **릴리스 목록, 모든 OCI 블롭, `files` 의 파일 하나하나**의 다이제스트다 — 산출물 하나하나가 서명된 출처 증명에 이름이 올라 있다.
  번들(`manifest.sigstore.json`)은 목록과 함께 레지스트리의 `release-<커밋>` 에 둔다. GitHub API 에 기대지 않고 서버가 번들만으로 확인한다.
- 서버는 목록의 서명을 확인하고(아래), 받은 산출물마다 sha256 이 목록과 **그리고** 출처 증명의 대상 목록 양쪽에 있는지 확인한다.
- 서버는 고정 판의 `cosign verify-blob-attestation --offline` 으로 **목록 하나**를 확인한다(`tools/pinned-artifacts.json` 에 판과 체크섬).
  cosign 은 128 MiB 넘는 파일을 거부하므로 블롭은 cosign 에 넣지 않고 sha256 을 서명된 대상 목록과 대조한다(§11):
  `--bundle manifest.sigstore.json`,
  `--certificate-oidc-issuer https://token.actions.githubusercontent.com`,
  `--certificate-identity https://github.com/<정본 저장소>/.github/workflows/foundation-release-build.yml@refs/heads/main`,
  `--certificate-github-workflow-sha <설치할 커밋>`, `--trusted-root <고정된 Sigstore 신뢰 루트>`.
  저장소 이름은 지금처럼 `tools/github/repository-identity.json` 에서 읽는다(ADR-0136).
- 신뢰 루트 파일도 고정 판으로 저장소에 두고, 바꾸는 것은 PR 이다.

### 4. 서버의 확인 순서 (승인 `install`, 빌드 대신)

1. 커밋이 정본 main 의 조상인지(지금 그대로, ADR-0134 §1).
2. 레지스트리에서 `release-<커밋>` 을 익명으로 받아 목록과 번들을 꺼내고 서명을 확인(§3). `source_commit` = 설치할 커밋.
   목록의 모든 다이제스트가 서명된 대상 목록에 있는지 확인한다.
3. 이미지마다 `docker pull <저장소>@<oci_manifest>` 로 **다이제스트로만** 받는다(태그로 받지 않는다). 받은 이미지 ID 가 목록의
   `config` 와 같은지 확인하고 `<이름>:<sha>` 로 태그한다.
4. `files` 이미지에서 실행 파일과 jar 를 꺼내 하나하나 sha256·크기를 목록과 대조하고, `artifacts/<sha>/` 에 목록의 모드로 설치한다.
5. 봉인(ADR-0184)과 설치 뒤 재검사는 그대로. `build.json` 자리는 서명 확인을 거친 목록 사본이 대신한다.
6. 실행할 때(`verify-current`, 작업의 `ExecStartPre`)도 지금처럼 바이트를 다시 확인하고, compose 는 `pull_policy: never`·빌드
   금지로 목록의 태그만 쓴다. 운영 compose 에 `build:` 를 쓰는 실행은 시험이 거부한다.

### 5. 보관과 권한 — GitHub 컨테이너 레지스트리 (사장 결정, 2026-10-19)

2026-10-18 의 R2 릴리스 전용 버킷 결정을 대신한다. 공개 워크플로가 저장된 비밀로 버킷에 쓰면 ADR-0007("비밀을 쓰는 live 워크플로는
비공개 영역, 공개 저장소에는 secretless CI 만")과 부딪힌다. GitHub 이 산출물을 보관하면 저장된 비밀이 하나도 필요 없다.

```text
ghcr.io/<정본 조직>/foundation-release            (공개 패키지 하나, 저장소에 연결)
  <커밋>-<이미지 이름>      이미지 하나하나(publisher, tippecanoe, foundation-platform-runtime, foundation-postgres-recovery, files)
  release-<커밋>            목록 + 번들, 맨 마지막에 올린다
```

| 누가 | 자격 | 할 수 있는 것 |
|---|---|---|
| `foundation-release-build.yml` 의 서명 잡(main) | 그 잡의 `GITHUB_TOKEN`, `packages: write`(저장된 비밀 아님) | 이 저장소의 패키지에 올리기 |
| 서버(릴리스 승인) | 없음(공개 패키지의 익명 pull) | 다이제스트로 받기 |
| 그 밖의 모두 | 없음 | 공개 패키지라 받을 수는 있다. 소스가 이미 공개이므로 새로 드러나는 것은 없고, 이미지에 비밀이 없는지는 지금처럼 공개 저장소 가드가 소스에서 막는다 |

- 비용: 공개 패키지는 저장·전송 모두 무료다(GitHub Packages 과금 문서, 2026-10-19). 문서에 한도는 적혀 있지 않다.
  레이어 하나에 10 GB, 업로드 하나에 10분 한도가 있다. 이번 최대 레이어는 281 MB jar 다.
- 바이트 그대로: 이미지는 CI 가 빌드한 OCI 레이아웃에서 `skopeo copy --preserve-digests` 로 올린다. 그래서 레지스트리의 다이제스트가
  서명된 다이제스트다. 올린 뒤 레지스트리가 돌려주는 manifest 의 sha256 을 다시 대조한다(`scripts/ci/release-push.sh`).
- 덮어쓰기: 다이제스트는 바뀌지 않는다. 태그는 옮길 수 있지만 서버는 태그로 받지 않고(`release-<커밋>` 은 목록을 찾는 이름일 뿐),
  옮겨진 목록은 커밋·워크플로에 묶인 서명 확인에서 거부된다. 같은 커밋의 `release-<커밋>` 이 다른 목록으로 이미 있으면 CI 는 덮어쓰지
  않고 실패로 보고한다. 저장소 관리자는 패키지를 지울 수 있다. 지워진 릴리스는 설치할 수 없을 뿐, 다른 것이 대신 설치되지는 않는다.
- 사장 손일(한 번): 첫 발행 때 패키지의 공개 범위 기본값은 private 이다. 패키지 설정에서 Public 으로 바꾼다(조직이 공개 패키지를 허용해야 한다).
- 워크플로 정책: 서명 잡 하나에만 `packages: write` 를 `id-token`·`attestations` 와 함께 허용한다(`check-workflow-policy.sh`). 권한이지
  저장된 비밀이 아니므로 ADR-0007 의 예외가 아니다.

### 6. 보존 (숫자)

| 무엇 | 얼마나 | 어떻게 |
|---|---|---|
| 릴리스 목록·서명(`release-<커밋>`) | **영구** | 지우지 않는다. 목록 하나는 수 KB, 감사 기록으로 남는다 |
| 이미지(`<커밋>-<이름>`) | **영구**, 정리는 필요해지면 따로 정한다 | 공개 패키지에는 보존 한도가 없다. 정리는 승인을 받고, 최근 목록이 가리키는 다이제스트는 지우지 않는다 |
| 서버에 설치된 릴리스 | 서버가 지울 때까지(지금 prune 규칙: current·previous·비상·최근 셋) | 산출물이 서버에 있으므로 레지스트리와 상관없이 되돌릴 수 있다 |

- 크기(2026-10-18 실측 기준 추정): 릴리스마다 새로 생기는 바이트는 바뀌는 실행 파일과 이미지 레이어(위 표의 "예" 행)뿐이고, 나머지는
  내용 주소로 다시 쓰인다. 비용 추정은 공개 저장소 규칙에 따라 여기 적지 않는다(비공개 문서).
- 정리의 첫 실행과 대량 삭제는 돌리기 전에 대상 수와 크기를 보고하고 승인을 받는다.

### 7. CI 가 멈췄을 때 — 기다린다 (사장 결정, 2026-10-18)

- **서버에서 대신 빌드하는 비상구는 없다**(사장 결정). 사람이 승인하는 서버 빌드도 두지 않는다. GitHub(Actions·Sigstore)가 멈추면
  새 릴리스는 만들어지지 않고, 우리는 **기다린다**.
- 운영은 그대로 돈다: 서버는 설치된 릴리스를 계속 쓴다. 자동 배포는 새 커밋의 목록이 아직 없으면 기다린다(검사가 아직 도는 것과 같다).
  목록이 없는 커밋은 배포되지 않는다.
- 되돌리기(§8)는 CI 없이 된다: 서버에 남은 릴리스, 또는 레지스트리에 남은 아무 커밋의 목록.
- **결과(받아들인 대가)**: GitHub 장애 동안에는 긴급 고침도 운영에 나가지 않는다. 그동안 쓸 수 있는 것은 이미 서명된 릴리스로의
  되돌리기와, 릴리스가 필요 없는 운영 조작(작업 멈추기·켜기, 설정·환경 파일 변경)뿐이다. 장애가 끝나면 그 사이 main 의 머리가 평소처럼
  빌드·배포된다.

### 8. 되돌리기

- 서버에 남은 릴리스로: 지금의 `rollback` 그대로. 활성화 때 보관된 번들로 서명을 다시 확인한다.
- 서버에서 지운 릴리스로: 레지스트리에서 일반 설치(§4)와 똑같이 받아 확인 후 활성화. 이미지가 지워진 릴리스는 설치할 수 없고,
  승인은 "이미지 없음"으로 거부한다(조용히 다른 것을 쓰지 않는다).

### 9. CI 쪽과의 경계 (ci-a 작업자가 맡는 부분)

| 경계 | 정함 |
|---|---|
| 워크플로 | `.github/workflows/foundation-release-build.yml`, `push` to `main` + 같은 ref 의 `workflow_dispatch`(커밋 다시 빌드) |
| 입력 | 커밋 하나. 빌드 정의는 지금의 Dockerfile 들(`services/foundation-outbox-publisher/Dockerfile.lakehouse-control`, `infra/tiles/tippecanoe/Dockerfile`, `services/foundation-api/Dockerfile`, `infra/postgres/Dockerfile.recovery`)과 jar 해석기 |
| 출력 | §2 목록 + 블롭 + §3 번들을 §5 의 패키지에. 이미지를 모두 올린 뒤 `release-<커밋>` 을 마지막에. 같은 커밋을 다시 빌드해 다른 바이트가 나와도 이미 있는 목록은 덮어쓰지 않는다(실패로 보고) |
| 실패 | 목록이 없으면 서버는 배포하지 않는다. 부분 업로드는 목록을 마지막에 올려서 보이지 않게 한다 |
| 열린 위험 | 메모리는 재 보니 8.2 GB 로 들어간다(§11). 빌드 캐시가 없어 매번 30분 안팎이다. 캐시는 Actions 캐시 한도(10 GB) 밖에 둬야 하며 R1 이후에 정한다 |

### 10. 옮기는 단계

| 단계 | 하는 일 | 끝났다의 기준 |
|---|---|---|
| R1 | CI 가 빌드·서명·업로드만(서버는 지금처럼 빌드) | main 커밋 10개 연속 목록·번들·블롭 생성, 무료 러너에서 메모리 실패 0 |
| R2 | 승인이 목록을 확인하는 명령을 얻음(설치에는 아직 안 씀) | 서버에서 10개 목록 확인 성공, 위조 번들·바뀐 블롭·다른 커밋을 심으면 거부 |
| R3 | 설치가 빌드 대신 목록을 씀, compose 빌드 금지 | 서버 빌드 0회, 배포 10번 연속 성공, 운영 compose 의 `build:` 실행 0 |
| R4 | 빌드 코드·ADR-0137 상한·메모리 예산의 빌드 항목 삭제 | 코드 삭제, 예산에서 22g 빠짐 |

### 11. R1 첫 측정 (2026-10-18, 무료 표준 러너)

`foundation-release-build.yml` 을 시험 브랜치에서 돌렸다(실행 39144325130). 러너는 ubuntu-24.04, 4 CPU, 16 GB 메모리다.
`scripts/ci/release-build-runner-room.sh` 가 쓰지 않는 도구를 지워 / 에 121 GB 를 남기고, /mnt 에 스왑 16 GB 를 더한다.
빌드 정의는 승인 스크립트(`RELEASE_IMAGES`, 빌드 계약의 인자, Spark 이미지와 엔진 패키지)와 compose 파일의 `build:` 에서 읽는다. 사본은 없다.

| 무엇 | 시간 | 최대 메모리 | 크기 |
|---|---:|---:|---:|
| publisher 이미지(빌드 계약의 `CARGO_BUILD_JOBS=2`, 캐시 없음) | 1,670초 | 8.2 GB | 블롭 59 MB(압축), 실행 파일 79 MB |
| tippecanoe 이미지 | 93초 | 1.9 GB | 53 MB |
| foundation-api 런타임 이미지(지금은 승인 밖) | 223초 | 5.1 GB | 36 MB |
| 복구 Postgres 이미지(지금은 승인 밖) | 8초 | 0.9 GB | 196 MB |
| jar 5개(Spark 이미지의 spark-submit) | 약 20초 | — | 393 MB(그중 aws-java-sdk-bundle 281 MB) |
| 출처 증명(대상 43개: 목록 + 블롭 42) | 4초 | — | 번들 하나 |
| 잡 전체 | 36분(빌드 단계 33분 51초) | — | Actions 아티팩트 699 MB |

- publisher 의 최대 메모리는 서버 측정(16.7 GiB, ADR-0137)의 절반이었다. 러너의 16 GB 안에 들어간다.
- 서버 쪽 확인(스크래치, cosign v3.1.3, 체크섬 확인한 공식 판)에서 목록과 블롭 42개 모두 확인됐다. 신원은 워크플로 경로, ref, 커밋 sha 다.
  - 목록 1바이트 변경, 다른 ref(`refs/heads/main`), 다른 커밋은 모두 거부됐다.
- 찾은 것 셋. R2·R3 에서 반영한다.
  1. `docker load` 는 OCI 레이아웃만 담긴 묶음을 읽지 못한다. BuildKit 의 `docker` 내보내기는 OCI 레이아웃과 Docker 의
     `manifest.json` 을 한 묶음에 쓴다. 블롭은 그대로 내용 주소이고 `docker load` 도 되며, 불러온 이미지 ID 가 목록의 `config` 와 같다.
  2. cosign 의 `verify-blob-attestation` 은 128 MiB 넘는 파일을 거부한다(281 MB jar). 그래서 서버는 §3 대로 cosign 으로 **목록 하나**를 확인한다.
     블롭은 sha256 을 목록, 그리고 서명된 대상 목록과 대조한다(이번 확인에서 둘 다 맞았다).
  3. 같은 커밋도 빌드마다 레이어 바이트가 조금씩 다르다(publisher 블롭 59,334,786 대 59,334,777). 재현 가능 빌드가 아니므로
     §9 대로 같은 커밋의 두 번째 목록은 덮어쓰지 않는다.

**GHCR 로 올리기 (2026-10-19, 시험 브랜치 실행 39159070370).**

- 같은 빌드에 `files` 이미지를 더했다. 목록 하나, 블롭 39개, 파일 7개가 출처 증명의 대상 46개다.
- `skopeo copy --preserve-digests` 로 이미지 다섯을 올리고 `release-<커밋>` 을 마지막에 올렸다. 업로드는 83초였다.
- 레지스트리가 돌려준 manifest 의 sha256 이 다섯 모두 목록의 `oci_manifest` 와 같았다.
- 서버 스크래치에서 `cosign verify-blob-attestation --offline --trusted-root <고정 파일>` 로 목록을 확인했다.
  - 다른 ref, 다른 커밋은 거부됐다.
  - 목록의 다이제스트 16개가 모두 서명된 대상 안에 있었다.
- 첫 발행으로 만들어진 패키지는 문서대로 private 이었다. 익명 토큰 요청은 `authentication required` 였다.
  - Public 전환(§5 사장 손일)은 되돌릴 수 없다("Once you make a package public, you cannot make it private again").
  - 그래서 결정권자 확인 뒤에 한다. 익명 pull 의 실증은 전환 뒤로 남는다.

### 12. R2 — 서버의 확인 명령과 스위치 (2026-10-19)

- `foundation-release.sh verify-registry <커밋>` 은 승인의 `verify-registry` 를 부른다. §4 의 2–4 를 스크래치 디렉터리에 모두 하고,
  아무것도 설치하거나 태그하지 않는다. 순서는 이렇다.
  1. `release-<커밋>` 을 익명으로 받는다.
  2. 고정한 cosign(`tools/cosign.env`, 받는 곳은 `tools/pinned-artifacts.json`)으로 `--offline` 확인한다. 신뢰 루트는 고정 파일
     (`tools/sigstore/public-good.trusted_root.json`)이고, 신원은 워크플로·ref·커밋이다.
  3. 목록의 모든 다이제스트가 서명된 대상 안에 있는지 본다.
  4. `files` 이미지의 manifest 와 레이어를 다이제스트로 받아 sha256 을 다시 계산하고, 파일 하나하나를 목록과 대조한다.
     목록에 없는 파일이 있으면 거부한다.
  5. publisher 와 tippecanoe 를 다이제스트로 pull 하고, 이미지 ID 가 서명된 `config` 인지 본다.
- 스위치는 `tools/release-build.contract.json` 의 `artifacts_from` 이다. 기본값은 `server-build` 이고, 지금처럼 서버가 빌드한다.
  - `registry` 로 바꾸면 승인 `build` 가 빌드 대신 위 확인을 거친 바이트만 `artifacts/<sha>/` 에 두고, 서명된 이미지만 태그한다.
  - 그 경로는 빌드 잠금이 필요 없고 돌던 작업도 기다리지 않는다.
  - 실행 파일의 `ldd` 검사와 봉인·재검사는 서버 빌드 때와 같다. 되돌리기는 스위치를 다시 `server-build` 로 바꾸는 것이다.
- 시험(`tests/release_admission/test_release_from_registry.py`)이 거부를 하나씩 본다.
  - 서명 거부, 서명되지 않은 다이제스트, 다른 커밋의 목록
  - 목록에 없는 파일, 다이제스트와 다른 바이트, 서명된 `config` 가 아닌 이미지
  - 서명이 거부되면 아무것도 pull 하지 않는다.
  - 대조 검사 둘을 지우면 각각 실패한다.
- R2 완료는 서버에서 10개 목록을 확인하는 것이다. 익명 pull 이 필요하므로 패키지 공개 전환 뒤에 센다.

### 13. R3 — 스위치를 켠다 (2026-10-19)

- R2 를 셌다. 운영 배포와 같은 sudo 경로로 main 커밋 10개를 `verify-registry` 했고 10개 모두 통과했다(한 개 57–95초).
- 스위치를 `registry` 로 바꾼다. 서버는 그 순간부터 승인 이미지(publisher·tippecanoe)와 파일을 빌드하지 않는다.
- 찾은 위험: 배포 관문은 merge queue 가 초록이면 바로 배포를 시작하는데(ADR-0167), 릴리스 빌드는 main push 뒤에 돌고 약 31분이
  걸린다. 그대로 켜면 관문이 통과시킨 커밋의 목록이 아직 없어 `verify-registry` 가 실패하고 배포가 멈춘다.
  - 그래서 `registry` 모드에서는 관문(`scripts/deploy/release_checks.py`)이 그 커밋의 `registry.check_run`
    (릴리스 워크플로의 잡 이름) 이 통과할 때까지 기다린다. 실패면 거부한다. 스위치가 `server-build` 면 예전과 같다.
  - 잡 이름이 바뀌면 관문이 영원히 기다리므로 `scripts/guard/release-build-self-test.sh` 가 계약의 이름이 워크플로에 있는지 본다.
- 남은 것: 운영 compose 가 `build:` 로 짓는 이미지(foundation-api, recovery)는 아직 서버에서 빌드한다. 이 둘을 목록에서 받고
  compose 빌드를 금지해야 §10 의 R3 기준(서버 빌드 0회)이 된다. 다음 PR 이다.
- compose 이미지 둘도 목록에서 받는다.
  - 승인은 `foundation-platform-runtime` 과 `foundation-postgres-recovery` 를 다이제스트로 pull 하고, 서명된 `config` 인지 보고,
    `build.json` 에 `<이름>_image` 로 적는다. 둘 다 있거나 둘 다 없어야 봉인한다. 태그는 붙이지 않는다.
  - `foundation-release.sh migrate` 는 `foundation-api` 를 빌드하는 대신 승인의 `tag-compose-images` 를 부른다.
    봉인을 다시 확인하고 적힌 ID 에 compose 가 쓰는 `:local` 태그를 붙인다. 적힌 것이 없으면(75, 서버 빌드 릴리스) 예전처럼 빌드한다.
  - 정리(prune)는 `build.json` 의 `_image` 를 모두 읽으므로, 지난 릴리스의 API·복구 이미지도 그 릴리스와 함께 지워진다.
  - compose 파일의 `build:` 는 남긴다. CI 빌드가 거기서 무엇을 지을지 읽고, 개발 환경도 쓴다.

### 14. 릴리스 빌드 캐시 (2026-10-19)

- 31분 중 25분은 publisher 의 cargo-chef 의존성 층(`cargo chef cook`)이었다. 이 층은 `Cargo.lock`·레시피가 같으면 같지만, 러너는
  매번 빈 상태라 커밋마다 다시 지었다.
- BuildKit 캐시를 릴리스 패키지(`ghcr.io/<owner>/foundation-release`) 안에 이미지별 `buildcache-<이름>` 태그로 둔다
  (`--cache-from`/`--cache-to type=registry,mode=max,ignore-error=true`).
  - 공개 패키지라 저장 비용이 없고, Actions 캐시 10 GB 예산(ADR-0269)을 쓰지 않는다.
  - 쓰는 권한은 이미 있는 이 잡의 `GITHUB_TOKEN`(packages: write) 뿐이라 저장한 비밀이 없다.
  - 캐시를 읽거나 쓰지 못하면 전체 빌드가 될 뿐 실패하지 않는다.
  - 승인은 `release-<커밋>` 과 다이제스트만 읽으므로 캐시 태그를 보지 않는다.
- 측정(시험 브랜치, 무료 표준 러너):

| 실행 | publisher | 잡 전체 |
|---|---:|---:|
| 캐시 없음(지금 main) | 1,830초 | 41분 |
| 캐시를 처음 씀 | 1,878초(내보내기 약 80초 포함) | 44분 |
| 소스만 바뀐 커밋, 캐시 읽음 | 400초 | 14분 51초 |

- `Cargo.lock` 이 바뀌는 커밋은 의존성 층을 다시 지으므로 약 31분 그대로다.
- 신뢰: 캐시는 같은 잡만 쓴다. 다른 브랜치의 수동 실행도 같은 패키지에 쓸 수 있지만, 그 실행은 릴리스 자체도 쓸 수 있으므로 새로 열리는 경계는 없다.

## Rejected

| 대안 | 기각 이유 |
|---|---|
| R2 릴리스 전용 버킷(2026-10-18 결정) | 2026-10-19 사장 결정으로 대신함. 공개 워크플로가 저장된 쓰기 비밀을 들어야 해서 ADR-0007 과 부딪히고, 버킷·토큰 둘을 사장이 만들어야 한다 |
| 서버가 Actions 아티팩트를 받음 | 비밀은 서버의 읽기 토큰뿐이지만 아티팩트 보존이 7일이라, 그보다 오래된 커밋으로는 되돌릴 수 없다 |
| 키로 서명(cosign 키 쌍) | 키를 보관·돌려써야 한다. 워크플로 신원에 묶는 키 없는 서명이 이 규모에 맞다 |
| CI 실패 때 서버에서 대신 빌드(사람이 승인하는 비상 빌드 포함) | 사장 결정으로 기각(2026-10-18). §1 불변식을 깨고, 메모리 문제도 돌아온다. 대가는 §7 |
| 자체 호스팅 러너 | 운영할 기계가 하나 더 생기고 비용이 든다(CI 비용 0 원칙) |
| 재현 가능 빌드로 서버가 다시 빌드해 대조 | 나중의 표본 검사로 둔다. 지금은 서명 신원과 다이제스트로 충분하다 |

## Consequences

- 서버에서 22g 빌드가 사라지고, 배포는 빌드를 기다리지 않는다. ADR-0270 D4 의 "빌드는 무거운 작업을 기다림"이 필요 없어진다.
- 믿는 대상에 GitHub Actions·Sigstore·GitHub 컨테이너 레지스트리가 더해진다. 완화: 워크플로 경로·ref·커밋 sha 를 모두 묶어 확인하고, 신뢰 루트와 cosign 판을 고정하며, 다이제스트로만 받는다.
- 실행물이 공개된다(§5). 저장된 비밀은 생기지 않는다.
- GitHub 장애 동안에는 새 릴리스를 배포할 수 없다(§7, 받아들인 대가). 되돌리기와 릴리스 없는 운영 조작은 된다.
- 승인이 compose 이미지(foundation-api, 복구 Postgres)까지 맡게 되어, 지금 승인 밖에서 빌드되던 두 이미지의 빈틈이 닫힌다.

2026-10-28: 구현 점검 — Proposed 유지. R1–R3은 있다(`.github/workflows/foundation-release-build.yml`, `tools/release-build.contract.json`, `scripts/deploy/foundation-release-admission.py`의 `verify-registry`). §10 R4(서버 빌드 경로 제거 — `foundation-release.sh`가 아직 종료 코드 75에서 직접 빌드한다)와 §4.6의 `build:` 거부 시험은 없다.
