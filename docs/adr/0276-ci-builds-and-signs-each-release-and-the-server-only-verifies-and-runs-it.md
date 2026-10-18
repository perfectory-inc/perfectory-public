# ADR 0276: CI 가 릴리스를 한 번 빌드해 서명하고, 서버는 확인만 하고 돌린다

- Status: Proposed — 방향은 사장 결정(2026-10-18, [ADR-0270 부록 A](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md)). 이 설계는 코디네이터 검토 대기
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

- 블롭: 바이트를 sha256 으로 이름 붙인 파일. publisher 실행 파일, 각 jar, 그리고 각 이미지의 OCI 이미지 블롭(index/manifest/config/
  layer). 같은 바이트는 한 번만 올라간다(바뀌지 않은 jar·tippecanoe·기반 레이어는 릴리스마다 다시 올라가지 않는다).
- 릴리스 목록 `foundation-platform.release_manifest.v1` (JSON):

  | 칸 | 뜻 |
  |---|---|
  | `source_commit` | 40자 커밋. 서버가 설치하려는 릴리스 id 와 같아야 한다 |
  | `workflow`, `run_url` | 만든 워크플로 경로와 실행 |
  | `toolchain` | `rust-toolchain.toml` 의 판, 기반 이미지 다이제스트 |
  | `files` | `{설치 경로: {sha256, size, mode}}` — `foundation-outbox-publisher`(0555), `jars/<이름>`(0444) |
  | `images` | `{publisher, tippecanoe, runtime, postgres_recovery}` 각각 `{oci_manifest, config}` 다이제스트. **이미지 ID = `config` 다이제스트** |

### 3. 서명

- CI 는 릴리스 목록 파일의 다이제스트에 GitHub artifact attestation(Sigstore, 키 없는 서명)을 붙이고, 그 번들
  (`manifest.sigstore.json`)을 목록 옆에 둔다. GitHub API 에 기대지 않고 서버가 번들만으로 확인한다.
- 서버는 고정 판의 `cosign verify-blob` 으로 확인한다(`tools/pinned-artifacts.json` 에 판과 체크섬):
  `--bundle manifest.sigstore.json`,
  `--certificate-oidc-issuer https://token.actions.githubusercontent.com`,
  `--certificate-identity https://github.com/<정본 저장소>/.github/workflows/foundation-release-build.yml@refs/heads/main`,
  `--certificate-github-workflow-sha <설치할 커밋>`, `--trusted-root <고정된 Sigstore 신뢰 루트>`.
  저장소 이름은 지금처럼 `tools/github/repository-identity.json` 에서 읽는다(ADR-0136).
- 신뢰 루트 파일도 고정 판으로 저장소에 두고, 바꾸는 것은 PR 이다.

### 4. 서버의 확인 순서 (승인 `install`, 빌드 대신)

1. 커밋이 정본 main 의 조상인지(지금 그대로, ADR-0134 §1).
2. R2 에서 목록과 번들을 받아 서명을 확인(§3). `source_commit` = 설치할 커밋.
3. 없는 블롭만 받아 서버의 내용 주소 저장소에 두고, 하나하나 sha256·크기를 확인.
4. `files` 를 `artifacts/<sha>/` 에 목록의 모드로 설치, 이미지는 OCI 레이아웃으로 `docker load` 한 뒤 **로드된 이미지 ID 가 목록의
   `config` 와 같은지** 확인하고 `<이름>:<sha>` 로 태그.
5. 봉인(ADR-0184)과 설치 뒤 재검사는 그대로. `build.json` 자리는 서명 확인을 거친 목록 사본이 대신한다.
6. 실행할 때(`verify-current`, 작업의 `ExecStartPre`)도 지금처럼 바이트를 다시 확인하고, compose 는 `pull_policy: never`·빌드
   금지로 목록의 태그만 쓴다. 운영 compose 에 `build:` 를 쓰는 실행은 시험이 거부한다.

### 5. R2 배치와 권한

```text
<릴리스 버킷>/
  releases/<sha>/manifest.json
  releases/<sha>/manifest.sigstore.json
  blobs/sha256/<다이제스트>
```

- 릴리스 전용 버킷 하나(데이터 버킷과 분리). 쓰기 열쇠는 CI 의 main 푸시 실행만(보호된 환경), 블롭·목록은 **만들기만**
  (`If-None-Match: *`; 같은 키에 다른 내용이 오면 실패). 서버는 읽기 열쇠만.
- PR 실행은 이 버킷에 아무 열쇠도 받지 않는다. 컴파일 캐시(sccache)는 별도 접두어·별도 열쇠로, 쓰기는 main 실행만(오염된 캐시가 릴리스로
  들어가는 길을 막는다).

### 6. 보존

- 목록: 최근 30일에 만든 것, 그리고 기간과 상관없이 서버에 설치된 릴리스(current·previous·비상)의 것은 지우지 않는다.
- 블롭: 주 1회 **표시 후 정리** — 남길 목록들이 가리키는 블롭만 남긴다(나이 규칙은 쓰지 않는다: 오래된 jar 를 새 목록이 다시 가리킨다).
- 크기(2026-10-18 실측 기준 추정): 릴리스마다 새로 생기는 바이트는 바뀌는 실행 파일과 이미지 레이어(위 표의 "예" 행)뿐이고, 나머지는
  내용 주소로 다시 쓰인다. 비용 추정은 공개 저장소 규칙에 따라 여기 적지 않는다(비공개 문서).
- 대량 삭제·복사는 처음 돌리기 전에 대상 수와 크기를 보고하고 승인을 받는다.

### 7. CI 가 멈췄을 때

- 운영은 그대로 돈다: 서버는 설치된 릴리스를 계속 쓴다.
- 자동 배포는 새 커밋의 목록이 아직 없으면 **기다린다**(검사가 아직 도는 것과 같다). 목록이 없는 커밋은 배포되지 않는다.
- 되돌리기(§8)는 CI 없이 된다.
- 서버에서 대신 빌드하는 길은 두지 않는다 — §1 불변식을 깨는 비상구이기 때문이다. 긴급 고침은 CI 가 돌아온 뒤 배포된다.

### 8. 되돌리기

- 서버에 남은 릴리스로: 지금의 `rollback` 그대로. 활성화 때 보관된 번들로 서명을 다시 확인한다.
- 서버에서 지운 릴리스로: 보존 기간 안이면 R2 에서 일반 설치(§4)와 똑같이 받아 확인 후 활성화.

### 9. CI 쪽과의 경계 (ci-a 작업자가 맡는 부분)

| 경계 | 정함 |
|---|---|
| 워크플로 | `.github/workflows/foundation-release-build.yml`, `push` to `main` + 같은 ref 의 `workflow_dispatch`(커밋 다시 빌드) |
| 입력 | 커밋 하나. 빌드 정의는 지금의 Dockerfile 들(`services/foundation-outbox-publisher/Dockerfile.lakehouse-control`, `infra/tiles/tippecanoe/Dockerfile`, `services/foundation-api/Dockerfile`, `infra/postgres/Dockerfile.recovery`)과 jar 해석기 |
| 출력 | §2 목록 + 블롭 + §3 번들을 §5 위치에, 만들기만. 같은 커밋을 다시 빌드해 다른 바이트가 나와도 이미 있는 목록은 덮어쓰지 않는다(실패로 보고) |
| 실패 | 목록이 없으면 서버는 배포하지 않는다. 부분 업로드는 목록을 마지막에 올려서 보이지 않게 한다 |
| 열린 위험 | 무료 러너는 16 GB 메모리, 서버 측정 최대 16.7 GiB. 먼저 잰다: `CARGO_BUILD_JOBS=1`, sccache(R2), 필요하면 DuckDB 를 미리 빌드된 라이브러리로 링크 |

### 10. 옮기는 단계

| 단계 | 하는 일 | 끝났다의 기준 |
|---|---|---|
| R1 | CI 가 빌드·서명·업로드만(서버는 지금처럼 빌드) | main 커밋 10개 연속 목록·번들·블롭 생성, 무료 러너에서 메모리 실패 0 |
| R2 | 승인이 목록을 확인하는 명령을 얻음(설치에는 아직 안 씀) | 서버에서 10개 목록 확인 성공, 위조 번들·바뀐 블롭·다른 커밋을 심으면 거부 |
| R3 | 설치가 빌드 대신 목록을 씀, compose 빌드 금지 | 서버 빌드 0회, 배포 10번 연속 성공, 운영 compose 의 `build:` 실행 0 |
| R4 | 빌드 코드·ADR-0137 상한·메모리 예산의 빌드 항목 삭제 | 코드 삭제, 예산에서 22g 빠짐 |

## Rejected

| 대안 | 기각 이유 |
|---|---|
| GHCR(GitHub 컨테이너 저장소) | 공개 저장소면 무료지만, 사장 결정은 R2 이고 이미 쓰는 공급자 하나로 둔다. 공개 패키지가 되면 실행물이 누구에게나 내려받아진다 |
| 키로 서명(cosign 키 쌍) | 키를 보관·돌려써야 한다. 워크플로 신원에 묶는 키 없는 서명이 이 규모에 맞다 |
| CI 실패 때 서버에서 대신 빌드 | §1 불변식을 깬다. 메모리 문제도 돌아온다 |
| 자체 호스팅 러너 | 운영할 기계가 하나 더 생기고 비용이 든다(CI 비용 0 원칙) |
| 재현 가능 빌드로 서버가 다시 빌드해 대조 | 나중의 표본 검사로 둔다. 지금은 서명 신원과 다이제스트로 충분하다 |

## Consequences

- 서버에서 22g 빌드가 사라지고, 배포는 빌드를 기다리지 않는다. ADR-0270 D4 의 "빌드는 무거운 작업을 기다림"이 필요 없어진다.
- 믿는 대상에 GitHub Actions 와 Sigstore 가 더해진다. 완화: 워크플로 경로·ref·커밋 sha 를 모두 묶어 확인하고, 신뢰 루트와 cosign 판을 고정한다.
- 승인이 compose 이미지(foundation-api, 복구 Postgres)까지 맡게 되어, 지금 승인 밖에서 빌드되던 두 이미지의 빈틈이 닫힌다.
