# ADR 0309: 릴리스는 CI 가 한 번 빌드하고 서버는 다이제스트로 받기만 한다 — 범위 밖 커밋은 빌드도 배포도 하지 않는다

- Status: Accepted (2026-10-28, 사장 승인)
- Date: 2026-10-28
- Accepts: [ADR-0276](./0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md) R1–R3 를 운영 결정으로
  확정한다(그 문서는 Proposed 로 남아 있었다)
- Amends: [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md) §3("관리자 호스트 빌드"),
  [ADR-0137](./0137-the-release-build-is-sized-from-a-measurement-and-runs-alone.md)(서버 빌드의 상한과 "혼자 돈다"는 되돌리기 경로에만 남는다)
- Builds on: [ADR-0202](./0202-a-pull-request-runs-only-the-areas-it-changes.md)(변경 범위의 정본 `tools/ci/change-scope.json`),
  [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md)(autodeploy), [ADR-0308](./0308-the-deploy-stops-only-the-jobs-its-release-holds-and-skips-a-step-whose-inputs-did-not-change.md)
- 관련 사건: 2026-10-27 웹만 바꾼 #879 가 릴리스 빌드 10.5분과 배포 12분을 받았다. 같은 날 Spark `.py` 만 바꾼 #881 이
  publisher 의 Rust 층을 다시 빌드해 351초를 썼다.

## Context

"CI 에서 한 번 빌드해 같은 산출물을 다이제스트로 승격한다"(Google 의 hermetic 빌드, Spinnaker 의 bake-once, Kubernetes 의 digest 고정)는
여기 이미 거의 다 있다. 사실 관계를 먼저 적는다(2026-10-28 main 77198c1f, 운영 서버 읽기 전용 확인).

| 무엇 | 어디에 | 상태 |
|---|---|---|
| CI 의 릴리스 빌드·서명·업로드 | `.github/workflows/foundation-release-build.yml`, `scripts/deploy/foundation-release-build.py`, `scripts/ci/release-push.sh` | main 의 모든 push 에서 돈다 |
| 출처 증명 | `actions/attest-build-provenance`(Sigstore 키 없는 서명, GitHub OIDC) | 목록과 모든 블롭이 대상 |
| 서버의 확인 | `scripts/deploy/foundation-release-admission.py` `registry_artifacts`: 고정 cosign `verify-blob-attestation --offline`, 신원 = 워크플로 경로·`refs/heads/main`·설치할 커밋 sha | 운영 중 |
| 스위치 | `tools/release-build.contract.json` `artifacts_from` | `registry`(#771 부터). 운영 `build.json` 에 레지스트리 이미지 넷 |
| 목록이 없을 때 | `release_checks.py` 가 `release-<커밋>` 이 없으면 `wait`, autodeploy 는 목록이 있는 가장 새 커밋만 배포 | 운영 중 |
| 비용 | 공개 패키지 저장·전송 무료, 표준 러너, 캐시는 패키지 안 `buildcache-*` 태그 | CI 지출 0 |

빠진 것은 둘이다.

1. **범위.** 릴리스 빌드는 main 의 모든 커밋에서 돈다. 웹만 바꾼 커밋도 30분짜리 잡을 받고(캐시가 맞으면 10–15분), 서버는 그 커밋을
   새 릴리스로 배포한다. 서버에서 바뀌는 것은 없다.
2. **층 순서.** publisher 이미지는 `COPY infra` 다음에 `cargo build` 를 한다. Spark 작업(`infra/lakehouse/spark/jobs/*.py`)만 바뀌어도
   Rust 층이 무효가 된다. Rust 코드가 컴파일해 넣는 `infra/` 는 `infra/lakehouse/contracts` 뿐이다(`include_str!` 전수 검색).

## Decision

### 1. 운영 경로는 레지스트리다 (ADR-0276 R1–R3 확정)

- 서버는 빌드하지 않는다. 승인은 `release-<커밋>` 의 목록과 번들을 익명으로 받는다.
- 고정 cosign 과 고정 Sigstore 신뢰 루트로 오프라인 확인한다.
  - 신원은 `https://github.com/<정본>/.github/workflows/foundation-release-build.yml@refs/heads/main` 이다.
  - `--certificate-github-workflow-sha` 는 설치할 커밋이다.
- 이미지는 다이제스트로만 받는다. 받은 이미지 ID 가 서명된 `config` 와 같아야 한다.
- 파일 하나하나는 sha256 을 서명된 대상 목록과 대조한다.
- 이 경로는 이미 운영 중이다. 이 ADR 은 그것을 결정으로 확정할 뿐이며 코드를 바꾸지 않는다.

### 2. 목록이 없거나 확인되지 않으면: 배포하지 않는다

두 가지 안전한 선택지 중 **거부(기다림)** 를 고른다. 이는 ADR-0276 §7 의 사장 결정(2026-10-18)과 같다.

- 목록이 아직 없으면 autodeploy 가 기다린다(`wait`). 서명이 틀리거나 다이제스트가 다르면 승인이 거부하고, 배포는 전환 전에 멈춘다.
  옛 릴리스가 계속 돈다(ADR-0173).
- 서버 빌드로 대신하지 않는다. 이유는 셋이다.
  - (a) "검증하지 않은 실행물은 돌지 않는다"는 불변식을 지킨다.
  - (b) 22g 빌더가 돌아오면 ADR-0308 에 따라 모든 작업을 멈춰야 한다. 이 ADR 이 없애려는 비용이 그것이다.
  - (c) 이미 서명된 릴리스로의 되돌리기는 CI 없이 된다.
- `artifacts_from: server-build` 는 사람이 PR 로 바꾸는 되돌리기 스위치로만 남는다. 그 경우 ADR-0308 이 모든 작업을 비운다.

### 3. 범위 밖 커밋은 빌드도 배포도 하지 않는다

범위의 정본은 새로 만들지 않는다. `tools/ci/change-scope.json` 에 `foundation-release-build.yml` 항목을 더한다(ADR-0202 의 틀).

- 입력: `platforms/foundation-platform/**` 와, 호스트의 제어 체크아웃이 릴리스를 승인·설치·실행하려고 실행하거나 읽는 저장소 루트
  파일이다. `scripts/deploy/**`, `scripts/github/**`, `scripts/ci/fetch_pinned.py`, `scripts/guard/migration-approval-gates.py`,
  `tools/*.json`, `tools/*.env`, `tools/github/**`, `tools/sigstore/**`, `tools/ci/migration-approval-gates.contract.json`,
  `rust-toolchain.toml` 이 여기에 든다.
  - Foundation 소스가 이름으로 읽는 파일도 넣는다. `change_scope.py check` 가 이것을 강제한다.
  - 계약·선택기 자체가 바뀌면 전부 돈다. 어느 영역도 주장하지 않는 경로가 바뀌어도 전부 돈다(기존 규칙).
- 워크플로: `scope` 잡이 `change_scope.py select --workflow foundation-release-build.yml` 을 돌린다.
  `Build and attest the release` 는 `affected == 'true'` 일 때만 돈다. push 는 `github.event.before..after` 로 판정한다.
  수동 실행(`workflow_dispatch`)은 언제나 빌드한다.
- 호스트: `foundation-autodeploy.sh` 가 같은 질문을 같은 함수(`change_scope.decide`)로 묻는다.
  - 대상은 돌고 있는 릴리스 다음부터 main 머리까지의 커밋이고, 제어 체크아웃의 계약과 미러의 diff 로 답한다.
  - 범위 밖이면 "이미 main 을 돌린다"로 본다. 배포하지 않고, Worker 배포만 따라잡게 한다.
  - 답을 못 하면(미러·계약 없음) 예전처럼 진행한다. 즉 "모르면 배포 쪽"이다.
- 받아들인 틈이 하나 있다. 범위 안 커밋의 릴리스 빌드가 실패한 뒤 범위 밖 커밋만 이어지면, 다음 범위 안 커밋까지 아무 커밋도 목록을
  갖지 않는다. 그 사이 배포는 일어나지 않는다. 빌드 실패는 이미 그 커밋의 check 실패로 보인다. 손으로 고치려면 main 에서 이 워크플로를
  수동 실행해 머리를 빌드한다.

### 4. publisher 이미지는 Rust 입력만으로 Rust 층을 짓는다

`services/foundation-outbox-publisher/Dockerfile.lakehouse-control` 은 `infra/` 전체 대신 `infra/lakehouse/contracts` 만 복사한다.
Rust 코드의 `include_str!` 가 컴파일해 넣는 `infra/` 경로가 이것뿐이다. 다른 `infra/` 경로를 컴파일해 넣는 코드가 생기면 CI 의
릴리스 빌드가 그 자리에서 컴파일 오류로 실패한다(조용히 틀리지 않는다).

## ADR-0134·ADR-0137 의 이유가 어떻게 지켜지나

| 원래 이유 | 지금 무엇이 지키나 |
|---|---|
| 0134 불변식 1: 설치 소스 = 독립적으로 가져온 정본 main 의 조상 커밋 트리, 파일 하나 더 있어도 거부 | 그대로. 소스 트리는 지금도 승인이 독립 fetch 한 Git 에서 `git archive` 로 쓴다. CI 는 실행물만 만든다 |
| 0134 불변식 2: 실행물은 같은 커밋에서 관리자 빌드가 만들고 sha256 이 봉인과 같다 | "관리자 빌드"는 "이 저장소의 정해진 워크플로가 main 의 그 커밋에서 만든 빌드"로 바뀐다. Sigstore 인증서의 워크플로 경로·ref·커밋 sha 를 오프라인으로 확인하고, 파일마다 sha256 을 서명된 대상과 대조한다. 봉인(`build.json`, ADR-0184 `chattr +i`)과 실행 전 재검사(`verify-current`)는 그대로다 |
| 0134 불변식 3·4: 설정은 릴리스 밖, 작업은 실행 직전에 다시 확인 | 바뀐 것 없음 |
| 0134 "호출자의 바이너리 + sha256 은 출처 증명이 아니다" | 서버는 호출자가 고른 값을 받지 않는다. 다이제스트와 신원은 서명에서 나오고, 신뢰 루트와 cosign 판은 PR 로만 바뀐다 |
| 0134 이 attestation 을 기각한 이유("공개 CI 정책이 `attestations:write`·`id-token:write` 를 금지") | `check-workflow-policy.sh` 가 이 워크플로의 서명 잡 하나에만 그 둘과 `packages: write` 를 허용한다. 저장된 비밀은 없다(ADR-0007 과 충돌 없음) |
| 0137: 4g 상한이 한 시간 동안 끝나지 않았다 → 측정에서 정한 상한 | 상한은 `tools/release-build.contract.json` 에 그대로 있고, CI 빌드가 같은 인자를 읽는다(ADR-0276 §11: 무료 러너 16 GB 에서 최대 8.2 GB) |
| 0137: 빌드는 호스트 메모리 예산의 일회성 작업이라 혼자 돈다 | 운영 경로에서는 호스트 빌드가 없다. 되돌리기 경로(`server-build`)에서는 ADR-0308 이 전부를 비운다. 예산 식 33.94 + 22 + 5 = 60.94g ≤ 62g 은 그대로 맞다 |
| 0137 §5: 빌드 의존성이 바뀌어 상한에 닿으면 다시 잰다 | CI 러너에서 같은 규칙이 적용된다. 넘으면 그 커밋의 빌드가 실패하고, 배포는 일어나지 않는다(2) |

## Consequences

- 웹·문서만 바꾼 커밋(#879 류)은 릴리스 빌드 잡이 `scope` 판정 수십 초로 끝난다. 서버는 배포하지 않는다.
- Spark 작업만 바꾼 커밋은 publisher 의 Rust 층을 캐시에서 쓴다. 다만 릴리스 자체는 새로 빌드되고 배포된다.
  Spark 작업은 릴리스 소스 트리의 일부이기 때문이다.
- 범위 계약이 하나 더 넓어졌다. 계약을 고치는 PR 은 `change_scope.py check` 가 지킨다. 범위를 너무 좁게 잡는 실수는
  "호스트가 새 제어 코드를 늦게 받는다"로 나타난다. 그래서 입력 목록은 넉넉하게(디렉터리 단위) 잡았다.
- ADR-0276 R4(서버 빌드 코드와 예산의 22g 항목 삭제)는 이 ADR 범위 밖이다. 되돌리기 스위치가 쓰는 코드이므로, 지우는 결정은 따로 한다.

## 기각한 대안

| 대안 | 기각 이유 |
|---|---|
| 워크플로 `paths:` 필터 | 정본이 둘이 된다. `change_scope.py check` 가 범위 워크플로의 `paths:` 를 거부한다(ADR-0202) |
| 범위 밖이면 빌드는 건너뛰되 서버는 직전 릴리스 산출물을 새 커밋 이름으로 재사용 | 서명 신원이 커밋 sha 에 묶여 있다(`--certificate-github-workflow-sha`). 다른 커밋의 서명으로 설치하면 ADR-0134 불변식 2 가 깨진다 |
| 목록이 없을 때 서버 빌드로 대신 | 2 의 (a)–(c). 사장 결정(ADR-0276 §7) |
| 직전 커밋 대신 "마지막으로 목록이 있는 커밋"과 비교해 빌드 판정 | CI 가 레지스트리를 읽어야 하고 경우가 늘어난다. 틈은 수동 실행 하나로 메워진다(3) |

## 개정 기록

- 2026-10-30: "받아들인 틈"(범위 안 커밋의 릴리스 빌드 실패 뒤 아무것도 배포되지 않음)은 이제 조용하지 않다. [ADR-0345](./0345-the-release-image-is-built-before-merge-and-a-host-behind-main-alerts-with-its-reason.md)의 `FoundationReleaseMissing`(90분)·`FoundationHostBehindMain`(6시간)이 이유와 함께 알린다. 2026-10-29 이 틈으로 11시간 아무것도 배포되지 않았다.
