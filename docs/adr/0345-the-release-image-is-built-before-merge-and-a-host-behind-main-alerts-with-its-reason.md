# ADR 0345: 릴리스 이미지는 병합 전에 지어 보고, main 에 뒤처진 호스트는 이유와 함께 경보한다

- Status: Accepted
- Date: 2026-10-30
- Builds on: [ADR-0276](./0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md)(CI 가 릴리스를 짓고 서명),
  [ADR-0309](./0309-ci-builds-the-release-once-and-only-for-a-commit-in-its-scope-the-host-only-pulls-it.md)(릴리스 범위),
  [ADR-0313](./0313-merge-queue-and-pull-requests-verify-only-what-changed-and-main-does-not-repeat-the-queue.md)·[ADR-0324](./0324-a-ci-job-with-no-work-does-not-take-a-runner.md)(바뀐 것만 검증, 일 없는 잡은 러너를 안 잡음),
  [ADR-0321](./0321-a-gold-commit-records-the-code-that-built-it.md) §5(배포 지표·경보),
  [ADR-0001](./0001-monorepo-governance-and-conventions.md)(가드는 실제 사고를 이름으로 댄다)

## Context — 11시간 동안 아무것도 운영에 닿지 않았고 아무도 몰랐다

2026-10-29 06:59Z 의 #939 부터 main 의 `foundation-release-build.yml` 이 전부 실패했다.
`foundation-publisher-core` 는 `contracts/schema-registry/registry.json` 을 `include_str!` 로 컴파일해 넣는데
(ADR-0336), `services/foundation-outbox-publisher/Dockerfile.lakehouse-control` 은 `contracts/` 를 이미지에
복사하지 않았다. 릴리스가 없으니 자동 배포는 매분 "no commit … has a published release yet" 만 남겼고, 호스트는
약 11시간 a879a889 에 머물렀다. 보안·신뢰성 수정을 포함한 병합 PR 약 25개가 운영에 닿지 않았다. 사람이 손으로
확인할 때까지 아무도 몰랐다. 고침은 #961(COPY 추가)이다. 이 ADR 은 같은 일이 다시 조용히 지나가지 않게 하는
두 구멍을 닫는다.

1. **PR CI 는 릴리스 이미지를 짓지 않는다.** ADR-0276 §9 이후 릴리스 빌드는 main 에서만 돈다. PR 의 Rust 검증은
   체크아웃에서 컴파일하므로 파일이 늘 있다. Dockerfile 의 COPY 와 `.dockerignore`(`**/tests` 제외) 가 만드는
   이미지 안에서만 빠진다. `scripts/guard/build-coupling-baseline.sh` 는 컴파일 시점 읽기(147곳)를 **세기만**
   했다.
2. **main 이 앞서가도 호스트가 따라오지 못하는 것을 알리는 경보가 없다.** ADR-0321 §5 의 `FoundationDeploy*` 는
   배포가 **도는 동안**의 지표뿐이다. 배포가 아예 시작되지 않으면 지표도 없다. ADR-0309 가 "받아들인 틈"으로
   적은 경우(범위 안 커밋의 릴리스 빌드 실패)도 같은 침묵이다.

실측(읽기 전용, 2026-10-29~30):

| 대상 | 값 |
|---|---|
| main 릴리스 빌드, 실제로 지은 140회(10-18~10-29, `gh run list`) | p50 15.4분, p90 39.5분, p99 59.9분, 최장 61.9분 |
| 호스트가 main 에 뒤처진 시간, 배포 288회(10-07~10-29, 호스트 `autodeploy/deployed/*` × main 병합 시각) | p50 1.1시간, p90 3.4시간, p95 4.2시간, p99 7.5시간, 최장 15.0시간(10-23~24 bake 가 배포를 잡음) |
| 배포 하나(ADR-0321 §5, 10-27 9회) | 11.7–14.9분 |
| Foundation CI PR 실행(10-22 이후 245회) | p50 16.7분, p90 25.7분 |

대기업과 오픈소스가 하는 방식:

- **이미지 빌드 입력은 선언된 집합이다.** Bazel·Buck2 는 샌드박스 안에서 선언한 입력만 보이게 해서, 선언 안 한
  파일을 읽는 빌드는 개발자 기계에서도 실패한다. Docker 빌드 컨텍스트(COPY + `.dockerignore`)는 같은 성질의
  "선언된 입력"인데 우리 PR 은 그 샌드박스를 거치지 않았다. 가드는 샌드박스가 할 판정을 정적으로 미리 한다.
- **배포 산출물은 병합 전에 지어 본다.** Google TAP·Uber SubmitQueue·GitHub 머지 큐는 main 에 들어갈 트리로
  비싼 빌드를 병합 **전에** 돌린다. 병합 뒤 빌드(postsubmit)는 깨진 main 을 사후에 알 뿐이다.
- **원하는 상태와 실제 상태의 차이를 시간으로 경보한다.** Argo CD 는 `argocd_app_info{sync_status="OutOfSync"}`
  가 오래 지속되면 경보하라고 권하고, Flux 는 `gotk_reconcile_condition{status="False"}` 의 지속 시간으로 경보한다.
  둘 다 "왜"(조건의 reason)를 레이블로 싣는다.

## Decision

1. **이미지는 자기가 컴파일하는 파일을 가진다(정적 가드).** `scripts/guard/build-coupling-baseline.sh` 가 세던 같은
   읽기를 `build-coupling-baseline.py` 가 이미지 안까지 따라간다. Rust 를 `cargo build` 하는 모든 추적 Dockerfile
   (지금: lakehouse-control, foundation-api, identity-api, identity-policy-worker)에 대해
   - 빌드 컨텍스트는 추적된 compose 파일의 `build:` 가 정한다(없으면 실패 — 컨텍스트를 모르는 Rust 이미지는 없다);
   - 컴파일되는 crate 는 `-p` 패키지와 그 normal·build 경로 의존성 전부이며, Cargo 의 기능 합치기를 따른다
     (dev-dependency 만 켜는 `test-fixtures` 는 릴리스 빌드에 없다);
   - 컴파일되는 소스는 각 crate 의 타깃과 build 스크립트에서 시작하는 모듈 트리이고, 릴리스 빌드에서 거짓인
     `#[cfg(...)]` 의 `mod`(`test`, 아무도 켜지 않은 기능)는 뺀다;
   - 그 소스의 `include_str!`·`include_bytes!`·`sqlx::migrate!` 경로는 rustc 처럼 풀어서, 빌드 단계(와 그 FROM
     단계)의 COPY 가 `<WORKDIR>/<경로>` 에 놓고 `.dockerignore` 가 빼지 않은 파일이어야 한다.
   풀 수 없는 읽기(문자열 리터럴이 아님)는 통과가 아니라 실패다. 가드 범위(`tools/ci/change-scope.json`
   `guards`)는 `**/*.rs` 에 `Cargo.toml`·Dockerfile·`.dockerignore`·compose 를 더한다. #961 이전 트리에서 이
   가드는 정확히 그 한 줄(`schema_registry.rs:16` → `contracts/schema-registry/registry.json`)로 실패한다.
2. **릴리스 빌드를 병합 전에 건조 실행한다.** Foundation CI 에 `release-image` 잡을 둔다. main 과 같은
   `scripts/deploy/foundation-release-build.py` 로 모든 이미지·실행 파일·jar 를 짓고, 서명(attestation)·push·
   업로드는 하지 않는다. 잡에는 쓰기 권한이 없고, BuildKit 캐시는 main 의 것을 읽기만 한다
   (`--cache-read-only`): PR 이 지은 층이 main 의 서명된 릴리스 재료가 되면 안 된다.
   - **언제:** 병합하면 main 이 릴리스 빌드를 할 변경일 때만 — 릴리스 워크플로 자신의 범위(ADR-0309)를 같은
     변경에 묻는다(`foundation_ci_scope.py` `RELEASE_JOB`, ADR-0324 의 `skipped` 목록). PR 과 머지 큐에서 돈다.
     main 푸시와 야간 실행은 진짜 릴리스 워크플로에 맡긴다. 웹만 바꾸는 PR 은 러너를 잡지 않는다.
   - `required/foundation` 이 이 잡을 요구하므로, 실패하면 머지 큐가 main 으로 넘기지 않는다.
   - Foundation CI 범위는 릴리스 범위를 포함한다(`tools/*.json`·`tools/*.env`·`tools/sigstore/**`·
     `scripts/ci/fetch_pinned.py` 를 더함). 시험이 포함 관계를 지킨다.
   - 표준 러너, 무료 캐시. 공개 저장소라 비용 0.
3. **자동 배포가 매 틱 판단을 지표로 남긴다.** `foundation-autodeploy.sh` 는 잠금을 잡은 틱마다
   `/var/lib/perfectory/metrics/autodeploy.prom` 을 통째로 바꿔 쓴다(node-exporter textfile, ADR-0321 §5 와 같은
   길). 이미 로그에 쓰던 이유가 지표가 된다.
   - `foundation_autodeploy_tick_timestamp_seconds{reason}` — 마지막 판단 시각과, 호스트가 main 에 있지 않은 이유
     (`no_release`, `checks_pending`, `checks_failed`, `deploy_failed`, `every_newer_commit_refused_or_failed`,
     `deploying`, `quiet_window`, `operator_unit`, `worker_deploy`, `off`, `*_unreadable`; main 에 있으면 빈 값).
   - `foundation_autodeploy_behind_since_timestamp_seconds{reason,commit}` — 호스트가 돌리지 않는 릴리스 범위 안
     커밋 중 가장 오래된 것이 main 에 들어온 시각.
   - `foundation_autodeploy_release_missing_since_timestamp_seconds{commit}` — 가장 새 릴리스보다 새로운 범위 안
     커밋(거부·실패한 것 제외) 중 가장 오래된 것이 main 에 들어온 시각.
   커밋마다 범위를 묻는 것은 같은 `change_scope.decide` 다. 그래서 웹만 바꾼 머리는 "릴리스 없음"이 되지 않는다.
   시각은 머지 커밋의 committer 시각(머지 큐가 씀)이라 상태 파일이 필요 없다. 배포가 성공하면 그 커밋까지의
   시계는 사라진다.
4. **경보 셋, 기준은 실측에서.** `infra/observability/prometheus/foundation-api.rules.yml`:
   - `FoundationReleaseMissing`: 90분(N). 릴리스 빌드 최장 62분, p99 60분의 1.5배. 넘으면 그 커밋의 빌드는
     실패했거나 돌지 않았다. 요약은 "main 의 <sha> 부터 릴리스가 없다"고 말한다. 이번 사고라면 08:29Z 에 울렸다
     (사람이 안 것은 약 11시간 뒤).
   - `FoundationHostBehindMain`: 6시간(M). 뒤처짐 p95 4.2시간 위, 사고의 11시간 아래. p99 7.5시간 이상의 긴
     뒤처짐은 bake 가 배포를 잡은 경우였고, 그것도 알아야 할 일이다. 요약은 `reason` 을 그대로 싣는다 —
     "릴리스 없음"과 "배포 실패"가 다르게 보인다.
   - `FoundationAutodeployStopped`: 틱 지표가 30분 넘게 낡았거나 없으면(배포 중은 제외, `FoundationDeployLong`
     이 맡는다). 위 둘이 울릴 수 없는 상태를 덮는다.
   런북: `platforms/foundation-platform/docs/runbooks/deploy-alerts.md`.

## Consequences

- #939 같은 변경은 PR 에서 가드 하나로 몇 초 만에, 그리고 건조 실행으로 다시 막힌다. 두 PR 이 각각은 멀쩡하고
  합쳐서 깨지는 경우는 머지 큐의 건조 실행이 막는다. #961 이전 트리(7ef5da4c)에서 가드는 실패하고, 이후 트리에서
  통과한다.
- 비용: 릴리스 범위 안 PR 마다 러너 하나가 약 15분(캐시 적중; 최장 약 60분) 더 돈다. 다른 Foundation 잡과
  병렬이라 PR 벽시계는 대개 그대로지만(이미 p50 16.7분), 무료 플랜의 동시 러너를 하나 더 쓴다 — 러너 대기가
  늘 수 있다. 측정해서 넘치면 PR 은 머지 큐에만 맡기는 ADR-0199 방식으로 좁힌다(새 ADR).
- 가드의 한계(모두 실패 쪽으로 기운다): `mod` 가 아닌 항목의 `#[cfg(test)]` 는 컴파일되는 것으로 본다; 인라인
  모듈 안의 파일 모듈(`mod a { mod b; }`)은 따라가지 않고 실패한다; `cfg` 의 기능 외 술어(target 등)는 참으로
  본다. 가드는 Rust 를 파싱하지 않고 텍스트로 읽는다(세기와 같은 결정).
- 경보의 한계: `off`·`main_unreadable` 틱은 main 의 커밋 목록을 읽기 전에 끝나므로 뒤처짐 시계를 남기지 않는다.
  미러를 못 읽는 틱도 목록이 없다. 그때는 틱의 `reason` 만 보인다. 새 스크립트와 규칙은 다음 배포로 호스트에
  간다 — 지금처럼 배포가 막힌 동안에는 이 경보도 배포되지 않는다(#961 이 먼저다).
