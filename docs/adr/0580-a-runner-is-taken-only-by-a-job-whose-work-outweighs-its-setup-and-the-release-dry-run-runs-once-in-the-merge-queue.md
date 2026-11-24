# ADR 0580: 러너는 준비보다 일이 큰 잡만 잡는다 — 작은 정책 검사는 영역의 Rust 잡 단계이고, 릴리스 빌드 시험은 머지 큐에서 한 번 돈다

- Status: Accepted
- Date: 2026-11-19 (3항은 2026-11-20)
- Builds on: [ADR-0324](./0324-a-ci-job-with-no-work-does-not-take-a-runner.md)(일 없는 잡은 러너를 잡지 않는다),
  [ADR-0199](./0199-verify-runs-side-suites-beside-cargo-and-pull-requests-leave-the-boundary-slice-to-the-merge-queue.md)(PR 은 경계 슬라이스를 큐에 맡긴다),
  [ADR-0420](./0420-ci-capacity-is-not-taken-from-the-production-host-and-a-self-hosted-runner-runs-only-trusted-events-on-a-dedicated-machine.md)(동시 상한 20, 유료·자체 러너 없음)
- Amends: [ADR-0345](./0345-the-release-image-is-built-before-merge-and-a-host-behind-main-alerts-with-its-reason.md) 의 "PR 과 머지 큐가 릴리스 빌드 시험을 돈다" 한 줄 — 이제 머지 큐만. 선택 규칙(릴리스 워크플로의 scope)은 그대로.
- Amends (3항): [ADR-0199](./0199-verify-runs-side-suites-beside-cargo-and-pull-requests-leave-the-boundary-slice-to-the-merge-queue.md) 의 "PR 은 경계 슬라이스만 큐에 맡긴다" — 이제 게이트 전부.
  [ADR-0324](./0324-a-ci-job-with-no-work-does-not-take-a-runner.md)·[ADR-0313](./0313-merge-queue-and-pull-requests-verify-only-what-changed-and-main-does-not-repeat-the-queue.md) 의 "Postgres 통합은 닿는 크레이트가 없을 때만 건너뛴다" — PR 에서는 언제나 건너뛴다.

## Context

소유자는 GitHub 요금제를 바꾸지 않는다(Free, 표준 러너 동시 20). 그러니 대기를 줄이는 길은 **러너를 잡는 잡의 수와 시간**뿐이다.

2026-11-18 22:17Z ~ 11-19 00:19Z 의 완료 실행 286개(작업 661개)를 쟀다(`gh api .../runs/{id}/jobs`, 읽기 전용).

| 지표 | 값 |
|---|---:|
| 작업 시간 비율 | 대기 42% · 준비 11% · 일 46% |
| 러너 대기 p50 / p90 | 42 s / 279 s |
| 러너를 잡은 작업 중 일이 60 s 미만인 것 | 501 / 661 |
| PR 시도 하나(같은 head) 러너·분 p50 | 18.0 |
| 머지 큐 항목 하나 러너·분 p50 / 작업 수 p50 | 92.8 / 32 |

할 일이 있는데도 러너 하나를 혼자 잡는 작은 잡이 영역마다 남아 있었다. 모두 그 영역의 Rust 잡과 **같은 준비**(체크아웃·고정 바이너리·Rust 설정·같은 의존성 캐시 읽기)를 되풀이했다.

| 워크플로 | 잡 | 일(평균) | 준비(평균) | 러너 대기 p50 |
|---|---|---:|---:|---:|
| identity-ci | Identity supply-chain policy | 4 s | 37 s | 154 s |
| identity-ci | Identity OpenAPI determinism | 8 s | 28 s | 161 s |
| dawneer-ci | Dawneer supply-chain policy | 4 s | 28 s | 156 s |
| intelligence-ci | Intelligence supply-chain policy | 6 s | 41 s | 45 s |
| CI (gongzzang) | cargo-deny | 5 s | 47 s | 42 s |
| CI (gongzzang) | Repo guardrails | 12 s | 40 s | 69 s |

이 여섯 잡은 창 안에서 러너를 62번 잡았다. 그 러너·분 48.8분 가운데 일은 7.0분이었고, 러너를 기다린 시간은 145.7분이었다.

릴리스 빌드 시험(`release-image`, 일 645~725 s)은 PR 에서 한 번, 머지 큐에서 다시 한 번 돌았다. 큐 실행이 PR 이 증명한 트리를 그대로 다시 돌린 것이 아니다. 머지 큐 head 트리가 그 워크플로의 초록 PR 실행 트리와 같았던 것은 큐 head 91개 중 2개였다(2026-11-13~19, `head_commit.tree_id` 대조). 그러니 판정은 큐의 것이 정본이고, PR 의 시험은 이른 알림일 뿐이다. 창 안에서 PR 시험 4번이 러너 33분을 썼다.

동시 실행 취소(`cancel-in-progress`)는 이미 모든 워크플로가 `pull_request` 에만 켜고 `merge_group`·main 에는 끈다. `foundation-release-build.yml` 은 main 푸시 전용이라 커밋마다 그룹을 따로 둔다(ADR-0276 §9). 바꿀 것이 없다.

### 큰 프로젝트는

| 프로젝트 | 작은 검사 |
|---|---|
| rust-lang CI | `citool` 이 잡 목록을 만들고, tidy·라이선스 같은 짧은 검사는 따로 러너를 받지 않고 `mingw-check`/`x86_64-gnu-llvm` 같은 큰 잡 안의 단계로 돈다 |
| Kubernetes prow | `verify-*` 들은 presubmit 하나(`pull-kubernetes-verify`)의 단계다 |
| Chromium | 정적 검사(`presubmit`)는 빌더 하나에서 묶어 돈다 |

공통점: **러너(기계) 하나의 고정 비용이 검사보다 크면 검사를 묶는다.**

## Decision

1. **준비가 일보다 큰 검사는 그 영역의 Rust 잡 단계다.** 영역의 Rust 잡이 이미 하는 준비 위에 단계로 둔다. 선택은 그대로다(영역이 바뀌면 돈다, ADR-0202).
   - identity: OpenAPI 결정성·cargo-deny → `rust-quality` 의 단계.
   - dawneer·intelligence: cargo-deny → `rust` 의 단계.
   - gongzzang: cargo-deny·repo-guard 셋·트래픽/인증 정책 drift → 잡 하나 `policy`. Rust 잡(임계경로, 일 약 200 s)은 늘리지 않는다.
   - 검사 내용·명령·required 컨텍스트 이름은 바뀌지 않는다. `required/*` 의 결과 검사는 남은 잡을 그대로 모은다.
2. **릴리스 빌드 시험은 머지 큐에서 돈다.** `foundation_ci_scope.py` 의 `RELEASE_EVENTS` 는 `merge_group`·`workflow_dispatch` 다. PR 은 ADR-0199 의 경계 슬라이스처럼 `release-image` 를 건너뛴다.
   - 큐 실행이 같은 선택(릴리스 워크플로의 scope)으로 고르므로, 시험이 빌드하지 않은 커밋은 여전히 main 에 들어가지 않는다.
   - 큐에서라면 골랐을 PR 에는 `::warning title=release-image deferred to the merge queue::` 를 남긴다.
   - 시험: `test_foundation_ci_scope.py` 가 PR·push·schedule 에서 고르지 않고 merge_group 에서 고르는 것을 고정한다.
3. **Foundation 의 PR 은 자기 실수를 싸게 잡는 잡만 돌린다(2026-11-20 추가).** PR 에 남는 것: Rust 잡(fmt·clippy·닿는
   크레이트의 단위 시험), Python·Node 묶음, 공급망 게이트, 저장소 가드, 비밀 검사. 큐에만 도는 것: 경로로 고르는 게이트
   여섯(경계 슬라이스·Kafka·Compose smoke·Spark 샤드·Windows 정적 릴리스 도구·Dagster 관찰), Postgres 통합 시험, 위 2 의
   릴리스 빌드 시험.
   - 근거(2026-11-12~19, Foundation CI 의 PR 실행 258개, 브랜치 152개, 같은 기간 병합 119건·큐 실행 325회 중 실패 23회):
     PR 실행 하나가 평균 러너 64.6분·시작 8.4번이었다(동시 상한 20). 무거운 잡이 혼자 잡은 실패 49건의 로그를 읽으니
     PR 탓은 12건이었다: Python·Node 8, Spark 2, Postgres 1, 릴리스 빌드 1. 나머지는 PR 과 무관했다(낡은 운영 모양 원장
     22, heapless 권고 8, Maven 6, 같은 PR 의 두 번째 실패 1).
   - "PR 탓" 의 기준: 실패한 단계가 시험 단계이고, 같은 시험이 같은 날 무관한 브랜치에서 실패하지 않았고, 로그에
     저장소·네트워크 해석 오류(`module not found`, `Could not resolve`, 권고 ID)가 없다. "혼자 잡았다" 는 같은 실행의
     Rust 잡이 통과했다는 뜻이다. 수는 `actions/runs`(사건 `pull_request`, 워크플로 `foundation-ci.yml`)의 작업 목록과
     실패한 작업의 로그에서 셌다.
   - 그래서 Python·Node 묶음은 PR 에 남긴다(12건 중 8건, 돌 때 러너 10분). 이 항이 옮기는 잡이 PR 에서 처음 잡던
     실수는 8일에 3건(Spark 2, Postgres 1)이고, 2항이 이미 옮긴 릴리스 빌드 시험이 1건이다. 이 4건은 이제 큐에서
     실패해 그 항목이 빠진다. 큐 실패율은 7.1% 에서 약 8.3%(27/325)로 오를 것으로 계산한다. 떨어진 항목 뒤의 그룹을
     다시 짓는 비용은 이 계산에 없다.
   - 기대: PR 실행당 러너 약 18분·시작 약 3.6번(같은 기간에 적용한 계산). 일주일 뒤 PR 실행당 러너분, 큐 실패율, 하루
     병합 수를 같은 방법으로 재서 여기에 적는다(2026-11-27, 이 변경이 병합된 뒤 7일 창).
   - 되돌리는 기준: 그 7일 창에서 "미룬 잡이 큐에서 실패했고 원인이 그 항목 자신의 변경" 인 경우가 큐 항목의 5% 를
     넘으면, 그런 실패가 가장 많은 잡 하나를 PR 로 되돌린다(`MERGE_QUEUE_ONLY`·`PULL_REQUEST_LEAVES` 에서 뺀다).
     Maven·권고·원장처럼 PR 과 무관한 실패는 세지 않는다: 잡을 PR 로 되돌려도 고쳐지지 않는다.
   - 장치는 새로 만들지 않는다. `foundation_ci_scope.py` 의 `MERGE_QUEUE_ONLY`(ADR-0199)가 게이트 전부가 되고,
     `PULL_REQUEST_LEAVES` 가 게이트가 아닌 Postgres 통합 잡을 PR 의 `skipped` 에 넣는다. `required/foundation` 은
     전처럼 건너뛴 잡을 통과로 본다. 큐에서라면 돌았을 잡마다 scope 잡이 PR 에 `deferred to the merge queue` 경고를
     남긴다(게이트 잡은 PR 에서 시작하지 않으므로 자기 경고를 낼 수 없다). 바뀐 경로를 비교할 수 없을 때는 게이트
     전부를 고른 것으로 보고, PR 은 그때도 전부 큐에 맡긴다.
   - 다른 영역은 바꾸지 않는다. 같은 8일에 여덟 워크플로를 합쳐 러너 73시간(Foundation 은 278시간)이고 잡 하나가
     1~5분이다.
4. **유지하는 것.** required 검사 12개, 보안 게이트(cargo-deny·SBOM·secret-scan·저장소 가드), 유료·자체 러너 금지(ADR-0420)는 그대로다. 새 가드는 없다. 기존 `check-workflow-policy.sh`·`change_scope.py check` 가 고친 워크플로를 그대로 검사한다.

## Consequences

- **PR 의 초록은 Foundation 의 통합 게이트가 통과했다는 뜻이 아니다(3항).** Spark·Kafka·Compose·Postgres·Windows 도구·
  Dagster 관찰은 큐에서 처음 돈다. 거기서 실패하면 그 항목이 빠지고(ALLGREEN), 작성자가 고쳐 다시 건다. 동시 빌드가 2 라
  바로 뒤 항목의 그룹도 다시 지어진다. PR 페이지의 경고가 무엇이 큐로 갔는지 알려 준다.

- 영역이 바뀐 이벤트 하나에서 러너를 잡는 잡이 최대 6개 준다(identity 2·dawneer 1·intelligence 1·gongzzang 2). 기준 창에 넣어 계산하면 러너 배정 62번(작업의 9.4%), 준비·정리 러너·분 약 42분이 사라진다. 일 7분은 남은 잡으로 옮겨 간다.
- PR 시도가 쓰는 릴리스 시험 러너·분이 0 이 된다(창 안 33분). 대가: 릴리스 이미지를 깨는 PR 은 큐에서야 실패하고 ALLGREEN 에 따라 큐에서 빠진다. 2026-10-29 #939 같은 사고는 여전히 main 전에 잡힌다.
- identity `rust-quality` 는 OpenAPI 내보내기와 cargo-deny 만큼(측정 약 12 s + 컴파일) 길어진다. gongzzang `policy` 는 cargo-deny·repo-guard 만큼(약 17 s) 길어진다. 둘 다 그 워크플로의 임계경로가 아니거나 임계경로의 10% 미만이다.
- 측정: 같은 스크립트와 같은 길이의 창으로 PR 본문에 전후를 적는다.
- 검토했고 하지 않은 것: 워크플로마다 하나씩 도는 `Change scope` 잡을 커밋당 하나로 합치는 일. 큐 커밋 하나에 8.6번 시작하고
  한 번이 8~27초라, 합치면 러너 시작 7.6번과 러너 약 1.5분(중앙값 53분의 3%)이 준다(2026-11-18 20:00Z~11-19 04:20Z,
  큐 커밋 16개). 대신 영역 워크플로 아홉 개를 한 워크플로가 부르는 재사용 워크플로로 다시 써야 한다. scope 잡이 30초를
  넘거나 워크플로 수가 늘면 다시 잰다.

## Revision 2026-11-25: 릴리스 빌드 시험은 머지 큐에서도 돌지 않는다 (소유자 승인, W95c)

- **무엇.** `foundation_ci_scope.py` 의 `RELEASE_EVENTS` 는 `workflow_dispatch` 하나다. 머지 큐(`merge_group`)는 `release-image` 를
  건너뛰고(scope 의 `skipped`), PR 에 "큐로 미룬다" 경고도 남기지 않는다. 잡과 `required/foundation` 의 자리는 그대로다: 손으로
  시작한 Foundation CI 실행만 그것을 고르고, 그때는 결과가 필수 검사에 든다. main 의 push 와 야간 실행은 전처럼 릴리스 워크플로
  (`foundation-release-build.yml`)가 진짜로 짓는다.
- **왜(측정: 2026-11-24 01:10~16:15Z 의 초록 큐 실행 25개, Foundation Platform CI).** 릴리스 시험은 중앙값 1,053 s, 느린 여섯은
  2,340~3,312 s 였고, 25개 중 11개에서 실행의 끝을 정했다. 가장 최근 셋은 실행 벽시계 2,700~3,406 s 중 2,666~3,312 s 가 이
  잡이었다(50분 실행의 49분). 같은 실행에서 이 잡만 빼고 다시 센 벽시계(계산): p50 1,460 → 1,450 s, p75 2,039 → 1,657 s,
  p90 3,162 → 2,027 s, 최대 3,793 → 2,300 s, 합 45,676 → 32,631 s(−29%). 가운데는 그대로이고 꼬리가 준다: 다음으로 긴 잡은
  경계 슬라이스(선택될 때 약 1,400 s, 발행기를 컨테이너 안에서 처음부터 컴파일)와 Postgres 통합 시험(460~1,690 s)이다. 큐는 한
  번에 둘만 지었으므로(ADR-0421 §3) 느린 항목 하나가 뒤의 모든 항목을 그만큼 늦췄다.
- **대가.** 릴리스를 깨는 변경(2026-10-29 #939 꼴)은 이제 main 에 들어간 뒤 main 의 릴리스 빌드에서 실패한다. 그 커밋부터 릴리스가
  없어 호스트는 앞 릴리스에 머물고 `FoundationReleaseMissing`(ADR-0345 §4, 90분)이 울린다. 큐에서 PR 하나가 빠지는 대신 main 의
  다음 PR 이 고친다. 릴리스 입력(Dockerfile·`tools/release-build.contract.json`·릴리스 스크립트)을 바꾸는 PR 은 병합 전에 자기
  가지에서 Foundation CI 를 손으로 시작해(`gh workflow run foundation-ci.yml --ref <가지>`) 같은 건조 실행을 돌릴 수 있다.
- **시험.** `test_foundation_ci_scope.py` 가 merge_group·pull_request·push·schedule 에서 고르지 않고 workflow_dispatch 에서만
  고르는 것, PR 에 릴리스 경고가 없는 것을 고정한다. 릴리스 범위의 경로가 Foundation CI 를 시작해야 한다던 시험은 이유가 사라져
  지웠다: 손으로 시작한 실행은 경로로 거르지 않는다.
- 같은 날 merge queue 의 `max_entries_to_build` 를 2 에서 5 로 올렸다: [ADR-0421](./0421-a-run-that-failed-only-because-its-runner-did-is-re-run-once-and-the-merge-queue-keeps-building-two.md) Revision 2026-11-25.
