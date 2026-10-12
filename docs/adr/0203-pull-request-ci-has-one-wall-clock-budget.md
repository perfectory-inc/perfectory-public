# ADR 0203: PR CI 벽시계 예산은 숫자 하나이고, 매주 실측과 비교한다

- Status: Proposed
- Date: 2026-10-13
- Related: [ADR-0149](./0149-local-hooks-are-a-fast-pre-filter-and-every-hook-step-runs-in-ci.md) (로컬 훅 예산), [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md) (CI 빌드 재사용·머지 큐)

## Context

PR 하나가 판정을 받기까지의 대기가 길어졌다. 2026-10-13 측정(`gh run list --event pull_request`,
최근 이틀): 같은 head SHA의 첫 워크플로 시작부터 마지막 워크플로 종료까지 p50 23.4분, p90 31.3분.
가장 느린 워크플로는 Foundation Platform CI(p50 24.5분)였다.

"CI를 빠르게"라는 목표는 문장으로만 있었고 숫자도, 측정 장치도 없었다. 숫자가 없으니 느려지는
변경이 들어와도 아무것도 붉어지지 않았고, 단축 작업의 효과도 말로만 보고됐다.

## Decision

1. **예산은 숫자 하나.** `tools/ci/ci-time-budget.contract.json` 의 `pull_request_wall_minutes`
   (현재 10)가 유일한 정본이다. 다른 파일·테스트·워크플로는 이 값을 다시 적지 않는다.
2. **측정 정의.** PR 시도 = `pull_request` 이벤트 실행 중 같은 `head_sha` 를 가진 것 전부. 벽시계 =
   max(`updated_at`) − min(`run_started_at`). 취소된 실행이 섞인 SHA는 새 푸시에 밀린 것이므로 뺀다.
   주는 UTC 월요일 0시에 시작하고, 판정 대상은 끝난 주 중 가장 최근 주다.
3. **판정.** 그 주의 p50이 예산을 넘으면 실패(종료 1), 측정된 시도가 없으면 판정 불가(종료 2).
   예산을 넘은 시도는 하나씩 `::warning::` 으로 남긴다.
4. **장치.** `scripts/ci/ci_time_budget.py`(표준 라이브러리만)가 측정·판정을 한다. 주간 워크플로
   `.github/workflows/ci-time-budget.yml`(schedule·workflow_dispatch 전용, `contents: read`)이 최근
   4주 추이를 실행 요약에 표로 쓰고 JSON을 아티팩트 `ci-time-trend` 로 올린 뒤 판정한다.
   보존 기간은 저장소 상한(`tools/github/artifact-retention.json`)을 따른다. 추이 자체는 실행 기록
   API에서 언제든 다시 계산되므로 아티팩트는 정본이 아니다.
5. **PR을 막지 않는다.** 예산 위반은 주간 실행을 붉게 만들 뿐 required 컨텍스트가 아니다. PR마다
   과거 실행을 판정하면 그 PR과 무관한 이유로 모든 PR이 막힌다. 대신 그룹화·통계·판정의 단위
   테스트는 `scripts/guard/ci-time-budget-self-test.sh` 로 모든 PR의 `required/repository` 에서 돈다.
6. **워크플로 정책의 새 부류.** `scripts/guard/check-workflow-policy.sh` 는 모든 워크플로에
   PR·merge_group 트리거와 required 컨텍스트 하나를 요구했다. 트리거가 `schedule` 과
   `workflow_dispatch` 뿐인 워크플로는 PR을 판정하지 않으므로 그 두 요구에서 빼고, 대신 required
   컨텍스트를 **가질 수 없게** 한다. 권한(`contents: read` 만)·러너·액션 핀·시크릿 금지 등 나머지
   규칙은 그대로 적용된다. 다른 트리거가 하나라도 붙으면 원래 규칙으로 돌아간다. 음성 픽스처는
   `scripts/guard/workflow-policy-self-test.sh` 에 있다.

## Alternatives rejected

- **`actions: read` 권한 부여.** 공개 저장소의 실행 기록 API는 추가 권한 없이 읽힌다. 권한 파서를
  부류별로 넓히는 것보다 `contents: read` 하나를 유지한다. 저장소를 비공개로 바꾸면 이 워크플로가
  403으로 붉어지며, 그때 권한 규칙을 함께 바꾼다.
- **기존 required 워크플로에 예약 잡 추가.** 그 워크플로들의 터미널 잡 규칙과 얽히고, 다른 변경과
  같은 파일을 다투게 된다.
- **워크플로마다 예산.** 작성자가 기다리는 것은 PR 전체의 벽시계다. 워크플로별 숫자는 필요해질 때
  추가한다(가장 느린 워크플로는 추이 표에 이미 나온다).

## Consequences

- 2026-10-13 기준 직전 4주 p50은 예산을 넘는다. 주간 실행은 단축 작업이 예산 안으로 들어올 때까지
  붉다 — 의도한 신호다.
- 예산을 바꾸는 것은 계약 파일 한 줄이며, 테스트는 그 값을 읽어 경계 양쪽을 검사한다.
