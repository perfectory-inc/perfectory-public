# ADR 0388: 보안 게이트의 거부는 매일 run 만이 아니라 그것을 처음 본 run 에서 알린다

- Status: Accepted
- Date: 2026-11-04
- Amends: [ADR-0278](./0278-security-advisories-reach-slack-through-the-existing-alertmanager.md)(래칫 출처는 "main 의 매일 run" 하나만 읽었다)
- Builds on: [ADR-0302](./0302-security-dependency-updates-automerge-and-a-failing-security-gate-must-alert.md) §3(실패한 보안 게이트는 반드시 알린다), [ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md) C(무음 실패 금지)
- 관련 사건: 2026-10-30 npm `glob`·`minimatch` 권고(GHSA-3g3m-2fjr-6pw4·GHSA-g3hv-75wg-m249)가 PR·main push 를 빨갛게 했는데 Slack 경보는 한 번도 오지 않았다(#987 로 고침).

## Context

`scripts/ops/service-checks.py` 는 매시 41분 `osv-ratchet`(`monorepo-guard.yml`)·`cargo-advisories`(`dependency-advisories.yml`)
의 **main 의 마지막 scheduled run** 하나만 읽었다(`?branch=main&event=schedule`). 같은 OSV 래칫은 push·PR·merge queue 에서도
돌고 같은 `advisory` 주석을 남기는데, 그 run 들은 경보가 보지 않았다.

실물(읽기 전용 `gh run list --workflow monorepo-guard.yml`, 10-29~11-02 의 274 run, 실패 run 의 주석을 하나씩 읽음):

| 시각(UTC) | 무엇 |
| --- | --- |
| 10-30 23:09 | 두 권고 공개(GitHub advisory `published_at`) |
| 10-30 23:16 | PR run(`fix/land-ownership-one-release-commit`)이 두 권고로 거부 — 처음 본 run |
| 10-30 23:24 | main push run(`afcfa5dd`)이 같은 두 권고로 거부 |
| 10-31 00:24 | #987 병합(고침) |
| 10-31 00:48 | 다음 scheduled run — 이미 고쳐져 녹색 |

경보는 scheduled run 만 봤으므로 이 사고를 **끝까지 몰랐다**. 고침이 사람 눈으로 먼저 왔기에 피해가 없었을 뿐이다.
고침이 없었다면 첫 경보는 00:48 run 이 끝난 뒤의 01:41 점검 = 공개 뒤 **2시간 32분**이고, 권고가 scheduled run 직후에
공개되면 하루 한 번 run(cron 21:17 이 실제로는 00:08~01:02 에 뜸)을 기다려 **최대 약 25시간**이다(계산).
Dependabot 알림도 빠르지 않았다: 같은 권고의 알림 #99·#100 은 10-31 09:52 에 생겼다(공개 뒤 10시간 43분, 실측).

PR run 의 거부를 그대로 main 의 것으로 셀 수는 없다. PR 이 자기 의존을 올려 취약한 버전을 들였다면 그 PR 만 빨갛고
main 은 멀쩡하다(그 PR 은 필수 검사라 병합되지 않는다). 반대로 lockfile 을 건드리지 않은 PR 의 거부는 main 의 lockfile 을
읽은 결과다.

비교: GitHub Dependabot 은 권고 DB 가 바뀌면 **기본 브랜치의 의존성 그래프**를 다시 판정해 알리고, PR 의 새 취약 의존은
dependency review(PR 검사)로 따로 막는다 — "main 이 노출됐나"와 "이 변경이 들였나"를 나눈다. Google OSV-Scanner 의
GitHub Action 도 같은 둘(scheduled/push 의 전체 스캔과 PR 의 "새로 들어온 것만" 비교 스캔)로 나뉜다. Renovate 는 권고를 받는
즉시(`vulnerabilityAlerts`, 일정 무시) 기본 브랜치 기준 PR 을 연다.

## Decision

1. **main 의 판정은 main 의 가장 새 완료 run 이다 — 이벤트를 가리지 않는다.** `schedule`·`push`·`workflow_dispatch`
   중 가장 새 run(성공·실패, 취소 제외)이 main 이 지금 어떤 권고에 노출됐는지 말한다. main 에 하루 수십 번 들어오는
   push run 이 매일 run 을 몇 시간 앞선다.
2. **실패했는데 권고를 하나도 말하지 않은 main run 은 판정이 아니다.** 래칫 앞 단계에서 죽은 run 은 권고가 없어졌다는
   뜻이 아니므로, 그 다음으로 새 run(최대 3개)을 본다. 셋 다 그렇다면 마지막으로 알던 것이 선다(`no-verdict`).
3. **main 의 판정보다 새 PR·merge queue 의 거부는, 그 변경이 lockfile(`Cargo.lock`·`pnpm-lock.yaml`)을 건드리지 않았을 때
   main 의 것으로 센다.** 변경은 PR 이면 runs API 의 `pull_requests`, merge group 이면 `gh-readonly-queue/<base>/pr-<n>-`
   의 n 으로 찾고 `pulls/<n>/files` 로 본다. 모르면(포크 PR, 파일 100개 이상) 변경 자신의 것으로 친다 — 오경보보다
   늦은 경보 쪽으로 기운다. main 에 더 새 녹색 run 이 오면 그 PR 근거는 지나간 것이 된다(새 증거가 이긴다).
4. **비용은 시간당 익명 예산 안에서.** run 목록 두 번(main 완료 10개, 실패 20개)과 새 run 하나에 최대 3요청(작업·주석·PR
   파일). 읽은 run 의 판정은 파일에 남겨(`verdicts`) 다시 읽지 않고, 예산(`API_RESERVE` 30 은 자동배포 몫)이 모자라면 남은
   run 은 다음 시간에 읽는다. PR 을 거는 게이트는 `monorepo-guard.yml` 하나(`GATES_CHANGES`); `dependency-advisories.yml` 은
   PR 에서 돌지 않으므로 실패 목록을 묻지 않는다.
5. 경보·지표·라벨은 그대로다(`foundation_security_advisory{source="osv-ratchet",…}`, 링크는 매일 같은 main 워크플로
   링크). 출처 이름도 같다. 그래서 foundation-api·Prometheus 규칙·Alertmanager 는 바뀌지 않는다.

장치: `scripts/ops/service-checks.py` `ratchet()`. 시험: `orchestration/tests/test_service_checks.py`
`SecurityGateBeyondTheDailyRun` — 합성 실패 사례(scheduled run 은 녹색, PR run 만 빨강 → 반드시 경보)와 lockfile 을
건드린 PR·포크 PR(경보 없음), main push, 새 녹색 run 의 대체, 래칫 전에 죽은 run, 예산 이월.

## Consequences

| | 전 | 후 |
| --- | --- | --- |
| 10-30 사고의 첫 경보 | 없음(scheduled run 은 고친 뒤라 녹색) | 23:41 점검(23:16 PR run 이 23:20 경 끝남) = 공개 뒤 32분 |
| 고침이 없었다면 | 10-31 01:41 = 공개 뒤 2시간 32분 | 23:41 = 32분 |
| 최악(권고가 매일 run 직후 공개) | 약 25시간 | 그 뒤 첫 push·lockfile 무관 PR run + 1시간 이내; 둘 다 없는 조용한 날은 여전히 매일 run |
| 시간당 GitHub 요청(래칫 둘) | 2~6 | 3(새 run 없을 때) ~ 예산 한도 |

(모두 실측 run 기록에서 계산. 점검이 매시 41분이라 경보는 run 끝 + 최대 1시간 + Alertmanager `for: 1m`.)

- 남는 틈: main 에 push 도, lockfile 을 안 건드린 PR 도 없는 날(11-01·11-02 가 그랬다)에는 여전히 매일 run 을 기다린다.
  그 틈을 닫는 것은 Dependabot 알림 출처(토큰)이지만 이번 사례에서 10시간 넘게 늦었다. host 에서 OSV 를 직접 돌리는 것은
  이 ADR 의 범위가 아니다.
- lockfile 을 건드린 PR 이 main 에도 있는 권고로 거부되면 그 PR 근거는 버려진다. 그래도 main push·매일 run 이 같은 권고를
  곧 본다.
- 이 규칙은 AGENTS.md `위생 불변식` 에 한 줄로 들어간다.
