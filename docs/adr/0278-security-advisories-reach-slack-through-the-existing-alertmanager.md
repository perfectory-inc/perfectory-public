# ADR 0278: 저장소의 보안 권고는 이미 있는 Alertmanager 로 Slack 에 간다

- Status: Accepted
- Date: 2026-10-18
- Related: [ADR-0208](./0208-the-data-flow-maps-problems-reach-slack-through-the-existing-alertmanager.md) (같은 경로),
  [ADR-0209](./0209-the-data-flow-map-shows-the-edge-path-from-korea-and-the-ci-time-trend.md) (service checks),
  [ADR-0271](./0271-dependency-updates-arrive-as-renovate-pull-requests-and-main-keeps-one-advisory-issue.md) (Cargo 권고 이슈),
  [ADR-0158](./0158-npm-overrides-have-one-source-and-a-new-advisory-arrives-as-a-prepared-change.md) (OSV 래칫)

## Context

소유자는 저장소의 보안 권고를 이미 쓰는 Slack `#alerts` 에서 받고 싶어 한다. 2026-10-18 실측:

| 사실 | 값 |
|---|---|
| Dependabot 알림 | 켜짐(`vulnerability-alerts` 204), 열린 알림 0. 보안 갱신 PR 은 꺼짐(ADR-0271 이 Renovate 로) |
| OSV 래칫 | `monorepo-guard.yml` 이 main 에서 매일 21:17Z 에 돈다. 10-16 실패, 10-17 성공 |
| Cargo 권고 | `dependency-advisories.yml` 이 매일 21:41Z 에 돈다. 실패하면 이슈 하나(ADR-0271) |
| GitHub 이 실패를 알리는 사람 | 마지막으로 cron 을 고친 사람뿐(ADR-0271 기각 표) |
| 서버의 GitHub 자격 | 없음. 계약의 유일한 GitHub 토큰(shape ledger, 쓰기)도 서버에 없다("no refresh token on this host") |
| 익명으로 읽히는 것 | 실행·잡 목록, check-run 주석, 공개 이슈. 아티팩트 내려받기와 Dependabot 알림은 인증이 필요하다 |
| Slack 으로 가는 길 | Prometheus 가 foundation-api `/metrics` 만 긁고, Alertmanager 가 봇 토큰으로 `#alerts` 에 보낸다(ADR-0208) |

## Decision

1. **발신은 이미 있는 Alertmanager 다.** 두 번째 발신자도 새 Slack 비밀도 없다. foundation-api `/metrics` 가 시계열을 내고,
   규칙 셋과 경로 하나(`service="security"`)를 더한다. 같은 수신자로 간다.
2. **읽는 것은 이미 매시 GitHub 을 읽는 service checks 다**(`scripts/ops/service-checks.py`, ADR-0209). 그 파일의
   `security` 부분에 출처마다 `readable`, 고정된 `reason`(`no-token`·`github-error`·`too-many`), 발견 목록을 쓴다.
   - `dependabot`: 열린 알림. 권고(GHSA)와 패키지마다 하나, 링크는 번호가 가장 작은 알림. 토큰이 있을 때만.
   - `osv-ratchet`, `cargo-advisories`: main 의 최신 완료 예약 실행에서, 실패한 잡의 `advisory` 주석
     (`<권고 ID> <패키지>`). 언제나 익명으로 읽는다. 이미 읽은 실행은 다시 읽지 않는다. 익명 예산이 자동배포 몫
     (API_RESERVE)에 닿으면 지난 시간의 답을 쓴다. 링크는 실행이 아니라 워크플로 페이지다. 실행 링크는 매일 바뀌어
     같은 권고를 다시 알리게 된다.
3. **래칫이 권고를 이름으로 남긴다.** `check-osv-vulnerability-baseline.py` 는 기준 목록보다 많아진 발견마다,
   `cargo-advisories.sh check` 는 거부한 워크스페이스의 RUSTSEC/GHSA ID 와 yank 마다, GitHub Actions 안에서
   `::error title=advisory::<ID> <패키지>` 를 하나 남긴다. 기준 목록이 낡은 것(stale baseline)은 노출이 아니므로 남기지 않는다.
   두 자기시험이 한 줄이 정확히 나오는지, GitHub 밖에서는 나오지 않는지 본다.
4. **"바뀔 때만"은 레이블의 안정성으로 보장한다**(ADR-0208 §2). 같은 권고인 동안 레이블이 같다.
   `foundation_security_advisory{source,advisory,package,advisory_severity,link} 1`.
   권고의 심각도는 `advisory_severity` 다. 규칙의 `severity` 레이블이 경로를 정하므로 이름을 나눈다.
   **못 읽은 출처는 마지막으로 알던 발견을 지킨다.** 권고가 해소되는 것은 읽어 보니 없어졌을 때뿐이다.
   그래서 GitHub 이 한 시간 거부해도 "해소 → 새 권고"가 오지 않는다. 출처마다
   `foundation_security_advisories_readable{source,reason}` 를 내고, 파일이 3시간 넘게 낡았으면 전부 `reason="stale"` 로 0 이다.
   토큰 만료가 알려져 있으면 `foundation_security_token_expires_days` 를 낸다.
5. **규칙**:
   - `FoundationSecurityAdvisory`: `for: 1m`, `keep_firing_for: 15m`.
   - `FoundationSecurityAdvisoriesUnreadable`: 출처 하나가 0 이거나 계열이 없음, 6시간.
   - `FoundationSecurityTokenExpiring`: 14일 미만.
   **경로**: `service="security"` 를 alertname·source·advisory·package 로 묶고 `repeat_interval: 8760h`.
6. **토큰은 하나, 읽기 하나**: fine-grained, 이 저장소만, Repository permissions 의 **Dependabot alerts: Read-only** 하나.
   runtime-secrets 계약의 선택 그룹 `security-alerts`(`/etc/foundation-platform/security-alerts.env`, root:foundation-platform 0640)이다.
   `foundation-release.sh env-set` 으로만 쓴다(ADR-0247). 토큰이 없으면 Dependabot 출처가 `no-token` 이라고 말하고, 두 래칫은 계속 알린다.

## Rejected

| 대안 | 판정 |
|---|---|
| GitHub Actions 에서 Slack 으로 직접 보내기 | 두 번째 발신자다. 공개 저장소의 워크플로에 Slack 비밀을 둬야 한다(ADR-0158 C 와 같은 이유) |
| 서버에서 osv-scanner·cargo-deny 를 다시 돌리기 | 판정이 두 벌이 되고 기준 목록도 두 곳이 된다 |
| 아티팩트 `osv-report.json` 받기 | 토큰에 Actions 읽기가 더 필요하다. 주석은 익명으로 읽힌다 |
| 새 systemd 타이머·작업 | service checks 가 이미 매시 같은 GitHub API 를 같은 예산 규칙으로 읽는다 |
| 못 읽으면 발견을 비우기(ADR-0208 의 "모르면 시계열 없음") | 권고는 시간 단위로만 읽어서, 한 번 못 읽으면 해소와 재발 메시지가 둘 간다. 권고는 "모르면 마지막으로 안 것"이 맞다 |
| 비밀 스캔(secret scanning) 알림도 읽기 | 이번 범위 밖. 토큰 권한이 하나 늘어난다. 켜져 있고 push protection 도 켜져 있다 |

## Consequences

- 새 권고가 main 에 오면, 다음 예약 래칫 실행 또는 Dependabot 알림 뒤 한 시간 안에 `#alerts` 에 한 줄이 간다.
- 익명 예산을 시간당 최대 6회씩 두 번 더 쓴다. 새 실행이 없으면 시간당 2회다.
- 소유자 손일은 토큰 하나뿐이다. 토큰을 넣는 법, 확인하는 법, 전달을 시험하는 법은
  [런북](../../platforms/foundation-platform/docs/runbooks/security-advisories.md)에 있다.
- 규칙은 저장소에서 문자열로만 검사한다(`deploy_contract.rs`). 2026-10-18 서버에서 `promtool test rules` 로
  열림, 해소, 6시간 못 읽음, 만료 임박을 시험했다. `keep_firing_for` 를 3h 로 바꾸면 그 시험이 실패한다.
