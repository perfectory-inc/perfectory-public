---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-18
---

# 보안 권고 알림 (#alerts) — 토큰·확인·전달 시험 런북

[루트 ADR-0278](../../../../docs/adr/0278-security-advisories-reach-slack-through-the-existing-alertmanager.md)의 운영 절차다.
저장소의 열린 보안 권고가 이미 있는 Alertmanager 로 Slack `#alerts` 에 간다. 새 발신자도, 새 Slack 비밀도 없다.

| 무엇 | 정본 |
| --- | --- |
| 읽기 (매시 41분) | `scripts/ops/service-checks.py` 의 `security` 부분, `infra/systemd/foundation-service-checks.service` |
| 출처 | `dependabot`(열린 Dependabot 알림, 토큰 필요), `osv-ratchet`(main 의 매일 `monorepo-guard`), `cargo-advisories`(main 의 매일 `dependency-advisories`). 두 래칫은 거부한 권고를 `advisory` 주석으로 남기고, 그 주석을 익명으로 읽는다 |
| 지표 | foundation-api `/metrics`: `foundation_security_advisory`, `foundation_security_advisories_readable`, `foundation_security_token_expires_days` |
| 규칙 | `infra/observability/prometheus/foundation-api.rules.yml` 의 `FoundationSecurity*` 셋 |
| 경로 | `infra/observability/alertmanager/alertmanager.yml` 의 `service = "security"`: 열릴 때 한 번, 없어질 때 한 번 |
| 토큰 파일 | `config/runtime-secrets.contract.json` 의 그룹 `security-alerts` (`/etc/foundation-platform/security-alerts.env`, root:foundation-platform 0640, 선택) |

## 무엇이 언제 오나

| 알림 | 뜻 | 할 일 |
| --- | --- | --- |
| `FIRING FoundationSecurityAdvisory` | 새 권고가 열렸다. 출처·권고 ID·패키지·심각도·링크 한 줄 | 링크를 연다. Renovate 보안 PR(ADR-0271)이 이미 왔는지 보고, 없으면 고친다 |
| `RESOLVED FoundationSecurityAdvisory` | 읽어 보니 그 권고가 없어졌다(고침·해제·래칫 통과) | 없음 |
| `FoundationSecurityAdvisoriesUnreadable` | 한 출처를 6시간째 못 읽는다. 괄호 안이 까닭: `no-token`(토큰 없음), `github-error`(GitHub 이 거부·시간 초과), `stale`(service checks 가 3시간 넘게 안 돌았다), `too-many`(열린 알림 100개 이상) | 까닭대로. 못 읽는 동안에도 마지막으로 알던 권고는 열린 채로 남는다 |
| `FoundationSecurityTokenExpiring` | 토큰이 14일 안에 만료된다 | 아래 1절로 새 토큰을 넣는다 |

같은 권고가 열려 있는 동안 다시 오지 않는다. 알림 기록 보관(`--data.retention=120h`)이 지나면 열린 권고는 한 번 더 온다.

## 1. 토큰 두기 (소유자, 한 번, 이 PR 이 배포된 뒤)

`env-set` 은 배포된 릴리스의 계약을 읽으므로, `security-alerts` 그룹이 들어간 릴리스가 배포된 **뒤에** 한다.
토큰이 없어도 두 래칫 알림은 온다. Dependabot 부분만 6시간 뒤 `no-token` 으로 한 번 알린다.

**토큰 만들기 (브라우저).** GitHub → 오른쪽 위 사진 → Settings → Developer settings → Personal access tokens →
Fine-grained tokens → Generate new token.

| 칸 | 값 |
| --- | --- |
| Token name | `foundation-security-alerts` |
| Resource owner | `perfectory-inc` |
| Expiration | 1년 (만료 14일 전에 `#alerts` 가 알린다) |
| Repository access | Only select repositories → `perfectory-public` 하나 |
| Repository permissions | **Dependabot alerts: Read-only** 하나. 나머지는 모두 No access 로 둔다(Metadata: Read-only 는 GitHub 이 자동으로 붙인다) |
| Organization permissions | 모두 No access |

Generate token 을 누르고 값을 복사한다. 조직이 fine-grained 토큰 승인을 요구하면, 조직 Settings → Personal access tokens →
Pending requests 에서 승인한다.

**서버에 넣기.** 아래 명령에는 `$` 도, 따옴표 안의 큰따옴표도 없다. PowerShell 에서 그대로 쳐도 된다.

```text
ssh ai-server
umask 077
nano ~/security-alerts.set
```

nano 에서 한 줄을 쓴다: `FOUNDATION_PLATFORM_SECURITY_ALERTS_GITHUB_TOKEN=` 다음에 복사한 토큰을 붙여 넣는다(띄어쓰기·따옴표 없이).
Ctrl+O, Enter, Ctrl+X 로 저장하고 나온다. 그다음:

```text
sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh env-set security-alerts ~/security-alerts.set
shred -u ~/security-alerts.set
```

`env-set` 은 이름만 출력한다. 그룹이 담지 않는 이름은 거부한다. 값은 셸 기록에 남지 않는다.

## 2. 확인

다음 매시 41분 실행을 기다리거나, 지금 돌린다(sudo 비밀번호를 묻는다):

```text
sudo systemctl start foundation-service-checks.service
journalctl -u foundation-service-checks.service -n 20 --no-pager -o cat
```

`security dependabot: findings=<n> reason=None` 이 보여야 한다. `reason=no-token` 이면 토큰 파일이 없거나 이름이 틀렸다.
`osv-ratchet`·`cargo-advisories` 도 `reason=None` 이어야 한다.

Prometheus 가 읽는지:

```text
docker exec foundation-platform-runtime-prometheus-1 promtool query instant http://localhost:9090 foundation_security_advisories_readable
```

세 출처 모두 `=> 1` 이어야 한다.

## 3. #alerts 전달 확인 (설치 뒤 한 번)

지금 열린 권고가 없으면 알림이 오지 않으므로, 시험 알림 하나를 Alertmanager 에 직접 넣어 같은 경로로 보낸다:

```text
docker exec foundation-platform-runtime-alertmanager-1 amtool alert add --alertmanager.url=http://localhost:9093 alertname=FoundationSecurityAdvisory service=security severity=ticket source=drill advisory=DRILL-0001 package=drill/none --annotation=summary=security-advisory-delivery-drill
```

- 30초쯤 뒤 `#alerts` 에 `FIRING FoundationSecurityAdvisory` 와 `security-advisory-delivery-drill` 이 온다.
- 5분쯤 뒤 같은 채널에 `RESOLVED FoundationSecurityAdvisory` 가 온다(시험 알림은 다시 보내지 않으므로 Alertmanager 가 해소한다).
- Slack Web API 는 실패(채널 없음, 토큰 취소)에도 HTTP 200 을 돌려준다. 그래서 **채널에서 메시지를 본 것만** 전달로 친다.
  안 오면 `docker logs foundation-platform-runtime-alertmanager-1 --since 10m` 에서 `notify` 줄을 본다.

## 4. 토큰 바꾸기·지우기

바꿀 때는 1절을 그대로 다시 한다. `env-set` 이 같은 이름을 새 값으로 쓴다. 옛 토큰은 GitHub 에서 Revoke 한다.
그만 쓸 때는 GitHub 에서 Revoke 하고 `sudo rm /etc/foundation-platform/security-alerts.env` 를 한다(그룹은 선택이다).
