# ADR 0444: 실패한 호스트 유닛은 주인과 런북을 이름 대고, 실패한 채 남은 유닛은 주인을 부른다

- Status: Accepted
- Date: 2026-11-07
- Builds on: [ADR-0385](./0385-the-host-says-when-its-disk-memory-or-a-cap-runs-out-and-each-rule-is-tested-by-promtool.md)(규칙은 promtool 로 시험),
  [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(자산 조정기, 5분마다 호스트 사실을 textfile 로),
  [ADR-0184](./0184-one-answer-for-running-jobs-and-a-release-the-kernel-keeps-immutable.md)(유닛 상태는 job_units.py 하나가 systemd 에 묻는다)

## Context

2026-11-07 07:30Z `systemctl --failed` 실측(ai-server, 읽기 전용): 실패한 유닛 35 개. W19·W20 몫(Silver 갱신, 서빙 모니터)을 빼도
아홉이 몇 시간에서 엿새까지 실패한 채였다.

| 유닛 | 실패한 때 | 그 뒤 |
|---|---|---|
| `foundation-identity-restore-drill` | 11-01 05:02Z | 주 1회라 엿새 동안 다시 안 돎, 알림 1번 |
| `foundation-court-auction-collect` | 11-06 17:31Z | 거절 표지가 다음 밤도 막음, 알림 2번 |
| `foundation-zoning-tile-bake` | 11-06 16:19Z | 매번 같은 거절 |
| `foundation-lineage-stewardship` | 11-07 06:50Z | 새 단계 첫 실행부터 |
| `foundation-rt-molit-silver-load` | 11-07 06:55Z | 저널에 `exit 1` 만 |
| `foundation-worker-autodeploy` | 11-06 07:01Z~ | 미리보기 되돌림 48번 |
| `foundation-by-pnu-public-auction-bake` | 11-05 16:40Z | 릴리스가 유닛 파일을 지웠는데 실패 상태가 남음(not-found) |
| `foundation-parcel-tile-bake@rehearsal-gold`·`@rehearsal-bake` | 10-15·16 | 운영자 리허설, 3 주째 실패, 저널도 사라짐 |

알림은 갔다. `OnFailure=foundation-unit-failed@%n.service` 가 실패마다 Slack `#alerts` 에 한 줄을 쓴다. 그런데 몇 시간·며칠 동안
아무도 손대지 않았다. 원인 넷:

1. **사건만 있고 상태가 없다.** 알림은 실패한 순간 한 번이다. 주 1회 드릴, 지워진 유닛, 막힌 수집기는 다시 돌지 않으니 두 번째 알림이 없다.
   Alertmanager 로 가는 유닛(`OnSuccess=foundation-unit-recovered@`)은 두 개뿐이고, 그 알림도 26 시간 뒤 끝난다.
2. **누구 몫인지 없다.** 유닛 54 개 중 런북(`Documentation=`)을 단 것이 2 개, 주인을 단 것은 0 개. 정의(`definitions/*.json`)의
   `owner` 는 199 개 중 195 개가 `foundation-platform` 한 값이라 아무도 가리키지 않는다.
3. **오래 가도 올라가지 않는다.** 하루가 지나도 첫 알림과 같은 무게다. 매시간 실패하는 유닛은 알림이 매시간 새로 와 묻힌다(10-28/29).
4. **의미 없는 실패가 섞인다.** 지워진 유닛과 버려진 리허설 셋은 고칠 것이 없는데 `--failed` 에 계속 남아, 목록을 읽는 사람이
   "늘 있는 것" 으로 읽게 만든다.

검토한 사례:

- Google SRE Book 의 *Being On-Call* 과 PagerDuty 의 에스컬레이션 정책: 모든 경보는 서비스 주인과 런북을 갖고, 정해진 시간 안에
  아무도 처리하지 않으면 다음 단계로 올라간다.
- Prometheus 의 상태 기반 경보: 사건(로그 한 줄)이 아니라 지금 상태의 시계열에 경보를 걸면, 상태가 이어지는 동안 경보도 이어지고
  상태가 끝나면 스스로 풀린다. "임계값/메타데이터는 info 시계열로"(Robust Perception) — 주인·런북을 시계열 라벨로 싣는다.
- Backstage 소프트웨어 카탈로그: 모든 구성 요소가 `spec.owner` 를 갖고, 주인 목록은 따로 한 곳에 있다.
- Kubernetes `ttlSecondsAfterFinished`: 끝난 일회성 Job 은 정해진 시간 뒤 치운다. 선언된 것만 상태로 남긴다.
- systemd: `Documentation=` 은 유닛의 표준 런북 칸이고(`systemctl status` 가 보인다), `X-` 로 시작하는 칸은 systemd 가 읽지 않는
  사용자 칸이다(systemd.unit(5)).

node_exporter 의 systemd 수집기도 검토했으나 쓰지 않는다. 컨테이너 안의 nobody 가 시스템 버스에 붙는 것을 Docker 기본
AppArmor 정책이 막는다(실측: `An AppArmor policy prevents this sender from sending this message`). 그 정책을 풀면 수집기 전체가
약해진다. 이미 호스트에서 5 분마다 사실을 textfile 로 쓰는 자산 조정기(Docker 네트워크·컨테이너 이름, ADR-0385)가 있다.

## Decision

1. **주인 목록은 한 곳이다.** `config/unit-owners.contract.json` 의 `owners` — `collection`(수집), `lakehouse`(적재),
   `serving`(서빙), `tiles`(지도 타일), `platform`(호스트·배포·백업). 주인이 하나도 없는 이름은 목록에 둘 수 없다.
2. **실패를 알리는 모든 유닛은 주인과 런북을 이름 댄다.** `infra/systemd/*.service` 중 `OnFailure=foundation-unit-failed@` 를 가진
   모든 유닛이 `[Unit]` 에 `X-Foundation-Owner=<주인>` 과 `Documentation=file:///opt/foundation-platform/current/docs/runbooks/<런북>.md`
   를 둔다. 런북 파일은 릴리스에 있어야 한다. 유닛 파일이 유닛의 정의라 그 자리에 둔다(작업이 아닌 유닛 17 개도 같은 규칙).
   `orchestration/tests/test_unit_owners.py` 가 CI 에서 어긴 유닛을 거부한다.
3. **실패는 상태로 쓴다.** 자산 조정기(`asset_reconcile.py`, 5 분)가 `job_units.failed`·`job_units.show` 로 systemd 에 묻고
   `/var/lib/foundation-jobs/metrics/foundation-units.prom` 에 실패한 유닛마다
   `foundation_unit_failed_since_seconds{unit,owner,runbook}` 를 쓴다. 값은 **연속으로 실패해 온 첫 시각**이다. 실패를 처음 본 tick
   부터, 실패도 실행 중도 아닌 상태를 본 tick(성공 또는 reset)까지 하나의 실패다. 그래서 매시간 실패하는 유닛도 매시간 새로 시작하지
   않는다. 그 표는 조정기 상태 디렉터리의 `failing-units.json` 에 남긴다. `foundation_units_failed` 가 지금 실패한 수다.
4. **오래 실패하면 주인을 부른다.** `foundation-units` 규칙 묶음(`foundation-api.rules.yml`):
   - `FoundationUnitFailedLong`: 6 시간 넘게 실패해 온 유닛. `<!channel>` 과 주인 이름, 런북을 싣고 `severity: page`.
     6 시간인 까닭: 하루 한 번 도는 작업이 실패하면 그때도 실패한 채이고, 주인이 그날 안에 고칠 수 있다.
   - `FoundationUnitFailedUnowned`: 주인 없는 실패가 2 시간. 지워진 유닛이면 다음 배포가 치운다(§5).
   - `FoundationUnitsUnread`: 목록 파일이 30 분 넘게 갱신되지 않음.
   Alertmanager 경로 `service="unit-state"` 는 유닛마다 한 알림, 12 시간마다 다시 알린다. 유닛이 성공하거나 치워지면 시계열이
   사라지고 경보는 스스로 풀린다. 규칙은 `foundation-units.rules.test.yml` 의 promtool 시험을 통과해야 한다(ADR-0385).
5. **실패 상태는 선언된 유닛이 지금 고장났다는 뜻만 갖는다.** 배포(`foundation-release.sh timers`)는 `daemon-reload` 뒤
   `job_units.py leftover-failed` 가 내놓은 유닛을 `systemctl reset-failed` 한다:
   - 파일이 사라진 유닛(`LoadState=not-found`) — 다시 돌 방법이 없다.
   - 어떤 작업의 템플릿 인스턴스인데 `jobs.v1.json` 의 어떤 작업도 그 이름을 대지 않는 것(운영자의 일회성 리허설), 실패한 지 24 시간 뒤.
     그 실패는 이미 실패 알림과 운영자 자신의 `status` 로 전해졌다.
   선언된 작업의 실패와, 작업이 아닌 템플릿(서빙 모니터·Silver 갱신 점검)의 인스턴스는 건드리지 않는다. systemd 에 물을 수 없으면 아무것도 치우지 않는다.
6. **첫 알림에도 주인을 싣는다.** `notify-unit-failure.sh` 가 유닛 파일의 `X-Foundation-Owner=` 를 읽어 Slack 줄에 `주인: <주인>` 을 붙인다.

## Consequences

- 2026-11-07 의 아홉 유닛은 이제 6 시간째에 주인 이름으로 불리고, 고칠 때까지 12 시간마다 다시 불린다. 주 1회 드릴처럼 다시 돌지
  않는 유닛도 그렇다.
- 배포 뒤 첫 `timers` 단계가 지금 남은 셋(`foundation-by-pnu-public-auction-bake`, `foundation-parcel-tile-bake@rehearsal-gold`·
  `@rehearsal-bake`)을 치운다. 운영 호스트에 읽기 전용으로 `leftover-failed` 를 돌려 정확히 이 셋만 나오는 것을 확인했다.
- 런북이 따로 없던 유닛 여럿은 가장 가까운 런북을 가리킨다(예: Gold 재생성 유닛들 → `gold-panel-rebuild.md`). 그 유닛이 실패해
  런북이 모자라면 그때 자기 런북을 쓴다.
- 주인은 지금 사람 하나와 그가 부리는 작업자들이라 다섯 주인이 모두 같은 Slack 채널로 간다. 주인마다 다른 수신자가 생기면
  Alertmanager 에 `owner` 경로를 더하면 된다(시계열이 이미 `owner` 라벨을 갖는다).
- 비용: 5 분마다 `systemctl` 두 번(수 ms), textfile 하나, 시계열 수는 실패한 유닛 수만큼. 돈은 들지 않는다.
