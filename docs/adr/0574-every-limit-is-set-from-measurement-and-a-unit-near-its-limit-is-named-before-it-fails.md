# ADR 0574: 모든 한도는 측정으로 정하고, 한도 가까운 유닛은 죽기 전에 이름이 불린다

- Status: Accepted
- Date: 2026-11-19
- Builds on: [ADR-0455](./0455-one-job-cannot-take-the-host-every-unit-declares-its-memory-and-batch-is-killed-first.md)(유닛마다 메모리 상한, `memory_max ≥ 1.5 × measured_peak`),
  [ADR-0386](./0386-every-job-states-its-per-run-bound-in-one-vocabulary-and-its-measured-runs-fit-its-unit.md)(run 시간 ≤ 제한의 90%),
  [ADR-0391](./0391-a-silver-refresh-run-starts-no-step-its-measured-time-cannot-finish-and-a-batch-reads-back-only-its-own-commit.md)(잰 단계 시간으로 마감),
  [ADR-0385](./0385-the-host-says-when-its-disk-memory-or-a-cap-runs-out-and-each-rule-is-tested-by-promtool.md)(promtool 로 시험한 경보),
  [ADR-0445](./0445-a-failed-host-unit-names-its-owner-and-runbook-and-a-unit-that-stays-failed-calls-its-owner.md)(실패한 유닛은 주인을 부른다),
  ADR-0573(foundation-api 아레나, #1439)

## Context

사장이 2026-11-18 에 물었다: 한도는 왜 있고, 왜 이렇게 많은 작업이 한도에서 죽는가. 한도가 있는 이유는 하나다 — 한
작업의 버그나 입력 증가가 호스트(62 GiB, 데이터베이스·백업·API 가 같이 사는)를 쓰러뜨리지 못하게(ADR-0455, 11-08 33 GB
사고). 문제는 한도가 아니라 **한도가 잰 값에서 멀어진 채 아무도 모른다는 것**이다. 상한 숫자는 계약에 있지만 그 근거
측정(`measured_peak`)은 10-18..11-08 한 번이고, 그 뒤 입력이 자라는 동안 아무것도 유닛이 한도에 다가가는 것을 말하지
않았다. 죽은 뒤에야 `failed` 가 보였다.

**측정 (ai-server, 읽기만, 2026-11-04..18, `scripts/ops/unit_limits.py`).** systemd 는 유닛이 끝날 때 cgroup v2
`memory.peak` 을 `MEMORY_PEAK` 로 저널에 남긴다(MESSAGE_ID `ae8f7b86…`). 상한에서 죽은 run 은 그 줄을 남기지 않는다 —
첫 측정이 이것을 놓쳐 parcel-number-change 의 OOM 6 번을 0 으로 셌다. 그래서 run 은 시작부터 resource 줄, 그것이 없으면
실패·비활성 줄까지다. 컨테이너는 cAdvisor working set 최고점과 커널의 `oom_memcg=…docker-<id>.scope` 줄로 잰다.
유닛 160 개 중 **19 개 failing**(창 안에 OOM 이나 timeout), **11 개 near**(한도의 80% 이상), 129 개 fits. 상주 컨테이너
36 개 중 1 개 failing, 4 개 near.

| 유닛·컨테이너 | 한도 | 잰 값 | OOM / timeout | 주인 |
|---|---|---|---:|---|
| foundation-api(컨테이너) | 512m | 512 MB(11-11..18 매일) | 69 / — | ADR-0573(#1439) |
| onbid-silver-load | 1G + Spark 2.5G | Spark 2,561M | 14 / 2 | W83b #1413 |
| rt-molit-silver-load | 512M | 460M(High 에 붙음) | 7 / 0 | W83c #1426 |
| parcel-tile-bake@bake | 1G, 115분 | 954M, 115분 | 4 / 4 | W83c·W86 #1422·#1434 |
| gold-lane-rebuild@parcel-real-transaction | 512M + Spark 20G | 461M, 150분 | 5 / 1 | W83c #1428 |
| media-reconcile | 512M, 10분 | 496M(97%), 10분 | 0 / 6 | W83c #1420 |
| industrial-complex-notice-silver-load | 512M, 40분 | 432M, 40분 | 0 / 8 | W75e #1417 |
| court-auction-collect | 2G, 300분 | 2,011M(98%) | 4 / 0 | ADR-0566 |
| gold-panel-rebuild(+unconditional) | 512M + Spark 20G, 160분 | 162분 | 4 / 18 | ADR-0386 §5 예외 |
| intelligence-document-text / -fact | 512M, 80분 | 461M / 312M, 84분 | 24·3 / 7·19 | 없음(이 ADR §7) |
| parcel-number-change | 256M | 252M(98%) | 6 / 0 | 없음(§7) |
| registered-factory | 256M | 256M(100%) | 2 / 0 | 없음(§7) |
| asset-reconcile | 768M, 180초 | 692M(90%), 181초 | 0 / 7 | 없음(§7) |
| source-sweep | 4G | 40.6G(11-08, #1183 전) | 1 / 0 | 고쳐짐(#1183) |

near 11 개: autodeploy 3,225M/3.5G·onbid-collect 2,489M/3G·industrial-complex-notice-collect 881M/1G·legal-dong-code
424M/512M·rt-molit-collect·vworld-parcel-edition·readiness-report(모두 High 에 붙음)·unit-price-versions-migrate 88%
시간·identity-backup 88% 시간 등. 상주 컨테이너 near: airflow-scheduler 99%·opensearch 91%·dagster-daemon 89%·kafka 86%.

**큰 곳은 이렇게 한다.** Kubernetes 는 `requests`/`limits` 를 두고 Vertical Pod Autoscaler 가 측정한 사용량의 백분위
(목표 p90, 상한 p95)에 여유 15% 를 더해 추천한다. Google Autopilot(Borg, EuroSys 2020)은 한도를 지난 사용량의 이동
창 최고점과 백분위로 정하고 OOM 이 나면 즉시 올린다. 어느 쪽도 손으로 고정한 숫자를 믿지 않고, 측정이 한도를 끌고,
근접은 죽기 전에 신호를 낸다. 시간 한도는 Kubernetes Job 의 `activeDeadlineSeconds` 처럼 작업 정의에 있지만, 큰 배치
(Spark·Flink)는 작업 전체가 아니라 작업 단위(task·shard)마다 시간을 둔다 — 전체 시간은 입력과 함께 자라기 때문이다.

## Decision

1. **측정은 스크립트 하나가 다시 만든다.** `scripts/ops/unit_limits.py` 가 표의 정본이다(손으로 만든 표 없음). 호스트에서
   `--table`(Markdown)·`--json`, `--prometheus` 를 주면 컨테이너까지. 읽기만 한다(`journalctl _PID=1` 의 여섯 종류 줄,
   `systemctl show`, `docker ps`, Prometheus API, `journalctl -k`). 유닛 상태: 창 안에 OOM·timeout 이 있으면 `failing`,
   메모리나 시간의 최대가 한도의 80% 이상이면 `near`, 아니면 `fits`. 컨테이너는 유닛 run 하나가 품을 때만 그 유닛에 센다
   (두 run 이 품으면 버린다: 다른 작업의 컨테이너를 붙이지 않는다). `job-run-times.py`(ADR-0386)는 작업 목록의 run 시간
   블록을 쓰는 같은 저널 읽기다; 두 읽기의 run 경계는 같고(시작 → 끝 줄), 하나로 합치는 것은 그 블록을 다시 재는 PR 의 몫이다.
2. **메모리 한도 = 잰 최고점 × 1.5**(ADR-0455 의 `memory_max_over_peak` 그대로, 바닥 128M). 창은 최근 14 일, 최고점은
   p95 가 아니라 최대다: p95 는 죽는 그 run 을 버린다. `memory.peak` 은 페이지 캐시를 포함하므로 최고점이 `MemoryHigh`
   (0.9 × 상한)에 붙고 OOM 이 없으면 캐시를 되찾은 것이고 늘릴 이유가 아니다. **돌려주기:** 상한이 잰 p95 의 4 배를 넘고
   `headroom_why` 가 없으면 예산을 쥐고만 있는 것이다; 그 상한을 1.5 × 최대로 내리는 PR 이 예산을 돌려준다. 다만 이미 결정된
   경계(by-PNU 굽기 14G = 행 상한 샤드의 측정, ADR-0138)는 창 안의 run 이 그 경계를 쓰지 않았다고 내리지 않는다.
3. **시간 한도 = 잰 최장 run ≤ 제한의 90%**(ADR-0386 그대로). 입력과 함께 자라는 작업의 시간은 작업 전체가 아니라
   **선언된 작업 단위(샤드·묶음·객체)마다** 잰다: 단위의 잰 최장 시간으로 마감을 정하고 마감 뒤에는 새 단위를 시작하지
   않는다(ADR-0391 `step_seconds_max`, ADR-0386 `deadline`). 새 작업은 이 형태로만 켠다; 지금 작업 전체 시간만 가진 작업은
   ADR-0386 §5 의 열린 예외로 남는다.
4. **입력이 자라 한도에 닿으면 한도가 아니라 성장을 고친다.** 잰 최고점이 입력 크기와 함께 자라는 작업(전국 색인·목록 전체·
   표 전체를 메모리에 드는 것)은 흘려 처리한다(ADR-0455 §7: 파일·조각으로 흘리고, 한 run 은 자기 몫만). 한도를 올리는 것은
   측정이 그 크기가 작업의 고정된 몫임을 보일 때만이고, 그 PR 은 `tools/host-memory-budget.contract.json` 의 가드
   (`every-container-has-a-memory-cap.sh`: 상주 합 + 가장 큰 일회성/풀 조합 + host_reserved ≤ 62g)를 통과한다. 예산 계약이
   합의 정본이고 다른 곳에서 한도를 바꾸지 않는다.
5. **근접은 죽기 전에 이름이 불린다.** 자산 조정기가 30 분마다 `unit_limits.measure` 로 `unit-limits.prom` 을 쓴다:
   유닛·한도(`memory`, `time`)마다 최근 run 이 연달아 80% 를 넘은 수(`foundation_unit_runs_near_limit`)와 마지막 run 의 비율.
   - `FoundationUnitNearItsLimit`(ticket): 연달아 **3 번**. 한 번은 입력, 세 번은 그 유닛의 크기다. Alertmanager 는 유닛·한도마다
     한 알림, 하루 한 번 다시(`service = "unit-limits"`).
   - `FoundationContainerMemoryNearCapLong`(page): 컨테이너가 하루 96 개 15 분 중 절반 넘게 상한의 90% 위. 기존 ticket
     (`FoundationContainerMemoryNearCap`)이 foundation-api 에 2 주 울리는 동안 69 번 죽었다(ADR-0573).
   - `FoundationUnitLimitsUnread`(ticket): 측정 파일이 두 시간 없음.
   모든 규칙은 promtool 시험(`foundation-unit-limits.rules.test.yml`, 가드 `prometheus-rules-pass-their-tests`)을 통과한다.
6. **한도에서 죽으면 그 이름으로 한 번 부른다.** 유닛 결과는 `oom-kill`·`timeout` 으로 보고된다(Spark 컨테이너의 OOM 도
   `oom-kill`, #1399). 알림은 유닛 실패 경로 하나다(`unit-failure` 하루 한 번, 오래 실패하면 `unit-state` 가 주인을 부름,
   ADR-0445; 원인 하나에 알림 하나, #1400). 이 결정은 실패 경로를 새로 만들지 않는다.
7. **주인 없는 failing 유닛은 잰 값과 함께 넘긴다.** intelligence-document-text/-fact(OCR 컨테이너 OOM 24 번·timeout 26 번),
   parcel-number-change(256M 에서 OOM 6 번, 10 월 측정 11M — 성장), registered-factory(256M 에서 OOM 2 번), asset-reconcile
   (180 초 timeout 7 번)은 이 결정의 표로 W87 보고에 실려 코디네이터가 주인을 정한다. 한도를 먼저 올리지 않는다(§4).

## Consequences

- 한도가 잰 값과 멀어지면 30 분 안에 보인다: 처음 켜질 때 near 11 개와 failing 의 대부분이 ticket 을 받는다 — 그것이 지금의
  실제 상태다.
- 측정은 호스트의 읽기뿐이고 30 분에 한 번 몇 초(14 일 저널 72k 줄 2.7 초)다. 비용 0.
- 계약의 `measured_peak`(ADR-0455)는 10 월 측정으로 남아 있다. 그것을 이 스크립트로 다시 쓰는 것은 한도를 고치는 PR 마다의
  몫이다(가드가 1.5 배를 검사하므로 새 측정이 상한을 끌어올리거나, §4 에 따라 성장을 고치게 한다).
- 더니어(통합 콘솔)는 이 측정을 읽어 유닛마다 한도·잰 값·비율·상태·마지막 OOM/timeout·예산 합을 보여 줄 수 있다; 한도 변경은
  화면에서 직접 쓰지 않고 이 계약을 바꾸는 PR 로 간다(후속).
