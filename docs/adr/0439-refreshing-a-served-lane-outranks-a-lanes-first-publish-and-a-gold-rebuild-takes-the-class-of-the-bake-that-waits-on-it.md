# ADR 0439: 서빙 중인 레인의 갱신이 새 레인의 첫 발행보다 앞서고, Gold 재생성은 자기를 기다리는 굽기의 등급을 받는다

- Status: Accepted
- Date: 2026-11-07
- Builds on: [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(용량·우선순위 등급, cut-over 청구), [ADR-0437](./0437-a-gold-without-a-recorded-code-version-rebuilds-whole-by-key-and-a-monitor-holds-a-404-to-what-the-lane-serves.md)(W20 의 parcel Gold 라이브락)
- 관련 사건: W20(SEV1), 2026-11-07 — parcel 레인이 193시간 낡은 manifest 를 서빙하는 동안 자동 승격의 lane cut-over 두 개(07:42Z..09:00Z parcel-regulation-zones, 09:12Z~ parcel-ownership)가 spark 풀을 청구했다. 그것을 갱신할 패널 Gold 재생성(`gold_panel_rebuild`, 등급 없음 = `default`)은 시작할 수 없었다.

## Context

ADR-0435 는 spark 풀의 순서를 등급으로 정했다: `publish`(1000) > `default`(100) > `backfill`(10). 기다리는 청구는
같거나 낮은 등급의 시작을 막는다(`job_units.py pool-full`). lane cut-over 청구는 `publish` 였고, 패널 Gold 재생성은
등급을 적지 않아 `default` 였다. 그래서 cut-over 가 하나라도 돌거나 기다리는 동안 패널 재생성은 막혔다. 자동 승격은
15분마다 다음 미발행 레인을 시작하므로, 미발행 레인이 남아 있는 한 그 상태가 이어질 수 있다.

이것은 우선순위 역전이다. 사용자가 **지금 읽는** 레인을 갱신하는 굽기는 `publish` 인데, 그 굽기가 기다리는 Gold
재생성은 `default` 라 새 레인의 첫 발행 뒤로 밀렸다. 운영체제의 우선순위 상속(Linux PI mutex·Solaris turnstile)이
푸는 바로 그 모양이다: 높은 등급이 기다리는 자원을 쥔(만들어야 하는) 일은 그 등급을 받는다.

## Decision

1. **등급 `first_publish`(500)** 를 둔다: lane cut-over 청구(`capacity_claimants.by_pnu_lane_cutover`)의 등급이다.
   `publish`(1000) 는 이제 "사용자가 지금 읽는 것을 갱신하는 일"이다: 서빙 중인 레인의 굽기와, 그 굽기가 기다리는
   Gold 재생성. 첫 발행은 여전히 `default`·`backfill` 위다.
2. **Gold 재생성은 기다리는 굽기의 등급을 받는다.** 정의에 `priority_class_from: <job id>` 를 적고 `priority_class` 를
   그 작업과 같게 둔다. `job_specs.inherited_class` 가드는 이름 붙은 작업이 이 작업의 출력을 읽고(파이프라인 그래프),
   두 등급이 같을 때만 받는다 — 굽기의 등급이 바뀌면 재생성의 등급도 함께 바뀌어야 목록이 읽힌다.
   `gold_panel_rebuild` → `by_pnu_serving_bake`(`publish`).
3. 가드가 막는 실제 사고(루트 ADR-0001): **2026-11-07 parcel 레인이 193시간 낡은 동안 첫 발행 cut-over 두 개가 spark
   풀을 쥐고 그것을 갱신할 패널 Gold 재생성을 막았다**(`test_a_served_lanes_refresh_is_not_held_for_a_first_publish`).

## Consequences

- 기다리는 cut-over 청구는 패널 재생성·굽기의 시작을 더는 막지 않는다. 반대로 `publish` 작업이 돌면 cut-over 는
  러너 자신의 "풀이 빌 때까지 기다림"으로 그 뒤에 선다.
- 재생성 자물쇠(`gold-panel-rebuild.sh` 의 flock)는 등급과 별개다: cut-over 의 gold 단계가 이미 자물쇠를 쥐었다면
  패널 재생성은 그 단계가 끝난 뒤 자산 조정기(#1136)가 다시 시작한다.
- 다른 Gold 재생성(종류 Gold 등)도 서빙 레인의 굽기를 먹이면 같은 방식으로 `priority_class_from` 을 적으면 된다.
