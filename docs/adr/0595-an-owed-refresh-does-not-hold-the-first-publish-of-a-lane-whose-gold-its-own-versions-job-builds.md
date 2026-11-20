# ADR 0595: 밀린 갱신은 자기 versions 작업이 Gold 를 짓는 레인의 첫 발행을 막지 않는다

- Status: Accepted
- Date: 2026-11-20
- Amends: [ADR-0439](./0439-refreshing-a-served-lane-outranks-a-lanes-first-publish-and-a-gold-rebuild-takes-the-class-of-the-bake-that-waits-on-it.md)
  (자동 승격의 붙잡기: 서빙 레인의 `publish` 작업이 밀린 동안 첫 발행은 시작하지 않는다)
- Builds on: [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(자동 승격),
  [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)(행 판 표),
  [ADR-0567](./0567-the-parcel-nearby-gold-is-born-on-duckdb-as-a-table-of-row-versions-with-the-exact-nearest-and-ties-broken-by-the-facility-key.md)(주변 Gold 는 DuckDB 로)

## Context

**2026-11-19..20 주변 시설이 화면에 없었다.**

- `foundation-gold-versions@parcel-nearby` 가 11-19 15:51Z 에 `gold.parcel_nearby_versions` 를 커밋했다(version_seq 1, 39,861,511 행,
  1500 s).
- 같은 시각 `by_pnu_parcel_nearby_bake` 는 "레인이 아직 cut over 되지 않았다"로 끝났다. 첫 발행은 자동 승격의 몫이다.
- 자동 승격(`by-pnu-auto-promote.sh`, 15분마다)은 11-19 12:12Z 부터 11-20 06:00Z 넘어서까지 한 레인도 시작하지 않았다. 매번
  "a served lane's refresh is owed (perfectory://parcel-real-transaction-by-pnu-section-packs)" 였다.
- 그 밀린 것은 실거래 굽기다. 11-19 13:25Z 에 timeout 으로 실패했고(coverage 가 모든 문서의 `row_digest` 안에 있어 변경 집합이
  전부다), 그 뒤 자산 조정기는 계속 밀렸다고 썼다(lag 395,493 s).
- 그 사이 엣지는 주변 문서마다 `503`(`Foundation-Outage: manifest-invalid`)을 답했고, 웹은 404 만 "문서 없음"으로 보아 섹션을
  숨겼다. 붙잡기 자체에는 경보가 없었다.

ADR-0439 의 붙잡기 이유는 하나다: cut-over 의 gold 단계가 **Gold 재생성 자물쇠 하나**(`gold-panel-rebuild.sh` 의 flock)를 몇 시간
쥐어, 밀린 서빙 레인의 Gold 재생성을 막는다(11-07 13:12Z). 그런데 Gold 가 자기 작업의 행 판 표인 레인은 gold 단계가 그 자물쇠를
쥐지 않는다. `by-pnu-lane-cutover.sh` 는 그런 레인의 gold 단계로 `foundation-gold-versions@<lane>` unit 을 시작한다. 그 unit 은
발행기 안의 DuckDB 이고 재생성 자물쇠를 쓰지 않는다. 주변은 그 Gold 가 이미 커밋되어 있어 그 단계가 "nothing to do"다.

## Decision

1. **밀린 갱신의 붙잡기는 gold 단계가 재생성 자물쇠를 쥐는 레인에만 걸린다.**
   - 서빙 레인의 `publish` 작업이 밀린 동안 자동 승격은 첫 발행을 기다리는 레인 가운데, `orchestration/jobs.v1.json` 에
     `systemd_service` 가 `foundation-gold-versions@<lane>.service` 인 작업이 있는 레인만 후보로 남긴다.
   - 그 판정은 cut-over 러너가 gold 단계를 그 unit 으로 고르는 판정(`by-pnu-lane-cutover.sh`)과 같다.
   - 남는 레인이 없으면 지금처럼 아무것도 시작하지 않고 그 이유를 쓴다.
   - 남는 레인이 있으면 그 사실과 레인들을 쓰고, 백오프·순서 규칙(ADR-0435)대로 하나를 시작한다.
2. **나머지는 그대로다.** gold 단계의 versions unit 은 자기 작업 정의의 풀·시간 상한을 따른다(재생성 자물쇠는
   `gold-panel-rebuild.sh` 에만 있다). cut-over 의 bake 단계는 여전히 spark 풀을 청구한다(등급 `first_publish`, ADR-0439 §1). 그래서
   서빙 레인의 `publish` 작업이 돌면 그 뒤에 선다. 이미 도는 cut-over 하나뿐이라는 규칙도 그대로다.
3. 시험이 막는 실제 사고(루트 ADR-0001): **11-19 주변 Gold 가 커밋된 뒤 실거래 굽기가 밀린 동안 첫 발행이 시작되지 않았다**
   (`test_2026_11_19_a_lane_whose_gold_its_own_versions_job_builds_is_not_held_by_an_owed_refresh`). 재생성 자물쇠를 쥐는 레인은
   여전히 붙잡힌다(같은 시험, 그리고 `test_no_first_publish_starts_while_a_served_lanes_refresh_is_owed`).

## Consequences

- 주변 레인은 다음 자동 승격 틱에 cut-over 를 시작한다. 실거래 굽기가 밀린 동안에도 그렇다.
- 실거래 레인 자체의 근본(문서의 coverage 를 레인 수준으로 빼서 변경 집합이 실제 바뀐 문서만 되게)은 따로다(W81c 조각 2).
- 다른 레인이 행 판 표로 옮겨 자기 versions 작업을 가지면, 정의만으로 이 예외에 든다. 새 목록은 없다.
