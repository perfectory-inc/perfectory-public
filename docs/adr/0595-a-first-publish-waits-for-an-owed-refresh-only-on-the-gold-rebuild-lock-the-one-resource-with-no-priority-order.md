# ADR 0595: 첫 발행은 밀린 갱신을 Gold 재생성 자물쇠에서만 기다린다 — 순서 없는 자원은 그것 하나다

- Status: Accepted
- Date: 2026-11-20
- Amends: [ADR-0439](./0439-refreshing-a-served-lane-outranks-a-lanes-first-publish-and-a-gold-rebuild-takes-the-class-of-the-bake-that-waits-on-it.md)
  (#1148 이 같은 PR 로 넣은 자동 승격의 붙잡기 — ADR-0439 본문에는 없고 `by-pnu-auto-promote.sh`·그 시험에만 있었다. 이 ADR 이 그것을
  처음 적고 고친다. ADR-0439 에 개정 메모를 달았다)
- Builds on: [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(용량·우선순위 등급),
  [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)(행 판 표),
  [ADR-0567](./0567-the-parcel-nearby-gold-is-born-on-duckdb-as-a-table-of-row-versions-with-the-exact-nearest-and-ties-broken-by-the-facility-key.md)(주변 Gold 는 DuckDB)

## Context

**붙잡기는 어디에 있었나.** `scripts/ops/by-pnu-auto-promote.sh` 는 자산 조정기가 `priority_class: publish` 작업의 자산을 밀렸다고
쓰는 동안(`foundation_asset_owed … 1`) 어떤 첫 발행도 시작하지 않는다. 1ab2859d(#1148, 2026-11-07)가 ADR-0439 와 함께 넣었고, 이유는
그 스크립트 주석에 있다: 11-07 13:12Z parcel-regulation-zones 의 cut-over gold 단계가 **Gold 재생성 자물쇠**(`gold-panel-rebuild.sh` 의
`flock -n`)를 쥐었고, parcel 레인이 199시간 낡은 manifest 를 서빙하는 동안 밀린 패널 재생성은 `waiting` 으로만 끝났다.

**2026-11-19..20 그 붙잡기가 주변 시설을 막았다.**

- `foundation-gold-versions@parcel-nearby` 가 11-19 15:51Z 에 `gold.parcel_nearby_versions` 를 커밋했다(version_seq 1, 39,861,511 행,
  1500 s). 그 Gold 는 발행기 안의 DuckDB 가 짓고, 재생성 자물쇠를 쓰지 않는다.
- 같은 시각 `by_pnu_parcel_nearby_bake` 는 `the pack-only parcel-nearby lane is not cut over yet` 로 끝났다. 첫 발행은 자동 승격의 몫이다.
- 자동 승격은 11-19 12:12Z 부터 11-20 06:00Z 넘어서까지 매번 `a served lane's refresh is owed
  (perfectory://parcel-real-transaction-by-pnu-section-packs); no first publish starts before it` 를 썼다.
- 그 밀린 것은 실거래 굽기다. 11-19 13:25Z 에 timeout 으로 실패했고, 자산 조정기는 계속 밀렸다고 썼다(lag 395,493 s).
- 엣지는 주변 문서마다 `503`(`Foundation-Outage: manifest-invalid`)을 답했다. 붙잡기 자체에는 경보가 없다. 기존 티켓
  `FoundationByPnuLaneUnmeasured` 는 등록 뒤 신선도 예산(48 h)이 지나야 울린다(주변: 11-21 07:00Z).

**자원마다 다투는 방식이 다르다.**

| 자원 | 누가 쓰나 | 순서 |
|---|---|---|
| spark 풀 | 모든 `pool: spark` 작업, cut-over 청구(gold·bake 단계 모두, 등급 `first_publish`) | 등급 순서가 있다. 기다리는 `publish` 청구는 더 낮은 등급의 시작을 막는다(ADR-0435). 이미 도는 cut-over 는 비키지 않는다 |
| Gold 재생성 자물쇠 | `gold-panel-rebuild.sh` 로 도는 unit: 패널·종류·레인 재생성과 cut-over 의 `gold-panel-rebuild.sh` gold 단계 | 순서가 없다. `flock -n` 이라 늦게 온 쪽은 기다리지 않고 거부된다 |

ADR-0439 의 등급은 spark 풀의 다툼을 이미 푼다. 붙잡기가 따로 필요한 것은 순서 없는 자물쇠뿐이다.

## Decision

1. **첫 발행은 밀린 갱신을, 자기 gold 단계가 Gold 재생성 자물쇠를 쥘 때만 기다린다.**
   - 판정은 한 곳이다. cut-over 러너가 레인마다 gold 단계의 unit 을 고르고(`by-pnu-lane-cutover.sh`), 새 출력
     `lanes --pending-gold-lock` 이 기다리는 레인마다 `yes`(그 unit 이 `gold-panel-rebuild.sh` unit) 또는 `no`(자기 작업의 `.service`)를 쓴다.
   - 자기 작업의 unit 은 Gold 가 재생성 계약의 표가 아니고, `_versions` 로 끝나고, `foundation-gold-versions@<lane>` 작업이 있을 때다.
     ADR-0564 §6 의 그림자 기간 레인(versions 표가 재생성 계약의 옛 표 옆에 있는 레인)은 `yes` 다.
   - 자동 승격은 `publish` 자산이 밀린 동안 `no` 레인만 후보로 남긴다. 없으면 지금처럼 아무것도 시작하지 않고 그 이유를 쓴다.
     있으면 `only lanes whose gold step takes no Gold rebuild lock may start: …` 를 쓰고, 백오프·순서 규칙(ADR-0435)대로 하나를 시작한다.
2. **spark 풀은 붙잡지 않는다.** `no` 레인의 cut-over 도 gold·bake 단계 모두 전에 spark 풀 전체를 청구한다(`first_publish`). 그 다툼은
   등급이 정한다.
3. **받아들이는 지연.** 풀린 cut-over 가 도는 동안 밀린 `publish` 굽기는 그 뒤에 선다. 한 시도의 상한은 이렇다:
   - gold 단계: versions 작업 상한 60 + 5 분(이미 커밋된 Gold 면 수 초, 11-19 15:52Z 8–9 s).
   - bake 단계: 전량 굽기. 주변 bake 는 아직 잰 적이 없다. 같은 수의 parcel 전량 굽기가 39,861,511 문서에 85 분이었다
     (`max_cumulative_change_ratio_reason`).
   - 나머지 단계(동등·지연·발행·확인)는 분 단위다.
   - 그래서 한 시도에 약 2.5 시간이다. 실패하면 백오프(1, 2, 4 … 24 시간)가 다음 시도를 미룬다.
   - 대기열 노화(시간당 100점, 최대 900): 5시간 넘게 기다린 `first_publish` 청구는 기다리는 `publish` 청구보다 앞설 수 있다. 이것은 지금도
     같다.
4. 시험이 막는 실제 사고(루트 ADR-0001): **11-19 주변 Gold 가 커밋된 뒤 실거래 굽기가 밀린 동안 첫 발행이 시작되지 않았다.**
   - `test_2026_11_19_a_lane_whose_gold_step_takes_no_rebuild_lock_is_not_held_by_an_owed_refresh`: 풀린 레인은 시작하고, 자물쇠를 쥐는 레인은
     여전히 붙잡힌다.
   - `test_the_pending_lanes_say_whether_their_gold_step_takes_the_rebuild_lock`: 러너의 `yes`/`no` 가 위 세 조건과 같다.

## Consequences

- 주변 레인은 배포 뒤 다음 자동 승격 틱에 cut-over 를 시작한다. 실거래 굽기가 밀린 동안에도 그렇다.
- 실거래 레인 자체의 근본은 따로다. 문서의 coverage 를 레인 수준으로 빼서, 변경 집합이 실제 바뀐 문서만 되게 한다(W81c 조각 2,
  실거래 Gold 의 DuckDB 이전).
- 붙잡힌 레인의 경보는 새로 만들지 않는다. 기존 `FoundationByPnuLaneUnmeasured` 티켓이 신선도 예산에 묶는다.
- **이 ADR 의 끝.** 마지막 레인이 `gold-panel-rebuild.sh` 를 떠나면(ADR-0564 §9) 재생성 자물쇠가 없어진다. 그때 붙잡기와
  `lanes --pending-gold-lock` 을 함께 지운다.

## 기각한 대안

- **실거래 레인을 먼저 고친다.** 근본이지만 며칠 걸린다(조각 2). 그동안 Gold 가 이미 커밋된 레인이 화면에 없다.
- **주변의 등급을 올린다.** 자물쇠가 아니라 풀의 순서를 바꾼다. 문제는 순서가 아니라 붙잡기였다. 다른 첫 발행보다 앞설 근거도 없다.
- **경보만 더한다.** 18시간 조용했던 것은 알리지만, 화면에 없는 것은 그대로다. 기존 티켓이 이미 예산에서 울린다.
- **레인 목록으로 예외를 둔다**(이 PR 의 첫 판). 원칙이 아니라 특례다. 자원을 묻지 않아, 그림자 기간 레인이 자물쇠를 쥘 수 있었다
  (EGM 2026-11-20 M1).
