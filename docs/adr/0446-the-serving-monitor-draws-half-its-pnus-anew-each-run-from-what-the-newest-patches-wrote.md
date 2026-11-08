# ADR 0446: 서빙 모니터는 매 회 PNU 절반을 최신 패치가 쓴 문서에서 새로 뽑는다 — cut-over 표본만으로는 그 뒤에 생긴 키를 읽지 않는다

- Status: Accepted
- Date: 2026-11-08
- Amends: [ADR-0437](./0437-a-gold-without-a-recorded-code-version-rebuilds-whole-by-key-and-a-monitor-holds-a-404-to-what-the-lane-serves.md) 의 모니터 표본("표본 중 레인이 지금 문서로 서빙하는 첫 `monitor.pnus` 개")
- Builds on: [ADR-0151](./0151-building-packs-serve-each-document-whole.md)·[ADR-0160](./0160-one-by-pnu-gateway-source-serves-both-lanes.md)(매시 서빙 모니터), [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md)(섹션 팩과 패치), [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(자동 첫 발행)
- 관련 사건: W20 발견 (a), 2026-11-07 — 모니터 표본은 cut-over 때 한 번 뽑히고, 키가 드나드는 레인에서 낡는다.

## Context

매시 서빙 모니터(`monitor-<lane>-by-pnu-serving`)가 읽는 PNU 는 레인 cut-over 의 동일성 증거
(`<lane>-pack-g<n>/equality.json`, `monitor-sample` 단계가 `MONITOR_SAMPLE_PATH` 로 가리킴)의 `sample` 이다. 그 파일은
cut-over 때 한 번 쓰이고, 그 뒤 매일의 굽기(`by-pnu-serving-bake.sh`)는 패치·새 세대를 발행해도 표본을 다시 쓰지 않는다.
ADR-0437 은 표본 중 지금 서빙하지 않는 PNU 를 건너뛰게 했다. 그래서 표본이 줄어도 모니터는 실패하지 않는다. 그러나
**읽는 대상은 cut-over 이후 그대로 남은 키뿐이다.**

실측(읽기 전용, ai-server, 2026-11-08 05:50Z):

| 무엇 | 측정값 |
|---|---|
| 공매 레인 cut-over 표본(10-24 `parcel-public-auction-pack-g1/equality.json`) | 4,677 PNU |
| 그중 지금 `gold.parcel_public_auction` 에 있는 것 | 3,443 (15일 만에 1,234 개, 26% 이탈) |
| `gold.parcel_public_auction` 의 지금 PNU 수 | 4,151 — 계산상 708 개(4,151 − 3,443)는 10-24 뒤에 들어온 공고 |
| 그 새 공고 중 모니터가 읽는 것 | 0 (cut-over 표본은 그날의 레인 전체였고, 그 뒤 다시 쓰이지 않았다) |
| 다른 레인 표본 파일 | 10-05..10-16 에 쓰인 10,000 개짜리 그대로(parcel·building·zoning·price…) |

키가 드나드는 레인(공매·공매 입찰·실거래)에서 모니터는 시간이 갈수록 **오래 산 키만** 본다. 새로 들어온 문서가
잘못 구워져도, 엣지가 그 dong 의 새 팩을 잘못 읽어도 매시 `passed` 다. 그리고 표본이 다 빠지면 그때서야
"take a new sample" 로 실패하는데, 다시 뽑는 길은 사람이 cut-over 를 다시 돌리는 것뿐이다
(ADR-0435 와, 사람 단계를 0 으로 하라는 사장의 11-07 지시에 어긋난다).

black-box 모니터의 원칙은 사용자가 **지금** 받는 것을 사용자처럼 읽는 것이다(Google SRE 책 6장 "Monitoring Distributed
Systems"의 black-box 감시). 고정 목록은 그 목록이 뽑힌 날의 레인을 읽는다. "무엇이 바뀌었는가"는 우리 매니페스트가 이미 안다. 각 패치(`SectionPacksState.patches`)는 자기가 팩을 쓴 unit(법정동
또는 그 조각) 목록을 지니고, 그 팩의 색인이 쓴 키를 지닌다.

## Decision

1. **모니터는 매 회 PNU 절반을 레인이 지금 서빙하는 것에서 새로 뽑는다**(`monitor.rs` `served_sample`,
   `recently_written`). 대상이 live manifest(`Target::Served`)이고 팩을 서빙하면, 앵커 섹션의 패치를 최신부터 돌며
   각 패치가 쓴 unit 을 게이트 표본과 같은 seeded 순서(`gate::sample_rank`)로 읽는다. unit 마다 그 팩이 문서로 쓴
   키 중 rank 가 가장 낮은 하나를 고른다. 더 최신 패치가 쓴 키는 옛 패치에서 고르지 않고, 고른 키는 지금 서빙하는
   답이 `document` 인지 다시 확인한다(`inspect`). 그래서 툼스톤은 고르지 않는다.
2. **나머지 절반은 cut-over 표본이다**(ADR-0437 그대로: 지금 문서로 서빙하는 것만). 오래 산 키와 방금 바뀐 키를
   함께 본다. 한쪽이 모자라면 다른 쪽이 채운다(패치가 없는 새 세대 직후, 또는 표본이 다 빠진 레인).
3. **한 회의 읽기는 상한이 있다.** 많아야 `4 × monitor.pnus` 팩(지금 80개)을 읽고 멈춘다. 패치가 수천 unit 을
   써도 모니터 한 번이 R2 를 훑지 않는다.
4. **상태도, 새 파일도, 새 스위치도 없다.** 표본은 매니페스트와 팩에서 매번 유도된다. 굽기·cut-over·`/etc` 의
   모니터 파일은 그대로이고 옮길 것이 없다. 미발행 세대(`Target::Generation`)와 기록된 상태(`Target::Recorded`, 롤백
   리허설)는 지금처럼 게이트 표본만 읽는다. 그 상태 자체가 검사 대상이기 때문이다.
5. 실제 PNU 는 저장소에 들어오지 않는다. 표본은 호스트의 R2 읽기에서만 나온다(공개 저장소 규칙).

## Consequences

- 공매처럼 키가 드나드는 레인도 매시 최신 발행분의 문서를 엣지에서 읽는다. 새 dong 의 첫 팩, 새 공고의 문서가 모니터에
  들어온다.
- 비용(계산): 패치가 있는 레인은 매시 패치 팩을 최대 80개(실제로는 고를 문서 10개를 채우면 멈춰 보통 10개 안팎) 더
  읽는다. 공매 팩 하나는 수 KB~수백 KB 이고 R2 Class B 읽기는 백만 건당 $0.36 이다. 10 레인 × 24회 × 80 = 하루 19,200건,
  한 달 약 $0.21 이 상한이다.
- 패치 없이 새 세대만 발행된 직후에는 2번의 cut-over 표본이 전부를 채운다. 다음 패치부터 다시 절반이 새 키다.
- 시험: `a_new_generation_is_served_and_patches_follow_it` 이 패치를 발행한 뒤, 게이트 표본이 비었거나 툼스톤
  하나뿐이어도 모니터가 패치의 문서를 고르는지 본다.
