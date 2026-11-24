# ADR 0614: 끝난 것을 내리는 제공자의 수집 작업은 꺼져 있어도 "끝까지 읽음"을 지켜야 한다

- Status: Accepted
- Date: 2026-11-24
- Amends: [ADR-0480](./0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md)
  의 기대 둘(`done`·`change`)에 셋째 `landed` 를 더한다. ADR-0480 의 나머지는 그대로다.

## Context

법원경매정보 사이트는 매각이 끝난 물건을 목록에서 내린다. 그날 밤 읽지 않으면 그 물건은 다시 읽을 수 없다.
`court_auction_collect` 는 18 밤을 아무것도 쌓지 못했다.

| 밤 | 왜 | 그때 울린 것 |
|---|---|---|
| 2026-10-21 ~ 11-05 (16 밤) | 작업이 꺼져 있었다 | 없음 |
| 11-07 | 실행 잠금(latch)이 거절했다 | Prometheus 보관(15 일) 밖이라 확인하지 못했다 |
| 11-12 | 메모리 부족으로 실패했다 | 아래 표 |

**11-12 밤에 운영 Prometheus 에서 잰 것**(2026-11-24, 읽기만, `ALERTS` 15 일치).

| 경보 | 등급 | 울린 때 (UTC) |
|---|---|---|
| `FoundationPlatformIngestionRunStale` (`auction_item`·`auction_sale_result`, ADR-0443) | ticket | 11-12 22:49 ~ 11-13 16:19 |
| `FoundationJobOutcomeMissing` `done` (ADR-0480) | ticket | 11-12 22:49 ~ |
| `FoundationJobOutcomeMissingLong` `done` | page | 11-13 22:49 ~ 11-15 20:39 |

경보는 울렸고 Slack 으로 가는 길도 맞았다. 그러나 ticket 은 한 시간에 한 번 묶여 가는 줄이고(ADR-0591; 그 주
`#alerts` 는 하루 약 480 줄이었다), page 는 기한을 하루 넘긴 11-13 22:49 에야 왔다. 다음 밤(11-13 16:05)이
이미 시작한 뒤다.

지금 있는 두 장치가 못 보는 곳은 이렇다.

| 장치 | 보는 것 | 못 보는 것 |
|---|---|---|
| 원천 Bronze 나이 (ADR-0443, 카탈로그 `expected_cadence.max_age_hours`) | 작업이 켜져 있든 아니든 원천마다 | 공고 게시판. `release: irregular` 라 나이 기한이 없다. Bronze 는 공고가 바뀔 때만 쓰이므로 나이는 건강 신호가 아니다(11-24 ILIS 공고 상세 90 시간, 정상). 등급은 ticket 뿐이다 |
| 작업 결과 `done`·`change` (ADR-0480) | 켜진 작업 | 꺼진 작업은 시계열이 없다. 다시 켜면 `enabled_since` 부터 다시 센다. 기한 뒤 하루는 ticket 이다 |

그래서 게시판을 읽는 작업이 꺼지면 아무것도 보지 않고, 법원경매의 잃은 밤은 하루 동안 ticket 이다.

## Decision

1. **카탈로그가 사실을 말한다.** `docs/catalog/public-source-endpoint-catalog.v1.json` 의 엔드포인트는 제공자가
   끝난 것을 내리면 `provider_retention: {"keeps_history": false, "basis_ko": …}` 를 적는다. 이력을 지키는
   제공자는 아무것도 적지 않는다. 지금 적은 그룹은 `court_auction`·`onbid_open_api`·`ilis`·
   `industrial_land_sale_notice` 이다(28 엔드포인트). 법원경매는 실제로 잃었다. 나머지 셋은 내려가는 속도를
   재지 않았고, `basis_ko` 가 그렇게 말한다.
2. **그 원천을 읽는 수집 작업은 `landed` 를 선언한다.** 작업 정의의 `expected_outcome.landed_within_hours`
   (정수 시간)와 `landed_basis`. 어느 작업인지는 손으로 고르지 않는다: 파이프라인 그래프에서 그 그룹의 원천
   노드(`endpoint_catalog_group`)를 떠나는 간선을 가진 `collect` 작업 전부다. 한 번도 켜진 적 없는 작업만
   `landed_within_hours: null` 과 `no_landed_bound_reason` 을 적을 수 있다.
3. **`landed` 는 원천을 끝까지 읽은 run 이다.** `changed` 또는 `unchanged` 로 끝난 run 이 채운다. 새 공고가 없는
   게시판을 끝까지 읽은 것도 읽은 것이다. 조정기의 `current`(산출물이 입력만큼 새롭다)는 채우지 못한다: 아무도
   읽지 않은 원천에 대해 아무 말도 하지 않는다.
4. **꺼져 있어도, 멈춰 있어도 센다.** `scripts/ops/job_outcomes.py` 는 `landed` 를 선언한 작업의 시계열을
   `enabled` 와 상관없이 낸다(유닛·주인·런북 줄도). 기준 시각은 원장뿐이다: 마지막으로 읽은 때, 없으면 원장의
   첫 줄. `enabled_since` 로 다시 세지 않는다. 껐다 켠다고 잃은 밤이 돌아오지 않는다.
5. **기한에서 바로 page 다.** `FoundationSourceNotRead` 는 기한을 넘기고 15 분 뒤 `severity: page` 로 울린다
   (기존 page 길로 Slack). ticket 단계가 없고, 배포가 DAG 을 멈춰 둔 동안에도 울린다. 제공자는 그동안에도
   내린다. `FoundationJobOutcomeMissing`·`Long` 은 `landed` 를 뺀다(한 사건, 한 알림).
6. **기한은 작업의 한 차례보다 길다.** 일정 한 주기 + `time_limit_minutes` 이상이어야 한다.

| 작업 | 기한 | 근거 |
|---|---|---|
| `court_auction_collect` | 30 시간 | 하루 + 상한 300 분 + 1 시간. 카탈로그의 원천 기한과 같다 |
| `onbid_public_auction_collect` | 6 시간 | 이미 잰 `change` 기한(437 run 모두 `changed`) |
| `industrial_complex_notice_collect`, `industrial_land_sale_notice_ilis_collect`·`_kicox_collect` | 24 시간 | 매시 작업, 하루 못 읽음. 내려가는 속도는 재지 않았다 |
| `industrial_land_sale_notice_factoryon_collect` | 30 시간 | 하루 + 상한 120 분 + 늦은 시작 |
| `industrial_land_sale_notice_lh_collect` | 없음 | 한 번도 켜지지 않았다(제공자 관문이 거절, ADR-0603 §6) |

**가드가 막는 실제 사고**(ADR-0001): `orchestration/tests/test_job_outcomes.py` 의
`ASourceWhoseProviderDropsWhatEndedIsHeldToItsReads` 는 위 18 밤을 오늘 정의로 다시 돌려, 꺼진 16 밤이 둘째
밤이 오기 전에 잡히고 11-12 밤이 다음 밤 전에 잡힘을 보인다. 또 카탈로그가 사실을 적은 그룹의 수집 작업이
`landed` 를 선언하지 않으면 거부한다.

## 대기업·오픈소스는 어떻게 하나

- Google SRE Workbook(Alerting on SLOs)과 Prometheus 문서(Alerting 실무)는 증상에 알리고, 되돌릴 수 없는
  손실은 사람을 바로 부르라고 한다. 되찾을 수 있는 지연과 되찾을 수 없는 손실을 같은 등급에 두지 않는다.
- Robust Perception 의 "기한을 시계열로"(ADR-0480 이 이미 쓴다): 작업마다 기한을 시계열로 내고 규칙 하나가
  비교한다. 이 결정은 새 규칙 체계를 만들지 않고 그 시계열에 `expectation="landed"` 하나를 더한다.
- Dagster 의 freshness check 는 자산의 마지막 materialization 나이를 일정과 무관하게 본다. 같은 생각이다:
  신선도는 일을 시키는 스케줄이 아니라 결과에 묶인다.

## Consequences

- 수집 작업을 끄는 PR 은 그 원천이 끝난 것을 내리면 기한 뒤 page 를 받는다. 의도한 끔이면 카탈로그의 사실이나
  작업 선언을 근거와 함께 고쳐야 한다.
- 작업을 정의에서 지우면 시계열도 사라진다. 그때는 시험이 막는다: 사실을 적은 그룹을 읽는 수집 작업이 그래프에
  하나도 없으면 거부한다.
- `ilis_address` 그룹의 공고 목록 엔드포인트는 적지 않았다. 그 그룹을 떠나는 간선을 가진 수집 작업이 그래프에
  없어서, 적으면 지킬 작업을 이름 지을 수 없다. 그래프가 그 읽기를 말하게 된 뒤에 적는다.
- 온비드와 게시판 셋의 기한은 잰 값이 아니다. 실제로 내려가는 속도를 재면 `landed_basis` 와 함께 고친다.
- 11-07 밤에 무엇이 울렸는지는 보관 기간 밖이라 확인하지 못했다.
