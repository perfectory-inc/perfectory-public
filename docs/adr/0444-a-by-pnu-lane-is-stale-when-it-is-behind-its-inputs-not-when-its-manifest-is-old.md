# ADR 0444: by-PNU 레인은 manifest 가 오래됐을 때가 아니라 입력보다 뒤처졌을 때 묵은 것이다

- Status: Accepted
- Date: 2026-11-07
- Supersedes: [ADR-0306](./0306-a-by-pnu-lane-alerts-when-what-it-serves-is-older-than-its-freshness-budget.md) §1(판정)·Alternatives rejected 셋째 줄 — 나이를 재고 알리는 것(§1 측정·§2 예산·§3 `passed` 분리·§4 런북)은 그대로
- Builds on: [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md) §7(자산 조정기), [ADR-0442](./0442-production-readiness-is-one-table-a-day-built-from-the-definitions-sorted-by-what-users-see.md)(운영 준비도 표)
- 관련 사건: 2026-11-07 — 서빙 모니터 9개가 매시 실패했다. 그중 parcel-zoning·parcel-price·parcel-characteristics·parcel-land-rights 넷은 manifest 가 497~498시간 묵었지만, 굽기가 매일 "nothing to do: the served section packs already reflect Gold snapshot …" 를 말했고 그 Gold 의 Silver 고정판은 지금 Silver 머리와 같았다(Trino `$snapshots`, 읽기 전용). 사용자는 있는 것 중 가장 새 데이터를 보고 있었다. 같은 날 parcel 레인(202시간)은 정말로 10-26 뒤의 토지이동이력 적재를 서빙하지 못하고 있었는데, 경보 문구는 둘을 구분하지 못했다.

## Context

ADR-0306 은 서빙 manifest 의 `published_at_utc` 나이가 48시간을 넘으면 모니터 실행을 실패시키고(§1) Prometheus
규칙 `FoundationByPnuServingStale` 로 알린다. 예산 48시간의 근거는 "모든 레인이 매일 갱신된다" 였다(§2). 그러나
연 1회·월 1회 공개되는 원천(용도지역·공시지가·토지특성·토지권리)의 레인은 입력이 움직이지 않으면 발행할 것이
없다. 굽기는 올바르게 아무것도 하지 않고, 모니터는 그 레인을 영원히 실패시킨다. 매시 실패하는 경보 넷이 정말
멈춘 레인 다섯(parcel·building·transfer-history·forest-ledger·unit-price) 사이에 섞여 무엇이 진짜인지 가렸다.

ADR-0306 은 "Gold 대비 지연" 을 기각했다: Gold 재생성이 멈추면 manifest 와 Gold 가 같아 초록이 된다는 이유였다.
그 이유는 Gold 하나만 볼 때 맞다. 자산 조정기(ADR-0333 §7)는 그 뒤에 생겼고, 이미 각 레인이 서빙하는 Gold 판
(`reads.jsonl`)과 각 Gold 가 커밋에 적은 Silver 고정판(`foundation.source-iceberg-snapshots`)을 5분마다 읽는다.
둘을 이으면 "사용자가 아직 못 받은 가장 오래된 입력 변경이 언제 커밋됐나" — 레인의 **데이터 시각**을 잴 수 있다.

업계의 같은 측정:

- Dagster 의 freshness policy 는 자산의 "data time"(그 시각까지의 모든 상위 원천 변경을 반영한 시각)으로 지연을
  잰다. 마지막 materialization 시각이 아니다. 상위가 움직이지 않으면 오래된 자산도 신선하다.
- Kafka 의 consumer lag(시간 기준, Burrow·LinkedIn)은 "처리 안 된 가장 오래된 메시지의 나이" 다. 새 메시지가
  없으면 마지막 커밋이 언제든 지연은 0 이다.
- Google SRE Workbook 데이터 파이프라인 장(ADR-0306 의 근거)도 신선도 SLI 를 "가장 최근 입력이 출력에 반영되기까지"
  로 정의한다. 산출물의 나이는 입력이 꾸준히 올 때만 그 대리값이다.

## Decision

1. **데이터 시각을 자산 조정기가 잰다.** `scripts/ops/asset_reconcile.py` 는 by-PNU 레인 자산마다
   `foundation_by_pnu_lane_behind_seconds{lane}` 를 쓴다: 레인이 서빙하는 Gold 판 뒤에 행을 바꾼 Gold 커밋, 그리고
   그 Gold 판이 적은 고정판 뒤에 행을 바꾼 Silver·reference 커밋 중 가장 오래된 것의 커밋 시각부터 지금까지(초).
   모두 서빙하면 0. "행을 바꿨다" 는 플래너와 같은 규칙(`gold_rebuild.input_change_since_pin`: 압축·빈 커밋은 변경
   아님)이다. 서빙하는 판이 메타데이터에서 만료됐으면 Gold 머리의 고정판을 본다. 날짜를 댈 수 없으면(읽은 판 없음,
   고정판 없음, 커밋 시각 없는 변경) 쓰지 않는다.
2. **경보 규칙 하나가 판정한다.** `FoundationByPnuServingStale` 은 `behind_seconds` 가 예산(48시간, ADR-0306 §2)을
   넘을 때 울린다. 조정기가 그 레인의 데이터 시각을 대지 못하면(`behind_seconds` 없음) 예전처럼 manifest 나이로
   판정한다 — 모르면 묵은 쪽으로 본다.
3. **모니터 실행은 서빙 충실도만 판정한다.** `monitor-<lane>-by-pnu-serving` 은 보고서에 나이와 `stale`(예산 초과)을
   계속 적고 지표로 내지만, 그것으로 실행을 실패시키지 않는다. 묵었는지는 입력을 아는 조정기의 값으로 규칙이 판정
   한다. 유닛 실패와 규칙이 같은 사실을 두 번 알리던 것도 없어진다(ADR-0306 §3 의 `passed` 분리는 그대로).
4. **운영 준비도 표도 같은 값을 쓴다.** `readiness_report.py` 의 레인 줄은 `behind_seconds` 가 있으면 그것으로,
   없으면 manifest 나이로 `stale` 을 정하고 `behind_hours` 를 함께 적는다.

가드가 막는 실제 사고(루트 ADR-0001): **2026-11-07 네 레인이 있는 것 중 가장 새 데이터를 서빙하는데 498시간 묵었다고
매시 실패해, 정말 멈춘 parcel·building 레인 경보를 가렸다.** 시험: `test_asset_reconcile.py`(같은 모양 — 압축만 있는
Silver 의 zoning 은 0, 서빙 못 한 적재가 있는 parcel 은 그 적재부터), `host-resources.rules.test.yml`(498시간 manifest +
뒤처짐 0 은 조용, 날짜 없는 공매는 나이로 울림), `test_readiness_report.py`, `monitor.rs` tests.

## Consequences

- 용도지역·공시지가·토지특성·토지권리 레인은 원천이 새 판을 낼 때까지 초록이다. 새 판이 Silver 에 들어오면 그
  커밋 시각부터 뒤처짐이 쌓이고, 48시간 안에 서빙되지 않으면 울린다.
- Gold 재생성이 멈춘 경우(ADR-0306 의 기각 이유)도 잡는다: Silver 가 움직였고 Gold 고정판이 그 뒤에 있으므로
  뒤처짐은 Silver 커밋부터 센다. 2026-11-07 parcel 레인은 이 값으로 약 290시간이었다.
- Silver 가 Bronze 보다 뒤처진 것(적재 실패, 예: 11-04 이후 flate2 디코드 실패)은 이 값에 없다. 그것은 Silver 갱신
  유닛의 실패 경보와 수집 신선도 경보(ADR-0443)의 몫이다.
- 조정기가 멈추면 `behind_seconds` 가 낡은 값으로 남는다. `FoundationAssetReconcilerStopped`(30분)가 그것을 알린다.
- 새 유닛·새 타이머·새 상태 파일은 없다. 조정기가 이미 읽는 카탈로그 이력과 `reads.jsonl` 에서 계산한다.

## Alternatives rejected

- **레인별 예산(원천 공개 주기만큼)** — 연간 원천 레인에 400일 예산을 주면 그 레인의 굽기가 정말 멈춰도 1년을
  모른다. 주기는 입력이 언제 오는지를 말할 뿐, 온 입력이 반영됐는지는 말하지 않는다.
- **입력이 없으면 굽기가 manifest 를 다시 발행(하트비트)** — 서빙 상태를 매일 새로 쓰는 것은 append-only 와 부분
  처리 원칙에 어긋나고, 나이가 "확인했다" 를 뜻하게 되어 반영 여부를 여전히 말하지 않는다.
- **모니터(Rust)가 카탈로그를 읽어 직접 판정** — 행 변경 규칙(`input_change_since_pin`)이 Python 플래너에 있다.
  Rust 로 두 벌을 두면 둘이 갈라진다. 이미 그 규칙으로 5분마다 판정하는 조정기가 값을 낸다.

2026-11-08 개정 주석: §1 의 "서빙하는 Gold 판이 적은 고정판" 은 서빙 판 뒤에 행을 바꾼 Gold 커밋이 없으면 **Gold 머리의 고정판**이다 — 머리가 고정판만 새로 적은 빈 커밋이면 레인이 서빙하는 행은 머리의 행과 같다. 2026-11-08 12:32Z `gold.building_panel` 이 바뀐 PNU 0 으로 새 Silver 고정판만 적었는데(빈 append), 굽기는 굽을 행이 없어 읽은 판을 그대로 두었고 building 레인은 서빙 판의 옛 고정판으로 106시간 뒤처졌다고 읽혔다. 같은 날 자산 조정기(ADR-0333 §7)의 `reconciles` 에 `gold_kind_rebuild` 를 더했다: kind Gold 는 host 패널이 고정한 판(`pins_from.inputs: all`, 그 kind 가 적은 입력)이 움직이면 owed 다. 11-07·11-08 kind 재생성의 매일 시작이 spark 풀 대기(`waiting`)로 끝났고 다시 시작하는 것이 없어, parcel-transfer-history 레인이 host 패널이 지나간 Gold 를 133시간 서빙했다. 그 대기는 kind 재생성이 `default` 등급이라 lane cut-over 의 `first_publish` 청구에 밀린 것이어서, 패널 재생성처럼 `priority_class: publish`·`priority_class_from: by_pnu_serving_bake`(ADR-0439)를 받는다.
