# ADR 0490: 새 지적 판은 사람 없이 서빙판이 되고, 서빙판이 Silver 의 최신 판보다 뒤처지면 알린다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0291](./0291-the-served-cadastral-edition-is-operator-chosen-and-readiness-gated.md),
  [ADR-0379](./0379-a-cadastral-edition-is-served-only-through-a-readiness-gated-promotion-recorded-in-the-contract.md)(준비 판정·승격 원장),
  [ADR-0381](./0381-a-promoted-edition-is-rebuilt-only-where-editions-differ-and-every-lane-switches-together.md)(다른 PNU 만 재빌드·레인 장벽),
  [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(사람 없는 자동 승격),
  [ADR-0329](./0329-official-evidence-supersedes-a-recorded-inference-automatically.md)(코드 변경 보기),
  [ADR-0482](./0482-a-parcel-source-is-placed-on-the-served-editions-form-of-its-code-and-a-codeless-public-sale-by-its-address.md)(서빙판 코드 형태로 놓기)
- Amends: ADR-0381 §4 — 승격 요청은 콘솔의 사람이 아니라 판정 작업 자신이 한다(콘솔 요청은 예외 경로로 남는다)

## Context

2026-11-11 서빙판은 여전히 202606 이다. 2026-07-01 개편(전남광주 통합, 인천) 전의 판이다. 202609 는 2026-10-05 부터 Silver 에 있었다.
W53b 의 실측으로는 그 뒤 바뀐 코드마다 해석이 실패했다(공매 4,122, 경매 6,020, 경매 결과 1,072 행). 202609 에만 있는 필지는 서빙되지 않는다.
판은 사람 손 없이 움직일 수 없었다. 원인은 여섯 겹이었고, 모두 실측했다(호스트 journal, Trino 읽기 전용).

| # | 막힌 곳 | 실측 |
|---|---|---|
| 1 | 준비 판정이 Silver 를 못 읽음 | 11-04 배포부터 매 run `silver not measured: Non-terminated statement`(Trino CLI `--file` 에 `;` 없음). 202609 는 `not ready: silver` |
| 2 | 대조 관문 무관용 | 202609 의 22,740 필지가 최신 코드 목록에 없다. 22,738 은 대구 달성군 구지면이다(공식 쌍 2771038xxx→2771026xxx, 2026-09-30 시행, 판보다 뒤). 나머지 2 는 쌍 없는 제공자 코드다. 판이 코드 변경보다 늦는 한 어느 판도 통과 못 한다 |
| 3 | 리허설은 사람이 침 | 유일한 리허설(10-15)은 관문 오류(그 뒤 고침)와 최대 줌 id 관문(그 뒤 ADR-0262 가 서브픽셀 허용)으로 실패했다. 다시 친 사람이 없다 |
| 4 | 승격은 콘솔 요청 + PR | `promotions` 원장은 첫 항목(202606, #339)뿐이다. 요청이 한 번도 없었다 |
| 5 | 굽기 두 반쪽이 꺼짐 | `parcel_served_gold`·`parcel_tile_bake` 는 `enabled: false` 다. `HELD_OFF` 이유는 "사장이 served_edition 을 옮긴 뒤" |
| 6 | 등록부가 한 판만 앎 | `silver.parcel_registry` 39,861,511 행이 모두 `current` 이고, 수는 202606 의 필지 수와 같다. 202609 필지 6,517,338 이 현재 id 가 없다. 대조 관문 (b) 가 거부한다 |
| — | 아무도 몰랐음 | 37 일 동안 알림 0 |

지도 타일은 PostGIS 시절 release(`parcels-40a0ce85…`, 5.37GB)를 서빙한다. 레이크하우스 굽기는 한 번도 승격되지 않았다(`baked-edition` 없음).

업계: Argo Rollouts·Flagger·Kargo 는 분석(AnalysisRun·검증 단계)을 통과한 후보를 **자동 승격**하고, 실패하면 자동으로 멈추거나 되돌린다.
사람은 알림으로만 낀다(Google SRE canarying). 데이터 쪽 WAP(Netflix·Tabular, ADR-0334)는 쓰고, 감사하고, 함께 게시한다.
dbt 시험의 `warn_if`/`error_if` 와 Deequ 제약의 비율 경계는 "몇 건이면 멈추나"를 숫자로 선언한다.

## Decision

1. **판정은 Silver 를 다시 잰다(#1259).** 파티션 질의를 다른 호스트 읽기처럼 `--execute` 로 보낸다.
   가짜 docker 시험은 CLI 처럼 끝없는 `--file` 문장을 거부한다.
2. **대조 관문은 코드 변경을 따라간다(#1265).** 공식 1:1 쌍이 있는 법정동은 이름 붙은 허용으로 센다. 그 쌍은 `renumbered_into`(ADR-0329 의 보기)로
   존재하는 코드로 옮긴 것이고, 경계는 후속 코드의 것으로 본다. 아무 쌍도 설명하지 못하는 법정동은 판마다 100 필지(0.00025%)까지
   표본으로 남기고, 넘으면 전처럼 거부한다(dbt `error_if`). 개편을 변경표가 놓치면 수천 필지라 여전히 거부된다.
3. **뒤처짐은 알린다(이 변경).** 판정 작업이 textfile 에 세 계열을 쓴다.
   - `foundation_parcel_served_edition_behind_seconds{served,newest}`: Silver 가 가진 가장 새 판이 계약 측정일(`measured_at_utc`)부터 기다린 시간
   - 예산: 계약 `readiness.max_served_lag_days`, 14 일
   - `silver_measured`, 판정 시각

   규칙은 셋이다. `FoundationParcelServedEditionBehind`(page)는 예산을 넘으면, `FoundationParcelEditionSilverUnmeasured`(ticket)는
   1 이 0 으로 숨으면, `FoundationParcelEditionReadinessSilent`(ticket)는 3 일 침묵하면 울린다. promtool 시험이 각 사고 계열에서 울리고
   건강한 계열에서 조용한지 본다. 막는 사고: 202609 가 37 일 서빙되지 않는 동안 알림 0.
4. **후보 판은 사람 없이 리허설된다(다음 PR).** 후보는 서빙판보다 새로 `source`·`silver` 를 통과한 가장 새 판이다.
   - 서빙판에 할 일이 없을 때 굽기 두 반쪽(`gold`·`bake`)이 후보를 리허설한다(서빙하지 않음).
   - 한 (판, release) 에 한 번만 한다. 실패한 리허설은 새 release(코드 수정)나 바뀐 Silver 가 와야 다시 돈다.
   - 첫 원장 항목의 판(202606)은 레이크하우스 이전 release 가 서빙하므로 다시 굽지 않는다.
   - 두 작업을 켜고 `HELD_OFF` 줄을 지운다. 첫 run 추정은 10-15 리허설 실측(gold 866 s, bake 관문까지 약 82 분)이다.
4a. **후보의 등록부는 판별로 쌓는다(다음 PR).** 등록부 행은 이미 `to_snapshot_id` 를 싣는다. 후보 판의 전이(202606 → 202609)를
   `parcel_registry` 작업이 후보 판으로 덧붙인다. 등록부를 접는 모든 읽기는 "판 X 까지"(`to_snapshot_id` ≤ X)로 접는다.
   서빙판을 읽는 쪽(place-identity, ADR-0288)은 승격 전까지 그대로이고, 후보의 대조 관문은 후보까지 접는다.
   Iceberg 의 WAP 가지(branch)처럼 쓰되 가지 대신 판 열로 가른다. 판이 이미 파티션 열이기 때문이다(ADR-0316).
5. **준비된 후보는 판정이 스스로 승격한다(다음 PR).** 판정 작업이 후보를 `ready` 로 보면 자기 이름("edition controller, ADR-0490")으로
   `promote` 를 부른다. 콘솔 요청(ADR-0381 §4)은 예외 경로로 남는다. 승격의 효력은 호스트의 덧붙이기 원장으로 낸다.
   - `vworld_parcel_editions.load()` 와 Rust `served_edition_view` 가 계약 원장 위에 호스트 원장을 겹쳐 읽는다.
   - 원장은 계약 원장의 마지막 항목을 이어야 하고, 항목마다 준비 판정을 싣는다(ADR-0379 §4 규칙 그대로).
   - Spark 컨테이너에는 읽기 전용으로 꽂는다.
   - 계약 `served_edition` 은 첫 판(부트스트랩)과 정책을 지킨다. 원장은 버킷에도 쌓는다(ADR-0476 처럼 CI 가 읽는다).
   - 서버가 저장소에 쓰는 자격증명은 만들지 않는다. 이 단계가 병합되기 전의 승격(202609)은 판정이 내준 승격 계약을 작업자가
     PR 로 옮긴다. 사장 단계는 아니다.
6. **타일까지 함께 옮긴다(다음 PR).** `edition_lockstep_gate` 에 레인 `tiles`(마지막으로 구운 판)를 더한다.
   - by-PNU 레인은 타일이 서빙판으로 구워질 때까지 기다린다.
   - 타일 승격은 장벽 안 Gold 가 모두 서빙판으로 지어질 때까지 기다린다(내보내기가 아니라 짓기 기준이라 서로 막지 않는다).
   - 화면이 판을 섞는 틈은 두 게시 사이 몇 분이다.
7. **되돌리기는 원장에 덧붙인다(다음 PR).** 승격 뒤 첫 게시의 관문(엣지 탐침·패널 대조)이 실패하면, 판정이 `from_edition` 으로 돌아가는
   항목을 덧붙인다. 레인은 같은 장치(ADR-0381 §2)로 다른 PNU 만 되돌려 짓는다. 지우는 것은 없다.
8. **부분 처리.** 패널 레인은 이미 두 판이 다른 PNU 만 다시 짓는다(ADR-0381 §2, 전국 약 12만 PNU 계산값). 타일은 이렇다.
   - 첫 레이크하우스 기본판은 선언된 전량 예외다: 지금 활성 release 는 PostGIS 판이라 feature 색인이 없다.
   - 그 뒤 판은 ADR-0323 패치로 간다. 도형 비교는 좌표를 1e-9 도로 양자화한 해시로 한다. 바이트 비교는 부동소수 잡음 때문에
     서울 76% 를 바뀐 것으로 본다(ADR-0381 측정).

## Consequences

- 이 변경 뒤 판정의 첫 run 부터 `FoundationParcelServedEditionBehind` 가 울린다(202609, 37 일 > 14 일). 202609 가 서빙될 때까지 4 시간마다 울린다.
  그것이 의도다.
- §1·§2 가 배포되면 202609 의 첫 실패 단계는 `rehearsal` 이 된다. 계산값으로 법정동 거부 0, 허용 22,738 + 2 다.
  다만 §4a 전에는 리허설 관문이 등록부(b)로 6,517,338 필지를 거부한다. 그래서 §4a·§4·§5 가 모두 서야 사람 없이 서빙판이 된다.
- 비용: 리허설 gold 약 15 분 + bake 약 85 분, 승격 뒤 실제 gold·bake 한 번 더(첫 기본판 전량, 약 5.5GB R2 쓰기, 무료 등급 내 계산값),
  패널 레인 증분 재빌드.
- 남은 일: §4–§8 의 구현. 코드 목록 최신 스냅숏이 2026-10-04 로 낡은 것은 이 ADR 범위 밖이다.

## 개정 각주 — 2026-11-11, §5 버킷 사본과 §7 되돌리기의 구현 (W58d)

결정은 그대로이고, 구현에서 정한 것을 적는다.

- **§7 의 관문은 레인의 변경 집합 대조다.** by-PNU 레인은 승격 뒤 Gold 를 엣지에 올리기 전에 서빙 중인 판과 대조한다
  (`by_pnu_panel_delta.py`, ADR-0141 §3·ADR-0346). 판이 바뀌는 것은 기록된 변경 집합(ADR-0381 §2)이라, 맞는 승격은
  `not_a_delta`(기록 없이 절반 넘게 다름)를 만나지 않는다. 엣지 탐침(`by-pnu-serving-monitor.sh`)은 레인이 서빙하는 문서끼리 비교해서
  판의 잘못을 보지 못하므로 쓰지 않는다. 타일은 리허설에서 같은 관문을 이미 통과했다.
  - `by-pnu-serving-bake.sh` 는 lockstep 장벽이 열린 뒤의 기록(`reads.jsonl`)에 `edition`(장벽 안 레인만,
    `edition_lockstep_gate.py edition <lane>`)과 거부 이름 `refused` 를 싣는다.
  - 판정은 매 run 처음에 `parcel_edition_readiness.py rollback` 을 돈다. 마지막 승격 뒤, 그 판으로 그 레인이 아직 한 번도 게시하지
    않았는데 `not_a_delta` 로 거부된 기록이 있으면, 승격이 떠난 판으로 돌아가는 줄(`rollback_of`·`cause`)을 호스트 원장에 덧붙인다.
    충돌·다른 거부·이미 게시한 뒤의 거부·다른 판의 거부는 되돌리지 않는다. 지우는 것은 없다.
  - 되돌린 판은 판정이 스스로 다시 승격하지 않는다(Kargo 가 검증에 실패한 Freight 를 자동 승격하지 않는 것과 같다). 더 새 판이 그것을
    대신하거나, 고친 뒤 콘솔 요청(ADR-0381 §4)이 다시 올린다. 배포마다 다시 올렸다 되돌리면 레인 전체가 하루 몇 번씩 판을 오간다.
  - 판정은 매시 10분에 돈다(전에는 하루 한 번). 되돌리기가 늦으면 다른 레인은 새 판, 실패한 레인은 옛 판을 서빙한다. 한 run 은 1초
    안팎이다(2026-11-04~05 실측 5 run).
- **되돌리기는 타일을 다시 굽지 않는다.** 타일 굽기의 "첫 판(부트스트랩)" 판정이 원장 길이(`len == 1`)를 읽어서, 첫 판으로 돌아가면
  레이크하우스에서 전국을 굽었을 것이다(약 85 분, 5.5 GB, 바뀌는 것 없음). 이제 서빙판이 원장의 첫 판이고 레이크하우스 굽기가 한 번도
  승격되지 않았으면(`baked-edition` 없음) PostGIS 시절 release 가 그것을 서빙한다고 본다. 승격된 판의 타일이 이미 구워졌다면 첫 판도
  다른 판처럼 굽는다(필요한 굽기다).
- **§5 버킷 사본은 ADR-0476 의 버킷을 함께 쓴다.** 같은 버킷의 `parcel-editions/` 아래에
  `objects/<SHA-256>.jsonl`(새것만, 잠금 규칙 `Indefinite`), `history/<시각>.json`, 머리 `latest.json` 을 둔다
  (`scripts/ops/parcel-promotions-publish.py`). 쓰기 열쇠는 판정 유닛의 `ExecStartPre=+` 가 매 run `parcel-editions/` 접두사만,
  20 분짜리로 받는다(`shape-ledger-writer-key.py parcel-editions/`). CI 의 suites 작업은 공개 주소에서 머리와 원장을 읽어 해시를
  맞추고, 이 트리의 계약 위에 겹쳐 읽는다(`scripts/ci/parcel-promotions-check.py`). 계약이 운영 원장을 잇지 못하면 PR 이 실패한다 —
  병합하면 첫 배포에서 모든 판독자가 멈출 계약이다. 첫 머리가 올라가기 전에는 경고하고 통과한다.
  - 열쇠 스크립트는 제어 체크아웃(`/opt/perfectory-control`)에서 돌아 한 배포 늦게 바뀐다. 그 사이 run 은 옛 스크립트가 만든
    `shape-ledger/` 열쇠를 받고, 발행 스크립트가 접두사가 다른 열쇠를 쓰지 않아 "not published" 로 넘어간다.
- 가드(ADR-0001): `test_parcel_edition_readiness.py` 의 `RollbackTest` — W58c 가 원장에 되돌리기 줄을 받게만 하고 아무도 쓰지 않던
  틈. `test_parcel_tile_bake.py` 의 `test_a_rollback_before_any_lakehouse_bake_bakes_nothing` — 원장 길이를 읽는 부트스트랩 판정.
  `scripts/ci/test_parcel_promotions_check.py` — 운영 원장과 어긋난 계약이 병합되는 것.
