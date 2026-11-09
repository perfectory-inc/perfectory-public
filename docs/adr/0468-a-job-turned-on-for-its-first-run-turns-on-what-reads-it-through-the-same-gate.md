# ADR 0468: 첫 run 으로 켠 작업은 그것을 읽는 작업도 같은 관문으로 켠다

- Status: Accepted
- Date: 2026-11-09
- Builds on: [ADR-0457](./0457-a-job-never-run-takes-its-first-run-unattended-bounded-by-its-unit-and-the-run-is-the-measurement.md)
  (사람 없는 첫 run: `first_run` 의 계산값·`as_it_goes`·`retries: 0`), ADR-0435(자동 승격), ADR-0464·ADR-0465(건축인허가 묶음·주변 레인)

## Context

**사고 (W30f, 2026-11-09 16:20Z 잰 값).** ADR-0457(#1201)이 첫 Silver 적재 10 개를 사람 없이 켰다: 건축인허가 묶음 7 레인,
정기점검이력, `data_go_kr_file_data`(주변 레인의 공항·역·학교·상가 등 11 표). 그 Silver 를 읽는 작업은 꺼진 채였다:

| 꺼진 작업 | 읽는 것 | 꺼진 이유(정의) |
|---|---|---|
| `gold_parcel_building_permits_rebuild` | 건축인허가 묶음 Silver 11 개(ADR-0464 가 8 개 더함) | 첫 전국 빌드를 사람이 재야 함 |
| `by_pnu_parcel_building_permits_bake` | 그 Gold | Gold 와 함께 |
| `gold_parcel_nearby_rebuild` | `data_go_kr_file_data` 의 Silver 7 개, 필지 경계 | Silver 가 없고 첫 빌드를 재야 함 |
| `by_pnu_parcel_nearby_bake` | 그 Gold | Gold 와 함께 |

첫 발행은 자동 승격(ADR-0435)의 cut-over 가 한다. cut-over 의 gold 단계는 이 작업들이 꺼져 있어도 Gold 를 짓는다
(11-09 parcel-building-permits gen1·gen2 가 그렇게 지어졌다, `gold.parcel_building_permits` 3,416,149 행). 그러나 발행
뒤로 Silver 가 바뀌면 그것을 Gold·서빙에 옮기는 것은 이 작업들뿐이다. 꺼져 있으면 첫 발행의 판이 영원히 남는다. ADR-0457 이
없앤 "사람이 한 번 손으로 돌려야 켜진다" 관문이 읽는 쪽에 그대로 남아 있었다.

ADR-0457 은 켜는 쪽만 말했다. 무엇을 위해 켜는가(화면에 보이려고)는 읽는 쪽이 켜져야 끝난다. 사장 규칙(11-07·11-09):
정상 경로에 사람 단계가 없다.

## Decision

1. **첫 run 으로 켠 작업을 읽는 작업은 켜져 있다.** `orchestration/tests/test_per_run_cap.py::idle_readers`: `first_run` 을
   적고 켜진 작업이 만드는 노드(pipeline graph 의 `to`)를 읽는 작업(`from`, 자기가 만드는 것은 빼고)이 꺼져 있으면 거부한다.
   켜진 읽는 쪽은 자기 첫 run 을 같은 관문(ADR-0457 §1–2: 계산값 ≤ 제한의 90%, `as_it_goes`, `retries: 0`)으로 지나야 하고,
   그 자신이 `first_run` 이므로 규칙이 사슬을 따라 굽기까지 내려간다. 측정이 끝나 `first_run` 이 지워진 작업의 읽는 쪽은 이 규칙
   밖이다(그때는 측정된 작업의 결정이다).
2. **이 PR 이 켜는 작업.** 위 표의 넷. 계산값(정의의 `estimated_from` 에 출처):
   - `gold_parcel_building_permits_rebuild` 1,800 s / 8,100 s: 그 레인 cut-over 의 gold 단계(같은 유닛의 운영자 사본)가 잰
     전량 빌드 203 s(Silver 3 개, 6,527,521 건). ADR-0464 의 8 파일로 행이 약 4 배.
   - `by_pnu_parcel_building_permits_bake` 900 s / 6,210 s: 그 레인 cut-over 의 bake 단계 163·68·159 s(3,416,149 문서).
   - `gold_parcel_nearby_rebuild` 5,400 s / 8,100 s: 한 번도 안 돎. 같은 템플릿의 parcel-ownership 전량 빌드가 39,396,906 필지와
     약 1.2 억 소유 행에 1,602 s(11-05 잰 값). 주변은 필지 경계 한 번 읽기, 최근접 다섯, H3 고리 7.6 억 행(필지마다 19 칸)과
     방송된 상가 칸 수의 조인 — 그런 통과 세 번으로 계산.
   - `by_pnu_parcel_nearby_bake` 1,500 s / 6,210 s: parcel-ownership 굽기 636 s(39,396,906 문서), 주변은 39,861,511 문서.
3. **켜도 안전한 이유.** 굽기는 cut-over 전에는 `nothing to do: the pack-only lane is not cut over yet` 로 `unchanged` 다
   (`by-pnu-serving-bake.sh`). Gold 작업과 cut-over 의 gold 단계는 rebuild 잠금 하나를 나눠 한쪽이 짓고 다른 쪽은
   `nothing to do` 이거나 잠금 대기다. 어느 쪽이 먼저 짓든 같은 Gold 다.
4. **두 굽기는 조정기가 맡는다.** 켜진 by-PNU 굽기는 모두 조정기의 `reconciles` 에 있다(`test_job_dags`, 2026-10-29 패널 굽기의
   사고). 그런데 cut-over 전 굽기는 읽은 판을 남기지 않아(`reads.jsonl` 없음) 조정기가 매 틱 "아직 읽은 것 없음 → owed" 로
   보고, `publish` 작업의 owed 자산은 자동 승격의 모든 첫 발행을 막는다(`by-pnu-auto-promote.sh`, ADR-0439). 그래서 읽은 판이
   없는 레인은 cut-over 증거(`by-pnu-bake/cutover/*.jsonl`)에 `publish` 단계가 통과한 적이 있을 때만 owed 다
   (`asset_reconcile.cut_over`). 그 전에는 `waiting`(첫 발행은 자동 승격의 몫). 11-09 의 parcel-ownership 처럼 cut-over 로 발행된
   레인도 `reads.jsonl` 이 없으므로, 발행 뒤 첫 틱에 굽기가 owed 가 되어 읽은 판을 남긴다.

## 버린 대안

- **모든 꺼진 읽는 쪽을 거부하기.** 소유·부속지번·에너지·멸실·규제구역·공장·대장·실거래·경매 Gold 9 개도 켜진 Silver 를
  읽으며 꺼져 있다. 그들은 첫 전국 빌드가 제한을 넘거나(규제구역 9,610 s 에 멈춤, 11-07) 따로 잰 결정이 남아 있어, 이 ADR 의
  사고와 다른 결정이다. 그 결정을 바꾸는 PR 이 이 규칙을 넓힌다.
- **cut-over 가 끝나면 작업을 자동으로 켜기.** 켜짐은 배포가 sudoers·DAG 을 만드는 정의의 사실이다(ADR-0122 §4). 호스트가
  스스로 정의를 바꾸면 정본이 둘이 된다.

## Consequences

- 건축인허가·주변 레인: 첫 발행 뒤 Silver 가 바뀌면 다음 입력 사건(또는 10:15·10:45 대체 일정)에 Gold 가 부분 갱신되고 굽기가
  그 커밋을 굽는다. 이 ADR 전: 첫 발행의 판에서 멈춤.
- 주변 Gold 는 Silver 가 생기는 run 의 출력 사건으로 바로 시작된다(`started_by: inputs`). 첫 전국 빌드가 150 분을 넘으면 유닛의
  `TimeoutStartSec` 가 멈추고 자동 승격의 gold 단계가 다시 시도한다(ADR-0435 개정 ②). 그 run 이 측정이 되고, 측정 PR 이
  `first_run` 을 `measured_run_seconds` 로 바꾼다(ADR-0457 §3).
- 계산이 틀릴 수 있다: 주변 Gold 의 5,400 s 는 같은 모양의 잰 run 이 없다. 상한은 계산이 아니라 유닛이다.
- 비용 0(시험은 파일 읽기). 켜진 작업의 계산은 그 작업이 원래 쓸 계산과 같다(spark 풀 안).
