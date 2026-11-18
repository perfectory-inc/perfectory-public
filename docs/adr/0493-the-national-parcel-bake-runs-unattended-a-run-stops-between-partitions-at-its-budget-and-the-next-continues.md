# ADR 0493: 전국 필지 굽기는 사람 없이 돈다 — 한 run 은 예산에 이르면 시도 사이에서 멈추고, 다음 run 이 이어 굽는다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0457](./0457-a-job-never-run-takes-its-first-run-unattended-bounded-by-its-unit-and-the-run-is-the-measurement.md)
  (사람 없는 첫 run), [ADR-0386](./0386-every-job-states-its-per-run-bound-in-one-vocabulary-and-its-measured-runs-fit-its-unit.md)
  (`deadline`), [ADR-0316](./0316-parcel-boundaries-are-partitioned-by-edition-and-range-ordered-by-pnu.md)
  (시도 조각 재사용), ADR-0492(z17 굽기)
- Amends: [ADR-0253](./0253-the-parcel-tile-bake-runs-in-two-halves-and-is-rehearsed-before-an-edition-is-served.md)
  2항(판을 옮기기 전 사람의 예행)

## Context

**사고.** 서빙 중인 필지 타일은 2026-09-28 PostGIS 오븐 판이다(정밀도 12, 측정: Catalog 의 활성 release
`40a0ce85…`, 5.37 GB). 그 뒤 레이크하우스 오븐의 두 작업 `parcel_served_gold`·`parcel_tile_bake` 는 한 번도 켜지지
않았다. 꺼진 이유는 ADR-0253 의 "사람이 예행한 뒤"와 ADR-0386 §3 의 "운영자 run 으로 먼저 잰다"였다. ADR-0457 이
뒤의 것을 풀었지만, 굽기 쪽은 그 길로도 켤 수 없었다. 2026-10-13 에 잰 전국 굽기는 GDAL 60 분, tippecanoe 109 분이다
(런북 parcel-tile-bake.md §4). 한 run 의 제한 115 분을 넘는다. ADR-0457 §1 은 계산이 제한의 90% 를 넘는 첫 run 을
켜지 않는다. 그래서 정밀도 14(ADR-0262)도, W41 의 모든 수정도 필지 지도에 닿지 못했다.

이미 있는 것: 필지는 시도마다 따로 굽고, 다 구운 시도 판은 `<work_root>/parcels-partitions/<시도>/<내용 해시>.pmtiles`
에 create-only 로 남는다(ADR-0316). 끊긴 run 이 잃는 것은 굽던 시도 하나다. 없는 것은 run 이 스스로 멈추는 지점이다.
시간제한에 걸려 죽으면 그 사이 시작한 Catalog build 가 실패로 남고, 다음 run 은 다음 날이다.

큰 곳의 방식: 큰 배치는 체크포인트를 남기며 시간 예산 안에서 끝나고 다음 실행이 이어 간다. Google 의 MapReduce·
Flume 의 재시도와 Spark 구조적 스트리밍의 `Trigger.AvailableNow`, AWS Batch 의 배열 작업이 그렇다. 우리의 같은 모양은
ADR-0386 의 `deadline`(그 초가 지나면 새 항목을 시작하지 않는다)이다. 공매·실거래 적재가 이미 쓴다.

## Decision

1. **굽기 예산.** `config/tile-bake-containers.contract.json` 의 `partition_run_budget_seconds.parcels = 4200`.
   run 시작에서 그 초가 지나면 새 시도를 시작하지 않는다. 작업의 `per_run_cap.deadline` 이 이 값을 가리킨다.
2. **조각을 먼저 굽는다, 아무것도 쓰기 전에.** `bake-lakehouse-tiles` 는 핸드오프를 읽은 뒤, Catalog build 를 시작하기
   전에 캐시에 없는 시도만 하나씩 굽는다(`warm_partitions`). 시도 판은 다 구워진 순간 create-only 로 캐시에 들어간다.
   예산에 이르면 `lakehouse-tile-bake-deferred unit=… baked=… remaining=…` 를 찍고 정상 종료한다. Catalog·R2 에는
   아무것도 쓰지 않았다.
3. **작업은 기다림으로 끝난다.** `parcel-tile-bake.sh bake` 는 그 줄을 보면 Gold 를 "준비됨" 으로 둔 채
   `foundation-job-outcome waiting <남은 시도>` 로 끝난다. 다음 시작은 캐시에 없는 시도만 굽는다. 모든 시도가 캐시에
   있는 run 이 tile-join·관문·업로드·승격을 한다. 시작은 하루 두 번(08:45·20:45 UTC, 할 일이 없으면 상태 파일만 읽고 끝난다)과 Gold 가 준비됐다는 사건이다. 두 시간마다는 spark 풀의 굶김 한계(낮은 우선순위가 run 20 번 안에 차례를 받아야 한다, `job_specs.py`)를 넘었다.
4. **두 반쪽을 켠다(ADR-0457).** `parcel_served_gold` 의 첫 run 계산은 5,400 s(대조 관문 13.5 분 + 서빙본 Gold 59 분,
   2026-10-13 잰 값에 4분의 1 여유), `parcel_tile_bake` 는 6,000 s(4,200 s 예산 + 가장 큰 시도 약 1,600 s + 멈춤
   120 s). 둘 다 `as_it_goes`·`retries: 0` 이다. 사람의 예행 대신 관문이 판을 지킨다: 대조 관문(ADR-0113 §7), maxzoom
   id 전수 관문(ADR-0262), 첫 판 동등성 관문(ADR-0133 §4·ADR-0251), create-only 업로드.
5. **새 판에 한 번만 굽는다.** 굽는 판은 계약의 `served_edition` 이다. 지금 202606 은 대조 관문이 거부한다
   (6,483,504 필지의 법정동이 공식 목록에 없음, 런북 §4). 그래서 이 PR 은 `served_edition` 이 202609 로 옮겨진 뒤(W58,
   ADR-0379 의 준비도 승격) 병합한다. 그 뒤 첫 Gold 가 202609 이고 첫 굽기가 ADR-0492 의 z17 레시피다.

## Consequences

- 첫 전국 굽기는 run 서너 번, 이틀쯤 걸친다(계산: 2026-10-13 경합 측정 GDAL 60 분 + tippecanoe 109 분에 z17 의 +44%(ADR-0492 인천 측정)를 더하면 시도 굽기 합계 약 217 분, run 당 약 70 분). 그 사이 지도는 지금 판을
  보여 준다. 승격은 모든 시도가 캐시에 있는 run 한 번에서만 일어난다.
- 다음 판부터는 바뀐 시도만 굽는다(ADR-0316). 행이 그대로인 시도는 캐시 판을 다시 쓴다.
- run 마다 핸드오프를 다시 읽고 시도 CSV 를 다시 쓴다(약 21 GB, 2026-10-13 측정의 GDAL 입력). 남은 시도가 있는
  run 의 고정 비용이다.
- 위험: 마지막 run(합치기·관문·동등성·업로드)은 따로 잰 적이 없다. 그것이 115 분을 넘으면 그 run 은 시간제한에 걸리고,
  build 는 실패로 남는다. 시도 캐시는 남는다. ADR-0457 §3 대로 첫 run 들의 저널 값이 측정이 되고, 그 PR 이
  `measured_run_seconds` 를 적는다.
- 비용: 계산 시간만 든다(호스트 안). R2 쓰기는 승격하는 run 의 판 하나(약 8.7 GB, ADR-0492 의 인천 측정으로 계산)다.
