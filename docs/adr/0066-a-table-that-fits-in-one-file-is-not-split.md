# ADR 0066: 한 파일에 들어가는 표는 나누지 않는다

- Status: Accepted
- Date: 2026-08-30

## Context

`silver.industrial_complex_boundaries` 는 1,343행 8 MB 를 `sido_code` 와
`bucket(32, complex_id)` 로 나눠, **파일 371개에 20 KB 씩** 담고 있었다. 파일 하나에 3~4행이다.

합치기로 고치려다 아무 일도 일어나지 않았다.

```
읽은 파일 0  ·  쓴 파일 0
```

**합치기는 같은 칸 안에서만 합친다.** 칸이 371개이고 칸마다 파일이 이미 1개라 합칠 상대가
없었다. 나누기가 파일 수의 바닥을 만들고, 합치기는 그 바닥을 못 뚫는다.

실측한 다른 표들도 같은 병이다.

```
표                              크기      칸    칸당      파일    평균
building_register_unit_areas   8.76 GB    32   274 MB     96   86.88 MB
parcel_boundaries              7.44 GB   257    29 MB    257   28.96 MB
building_register_units        1.17 GB   257   4.6 MB    257    4.44 MB
industrial_complex_boundaries  0.01 GB   371  0.02 MB    371    0.02 MB
```

**같은 저장소인데 칸당 크기가 274 MB 에서 0.02 MB 까지 흩어져 있다.** 아무도 계산해 본 적이
없다는 뜻이다.

업계 기준은 일치한다.

```
Databricks   1 TB 미만이면 나누지 마라 · 칸당 1 GB 미만이면 과하게 나눈 것
Iceberg 계열 칸당 128 MB ~ 1 GB · 나누기보다 정렬이 효과적
Netflix      목표 파일 크기 512 MB
```

우리 레이크하우스 **전체**가 16.93 GB 다. 가장 큰 표가 8.76 GB 로, Databricks 기준의 1/120 이다.

**나누기가 우리 질의를 좁혀 주지도 않는다.** 이 표들을 읽는 코드를 전수 확인했다. `bbox` 를
쓰는 곳은 전부 값 검사이고, 좌표나 시군구로 걸러 읽는 곳이 없다. 소비자는
PostGIS 미러와 타일 산출물이며, 둘 다 표를 **통째로** 읽는다. 건너뛸 것이 없는 읽기에
나누기는 파일만 늘린다.

## Decision

1. 표 전체가 목표 파일 하나(512 MB)에 들어가면 **나누지 않는다**. `partition_spec` 을 빈
   목록으로 선언하는 것이 그 표현이다.

2. 목표 파일 크기는 **512 MB** 로 한다. Iceberg 기본값이자 Netflix 가 쓰는 값이며, 파일당
   고정 비용과 병렬 처리 사이의 통상적 절충점이다.

3. `PARTITIONED BY` 절은 `partition_clause_sql()` 이 통째로 만든다. 조각 목록을 괄호로 감싸는
   방식은 빈 나누기에서 `PARTITIONED BY ()` 라는 문법 오류가 되고, 그 오류는 표를 만들려는
   순간에야 드러난다.

4. 나누기 항목을 더할 때는 **그것이 어떤 질의를 좁히는지** 적는다. 좁히는 질의가 없으면 넣지
   않는다. root ADR-0063 의 뒤집힌 짝이다 — 그쪽은 못 좁히는 항목을 빼라고 했고, 이쪽은
   좁히지도 않을 항목을 넣지 말라고 한다.

## Consequences

`silver.industrial_complex_boundaries` 가 **파일 371개에서 1개**가 됐다. 1,343행 그대로,
5.30 MB 하나다. Iceberg 의 나누기 진화(`ALTER TABLE DROP PARTITION FIELD`)와 전량 재작성으로
표를 지우지 않고 바꿨다.

**카탈로그가 연속 변경 사이에 시간 간격을 요구한다.** 나누기 항목 두 개를 한 실행에서 연달아
지우려다 두 번째가 거부됐다.

```
Invalid update timestamp ...: before the latest metadata log entry timestamp
```

R2 Data Catalog 의 동작이며, 나눠 실행하면 통과한다. 남은 표를 바꿀 때도 마주친다.

**남은 두 표는 아직 안 바꿨다.** `silver.building_register_units`(1.17 GB, 칸 257)와
`silver.parcel_boundaries`(7.44 GB, 칸 257)가 같은 처방 대상이다.
`silver.building_register_unit_areas` 는 칸당 274 MB 로 유일하게 기준에 드므로 그대로 둔다.

**정렬은 그대로 남는다.** 나누기를 없애도 `sort_order` 는 유지되며, 파일별 최소/최대 통계가
그 위에서 가지치기를 한다. 다만 그 가지치기가 실제로 얼마나 되는지는 재지 않았다 — 우리
소비자가 통째로 읽는 이상 잴 대상이 없었다.

**되돌리기는 다시 쓰는 일이다.** 나중에 나누기가 필요해지면 표 전체를 재작성해야 한다. 그
비용을 감수할 근거로 이 ADR 이 남는다.
