# ADR 0253: 필지 타일 굽기는 두 작업으로 나뉘고, 새 판은 지도에 올리기 전에 예행한다

- Status: Accepted
- Date: 2026-10-15
- Amends: [ADR-0133](./0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md) §7 (작업 하나 → 둘)
- Builds on: [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md) §5,
  [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md),
  [ADR-0251](./0251-a-first-parcel-release-on-a-new-edition-counts-parcels-that-left-it.md)

## Context

ADR-0133 §7은 `parcel_tile_bake` 작업 하나가 대조 관문, 서빙본 Gold, 굽기, 승격을 모두 하게 했다. 그 작업은 spark 풀
3자리를 쥔다. spark 풀의 굶김 한계 때문에 작업 하나는 165분을 넘길 수 없다.

2026-10-14 운영 서버에서 202609 판 전국(39,876,546필지)을 같은 이미지·인자로 쟀다. 다른 무거운 작업이 없는 상태였다.

| 단계 | 시간 |
|---|---:|
| 대조 관문 | 13.5분 (10-13, 202606) |
| 서빙본 Gold | 59분 (10-13, 202606) |
| 핸드오프 → GDAL 입력 | 약 8분 |
| GDAL `ogr2ogr -makevalid` | 18.3분, 최대 171MB |
| tippecanoe z14–16 | 49.4분, 최대 4.21GB, 결과 5.55GB |
| maxzoom id 검사, 첫 판 동등성, 업로드 | 약 15분 (추정) |

합계는 약 166분이다. 한계를 넘는다. 10-13에 같은 디스크에서 PostGIS 스캔과 겹쳐 잰 값은 약 4시간이었다.

판을 고르는 문제도 있다. 필지 패널 Gold도 계약의 `served_edition`을 읽는다. 그래서 타일만 새 판으로 올리면, 번호가 바뀐
필지를 눌러도 패널이 없다. 판을 바꾸는 일은 계약 한 줄로 타일과 패널을 함께 옮기는 결정이다. 그 결정 전에 새 판이
관문을 통과하는지, 시간 안에 구워지는지, 동등성 분류(ADR-0251)가 어떻게 나오는지를 알아야 한다. 그런데 지금 작업은
서빙 판만 굽고, 통과하면 곧바로 승격한다.

## Decision

1. **작업 둘.** 둘 다 spark 풀 3자리를 쓰고, 시간 한계는 120분이다. 유닛은 템플릿
   `foundation-parcel-tile-bake@.service`(115분) 하나다.
   - `parcel_served_gold`(`@gold`): 대조 관문과 서빙본 Gold를 돌린다. 끝나면 그 실행을 "굽기 대기"로 적고 `changed`를
     낸다. 관문이 거부하면 아무것도 쓰지 않는다.
   - `parcel_tile_bake`(`@bake`): 대기 중인 실행을 굽고, 관문을 돌리고, 승격한다. Gold 작업이 `changed`를 내면 시작하고,
     매일 한 번 대체로도 돈다. 대기가 없으면 할 일 없음이다.
   - 오븐(GDAL·tippecanoe)은 Spark가 아니지만 spark 풀에 둔다. 호스트 메모리 예산은 "Spark 작업 하나 + 오븐"을 함께 센다.
     오븐이 다른 Spark 작업 옆에서 돌지 않게 하려는 것이다.
2. **예행.** 사람이 `by-pnu-pack-operator.sh parcel-tiles rehearse gold`로 시작하고, 끝나면 `... rehearse bake`를
   돌린다. 각각 `@rehearsal-gold`·`@rehearsal-bake` 인스턴스로 돌고, 시간 한계는 같다.
   - 계약이 가진 가장 새 판을 굽는다.
   - 관문의 거부는 기록만 하고 멈추지 않는다.
   - 서빙본 Gold는 다른 판처럼 쓴다. 굽기가 승격하기 전에는 아무것도 서비스하지 않는다.
   - 굽기는 오븐의 예행 모드(`FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_REHEARSAL=1`)로 모든 관문을 돌린 뒤 멈춘다. build
     행, 업로드, 승격, 접기를 하지 않는다. 값은 `0`이나 `1`만 받는다. 오타는 거부한다.
   - 결과 줄 `lakehouse-tile-bake-rehearsal`이 판 크기, id 다이제스트, 동등성 건수를 남긴다.
3. **판을 바꾸는 것은 계약이다.** 예행 결과를 보고 사장이 `served_edition`을 옮긴다. 그 뒤 두 작업이 그 판을 굽고
   승격한다. 패널도 같은 계약을 읽으므로 함께 따라간다.
4. **작업을 켜는 조건.** 예행의 두 반쪽이 각각 115분 안에 끝나야 한다. GDAL을 조각별로 병렬화하는 것은 다음 변경이다.
   목표는 전국 120분 이하다.

## Consequences

- 새 판이 나온 날, 두 작업은 각자 굶김 한계 안에 든다. 다른 3자리 작업(09:15 종류 Gold, 09:30·09:50 경계)은 굽기 뒤로
  밀릴 수 있다. 판이 바뀌는 날에만 그렇다.
- 예행은 Gold 스냅숏을 하나 더 남기고, `/data`에 실행 폴더를 하나 더 남긴다(약 25GB). R2에는 쓰지 않는다.
- 옛 유닛 `foundation-parcel-tile-bake.service`는 한 번도 돌지 않았다. 릴리스가 지운다.
