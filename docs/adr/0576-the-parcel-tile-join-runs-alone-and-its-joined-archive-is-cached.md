# ADR 0576: 필지 tile-join 은 자기 run 에서 혼자 돈다 — 합친 판은 create-only 로 남고, 다음 run 이 관문·업로드·승격을 한다

- Status: Accepted
- Date: 2026-11-19
- Amends: [ADR-0493](./0493-the-national-parcel-bake-runs-unattended-a-run-stops-between-partitions-at-its-budget-and-the-next-continues.md)
  3항("모든 시도가 캐시에 있는 run 이 tile-join·관문·업로드·승격을 한다")
- Builds on: [ADR-0316](./0316-parcel-boundaries-are-partitioned-by-edition-and-range-ordered-by-pnu.md)(시도 판 캐시)

## Context

ADR-0493 의 결과 절은 "마지막 run(합치기·관문·동등성·업로드)은 따로 잰 적이 없다. 그것이 115 분을 넘으면 그 run 은
시간제한에 걸린다" 를 위험으로 적었다. 2026-11-19 에 그렇게 됐다(운영 저널·캐시 파일 시각, 읽기 전용 측정).

| run (UTC) | 한 일 | 끝 |
|---|---|---|
| 02:39 | 핸드오프 읽기·시도 CSV 약 40 분, 시도 44·47 굽기 | 03:46 배포가 멈춤(중단 가능) |
| 04:22 | 같은 읽기 약 40 분, 시도 48·50·51·52 굽기, 05:26 에 16 시도 모두 캐시 | 05:26 부터 tile-join, 06:17 시간제한(115 분) |

tile-join 은 51 분을 넘게 돌고도 끝나지 않았다. 시도 판 16 개는 합해 약 8.7 GB 다. tippecanoe 2.79.0 의 tile-join 은
타일을 그대로 잇지 않는다. 모든 타일을 풀어(`tile-join.cpp` 196 행 `tile.decode`) 다시 묶고, 결과를 먼저 sqlite 에 쓴 뒤
PMTiles 로 옮긴다(1825 행 `mbtiles_map_image_to_pmtiles`). 그래서 전국 합치기는 그 자체로 긴 한 단계다. 시도가 모두
캐시에 있는 run 도 핸드오프 읽기 약 40 분 + 합치기 51 분 이상 + 관문·업로드를 한 run 에 넣어야 해서 115 분 안에 든다는
근거가 없다. 합치다 죽으면 합친 결과는 남지 않고, 다음 run 은 같은 일을 처음부터 한다. 그러면 지도는 끝내 승격되지 않는다.

## Decision

1. **합치기는 자기 run 이다.** 모든 시도가 캐시에 있고 합친 판이 아직 없으면, run 은 Catalog·R2 에 아무것도 쓰기 전에
   `<work_root>/parcels-partitions/join-next.json`(Gold 스냅샷과 시도 판 목록)을 남기고
   `lakehouse-tile-bake-deferred … remaining=tile-join` 으로 끝난다. 작업은 ADR-0493 §3 대로 `waiting` 이다.
2. **합치기 run 은 핸드오프를 읽지 않는다.** 다음 시작은 서빙 요약의 Gold 스냅샷이 `join-next.json` 과 같고 그 시도 판이
   모두 있으면, 읽기·패치·CSV 없이 tile-join 만 돌린다. 결과는
   `parcels-partitions/joined/<시도 판 이름들의 SHA-256>.pmtiles` 에 create-only 로 남는다(다 만들어진 뒤 하드 링크).
   그리고 `remaining=gate,upload,promote` 로 미룬다. 한 run 의 일은 tile-join 하나다.
3. **그다음 run 이 관문·업로드·승격을 한다.** 읽기 뒤 시도 판 이름이 같으면 합친 판을 링크해 쓰고 tile-join 을 건너뛴다.
   시도 하나라도 다시 구워졌으면 이름이 달라 그 run 이 다시 합친다(옛 합친 판은 쓰지 않는다).
4. 한 번에 끝나는 판은 없다: 첫 전국 판은 굽기 run 들 → 합치기 run → 승격 run 이다. 하루 두 번(08:45·20:45 UTC)이면
   캐시가 다 찬 뒤 하루 안이다.

## Consequences

- 합치기 run 의 길이는 아직 모른다(51 분 넘게 걸리다 멈췄다). 그 run 이 처음 잰다. 115 분 안에 안 끝나면 이 ADR 로도
  모자라다. 그때 다음 단계는 겹치지 않는 타일을 바이트 그대로 옮기고 시도 경계에 걸친 타일만 합치는 Rust 합치기다.
- 디스크: 판마다 합친 판 약 8.7 GB 가 `/data` 에 더 남는다. 시도 캐시처럼 지우는 길이 아직 없다. 옛 판 정리는 시도 캐시와
  함께 다룰 빚이다.
- 승격 run 의 관문(전 타일 id 관문·첫 판 동등성)과 업로드도 따로 잰 적이 없다. 그 run 의 저널이 측정이다.
