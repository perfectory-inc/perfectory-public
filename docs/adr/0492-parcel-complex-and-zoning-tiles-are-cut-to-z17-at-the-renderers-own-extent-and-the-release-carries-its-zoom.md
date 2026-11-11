# ADR 0492: 필지·산업단지·용도지역 타일은 z17 까지 렌더러의 격자(8192)로 굽고, release 가 제 줌을 싣는다

- Status: Accepted
- Date: 2026-11-11
- Builds on: ADR-0485(#1258, maxzoom 한 칸은 render_max_zoom 에서 한 화소 이하),
  [ADR-0262](./0262-parcel-tiles-are-baked-at-maxzoom-detail-14-and-only-subpixel-parcels-may-be-missing.md)
  (maxzoom 정밀도), [ADR-0373](./0373-regulation-zones-are-served-as-map-tiles-patched-where-a-zone-changed.md)
  (레시피가 바뀌면 다시 굽는다), [ADR-0133](./0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md) §4
  (첫 판 동등성)

## Context

**사고 (W41, 2026-11-09).** 사장이 남동산단을 z21 까지 확대하자 경계선이 계단처럼 보였다. 굽는 쪽 정밀도는 이미
충분하다(ADR-0485: 필지 z16 정밀도 14, 한 칸 약 3 cm). 손실은 그리는 쪽에서 한 번 더 생긴다. mapbox-gl(Naver GL 안의
엔진)은 타일을 읽을 때 모든 좌표를 제 격자 `EXTENT = 8192` 로 다시 맞춘다. z16 타일을 z22 에서 그리면 그 타일은
64 × 256 = 16,384 화소이고 한 칸이 2 화소다. 정밀도 14(16,384 칸)로 구운 꼭짓점도 8,192 칸으로 반올림되어 한 화소까지
움직인다. 용도지역(z14)은 z22 에서 한 칸이 8 화소다.

굽기의 줌은 바꿀 수 없었다. 오븐은 레이어를 활성 release 에서 읽고, 승격은 활성 release 의 레이어 행을 그대로
복사했다(`copy_release_layers_tx`). 새 단위의 탄생만 계약에서 줌을 받았다(ADR-0374).

큰 곳의 방식: 일반 지도 타일(Mapbox Streets v8 는 z16, OpenMapTiles·Protomaps 는 z14–15)은 maxzoom 타일을 z22 까지
확대해 그리며 그 사이의 화소 오차를 받아들인다. 도로·건물 같은 배경이기 때문이다. 경계 자체가 정보인 지적·
토지 지도는 렌더러가 확대하지 않을 만큼 깊게 자른다. 우리 필지·용도지역 경계는 뒤의 경우다.

## Decision

1. **계약이 단위의 maxzoom 을 정한다.** `config/tile-bake-containers.contract.json` 의 `tile_max_zoom`:
   `parcels`·`complex`·`zoning` 모두 17. 오븐은 활성 release 의 레이어에서 maxzoom 만 바꿔 굽는다(`read_active_static`).
   1–22 밖이거나, 탄생 레이어(ADR-0374)와 다르면 계약을 거부한다.
2. **정밀도는 13(8,192 칸).** 세 단위 모두 `maxzoom_detail` 13. z17·13 의 한 칸은 z16·14, z14·16 과 같은 땅 크기(한국에서
   약 3 cm)다. 정밀도는 그대로이고, 타일의 격자가 렌더러의 격자와 같아져 두 번 반올림되지 않는다. z22 에서 한 칸은
   한 화소다(ADR-0485 의 검사 `detail ≥ 8 + 22 − 17 = 13` 를 통과).
3. **release 가 제 줌을 싣는다.** 레이크하우스 build 는 입력 release 와 다른 maxzoom 으로 구웠으면 그 줌을
   `catalog.vector_tile_build_job.tile_max_zoom` 에 적는다(불변 입력, 마이그레이션 `20261111120000`). 승격은 레이어 행을
   복사할 때 그 값을 쓴다. 매니페스트·지도 소스의 `tile_max_zoom` 은 그 행에서 나온다.
4. **패치는 같은 줌의 기본판에만 쌓인다.** 계약의 줌이 기본판과 다르면 패치 경로는 `patch-skipped` 로 새 기본판을 굽게
   한다(ADR-0323). 레시피 해시(`-z` 가 들어 있음)도 바뀌므로 ADR-0373 대로 같은 스냅숏도 한 번 다시 굽는다. 이것이
   선언한 전량 예외다(이유: 모든 maxzoom 타일의 격자가 바뀐다, 비용: 아래 측정).
5. **첫 판 동등성은 두 판이 함께 가진 줌에서 잰다.** 더 깊게 구운 새 판은 활성 판의 maxzoom 까지 비교하고
   (`collected_at_zoom` 이 표본을 그 줌으로 올린다), 새 maxzoom 은 id 전수 관문(ADR-0262)이 모든 타일을 읽는다.

## Consequences

측정(2026-11-11, 노트북, 고정 이미지 GDAL 3.10.2·tippecanoe 2.79.0, 202609 핸드오프의 인천 667,777 필지 = 전국의 1.67%):

| | 전: z14–16, 정밀도 14 | 후: z14–17, 정밀도 13 |
|---|---|---|
| 타일 바이트 합 | 87.0 MB | 124.9 MB (+43.5%) |
| tippecanoe 시간 | 386 s | 557 s (+44%) |
| 줌별 가장 큰 타일 | z14 419 KB, z15 202 KB, z16 92 KB | z14·z15 같음, z16 75 KB, z17 31 KB |
| z22 에서 한 칸 | 2 화소(렌더러 재양자화) | 1 화소, 재양자화 없음 |

- 전국 계산(인천 비율을 그대로 곱함): 판 6.05 GB → 약 8.7 GB, tippecanoe 51.5 분 → 약 74 분(경합 109 분 → 약 157 분).
  가장 큰 타일은 z14 의 419 KB 로 바뀌지 않는다(용도지역의 `tile_bytes_budget` 500,000 B 와 같은 기준 안).
- 산업단지(현재 판 6 MB)와 용도지역(480 MB)도 같은 비율이면 약 9 MB·690 MB 다(계산).
- R2 저장: 판이 늘어나는 만큼(필지 약 +2.7 GB). 앞문 캐시(ADR-0461)의 타일 수는 z17 을 보는 화면에서만 는다.
- 지도는 z16 에서 z17 사이에 타일 수가 네 배가 되는 대신 타일이 작아진다(z17 최대 31 KB).
- 이 결정은 굽기가 일어나야 화면에 닿는다. 필지는 ADR-0491 이 켜는 굽기, 용도지역은 다음 `zoning_tile_bake`, 산업단지는
  다음 complex 굽기다.
