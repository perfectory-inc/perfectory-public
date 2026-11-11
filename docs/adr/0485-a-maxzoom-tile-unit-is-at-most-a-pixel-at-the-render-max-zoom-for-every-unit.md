# ADR 0485: 모든 단위의 maxzoom 타일 한 칸은 render_max_zoom 에서 한 화소를 넘지 않는다 — 굽기마다 검사하고, 산업단지도 14로 굽는다

- Status: Accepted
- Date: 2026-11-09
- Amends: [ADR-0262](./0262-parcel-tiles-are-baked-at-maxzoom-detail-14-and-only-subpixel-parcels-may-be-missing.md) §1
  (필지·용도지역만 손으로 적던 maxzoom 정밀도를 모든 단위의 규칙으로)
- Builds on: ADR-0133(lakehouse 굽기), ADR-0316(굽기 입력), ADR-0374(용도지역 단위)

## Context

**사고 (W41, 2026-11-09).** 사장: "지도 위 모양(필지·규제·단지)이 지저분하다. 소수점을 자르나?" 사장 화면은 남동국가산업단지
근처 z15 안팎에서 산업단지 경계가 굵은 꺾은선으로 보였다가, 조금 뒤 "갑자기 정상"이 됐다.

사슬 전체를 실물로 쟀다(전부 **측정**, 2026-11-09).

| 단계 | 무엇을 | 결과 |
|---|---|---|
| 원천 | VWorld 연속지적 SHP(강남구 2026-09 판), `.prj` = Korea 2000 중부원점 2010(GRS80) | 좌표는 float64 그대로(소수 1–11자리), 잘림 없음 |
| Bronze→Silver | 같은 ZIP 을 pyproj(PROJ, `KGD2002 to WGS 84 (1)` = 무변위)로 바꾼 값 vs `gold.parcel_boundary_served` 1,500필지 | 꼭짓점 8,931개 **개수 동일**, 최대 차 **0.10 mm**. 반올림·단순화·스냅 없음(WKB double) |
| Silver 투영 | `foundation-shapefile` 의 proj4rs `tmerc` | 기본이 Poder/Engsager(PROJ 와 같은 정밀 알고리즘), towgs84 0 검증. 1–2 m 이동 없음 |
| Gold→굽기 | GDAL `COORDINATE_PRECISION=7`, `-makevalid` | 1e-7° ≈ 1 cm, 꼭짓점을 옮기지 않음 |
| 타일 | 서빙 중인 타일을 내려 풀어 Gold 경계와 대칭 Hausdorff(아래 표) | 손실은 **여기서만** 생긴다 |

서빙 타일 오차(m, p50 / p95 / 최대). 화소는 지도 화소(네이버 GL 안의 mapbox 는 512 px 타일, 줌 z 에서
0.0296 × 2^(21−z) m/px, 위도 37.5°).

| 단위(릴리스 날짜, maxzoom 정밀도) | z8 | z12 | z14 | z15 | z16(maxzoom) | z16 타일을 z21 에서 |
|---|---|---|---|---|---|---|
| parcels(09-28, 202606 판, **12**) | — | — | 0.24 / 0.30 | 0.10 / 0.15 | 0.05 / 0.07 / 0.21 | p95 2.5 px, 최대 7 px |
| complex(09-29, **12**) | 35 / 35 / 36 | 2.3 / 2.2 / 2.5 | 0.49 / 0.58 / 0.60 | 0.20 / 0.24 / 0.29 | 한 칸 0.12 m | 한 칸 4 px |
| admin(10-16, 12, 16까지만 그림) | 25 / 27 / 38 | 1.8 / 2.4 / 2.6 | 0.31 / 0.55 / 0.59 | — | — | 그리지 않음 |
| zoning(11-08, z14 에서 **16**) | — | 1.1 / 2.4 / 5.4 | 0.02 / 0.05 / 0.09 | — | — | z14 타일 p95 1.6 px |

제 줌에서 보이는 타일은 모두 화소 미만이다(z14 타일 p95 0.6 m ÷ 3.8 m/px = 0.15 px). 손실이 보이는 곳은 둘이다.

1. **maxzoom 타일을 깊이 확대할 때.** complex 는 z16 타일을 22까지 그리는데 `maxzoom_detail` 에 없어 tippecanoe 기본 12로
   구워졌다. 한 칸이 z21 에서 4 px 이다. ADR-0262 는 필지·용도지역만 손으로 적었고, 적지 않은 단위는 조용히 12로 굽는다.
2. **부모 타일 대역.** 사장 화면 그대로 다시 열어 mapbox 타일 캐시를 읽으니 complex·admin 은 z15 화면에 **z8 타일**
   (`8/218/99`)을 붙들고 z15 타일을 기다리고 있었다. z8 타일의 오차 36 m 는 z15 에서 19 px 다. 그 기다림이 길다:
   노트북(한국)에서 `tiles.perfectory.io` 응답은 Cloudflare **LAX** 에서 오고(`cf-ray …-LAX`, 같은 순간
   `www.cloudflare.com` 은 ICN), 캐시 HIT 도 첫 바이트까지 0.85–2.6 초다. "갑자기 정상"은 z15 타일이 도착한 순간이다.

## Decision

1. **규칙: 모든 단위의 maxzoom 한 칸은 render_max_zoom 에서 한 화소(256 px 타일 기준)를 넘지 않는다.**
   `2^detail ≥ 256 × 2^(render_max_zoom − tile_max_zoom)`, 곧 `detail ≥ 8 + (render_max_zoom − tile_max_zoom)`.
   `bake_unit`(전체 굽기·조각 굽기·패치 굽기가 모두 지나는 곳)이 굽기 전에 검사하고, 모자라면 굽지 않는다
   (`ensure_maxzoom_detail_reaches_render_max_zoom`). 새 단위가 정밀도를 빠뜨리면 첫 굽기가 이유와 필요한 값을 말하고 멈춘다.
   계약 `maxzoom_detail` 은 그대로 정본이다(값을 바꾸면 굽기 입력이 바뀌어 ADR-0316 조각을 다시 굽는다). 지금 값은
   모두 규칙을 지킨다: parcels 14(16→22), zoning 16(14→22), admin 12(16→16).
2. **complex 는 14로 굽는다.** z16 한 칸이 0.12 m → 0.03 m, z21 에서 4 px → 1 px. 아카이브는 6 MB 라 크기·시간은
   따질 거리가 아니다(용도지역 표본은 12→16 에서 +17%, 계약 `maxzoom_detail_reason`).
3. 가드가 막는 사고: 위 표의 complex(2026-09-29 릴리스, z21 에서 한 칸 4 px). 시험
   `a_maxzoom_unit_is_at_most_a_pixel_at_render_max_zoom` 이 그 단위 모양으로 거부를 고정한다.

큰 곳의 방식: tippecanoe(Mapbox 가 만든 도구)는 maxzoom 정밀도(`-d`/`--full-detail`)를 아래 줌과 따로 두고, 그 타일이 깊이
확대돼 쓰인다는 전제로 기본값 12를 정한다. 깊이 확대하는 레이어는 그 값을 올리거나 maxzoom 을 올린다. 같은 규칙을 단위마다
손으로 기억하지 않고 굽기가 계산해 검사하는 것이 이 결정이다.

## 남은 것 (이 ADR 이 고치지 않는 것, 측정과 함께)

- **필지 서빙 릴리스가 오래됐다.** 서빙 중인 `parcels-40a0ce85…` 는 2026-09-28 에 202606 판을 정밀도 12로 구운 것이다
  (ADR-0262 이전). `parcel_tile_bake` 가 `enabled: false` 라 202609 판·정밀도 14 가 한 번도 나가지 않았다. 표본
  1,500필지 중 1필지는 202609 에 모양이 바뀌어 8.6 m 어긋난다. 켜는 일은 전국 굽기 시간(ADR-0457 의 첫 run 상한)이 걸린 별도 결정이다.
- **굽기 입력이 바뀌면 다시 굽는다(ADR-0373, #1234 이후).** 릴리스가 Gold 스냅숏과 굽기 레시피
  (`bake_inputs_sha256`)로 이름 붙으므로, complex 의 `maxzoom_detail` 14 는 같은 스냅숏이라도 다음 굽기에서 새 릴리스가 된다.
- **GL 클라이언트의 8192 상한.** mapbox-gl 은 타일 좌표를 `EXTENT = 8192` 로 다시 맞춘다(`load_geometry`). 그래서
  z21 에서 z16 타일은 한 칸 2 px, z14 타일(zoning)은 8 px 로 반올림되고, 정밀도를 더 올려도 소용없다. z21 에서 1 px 이하는
  tile_max_zoom 17 이상이어야 한다. 단위별 타일 수·크기를 잰 뒤의 결정이다.
- **부모 타일 대역과 전달 지연.** z8 부모가 z15 에 보이는 것은 정밀도가 아니라 지연의 문제다(z8 타일은 어떤 정밀도로도
  z15 에서 정확할 수 없다: GL 상한에서 한 칸 16 px). 한국 사용자가 서울에서 받게 하는 것은 ADR-0461(CloudFront ICN
  앞문)이 맡았다. 먼 부모를 쓰지 않게 소스를 줌 대역으로 나누는 것은 남은 후보다.
