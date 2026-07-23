# packages/map

> **⚠️ 스텁 (README-only 자리표시자).** 이 패키지에 코드는 없다.
> **지도 코드 실물은 [`apps/web/lib/map/`](../../apps/web/lib/map/)** 이다 (2026-07-20 기준).
> 이전 판의 PMTiles·Supercluster 지시는 **폐기됨** — 현행 결정과 모순:
> [ADR-0021](../../docs/adr/0021-static-vector-tile-decomposition.md) 이 PMTiles 를 flat `.pbf`
> 분해로 대체했고, [ADR-0037](../../docs/adr/0037-pnu-anchor-pbf-marker-tiles.md)/0038 이
> PNU-anchor PBF 마커 타일 계약을 채택했다 (Supercluster·PMTiles 미사용).

당초 계획: Naver Maps 통합 + Canvas 마커 + 좌표 헬퍼 패키지.

## 현행 정책 (apps/web/lib/map 에 적용 중)

- 좌표 입출력: EPSG:4326 (WGS84); 클라이언트는 *표시만*, 공간 연산은 PostGIS (서버)
- 마커: PNU-anchor PBF 마커 타일 (ADR-0037/0038) — Supercluster 클러스터링 아님
- 벡터 타일: flat `.pbf` 분해 정적 타일 (ADR-0021) — PMTiles 아님
- 출처 표기: 지도 위 "Naver Maps" 로고 + 공공 데이터 출처

→ ADR-0021, ADR-0037/0038, `apps/web/lib/map/`
