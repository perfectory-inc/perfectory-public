-- ADR-0108 1차: R2 로 서빙이 이전된 상세 투영 중, 코드 소비자가 없는 것부터 폐기한다.
-- unit_official_price_legacy_year 는 옛 세대별 가격 postgres 사본(5.3GB)이고, 정본은
-- R2 silver.unit_official_price(188,741,514행, 2026-09-26 실측)에 있다. 코드 소비자는
-- 없고(시험 1건만 참조) 서빙은 건물 R2 굽기가 덮는다. 개발 환경이며 R2 에서 재생성
-- 가능하므로 forward-only DROP 으로 폐기한다. 큰 상세 표(transfer_event·zoning 등)는
-- foundation-api 읽기 경로를 함께 은퇴시키는 후속 변경에서 폐기한다.
DROP TABLE IF EXISTS catalog.unit_official_price_legacy_year;
