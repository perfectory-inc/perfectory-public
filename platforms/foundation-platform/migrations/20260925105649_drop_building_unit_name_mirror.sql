-- catalog.building_unit.building_name 은 적재 이후 전 행이 dong_name 의 복사본이었다
-- (2026-09-25 운영 실측: 다른 행 0 / 같은 비어있지 않은 값 15,192,377). 같은 사실은
-- 한 곳에만 산다 — 저장소의 거울 칸을 제거하고, 서빙 계약의 building_name 필드는
-- 유지하되 dong_name 에서 파생한다 (foundation-api unit_response).
ALTER TABLE catalog.building_unit DROP COLUMN building_name;
