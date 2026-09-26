-- ADR-0109 1단계: 필지 상세 6표를 폐기한다.
--
-- 정본은 R2 레이크하우스(gold.parcel_panel)이고, 상세 서빙은 R2 엣지 미리구운 객체
-- (catalog.perfectory.io/parcels/by-pnu/{pnu})가 덮는다(실측 200). 이 6표는 그 사실의
-- postgres 사본(역-ETL 투영)일 뿐이며, 유일한 런타임 소비자였던 공짱 백엔드는 이미 R2
-- 엣지로 옮겼다(PR #232, ADR-0109). foundation-api 의 필지 상세 읽기 경로·적재기도 같은
-- 변경에서 은퇴시킨다. 개발 환경이며 R2 에서 재적재 가능하므로 forward-only DROP 으로
-- 폐기한다(ADR-0108 게이트: (a) R2 객체가 사실을 실음 확인, (b) postgres 소비자 0 확인).
--
-- 다른 catalog 표가 이들을 조인하지 않고 FK 도 없다(ADR-0108 판정표). 회수: 약 62GB.
DROP TABLE IF EXISTS catalog.parcel_transfer_event;
DROP TABLE IF EXISTS catalog.parcel_zoning;
DROP TABLE IF EXISTS catalog.parcel_characteristic;
DROP TABLE IF EXISTS catalog.parcel_forest_ledger;
DROP TABLE IF EXISTS catalog.parcel_land_right;
DROP TABLE IF EXISTS catalog.parcel_price;
