-- ADR-0073 §5: 원천에 없는 값은 NULL 이다 — 이웃 행에서도, 기본값으로도 지어내지 않는다.
--
-- 첫 국가 단위 표제부 내보내기(2026-09-03, 8,051,204동)가 잰 채움율이 이 마이그레이션의
-- 근거다: 사용승인연도 81.3%, 연면적 99.27%. NOT NULL 인 채로는 연도 없는 약 150만 동이
-- 적재에서 통째로 떨어지거나, 아무도 승인한 적 없는 연도가 지어내져 실린다. 필지가 먼저
-- 같은 결정을 지났다(ADR-0070: kind·area_m2 의 NOT NULL 해제).
--
-- parcel_id 와 register_pk 는 그대로 NOT NULL 이다: 필지 없는 건물은 적재기가 세고
-- 건너뛰는 orphan 이지 NULL 행이 아니고, 자연키 없는 행은 재적재가 식별할 수 없다.

ALTER TABLE catalog.building
    ALTER COLUMN purpose_code DROP NOT NULL;

ALTER TABLE catalog.building
    ALTER COLUMN structure_code DROP NOT NULL;

ALTER TABLE catalog.building
    ALTER COLUMN floor_area_m2 DROP NOT NULL;

ALTER TABLE catalog.building
    ALTER COLUMN stories DROP NOT NULL;

ALTER TABLE catalog.building
    ALTER COLUMN built_year DROP NOT NULL;

COMMENT ON COLUMN catalog.building.built_year IS
    'Construction-approval year from the title register (사용승인일의 연도). NULL when the register states no plausible date — measured 18.7% of the national snapshot, mostly older buildings (ADR-0073). Never fabricated.';

COMMENT ON COLUMN catalog.building.floor_area_m2 IS
    'Total floor area (연면적) in m2 from the title register. NULL when the register writes 0 or nothing — a zero-area building would otherwise look measured (ADR-0073).';
