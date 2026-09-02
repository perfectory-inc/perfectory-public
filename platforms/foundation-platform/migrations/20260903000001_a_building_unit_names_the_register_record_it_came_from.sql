-- ADR-0072: 호(戶)는 PNU 로 필지에 붙고, 재적재는 같은 행을 다시 낳지 않아야 한다.
--
-- catalog.building_unit 은 그릇만 있고 원천 레코드를 가리키는 칸이 없다. 자연키 없는 표에
-- 19,765,555행을 붓는 재적재는 매번 전체가 새 행이 된다 — 133,583,046행이 이중 적재될 뻔한
-- 결함(root ADR-0069)과 같은 모양이다. 관리건축물대장 PK(mgm_bldrgst_pk)가 원천이 주는
-- 레코드 식별자이고, 적재기는 이 칸에 ON CONFLICT 를 건다.
--
-- 표가 비어 있는 지금이 NOT NULL 을 붙일 수 있는 유일한 시점이다. 행이 실린 뒤라면 이
-- 마이그레이션은 백필 결정을 함께 실어야 했다.

ALTER TABLE catalog.building_unit
    ADD COLUMN register_pk text NOT NULL;

ALTER TABLE catalog.building_unit
    ADD CONSTRAINT building_unit_register_pk_key UNIQUE (register_pk);

COMMENT ON COLUMN catalog.building_unit.register_pk IS
    'Management building-register PK (관리건축물대장 PK) of the exclusive-part row this unit was loaded from — the natural key the loader upserts on (ADR-0072). The row id is derived from it, so a reload yields the same id.';
