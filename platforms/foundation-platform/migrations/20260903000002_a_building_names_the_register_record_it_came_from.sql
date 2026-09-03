-- ADR-0073: 표제부가 필지와 호 사이의 건물을 채운다.
--
-- catalog.building 은 catalog.building_unit 이 ADR-0072 직전에 가졌던 것과 같은 구멍을
-- 갖고 있다: 원천 레코드를 가리키는 칸이 없어서, 자연키 없는 표에 8,051,204행을 붓는
-- 재적재는 매번 전체가 새 행이 된다. 관리건축물대장 PK([0]칸)가 원천의 레코드 식별자이고,
-- 적재기는 이 칸에 ON CONFLICT 를 건다.
--
-- 표가 비어 있는 지금이 NOT NULL 을 붙일 수 있는 유일한 시점이다.

ALTER TABLE catalog.building
    ADD COLUMN register_pk text NOT NULL;

ALTER TABLE catalog.building
    ADD CONSTRAINT building_register_pk_key UNIQUE (register_pk);

COMMENT ON COLUMN catalog.building.register_pk IS
    'Management building-register PK (관리건축물대장 PK) of the title-register row this building was loaded from — the natural key the loader upserts on (ADR-0073). The row id is derived from it, so a reload yields the same id.';
