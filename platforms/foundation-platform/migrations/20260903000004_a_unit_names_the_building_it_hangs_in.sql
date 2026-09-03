-- ADR-0074 §1: 호는 자기 건물에 매달리고, NULL 도 답이다.
--
-- 실측(2026-09-03, Silver 조인)이 이 칸의 근거다: 호 19,765,555 중 19,624,045(99.28%)가
-- 실존하는 표제부 건물을 가리킨다. NULL 로 남는 것은 Silver 가 미해결로 기록한 140,105호,
-- 열쇠가 표제부 밖인 1,405호, 그리고 건물이 필지 orphan 이라 catalog.building 에 없는 호다 —
-- 세 경우 모두 지어 붙이지 않는다(ADR-0074 §2).
--
-- parcel_id 는 그대로 둔다: "이 필지의 모든 호"는 건물을 거치지 않는 조회이고 원천이 준
-- 독립 사실이다. 건물 연결은 추가이지 대체가 아니다.

ALTER TABLE catalog.building_unit
    ADD COLUMN building_id uuid;

ALTER TABLE catalog.building_unit
    ADD CONSTRAINT building_unit_building_id_fkey
    FOREIGN KEY (building_id) REFERENCES catalog.building(id);

CREATE INDEX building_unit_building_id_idx
    ON catalog.building_unit (building_id)
    WHERE building_id IS NOT NULL;

COMMENT ON COLUMN catalog.building_unit.building_id IS
    'The building this unit hangs in, derived from the unit register''s building key (건물 관리대장 PK). NULL is an answer, not a failure: the register''s own link was unresolved, pointed outside the title register, or the building is a parcel orphan absent from catalog.building (ADR-0074).';
