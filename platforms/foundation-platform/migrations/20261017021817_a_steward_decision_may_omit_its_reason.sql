-- 사장 결정(2026-10-17): 필지 계보든 법정동 짝이든 스튜어드 결정의 이유 코드와 메모는 선택이다.
-- 누가·언제·무엇을·어떤 근거(evidence_etag) 위에서 정했는지는 결정 행이 이미 늘 기록한다.
--
-- 20260930120000_lineage_stewardship.sql 의 이름 없는 표 CHECK
-- `outcome NOT IN ('link', 'not_a_link') OR reason_code IS NOT NULL` 하나만 지운다. 이름은 Postgres 가
-- 지었으므로 정의로 찾고, 정확히 하나가 아니면 멈춘다. 지우는 행·칸은 없다.
DO $$
DECLARE
    found text[];
BEGIN
    SELECT array_agg(conname) INTO found
    FROM pg_constraint
    WHERE conrelid = 'catalog.lineage_steward_decision'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%reason_code IS NOT NULL%';
    IF coalesce(array_length(found, 1), 0) <> 1 THEN
        RAISE EXCEPTION 'expected one reason-required check on catalog.lineage_steward_decision, found %', found;
    END IF;
    EXECUTE format('ALTER TABLE catalog.lineage_steward_decision DROP CONSTRAINT %I', found[1]);
END
$$;
