-- ADR-0263: 법정동 코드 짝(ADR-0144 §3·§4)이 필지 계보와 같은 스튜어드 입구의 두 번째 종류(unit)가 된다.
--
-- 표를 새로 만들지 않는다. 20260930120000_lineage_stewardship.sql 이 처음부터 `unit` 칸을 두었고, 여기서는
-- 그 칸이 받는 값을 넓히고 코드 형식을 unit 마다 따로 검사한다. 지우는 행·칸은 없다(CHECK 만 바꾼다).
--
-- legal_dong 항목: subject_code 는 폐지된 법정동 코드(10자리), candidates_json 은 짝 맞추기가 고른 새 코드
-- 후보([{code, name, created_date}]), evidence_json 은 짝 맞추기가 기록한 근거(이름·날짜·규칙 결과·30527 상태).
-- legal_dong 결정의 predecessor_code 는 스튜어드가 고른 새 코드다(아래 COMMENT).

ALTER TABLE catalog.lineage_review_item
    DROP CONSTRAINT lineage_review_item_unit_check,
    DROP CONSTRAINT lineage_review_item_subject_code_check,
    DROP CONSTRAINT lineage_review_item_status_check,
    ADD COLUMN evidence_json text,
    ADD CONSTRAINT lineage_review_item_unit_check CHECK (unit IN ('parcel', 'legal_dong')),
    ADD CONSTRAINT lineage_review_item_subject_code_check CHECK (
        (unit = 'parcel' AND subject_code ~ '^[0-9]{19}$')
        OR (unit = 'legal_dong' AND subject_code ~ '^[0-9]{10}$')),
    ADD CONSTRAINT lineage_review_item_status_check CHECK (
        (unit = 'parcel' AND status IN ('needs_review', 'pending', 'sample'))
        OR (unit = 'legal_dong' AND status IN ('steward', 'split', 'awaiting_data'))),
    -- 법정동 항목만 근거 문서를 가지고, 반드시 가진다.
    ADD CONSTRAINT lineage_review_item_evidence_json_check CHECK (
        (unit = 'legal_dong') = (evidence_json IS NOT NULL)
        AND (evidence_json IS NULL OR jsonb_typeof(evidence_json::jsonb) = 'object'));

ALTER TABLE catalog.lineage_steward_decision
    DROP CONSTRAINT lineage_steward_decision_unit_check,
    DROP CONSTRAINT lineage_steward_decision_subject_code_check,
    DROP CONSTRAINT lineage_steward_decision_predecessor_code_check,
    ADD CONSTRAINT lineage_steward_decision_unit_check CHECK (unit IN ('parcel', 'legal_dong')),
    ADD CONSTRAINT lineage_steward_decision_subject_code_check CHECK (
        (unit = 'parcel' AND subject_code ~ '^[0-9]{19}$')
        OR (unit = 'legal_dong' AND subject_code ~ '^[0-9]{10}$')),
    ADD CONSTRAINT lineage_steward_decision_predecessor_code_check CHECK (
        predecessor_code IS NULL
        OR (unit = 'parcel' AND predecessor_code ~ '^[0-9]{19}$')
        OR (unit = 'legal_dong' AND predecessor_code ~ '^[0-9]{10}$' AND predecessor_code <> subject_code));

COMMENT ON COLUMN catalog.lineage_steward_decision.predecessor_code IS
    'The candidate a link names: the older PNU for unit parcel, the new 법정동 code for unit legal_dong (ADR-0263).';
COMMENT ON COLUMN catalog.lineage_review_item.evidence_json IS
    'unit legal_dong only: the evidence the pairing run recorded for the abolished code (ADR-0263).';
