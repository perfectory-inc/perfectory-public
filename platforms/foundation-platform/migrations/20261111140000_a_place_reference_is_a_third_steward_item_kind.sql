-- 루트 ADR-0483 §5: 필지 원천이 놓지 못한 행이 필지 계보·법정동 짝과 같은 스튜어드 입구의 세 번째 종류(unit
-- place_reference)가 된다. 레이크하우스의 gold.lineage_review_queue 가 이미 이 unit 으로 항목을 세우고, 같은 적재
-- 명령(load-lineage-review-items)이 그 unit 의 목록을 통째로 바꾼다.
--
-- 표를 새로 만들지 않는다. 20261016120000 이 unit 마다 따로 둔 CHECK 를 넓힌다. 지우는 행·칸은 없다.
--
-- place_reference 항목: subject_code 는 `silver.<표>|<pnu_unresolved_reason>|<법정동 10자리 또는 ->`, 상태는
-- steward(해석기 규칙이 고칠 것) 또는 awaiting_data(원천이 값을 가렸음), candidates_json 은 큐가 적은 근거 객체
-- ({source, reason, legal_dong_code, rows, sample_row_keys}), evidence_json 은 없다. 결정은 모르겠음·조정자에게
-- 넘김만 받는다(predecessor_code 없음; 도메인 check_decision 이 연결·연결 아님을 거부한다).

ALTER TABLE catalog.lineage_review_item
    DROP CONSTRAINT lineage_review_item_unit_check,
    DROP CONSTRAINT lineage_review_item_subject_code_check,
    DROP CONSTRAINT lineage_review_item_status_check,
    ADD CONSTRAINT lineage_review_item_unit_check CHECK (unit IN ('parcel', 'legal_dong', 'place_reference')),
    ADD CONSTRAINT lineage_review_item_subject_code_check CHECK (
        (unit = 'parcel' AND subject_code ~ '^[0-9]{19}$')
        OR (unit = 'legal_dong' AND subject_code ~ '^[0-9]{10}$')
        OR (unit = 'place_reference' AND subject_code ~ '^silver\.[a-z0-9_]+\|[a-z0-9_]+\|([0-9]{10}|-)$')),
    ADD CONSTRAINT lineage_review_item_status_check CHECK (
        (unit = 'parcel' AND status IN ('needs_review', 'pending', 'sample'))
        OR (unit = 'legal_dong' AND status IN ('steward', 'split', 'awaiting_data'))
        OR (unit = 'place_reference' AND status IN ('steward', 'awaiting_data')));

ALTER TABLE catalog.lineage_steward_decision
    DROP CONSTRAINT lineage_steward_decision_unit_check,
    DROP CONSTRAINT lineage_steward_decision_subject_code_check,
    DROP CONSTRAINT lineage_steward_decision_predecessor_code_check,
    ADD CONSTRAINT lineage_steward_decision_unit_check CHECK (unit IN ('parcel', 'legal_dong', 'place_reference')),
    ADD CONSTRAINT lineage_steward_decision_subject_code_check CHECK (
        (unit = 'parcel' AND subject_code ~ '^[0-9]{19}$')
        OR (unit = 'legal_dong' AND subject_code ~ '^[0-9]{10}$')
        OR (unit = 'place_reference' AND subject_code ~ '^silver\.[a-z0-9_]+\|[a-z0-9_]+\|([0-9]{10}|-)$')),
    ADD CONSTRAINT lineage_steward_decision_predecessor_code_check CHECK (
        predecessor_code IS NULL
        OR (unit = 'parcel' AND predecessor_code ~ '^[0-9]{19}$')
        OR (unit = 'legal_dong' AND predecessor_code ~ '^[0-9]{10}$' AND predecessor_code <> subject_code));
