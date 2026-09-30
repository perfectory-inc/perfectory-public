-- ADR-0115: 스튜어드가 필지 계보 검토 항목을 결정하는 작은 입구.
--
-- 정본은 레이크하우스다. 여기 결정 표는 접기 작업(ADR-0115 §9)이 `silver.parcel_lineage` 에
-- steward 행으로 옮기기 전까지 결정을 담는다. 검토 항목 표는 `gold.lineage_review_queue` 의 투영이라
-- 언제든 다시 적재한다. 임대(claim)는 작업 상태라 레이크하우스에 가지 않는다.

-- 검토 항목: Gold 검토 목록의 투영. 적재 작업이 통째로 바꾼다.
-- candidates_json 은 Gold 가 쓴 문자열 그대로다 — evidence_etag 가 그 바이트를 해시한다.
CREATE TABLE catalog.lineage_review_item (
    item_id uuid PRIMARY KEY,
    unit text NOT NULL CHECK (unit IN ('parcel')),
    subject_code text NOT NULL CHECK (subject_code ~ '^[0-9]{19}$'),
    status text NOT NULL CHECK (status IN ('needs_review', 'pending', 'sample')),
    candidates_json text NOT NULL CHECK (jsonb_typeof(candidates_json::jsonb) = 'array'),
    evidence_etag text NOT NULL CHECK (evidence_etag ~ '^[0-9a-f]{64}$'),
    from_snapshot_id text,
    to_snapshot_id text,
    queue_published_at timestamptz NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (unit, subject_code)
);

-- 맡기: 한 항목을 한 사람이 시간 제한 동안 잡는다(ADR-0115 §5). 만료된 행은 없는 것과 같다.
CREATE TABLE catalog.lineage_review_claim (
    item_id uuid PRIMARY KEY,
    claimed_by uuid NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    CHECK (expires_at > claimed_at)
);

-- 결정: append-only. 되돌리기는 supersedes_decision_id 를 가진 새 결정이다(ADR-0115 §8).
CREATE TABLE catalog.lineage_steward_decision (
    decision_id uuid PRIMARY KEY,
    item_id uuid NOT NULL,
    unit text NOT NULL CHECK (unit IN ('parcel')),
    subject_code text NOT NULL CHECK (subject_code ~ '^[0-9]{19}$'),
    outcome text NOT NULL CHECK (outcome IN ('link', 'not_a_link', 'unsure', 'escalate')),
    predecessor_code text CHECK (predecessor_code IS NULL OR predecessor_code ~ '^[0-9]{19}$'),
    reason_code text CHECK (reason_code IS NULL OR reason_code IN (
        'building_register', 'ownership_record', 'site_survey', 'official_document',
        'cadastral_map', 'other')),
    note text NOT NULL DEFAULT '' CHECK (char_length(note) <= 2000),
    evidence_etag text NOT NULL CHECK (evidence_etag ~ '^[0-9a-f]{64}$'),
    idempotency_key text NOT NULL CHECK (char_length(idempotency_key) BETWEEN 8 AND 200),
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    decided_by uuid NOT NULL,
    decided_at timestamptz NOT NULL DEFAULT now(),
    supersedes_decision_id uuid REFERENCES catalog.lineage_steward_decision (decision_id),
    requires_approval boolean NOT NULL,
    UNIQUE (decided_by, idempotency_key),
    -- link 는 후보 하나를 가리키고, 나머지는 옛 번호를 들지 않는다. 계보를 만드는 둘은 이유가 있어야 한다.
    CHECK ((outcome = 'link') = (predecessor_code IS NOT NULL)),
    CHECK (outcome NOT IN ('link', 'not_a_link') OR reason_code IS NOT NULL),
    CHECK (outcome IN ('link', 'not_a_link') OR NOT requires_approval),
    CHECK (supersedes_decision_id IS DISTINCT FROM decision_id)
);

CREATE INDEX lineage_steward_decision_item_idx
    ON catalog.lineage_steward_decision (item_id, decided_at DESC);

-- 승인: 두 사람이 필요한 결정에만, 결정자가 아닌 사람이 한 번(ADR-0115 §7).
CREATE TABLE catalog.lineage_steward_approval (
    approval_id uuid PRIMARY KEY,
    decision_id uuid NOT NULL UNIQUE REFERENCES catalog.lineage_steward_decision (decision_id),
    verdict text NOT NULL CHECK (verdict IN ('approved', 'rejected')),
    note text NOT NULL DEFAULT '' CHECK (char_length(note) <= 2000),
    approved_by uuid NOT NULL,
    approved_at timestamptz NOT NULL DEFAULT now()
);

-- 접힘: 결정이 레이크하우스 계보 행이 된 기록(ADR-0115 §9). 한 결정은 한 번 접힌다.
CREATE TABLE catalog.lineage_steward_fold (
    decision_id uuid PRIMARY KEY REFERENCES catalog.lineage_steward_decision (decision_id),
    derivation_run_id text NOT NULL CHECK (btrim(derivation_run_id) <> ''),
    folded_at timestamptz NOT NULL DEFAULT now()
);

CREATE FUNCTION catalog.reject_steward_record_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
BEGIN
    RAISE EXCEPTION 'steward records are append-only; supersede a decision with a new one'
        USING ERRCODE = '42501';
END;
$function$;

CREATE TRIGGER lineage_steward_decision_append_only
BEFORE UPDATE OR DELETE ON catalog.lineage_steward_decision
FOR EACH ROW EXECUTE FUNCTION catalog.reject_steward_record_mutation();

CREATE TRIGGER lineage_steward_approval_append_only
BEFORE UPDATE OR DELETE ON catalog.lineage_steward_approval
FOR EACH ROW EXECUTE FUNCTION catalog.reject_steward_record_mutation();

CREATE TRIGGER lineage_steward_fold_append_only
BEFORE UPDATE OR DELETE ON catalog.lineage_steward_fold
FOR EACH ROW EXECUTE FUNCTION catalog.reject_steward_record_mutation();

-- 승인자는 결정자가 아니고, 승인이 필요한 결정에만 승인한다. API 도 막지만, 권한 경계는 역할이
-- 늘면 새므로 표가 스스로 막는다.
CREATE FUNCTION catalog.guard_lineage_steward_approval()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
DECLARE
    decider uuid;
    needs_approval boolean;
BEGIN
    SELECT decided_by, requires_approval INTO decider, needs_approval
    FROM catalog.lineage_steward_decision
    WHERE decision_id = NEW.decision_id;
    IF NOT needs_approval THEN
        RAISE EXCEPTION 'decision % does not require approval', NEW.decision_id
            USING ERRCODE = '23514';
    END IF;
    IF decider = NEW.approved_by THEN
        RAISE EXCEPTION 'the decider of % cannot approve it', NEW.decision_id
            USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER lineage_steward_approval_four_eyes
BEFORE INSERT ON catalog.lineage_steward_approval
FOR EACH ROW EXECUTE FUNCTION catalog.guard_lineage_steward_approval();
