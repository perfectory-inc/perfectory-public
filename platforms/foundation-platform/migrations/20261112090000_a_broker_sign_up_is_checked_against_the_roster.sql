-- The roster a Gongzzang broker sign-up is checked against (root ADR-0550).
--
-- Loaded from silver.real_estate_broker_office and silver.real_estate_broker_member by changed
-- office only (`load-real-estate-broker-lookup`); foundation-api reads it to answer one
-- match/no-match. Both roster tables only grow: a row names the load that inserted it, and the
-- next load that changes its office fills `retired_by_load_id` once. A current row has none.
-- A load writes its rows before its ledger row, in one transaction, so the references to the
-- ledger are checked at commit.
-- Postgres is not the record: the Silver tables are, and the roster can be rebuilt from them.

CREATE TABLE catalog.real_estate_broker_lookup_load (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    office_snapshot_id bigint NOT NULL CHECK (office_snapshot_id > 0),
    member_snapshot_id bigint NOT NULL CHECK (member_snapshot_id > 0),
    -- The newest `ingested_at_utc` this load read from each Silver table, as Trino writes it: the
    -- next load reads the rows at or after it.
    office_watermark text NOT NULL CHECK (office_watermark ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,12})?( [A-Za-z_/+:-]+)?$'),
    member_watermark text NOT NULL CHECK (member_watermark ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,12})?( [A-Za-z_/+:-]+)?$'),
    changed_offices integer NOT NULL CHECK (changed_offices >= 0),
    inserted_rows integer NOT NULL CHECK (inserted_rows >= 0),
    retired_rows integer NOT NULL CHECK (retired_rows >= 0),
    -- A load is written in batches of offices, each committed alone (root ADR-0457: a run stopped at its
    -- limit keeps what it committed). Only the last batch completes it: its watermarks are the ones the
    -- next load reads from; an earlier batch carries the watermarks it started from.
    completes_load boolean NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE catalog.real_estate_broker_office (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sigungu_code text NOT NULL CHECK (length(btrim(sigungu_code)) > 0),
    registration_no text NOT NULL CHECK (length(btrim(registration_no)) > 0),
    -- `registration_no` without spaces or hyphens: what a lookup compares.
    registration_key text NOT NULL CHECK (registration_key ~ '^[^[:space:]-]+$'),
    office_name text,
    status_code text,
    status_name text,
    data_reference_date text,
    loaded_by_load_id bigint NOT NULL REFERENCES catalog.real_estate_broker_lookup_load (id) DEFERRABLE INITIALLY DEFERRED,
    retired_by_load_id bigint REFERENCES catalog.real_estate_broker_lookup_load (id) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT real_estate_broker_office_retired_later CHECK (retired_by_load_id IS NULL OR retired_by_load_id > loaded_by_load_id)
);

CREATE UNIQUE INDEX real_estate_broker_office_current_key
    ON catalog.real_estate_broker_office (sigungu_code, registration_no) WHERE retired_by_load_id IS NULL;
CREATE INDEX real_estate_broker_office_current_registration
    ON catalog.real_estate_broker_office (registration_key) WHERE retired_by_load_id IS NULL;

CREATE TABLE catalog.real_estate_broker_member (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sigungu_code text NOT NULL CHECK (length(btrim(sigungu_code)) > 0),
    registration_no text NOT NULL CHECK (length(btrim(registration_no)) > 0),
    registration_key text NOT NULL CHECK (registration_key ~ '^[^[:space:]-]+$'),
    member_name text NOT NULL,
    -- `member_name` trimmed with inner whitespace collapsed: what a lookup compares.
    name_key text NOT NULL CHECK (length(name_key) > 0),
    member_kind_code text,
    member_kind_name text,
    license_no text,
    -- `license_no` without spaces or hyphens; null when the person holds no licence (중개보조원).
    license_key text CHECK (license_key IS NULL OR license_key ~ '^[^[:space:]-]+$'),
    position_code text,
    position_name text,
    data_reference_date text,
    loaded_by_load_id bigint NOT NULL REFERENCES catalog.real_estate_broker_lookup_load (id) DEFERRABLE INITIALLY DEFERRED,
    retired_by_load_id bigint REFERENCES catalog.real_estate_broker_lookup_load (id) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT real_estate_broker_member_retired_later CHECK (retired_by_load_id IS NULL OR retired_by_load_id > loaded_by_load_id)
);

CREATE INDEX real_estate_broker_member_current_office
    ON catalog.real_estate_broker_member (sigungu_code, registration_no) WHERE retired_by_load_id IS NULL;
CREATE INDEX real_estate_broker_member_current_license
    ON catalog.real_estate_broker_member (name_key, license_key) WHERE retired_by_load_id IS NULL;
CREATE INDEX real_estate_broker_member_current_registration
    ON catalog.real_estate_broker_member (name_key, registration_key) WHERE retired_by_load_id IS NULL;

-- Nothing is removed and a row changes once: its `retired_by_load_id`, from null.
CREATE FUNCTION catalog.real_estate_broker_roster_is_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
        RAISE EXCEPTION '% is append-only: no row is removed (root ADR-0550)', TG_TABLE_NAME
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF TG_TABLE_NAME = 'real_estate_broker_lookup_load' THEN
        RAISE EXCEPTION 'a broker roster load is recorded once (root ADR-0550)'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF OLD.retired_by_load_id IS NOT NULL OR NEW.retired_by_load_id IS NULL
       OR (to_jsonb(NEW) - 'retired_by_load_id') IS DISTINCT FROM (to_jsonb(OLD) - 'retired_by_load_id') THEN
        RAISE EXCEPTION '% %: only retired_by_load_id is filled, once (root ADR-0550)', TG_TABLE_NAME, OLD.id
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER real_estate_broker_lookup_load_append_only
BEFORE UPDATE OR DELETE ON catalog.real_estate_broker_lookup_load
FOR EACH ROW EXECUTE FUNCTION catalog.real_estate_broker_roster_is_append_only();
CREATE TRIGGER real_estate_broker_lookup_load_never_truncated
BEFORE TRUNCATE ON catalog.real_estate_broker_lookup_load
FOR EACH STATEMENT EXECUTE FUNCTION catalog.real_estate_broker_roster_is_append_only();
CREATE TRIGGER real_estate_broker_office_append_only
BEFORE UPDATE OR DELETE ON catalog.real_estate_broker_office
FOR EACH ROW EXECUTE FUNCTION catalog.real_estate_broker_roster_is_append_only();
CREATE TRIGGER real_estate_broker_office_never_truncated
BEFORE TRUNCATE ON catalog.real_estate_broker_office
FOR EACH STATEMENT EXECUTE FUNCTION catalog.real_estate_broker_roster_is_append_only();
CREATE TRIGGER real_estate_broker_member_append_only
BEFORE UPDATE OR DELETE ON catalog.real_estate_broker_member
FOR EACH ROW EXECUTE FUNCTION catalog.real_estate_broker_roster_is_append_only();
CREATE TRIGGER real_estate_broker_member_never_truncated
BEFORE TRUNCATE ON catalog.real_estate_broker_member
FOR EACH STATEMENT EXECUTE FUNCTION catalog.real_estate_broker_roster_is_append_only();
