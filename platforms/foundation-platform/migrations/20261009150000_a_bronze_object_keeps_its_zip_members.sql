-- Root ADR-0169 §1: the names inside a Bronze ZIP sit beside the ledger.
--
-- `catalog.bronze_object.provider_file_name` is the dataset title; which province and which
-- edition a VWorld file holds is written only in the member names inside the ZIP. The command
-- `measure-bronze-object-members` reads each ZIP's central directory with ranged requests and
-- appends what it found here. Both tables are append-only (ADR 전 데이터 원칙): a reading is
-- never corrected in place; a changed reading is a new reader version on a new object.

-- One reading of one object. `failed` means the bytes could not be fetched; it is history and
-- the object is read again. The other three are settled, and an object has at most one.
CREATE TABLE catalog.bronze_object_measurement (
    id uuid PRIMARY KEY,
    bronze_object_id uuid NOT NULL REFERENCES catalog.bronze_object (id) ON DELETE RESTRICT,
    outcome text NOT NULL CHECK (outcome IN ('zip', 'not_zip', 'unreadable', 'failed')),
    member_count integer CHECK (member_count IS NULL OR member_count >= 0),
    detail text CHECK (detail IS NULL OR btrim(detail) <> ''),
    measured_at timestamptz NOT NULL DEFAULT now(),
    measured_by text NOT NULL CHECK (btrim(measured_by) <> ''),
    -- A ZIP has a count and needs no reason; anything else has a reason and no count.
    CHECK ((outcome = 'zip') = (member_count IS NOT NULL)),
    CHECK ((outcome = 'zip') = (detail IS NULL)),
    -- The members' composite key, so a member cannot name another object's measurement.
    UNIQUE (id, bronze_object_id)
);

-- Measuring the same object twice adds nothing: the writer inserts ON CONFLICT DO NOTHING here.
CREATE UNIQUE INDEX bronze_object_measurement_settled_key
    ON catalog.bronze_object_measurement (bronze_object_id)
    WHERE outcome <> 'failed';

CREATE INDEX bronze_object_measurement_object_idx
    ON catalog.bronze_object_measurement (bronze_object_id, measured_at);

-- One central directory entry. `member_index` is its position in the directory: a ZIP may
-- repeat a name, so the name is not a key. Sizes come from the ZIP64 extra field when the
-- 32-bit field is saturated; `member_modified` is the MS-DOS date and time as written (no time
-- zone), NULL when the writer recorded none.
CREATE TABLE catalog.bronze_object_member (
    measurement_id uuid NOT NULL,
    bronze_object_id uuid NOT NULL,
    member_index integer NOT NULL CHECK (member_index >= 0),
    member_name text NOT NULL CHECK (member_name <> ''),
    member_name_encoding text NOT NULL CHECK (member_name_encoding IN (
        'utf8_flagged', 'unicode_path_extra', 'utf8_unflagged', 'cp949', 'lossy')),
    member_uncompressed_size bigint NOT NULL CHECK (member_uncompressed_size >= 0),
    member_compressed_size bigint NOT NULL CHECK (member_compressed_size >= 0),
    member_modified timestamp without time zone,
    PRIMARY KEY (measurement_id, member_index),
    FOREIGN KEY (measurement_id, bronze_object_id)
        REFERENCES catalog.bronze_object_measurement (id, bronze_object_id) ON DELETE RESTRICT
);

CREATE INDEX bronze_object_member_object_idx
    ON catalog.bronze_object_member (bronze_object_id, member_name);

-- A measurement has exactly the members it counts (a non-ZIP outcome: none). Two checks hold
-- this together: after each members statement, every measurement it touched is complete (so no
-- later statement can add to one), and at commit, a new measurement has its members (so a ZIP
-- measurement cannot be left without them). The writer does both in one transaction.
CREATE FUNCTION catalog.check_bronze_object_members_complete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
DECLARE
    broken uuid;
BEGIN
    IF TG_TABLE_NAME = 'bronze_object_member' THEN
        SELECT measurement.id INTO broken
          FROM (SELECT DISTINCT measurement_id FROM inserted_members) AS touched
          JOIN catalog.bronze_object_measurement AS measurement
            ON measurement.id = touched.measurement_id
         WHERE COALESCE(measurement.member_count, 0) <> (
                   SELECT count(*) FROM catalog.bronze_object_member AS member
                    WHERE member.measurement_id = measurement.id)
         LIMIT 1;
    ELSIF COALESCE(NEW.member_count, 0) <> (
              SELECT count(*) FROM catalog.bronze_object_member AS member
               WHERE member.measurement_id = NEW.id) THEN
        broken := NEW.id;
    END IF;
    IF broken IS NOT NULL THEN
        RAISE EXCEPTION 'measurement % does not hold the members it counts', broken
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$function$;

CREATE TRIGGER bronze_object_member_completes_its_measurement
AFTER INSERT ON catalog.bronze_object_member
REFERENCING NEW TABLE AS inserted_members
FOR EACH STATEMENT EXECUTE FUNCTION catalog.check_bronze_object_members_complete();

CREATE CONSTRAINT TRIGGER bronze_object_measurement_holds_its_members
AFTER INSERT ON catalog.bronze_object_measurement
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION catalog.check_bronze_object_members_complete();

CREATE FUNCTION catalog.reject_bronze_object_member_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME USING ERRCODE = '55000';
END;
$function$;

CREATE TRIGGER bronze_object_measurement_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON catalog.bronze_object_measurement
FOR EACH STATEMENT EXECUTE FUNCTION catalog.reject_bronze_object_member_mutation();

CREATE TRIGGER bronze_object_member_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON catalog.bronze_object_member
FOR EACH STATEMENT EXECUTE FUNCTION catalog.reject_bronze_object_member_mutation();
