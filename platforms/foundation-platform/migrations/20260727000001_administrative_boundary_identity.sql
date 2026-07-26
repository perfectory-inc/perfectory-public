-- Versioned administrative identity and boundary facts.
--
-- `catalog.parcel.id` remains the stable internal identity. Official PNU/codes/names
-- are evidence-backed, effective-dated facts. The old `catalog.parcel.pnu` column is
-- retained as a compatibility projection until every producer uses the publisher
-- function below; reads use the views at the end of this migration.

CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE catalog.administrative_boundary_revision (
    id uuid NOT NULL,
    canonical_iceberg_snapshot_id text NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'candidate',
    created_at timestamptz NOT NULL DEFAULT now(),
    validated_at timestamptz,
    CONSTRAINT administrative_boundary_revision_pkey PRIMARY KEY (id),
    CONSTRAINT administrative_boundary_revision_snapshot_check CHECK (canonical_iceberg_snapshot_id ~ '^[1-9][0-9]*$' AND source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'),
    CONSTRAINT administrative_boundary_revision_status_check CHECK (status IN ('candidate', 'validated', 'published', 'superseded')),
    CONSTRAINT administrative_boundary_revision_validation_check CHECK (status IN ('candidate', 'superseded') OR validated_at IS NOT NULL)
);

CREATE TABLE catalog.administrative_unit (
    id uuid NOT NULL,
    unit_kind text NOT NULL,
    stable_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    retired_at timestamptz,
    CONSTRAINT administrative_unit_pkey PRIMARY KEY (id),
    CONSTRAINT administrative_unit_kind_check CHECK (unit_kind IN ('sido', 'sigungu', 'legal_dong')),
    CONSTRAINT administrative_unit_stable_key_check CHECK (btrim(stable_key) <> ''),
    CONSTRAINT administrative_unit_stable_key_key UNIQUE (stable_key),
    CONSTRAINT administrative_unit_retired_at_check CHECK (retired_at IS NULL OR retired_at >= created_at)
);

CREATE TABLE catalog.administrative_unit_identifier (
    id uuid NOT NULL,
    administrative_unit_id uuid NOT NULL,
    authority text NOT NULL,
    code text NOT NULL,
    display_name text NOT NULL,
    effective_period daterange NOT NULL,
    data_revision uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT administrative_unit_identifier_pkey PRIMARY KEY (id),
    CONSTRAINT administrative_unit_identifier_authority_check CHECK (btrim(authority) <> ''),
    CONSTRAINT administrative_unit_identifier_code_check CHECK (btrim(code) <> ''),
    CONSTRAINT administrative_unit_identifier_name_check CHECK (btrim(display_name) <> ''),
    CONSTRAINT administrative_unit_identifier_period_check CHECK (NOT isempty(effective_period) AND lower_inc(effective_period)),
    CONSTRAINT administrative_unit_identifier_snapshot_check CHECK (source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'),
    CONSTRAINT administrative_unit_identifier_code_period_excl EXCLUDE USING gist
        (authority WITH =, code WITH =, effective_period WITH &&),
    CONSTRAINT administrative_unit_identifier_unit_period_excl EXCLUDE USING gist
        (administrative_unit_id WITH =, authority WITH =, effective_period WITH &&)
);

CREATE TABLE catalog.administrative_unit_transition (
    id uuid NOT NULL,
    from_unit_id uuid NOT NULL,
    to_unit_id uuid NOT NULL,
    transition_kind text NOT NULL,
    effective_period daterange NOT NULL,
    data_revision uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    note text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT administrative_unit_transition_pkey PRIMARY KEY (id),
    CONSTRAINT administrative_unit_transition_distinct_units_check CHECK (from_unit_id <> to_unit_id),
    CONSTRAINT administrative_unit_transition_kind_check CHECK (transition_kind IN ('replaced_by', 'merged_into', 'split_from')),
    CONSTRAINT administrative_unit_transition_period_check CHECK (NOT isempty(effective_period) AND lower_inc(effective_period)),
    CONSTRAINT administrative_unit_transition_snapshot_check CHECK (source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'),
    CONSTRAINT administrative_unit_transition_edge_excl EXCLUDE USING gist
        (from_unit_id WITH =, to_unit_id WITH =, transition_kind WITH =, effective_period WITH &&)
);

CREATE TABLE catalog.administrative_unit_parent (
    id uuid NOT NULL,
    child_unit_id uuid NOT NULL,
    parent_unit_id uuid NOT NULL,
    effective_period daterange NOT NULL,
    data_revision uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT administrative_unit_parent_pkey PRIMARY KEY (id),
    CONSTRAINT administrative_unit_parent_distinct_check CHECK (child_unit_id <> parent_unit_id),
    CONSTRAINT administrative_unit_parent_period_check CHECK (NOT isempty(effective_period) AND lower_inc(effective_period)),
    CONSTRAINT administrative_unit_parent_snapshot_check CHECK (source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'),
    CONSTRAINT administrative_unit_parent_one_parent_excl EXCLUDE USING gist
        (child_unit_id WITH =, effective_period WITH &&)
);

CREATE TABLE catalog.parcel_identifier (
    id uuid NOT NULL,
    parcel_id uuid NOT NULL,
    identifier_kind text NOT NULL,
    identifier_value text NOT NULL,
    effective_period daterange NOT NULL,
    data_revision uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT parcel_identifier_pkey PRIMARY KEY (id),
    CONSTRAINT parcel_identifier_kind_check CHECK (identifier_kind = 'pnu'),
    CONSTRAINT parcel_identifier_value_check CHECK (identifier_value ~ '^[0-9]{10}[1289][0-9]{8}$'),
    CONSTRAINT parcel_identifier_period_check CHECK (NOT isempty(effective_period) AND lower_inc(effective_period)),
    CONSTRAINT parcel_identifier_snapshot_check CHECK (source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'),
    CONSTRAINT parcel_identifier_value_key UNIQUE (identifier_kind, identifier_value),
    CONSTRAINT parcel_identifier_parcel_period_excl EXCLUDE USING gist
        (parcel_id WITH =, identifier_kind WITH =, effective_period WITH &&)
);

CREATE TABLE catalog.parcel_administrative_unit (
    id uuid NOT NULL,
    parcel_id uuid NOT NULL,
    administrative_unit_id uuid NOT NULL,
    relation_kind text NOT NULL,
    effective_period daterange NOT NULL,
    data_revision uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT parcel_administrative_unit_pkey PRIMARY KEY (id),
    CONSTRAINT parcel_administrative_unit_relation_kind_check CHECK (relation_kind IN ('located_in_sido', 'located_in_sigungu', 'located_in_legal_dong')),
    CONSTRAINT parcel_administrative_unit_period_check CHECK (NOT isempty(effective_period) AND lower_inc(effective_period)),
    CONSTRAINT parcel_administrative_unit_snapshot_check CHECK (source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'),
    CONSTRAINT parcel_administrative_unit_membership_period_excl EXCLUDE USING gist
        (parcel_id WITH =, relation_kind WITH =, effective_period WITH &&)
);

ALTER TABLE catalog.administrative_boundary_revision
    ADD CONSTRAINT administrative_boundary_revision_source_record_fkey
    FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id) ON DELETE RESTRICT;
ALTER TABLE catalog.administrative_unit_identifier
    ADD CONSTRAINT administrative_unit_identifier_unit_fkey
    FOREIGN KEY (administrative_unit_id) REFERENCES catalog.administrative_unit(id) ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_unit_identifier_revision_fkey
    FOREIGN KEY (data_revision) REFERENCES catalog.administrative_boundary_revision(id) ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_unit_identifier_source_record_fkey
    FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id) ON DELETE RESTRICT;
ALTER TABLE catalog.administrative_unit_transition
    ADD CONSTRAINT administrative_unit_transition_from_fkey
    FOREIGN KEY (from_unit_id) REFERENCES catalog.administrative_unit(id) ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_unit_transition_to_fkey
    FOREIGN KEY (to_unit_id) REFERENCES catalog.administrative_unit(id) ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_unit_transition_revision_fkey
    FOREIGN KEY (data_revision) REFERENCES catalog.administrative_boundary_revision(id) ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_unit_transition_source_record_fkey
    FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id) ON DELETE RESTRICT;
ALTER TABLE catalog.administrative_unit_parent
    ADD CONSTRAINT administrative_unit_parent_child_fkey
    FOREIGN KEY (child_unit_id) REFERENCES catalog.administrative_unit(id) ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_unit_parent_parent_fkey
    FOREIGN KEY (parent_unit_id) REFERENCES catalog.administrative_unit(id) ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_unit_parent_revision_fkey
    FOREIGN KEY (data_revision) REFERENCES catalog.administrative_boundary_revision(id) ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_unit_parent_source_record_fkey
    FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id) ON DELETE RESTRICT;
ALTER TABLE catalog.parcel_identifier
    ADD CONSTRAINT parcel_identifier_parcel_fkey
    FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id) ON DELETE RESTRICT,
    ADD CONSTRAINT parcel_identifier_revision_fkey
    FOREIGN KEY (data_revision) REFERENCES catalog.administrative_boundary_revision(id) ON DELETE RESTRICT,
    ADD CONSTRAINT parcel_identifier_source_record_fkey
    FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id) ON DELETE RESTRICT;
ALTER TABLE catalog.parcel_administrative_unit
    ADD CONSTRAINT parcel_administrative_unit_parcel_fkey
    FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id) ON DELETE RESTRICT,
    ADD CONSTRAINT parcel_administrative_unit_unit_fkey
    FOREIGN KEY (administrative_unit_id) REFERENCES catalog.administrative_unit(id) ON DELETE RESTRICT,
    ADD CONSTRAINT parcel_administrative_unit_revision_fkey
    FOREIGN KEY (data_revision) REFERENCES catalog.administrative_boundary_revision(id) ON DELETE RESTRICT,
    ADD CONSTRAINT parcel_administrative_unit_source_record_fkey
    FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id) ON DELETE RESTRICT;

-- Preserve lineage for pre-versioning rows. This is a deterministic compatibility bridge,
-- not a claim that the legacy rows had a richer source than the migration can prove.
INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256)
SELECT md5('legacy:parcel:' || p.id::text)::uuid,
       'foundation.migration',
       'legacy:catalog.parcel:' || p.id::text,
       repeat('0', 64)
  FROM catalog.parcel AS p
 WHERE NOT EXISTS (
       SELECT 1 FROM catalog.source_record AS sr
        WHERE sr.id = md5('legacy:parcel:' || p.id::text)::uuid
   );

INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256)
SELECT md5('legacy:administrative-boundary-revision')::uuid,
       'foundation.migration',
       'legacy:administrative-boundary-revision',
       repeat('0', 64)
 WHERE NOT EXISTS (
       SELECT 1 FROM catalog.source_record
        WHERE id = md5('legacy:administrative-boundary-revision')::uuid
   );

-- Bind pre-existing tile releases to the same canonical revision ledger before adding
-- the composite FK. Existing release rows already carry a numeric Iceberg snapshot and
-- a Catalog source-record FK, so this backfill does not invent tile provenance.
DO $function$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_release
         GROUP BY data_revision
        HAVING count(DISTINCT canonical_iceberg_snapshot_id) > 1
    ) THEN
        RAISE EXCEPTION 'one data_revision maps to multiple canonical Iceberg snapshots';
    END IF;
END;
$function$;

INSERT INTO catalog.administrative_boundary_revision
    (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status, validated_at)
SELECT release.data_revision,
       (array_agg(release.canonical_iceberg_snapshot_id ORDER BY release.id))[1],
       'iceberg:vector-tile-release:' || release.data_revision::text,
       (array_agg(release.source_record_id ORDER BY release.id))[1],
       'published', now()
  FROM catalog.vector_tile_release AS release
 WHERE NOT EXISTS (
       SELECT 1
         FROM catalog.administrative_boundary_revision AS revision
        WHERE revision.id = release.data_revision
   )
 GROUP BY release.data_revision;

INSERT INTO catalog.administrative_boundary_revision
    (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status, validated_at)
VALUES
    (md5('legacy:administrative-boundary-revision')::uuid,
     '1',
     'iceberg:legacy-administrative-boundary',
     md5('legacy:administrative-boundary-revision')::uuid,
     'published', now())
ON CONFLICT (id) DO NOTHING;

ALTER TABLE catalog.administrative_boundary_revision
    ADD CONSTRAINT administrative_boundary_revision_id_snapshot_key
    UNIQUE (id, canonical_iceberg_snapshot_id);
ALTER TABLE catalog.vector_tile_release
    ADD CONSTRAINT vector_tile_release_data_revision_fkey
    FOREIGN KEY (data_revision, canonical_iceberg_snapshot_id)
    REFERENCES catalog.administrative_boundary_revision(id, canonical_iceberg_snapshot_id)
    ON DELETE RESTRICT;

INSERT INTO catalog.parcel_identifier
    (id, parcel_id, identifier_kind, identifier_value, effective_period, data_revision,
     source_snapshot_id, source_record_id)
SELECT md5('legacy:parcel-identifier:' || p.id::text)::uuid,
       p.id,
       'pnu',
       btrim(p.pnu::text),
       daterange(p.created_at::date, NULL, '[)'),
       md5('legacy:administrative-boundary-revision')::uuid,
       'iceberg:legacy-administrative-boundary',
       md5('legacy:parcel:' || p.id::text)::uuid
  FROM catalog.parcel AS p
 WHERE btrim(p.pnu::text) ~ '^[0-9]{10}[1289][0-9]{8}$'
   AND NOT EXISTS (
       SELECT 1 FROM catalog.parcel_identifier AS pi
        WHERE pi.identifier_kind = 'pnu' AND pi.identifier_value = btrim(p.pnu::text)
   );

CREATE OR REPLACE FUNCTION catalog.reject_temporal_history_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
BEGIN
    IF current_setting('foundation.temporal_publisher', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'temporal identity facts are append-only; use the Foundation publisher'
            USING ERRCODE = '42501';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$function$;

CREATE TRIGGER administrative_boundary_revision_append_only
BEFORE UPDATE OR DELETE ON catalog.administrative_boundary_revision
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();

CREATE TRIGGER administrative_unit_identifier_append_only
BEFORE UPDATE OR DELETE ON catalog.administrative_unit_identifier
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();
CREATE TRIGGER administrative_unit_transition_append_only
BEFORE UPDATE OR DELETE ON catalog.administrative_unit_transition
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();
CREATE TRIGGER administrative_unit_parent_append_only
BEFORE UPDATE OR DELETE ON catalog.administrative_unit_parent
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();

CREATE OR REPLACE FUNCTION catalog.validate_administrative_unit_parent()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
DECLARE
    child_kind text;
    parent_kind text;
BEGIN
    SELECT unit_kind INTO child_kind FROM administrative_unit WHERE id = NEW.child_unit_id;
    SELECT unit_kind INTO parent_kind FROM administrative_unit WHERE id = NEW.parent_unit_id;
    IF child_kind IS NULL OR parent_kind IS NULL
       OR (child_kind = 'sigungu' AND parent_kind <> 'sido')
       OR (child_kind = 'legal_dong' AND parent_kind <> 'sigungu')
       OR child_kind = 'sido' THEN
        RAISE EXCEPTION 'administrative unit parent hierarchy is invalid' USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM administrative_unit_identifier AS parent_identifier
         WHERE parent_identifier.administrative_unit_id = NEW.parent_unit_id
           AND parent_identifier.effective_period @> lower(NEW.effective_period)
           AND (upper_inf(parent_identifier.effective_period)
                OR upper(parent_identifier.effective_period) >= upper(NEW.effective_period))
    ) THEN
        RAISE EXCEPTION 'administrative unit parent is not valid for the complete child interval'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER administrative_unit_parent_hierarchy_guard
BEFORE INSERT ON catalog.administrative_unit_parent
FOR EACH ROW EXECUTE FUNCTION catalog.validate_administrative_unit_parent();
CREATE TRIGGER parcel_identifier_append_only
BEFORE UPDATE OR DELETE ON catalog.parcel_identifier
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();
CREATE TRIGGER parcel_administrative_unit_append_only
BEFORE UPDATE OR DELETE ON catalog.parcel_administrative_unit
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();

CREATE OR REPLACE FUNCTION catalog.protect_parcel_pnu_projection()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'UPDATE'
       AND NEW.pnu IS DISTINCT FROM OLD.pnu
       AND current_setting('foundation.temporal_publisher', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'catalog.parcel.pnu is a compatibility projection; use the Foundation publisher'
            USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER parcel_pnu_projection_guard
BEFORE UPDATE ON catalog.parcel
FOR EACH ROW EXECUTE FUNCTION catalog.protect_parcel_pnu_projection();

CREATE OR REPLACE FUNCTION catalog.protect_administrative_unit_identity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
BEGIN
    IF current_setting('foundation.temporal_publisher', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'administrative unit identity is immutable; create a new stable unit'
            USING ERRCODE = '42501';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$function$;

CREATE TRIGGER administrative_unit_identity_guard
BEFORE UPDATE OR DELETE ON catalog.administrative_unit
FOR EACH ROW EXECUTE FUNCTION catalog.protect_administrative_unit_identity();

CREATE OR REPLACE FUNCTION catalog.prevent_administrative_unit_transition_cycle()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
BEGIN
    -- Serialize the graph check so concurrent A->B and B->A inserts cannot both
    -- observe a cycle-free snapshot.
    PERFORM pg_advisory_xact_lock(hashtextextended('administrative-unit-transition-graph', 0));
    IF EXISTS (
        WITH RECURSIVE reachable(unit_id) AS (
            SELECT NEW.to_unit_id
            UNION
            SELECT t.to_unit_id
              FROM catalog.administrative_unit_transition AS t
              JOIN reachable AS r ON r.unit_id = t.from_unit_id
        )
        SELECT 1 FROM reachable WHERE unit_id = NEW.from_unit_id
    ) THEN
        RAISE EXCEPTION 'administrative unit transition would create a cycle'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER administrative_unit_transition_acyclic
BEFORE INSERT ON catalog.administrative_unit_transition
FOR EACH ROW EXECUTE FUNCTION catalog.prevent_administrative_unit_transition_cycle();

CREATE OR REPLACE FUNCTION catalog.validate_parcel_administrative_unit_kind()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
DECLARE
    expected_kind text;
BEGIN
    SELECT 'located_in_' || unit_kind
      INTO expected_kind
      FROM catalog.administrative_unit
     WHERE id = NEW.administrative_unit_id;
    IF expected_kind IS NULL OR NEW.relation_kind <> expected_kind THEN
        RAISE EXCEPTION 'parcel administrative membership kind does not match the administrative unit kind'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER parcel_administrative_unit_kind_guard
BEFORE INSERT ON catalog.parcel_administrative_unit
FOR EACH ROW EXECUTE FUNCTION catalog.validate_parcel_administrative_unit_kind();

CREATE OR REPLACE FUNCTION catalog.validate_temporal_revision_snapshot()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
DECLARE
    expected_snapshot text;
BEGIN
    SELECT source_snapshot_id
      INTO expected_snapshot
      FROM administrative_boundary_revision
     WHERE id = NEW.data_revision;
    IF expected_snapshot IS NULL OR expected_snapshot <> NEW.source_snapshot_id THEN
        RAISE EXCEPTION 'temporal fact snapshot does not match its data revision'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER administrative_unit_identifier_revision_snapshot_guard
BEFORE INSERT ON catalog.administrative_unit_identifier
FOR EACH ROW EXECUTE FUNCTION catalog.validate_temporal_revision_snapshot();
CREATE TRIGGER administrative_unit_transition_revision_snapshot_guard
BEFORE INSERT ON catalog.administrative_unit_transition
FOR EACH ROW EXECUTE FUNCTION catalog.validate_temporal_revision_snapshot();
CREATE TRIGGER administrative_unit_parent_revision_snapshot_guard
BEFORE INSERT ON catalog.administrative_unit_parent
FOR EACH ROW EXECUTE FUNCTION catalog.validate_temporal_revision_snapshot();
CREATE TRIGGER parcel_identifier_revision_snapshot_guard
BEFORE INSERT ON catalog.parcel_identifier
FOR EACH ROW EXECUTE FUNCTION catalog.validate_temporal_revision_snapshot();
CREATE TRIGGER parcel_administrative_unit_revision_snapshot_guard
BEFORE INSERT ON catalog.parcel_administrative_unit
FOR EACH ROW EXECUTE FUNCTION catalog.validate_temporal_revision_snapshot();

CREATE OR REPLACE FUNCTION catalog.publish_parcel_identifier(
    p_parcel_id uuid,
    p_identifier_value text,
    p_effective_from date,
    p_data_revision uuid,
    p_expected_data_revision uuid,
    p_source_snapshot_id text,
    p_source_record_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
DECLARE
    current_revision uuid;
    current_identifier text;
    expected_snapshot text;
    revision_status text;
BEGIN
    PERFORM set_config('foundation.temporal_publisher', 'on', true);
    IF p_identifier_value !~ '^[0-9]{10}[1289][0-9]{8}$' THEN
        RAISE EXCEPTION 'PNU must contain exactly 19 digits' USING ERRCODE = '22023';
    END IF;
    IF p_effective_from IS NULL OR p_source_snapshot_id IS NULL OR btrim(p_source_snapshot_id) = '' THEN
        RAISE EXCEPTION 'effective date and source snapshot are required' USING ERRCODE = '22023';
    END IF;
    SELECT source_snapshot_id, status
      INTO expected_snapshot, revision_status
      FROM administrative_boundary_revision
     WHERE id = p_data_revision;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown administrative boundary data revision %', p_data_revision
            USING ERRCODE = '23503';
    END IF;
    IF revision_status NOT IN ('validated', 'published') THEN
        RAISE EXCEPTION 'administrative boundary revision % is not validated', p_data_revision
            USING ERRCODE = '23514';
    END IF;
    IF expected_snapshot <> p_source_snapshot_id THEN
        RAISE EXCEPTION 'source snapshot does not match administrative boundary revision'
            USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM source_record WHERE id = p_source_record_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown source record %', p_source_record_id USING ERRCODE = '23503';
    END IF;

    PERFORM 1 FROM parcel WHERE id = p_parcel_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown parcel %', p_parcel_id USING ERRCODE = '23503';
    END IF;
    SELECT data_revision, identifier_value
      INTO current_revision, current_identifier
      FROM parcel_current_identifier
     WHERE parcel_id = p_parcel_id;
    IF current_revision IS NOT NULL AND p_expected_data_revision IS NULL THEN
        RAISE EXCEPTION 'expected current parcel identity revision is required for %', p_parcel_id
            USING ERRCODE = '40001';
    END IF;
    IF current_revision IS DISTINCT FROM p_expected_data_revision AND current_revision IS NOT NULL THEN
        RAISE EXCEPTION 'stale parcel identity revision for %', p_parcel_id USING ERRCODE = '40001';
    END IF;
    IF current_revision IS NOT NULL AND current_identifier = p_identifier_value THEN
        PERFORM set_config('foundation.temporal_publisher', 'off', true);
        RETURN;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('parcel-identity:' || p_parcel_id::text, 0));

    UPDATE parcel_identifier
       SET effective_period = daterange(lower(effective_period), p_effective_from, '[)')
     WHERE parcel_id = p_parcel_id
       AND identifier_kind = 'pnu'
       AND upper_inf(effective_period)
       AND lower(effective_period) < p_effective_from;

    INSERT INTO parcel_identifier
        (id, parcel_id, identifier_kind, identifier_value, effective_period, data_revision,
         source_snapshot_id, source_record_id)
    VALUES
        (public.gen_random_uuid(), p_parcel_id, 'pnu', p_identifier_value,
         daterange(p_effective_from, NULL, '[)'), p_data_revision,
         p_source_snapshot_id, p_source_record_id);

    UPDATE parcel
       SET pnu = p_identifier_value, updated_at = now(), version = version + 1
     WHERE id = p_parcel_id;
    PERFORM set_config('foundation.temporal_publisher', 'off', true);
END;
$function$;

REVOKE ALL ON FUNCTION catalog.publish_parcel_identifier(uuid, text, date, uuid, uuid, text, uuid)
    FROM PUBLIC;

CREATE OR REPLACE VIEW catalog.parcel_identifier_lookup AS
SELECT pi.parcel_id, pi.identifier_value, pi.effective_period, pi.data_revision
  FROM catalog.parcel_identifier AS pi
UNION ALL
SELECT p.id, btrim(p.pnu::text), daterange(p.created_at::date, NULL, '[)'), NULL::uuid
  FROM catalog.parcel AS p
 WHERE NOT EXISTS (SELECT 1 FROM catalog.parcel_identifier AS pi WHERE pi.parcel_id = p.id);

CREATE OR REPLACE VIEW catalog.parcel_current_identifier AS
SELECT p.id AS parcel_id,
       COALESCE(current_identifier.identifier_value, btrim(p.pnu::text)) AS identifier_value,
       current_identifier.data_revision
  FROM catalog.parcel AS p
  LEFT JOIN LATERAL (
      SELECT pi.identifier_value, pi.data_revision
        FROM catalog.parcel_identifier AS pi
       WHERE pi.parcel_id = p.id
         AND pi.identifier_kind = 'pnu'
         AND pi.effective_period @> CURRENT_DATE
       ORDER BY lower(pi.effective_period) DESC
       LIMIT 1
  ) AS current_identifier ON true;

CREATE OR REPLACE VIEW catalog.parcels_missing_temporal_identifier AS
SELECT p.id AS parcel_id, btrim(p.pnu::text) AS legacy_pnu, p.created_at
  FROM catalog.parcel AS p
 WHERE btrim(p.pnu::text) ~ '^[0-9]{10}[1289][0-9]{8}$'
   AND NOT EXISTS (
       SELECT 1 FROM catalog.parcel_identifier AS pi WHERE pi.parcel_id = p.id
   );

CREATE INDEX administrative_unit_identifier_lookup_idx
    ON catalog.administrative_unit_identifier (authority, code, lower(effective_period));
CREATE INDEX administrative_unit_transition_from_idx
    ON catalog.administrative_unit_transition (from_unit_id, lower(effective_period));
CREATE INDEX administrative_unit_parent_child_idx
    ON catalog.administrative_unit_parent (child_unit_id, lower(effective_period));
CREATE INDEX parcel_identifier_lookup_idx
    ON catalog.parcel_identifier (identifier_kind, identifier_value);
CREATE INDEX parcel_identifier_parcel_idx
    ON catalog.parcel_identifier (parcel_id, lower(effective_period));
CREATE INDEX parcel_administrative_unit_lookup_idx
    ON catalog.parcel_administrative_unit (parcel_id, relation_kind, lower(effective_period));

COMMENT ON TABLE catalog.administrative_boundary_revision IS
    'Canonical administrative-boundary snapshot. Every temporal fact and tile release binds to one revision.';
COMMENT ON TABLE catalog.administrative_unit IS
    'Stable Foundation identity for a legal administrative unit; official codes and names live in identifier history.';
COMMENT ON TABLE catalog.administrative_unit_identifier IS
    'Effective-dated official code/name facts. Rename = a new fact on the same stable unit.';
COMMENT ON TABLE catalog.administrative_unit_transition IS
    'Auditable merge/split/replacement graph. It intentionally has no renamed self-edge.';
COMMENT ON TABLE catalog.parcel_identifier IS
    'Effective-dated standard cadastral PNU facts. Block/register keys without a standard PNU remain on their existing register-key contract.';
