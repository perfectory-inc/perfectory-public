-- Membership of a parcel in an industrial complex is a dated fact with its own row.
--
-- `catalog.parcel.complex_id NOT NULL` says every parcel belongs to exactly one complex, forever,
-- with no record of when or on what evidence. `serving_postgis.parcel_boundary_mirror` — the table
-- that actually carries national parcel boundaries — has said otherwise since the day it was
-- written: its `complex_id` is nullable. This migration writes membership down as its own fact.
-- See ADR-0019.
--
-- The shape is not new. `20260727000001_administrative_boundary_identity.sql` solved exactly this
-- problem for administrative units in `catalog.parcel_administrative_unit`, and this table is that
-- template applied a second time: `effective_period daterange`, GiST `EXCLUDE`, append-only
-- trigger, and `data_revision`/`source_snapshot_id`/`source_record_id` lineage.
--
-- What this table does NOT carry is a geometric judgement. ADR-0020: a polygon inside another
-- polygon is not evidence that one belongs to the other, because source polygons disagree on
-- accuracy, epoch, projection and generalisation, and a parcel near a boundary lands on either side
-- depending on which file was read. Membership is what a record asserts — the government's
-- `(industrial_complex_code, pnu)` list — never what an overlay computed.
--
-- Step 1 of ADR-0019 §이행 순서 only. `catalog.parcel.complex_id` is not touched — not dropped, not
-- relaxed to nullable — so this migration is deployable on its own and nothing that reads the
-- column changes behaviour.

CREATE TABLE catalog.parcel_complex_membership (
    id uuid NOT NULL,
    parcel_id uuid NOT NULL,
    complex_id uuid NOT NULL,
    asserted_by text NOT NULL,
    effective_period daterange NOT NULL,
    data_revision uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT parcel_complex_membership_pkey PRIMARY KEY (id),
    -- There is no `membership_kind`. Without a geometric judgement there is only one kind of
    -- membership left, and a column with one value distinguishes nothing: the row's existence is
    -- the assertion. `asserted_by` survives because an official list and a human reviewer are
    -- different KINDS of claim about the same row, and `source_record_id` names the record a row
    -- came from without saying who asserted it. `catalog_domain::parcel_complex_membership` spells
    -- this same list in Rust, and
    -- `a_database_vocabulary_is_spelled_the_same_way_in_both_languages` reads this constraint out
    -- of the installed schema and compares it against the enum's `ALL` (ADR-0018), so a third
    -- spelling cannot appear without a red test.
    CONSTRAINT parcel_complex_membership_asserted_by_check
        CHECK (asserted_by IN ('official_list', 'manual_review')),
    CONSTRAINT parcel_complex_membership_period_check
        CHECK (NOT isempty(effective_period) AND lower_inc(effective_period)),
    -- One parcel, at most one complex, at any instant. This is the only true part of what
    -- `complex_id NOT NULL` asserted, and it is an EXCLUDE rather than a NOT NULL because it says
    -- "at most one", not "exactly one" — most parcels in the country belong to no complex at all.
    --
    -- Whether the government's own list ever puts one PNU under two complex codes is unverified:
    -- `sandan_parcel` is registered in the endpoint catalogue but has no collector yet (ADR-0020
    -- 남은 부채 1, 2). It is enforced anyway because the two costs are not symmetric. With the
    -- constraint, violating data stops loudly under a named error and relaxing it later is one
    -- migration. Without it, one parcel accumulates quietly under two complexes and the damage
    -- surfaces much later as doubled per-complex counts, by which time nothing says which row was
    -- right.
    CONSTRAINT parcel_complex_membership_one_complex_excl EXCLUDE USING gist
        (parcel_id WITH =, effective_period WITH &&)
);

-- Lineage, on the same four references the template uses. The revision ledger is
-- `catalog.administrative_boundary_revision` and not `catalog.publication_revision` because ADR-0017
-- split the PUBLICATION ledger only — its 남은 부채 4 records that the fact ledger is still
-- undivided and that every effective-dated catalog fact, `catalog.parcel_identifier` included,
-- still binds here — while a publication revision is scoped by FK to a vector tile publication
-- unit, which a membership fact is not a revision of.
ALTER TABLE catalog.parcel_complex_membership
    ADD CONSTRAINT parcel_complex_membership_parcel_fkey
    FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id) ON DELETE RESTRICT,
    ADD CONSTRAINT parcel_complex_membership_complex_fkey
    FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id) ON DELETE RESTRICT,
    ADD CONSTRAINT parcel_complex_membership_revision_fkey
    FOREIGN KEY (data_revision) REFERENCES catalog.administrative_boundary_revision(id) ON DELETE RESTRICT,
    ADD CONSTRAINT parcel_complex_membership_source_record_fkey
    FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id) ON DELETE RESTRICT;

-- Membership accumulates; it is never overwritten. A complex that gains or loses a parcel closes
-- the old row's interval and inserts a new one, so "which complex was this parcel in last year"
-- stays answerable. ADR-0020 §Decision 4.
CREATE TRIGGER parcel_complex_membership_append_only
BEFORE UPDATE OR DELETE ON catalog.parcel_complex_membership
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();

-- `source_snapshot_id` carries no format CHECK of its own on purpose: this trigger requires it to
-- equal the revision's, and `administrative_boundary_revision_snapshot_check` already constrains
-- that value to `^iceberg:…`. A second spelling of the same rule is the defect ADR-0018 names.
CREATE TRIGGER parcel_complex_membership_revision_snapshot_guard
BEFORE INSERT ON catalog.parcel_complex_membership
FOR EACH ROW EXECUTE FUNCTION catalog.validate_temporal_revision_snapshot();

-- Backfill. The three statements below are `20260727000001`'s, deliberately unchanged in form and
-- in identifier, so re-issuing them yields the rows that migration already created rather than a
-- second set claiming a different provenance for the same parcels. Parcels created after that
-- migration have no legacy source record yet, which is why this is an INSERT and not an assumption.
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

-- `status` is `validated`, not the `published` `20260727000001` wrote: `20260731000002` folded that
-- value away and the CHECK no longer admits it.
INSERT INTO catalog.administrative_boundary_revision
    (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status, validated_at)
VALUES
    (md5('legacy:administrative-boundary-revision')::uuid,
     '1',
     'iceberg:legacy-administrative-boundary',
     md5('legacy:administrative-boundary-revision')::uuid,
     'validated', now())
ON CONFLICT (id) DO NOTHING;

-- Every existing `catalog.parcel` row is asserted `official_list`: these parcels were written from
-- the complex's official parcel list, and no overlay produced them. The lower bound is
-- `created_at::date`, which is the earliest date this repository can prove the membership held;
-- nothing here knows when the designation actually took effect, and the migration does not pretend
-- to. Because the old column held exactly one complex per parcel, this backfill cannot violate the
-- one-complex exclusion.
INSERT INTO catalog.parcel_complex_membership
    (id, parcel_id, complex_id, asserted_by, effective_period,
     data_revision, source_snapshot_id, source_record_id)
SELECT md5('legacy:parcel-complex-membership:' || p.id::text)::uuid,
       p.id,
       p.complex_id,
       'official_list',
       daterange(p.created_at::date, NULL, '[)'),
       md5('legacy:administrative-boundary-revision')::uuid,
       'iceberg:legacy-administrative-boundary',
       md5('legacy:parcel:' || p.id::text)::uuid
  FROM catalog.parcel AS p
 WHERE NOT EXISTS (
       SELECT 1 FROM catalog.parcel_complex_membership AS m
        WHERE m.id = md5('legacy:parcel-complex-membership:' || p.id::text)::uuid
   );

-- The parcel-scoped direction answers "which complex is this parcel in", and the exclusion's own
-- GiST index cannot serve it: a range operator class does not support the equality-and-order
-- lookup a btree gives here.
CREATE INDEX parcel_complex_membership_parcel_idx
    ON catalog.parcel_complex_membership (parcel_id, lower(effective_period));
-- The complex-scoped direction is the one `GET /catalog/v1/complexes/{id}/parcels` will read in
-- step 2, and it has no other index to fall back on: the exclusion's index leads with `parcel_id`.
CREATE INDEX parcel_complex_membership_complex_idx
    ON catalog.parcel_complex_membership (complex_id, lower(effective_period));

COMMENT ON TABLE catalog.parcel_complex_membership IS
    'Effective-dated membership of a parcel in an industrial complex, as asserted by a record and never computed from geometry (ADR-0020). Membership is a fact between two entities, not a column on either of them; catalog.parcel.complex_id is the projection this table replaces.';
