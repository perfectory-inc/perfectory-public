-- A publication built from a collected file names the collection record, not a hand-written one.
--
-- Root ADR-0046 records the decision and the alternative it rejects.
--
-- Measured on the local service database on 2026-08-21, before this migration:
--
--     catalog.bronze_object    10,867 rows   source_record_id filled: 0
--     catalog.source_record         0 rows
--
-- `publish-industrial-complex-boundary-postgis` refused with "the industrial complex boundary
-- source_record does not exist", and it was right to: nothing in this repository writes
-- `catalog.source_record` from a collection. FP-ADR-0016 gives `BronzeCommitter` the duty of always
-- recording a `catalog.bronze_object` row and says nothing about `catalog.source_record`; the
-- writers of `catalog.source_record` are `promote_vector_tile_manifest`, the Gold pointer publish
-- and two migration seeds — every one of them a *publisher* describing a catalog entity, none of
-- them a collector. The lakehouse already agrees: `industrial_complex_boundary_silver_export` sets
-- Silver's `source_record_id` to the **Bronze object key**, so every one of the 1,343 exported rows
-- cites an object `catalog.bronze_object` holds and `catalog.source_record` has never heard of.
--
-- So the anchor moves rather than the gap being filled. Filling it would mean writing, today, a row
-- asserting a capture that happened months ago and was recorded elsewhere at the time.
--
-- Two columns change. Both keep a real foreign key; neither loses one.

-- Taken as statement zero, not where it is first needed. `catalog.publication_revision` carries a
-- BEFORE INSERT/UPDATE/DELETE capability trigger (`20260731000002`), and while DDL does not fire
-- row-level triggers, the report block below reads rows under the same transaction sqlx wraps this
-- file in. Kept for the same reason that migration states it: transaction-local, and the whole file
-- is the transaction.
SELECT set_config('foundation.temporal_publisher', 'on', true);

-- Report before reinterpreting. This migration re-reads one existing column as naming a different
-- table, and a row that cannot be re-read that way must name itself rather than surface later as a
-- foreign-key abort naming a constraint. Shape copied from `20260731000002`'s guard block.
DO $$
DECLARE
    offending text;
BEGIN
    SELECT string_agg(DISTINCT publication.source_record_id::text, ', ')
      INTO offending
      FROM serving_postgis.industrial_complex_boundary_publication AS publication
     WHERE NOT EXISTS (
         SELECT 1 FROM catalog.bronze_object AS object
          WHERE object.id = publication.source_record_id
     );
    IF offending IS NOT NULL THEN
        RAISE EXCEPTION 'published industrial complex boundaries cite source records that are not collected objects: %; re-publish them before this migration',
            offending
            USING ERRCODE = '23503';
    END IF;
END
$$;

-- One anchor per revision, and which kind it is is a fact about the unit's source rather than a
-- choice. A revision published from a collected file names the `catalog.bronze_object` the
-- collector committed; a revision published from a catalog entity the platform itself minted names
-- the `catalog.source_record` that describes it. `num_nonnulls(...) = 1` refuses both "neither" and
-- "both": a row carrying two anchors would be two provenance claims about one version of the data,
-- and the schema could not say which one a reader should believe.
ALTER TABLE catalog.publication_revision
    ALTER COLUMN source_record_id DROP NOT NULL,
    ADD COLUMN bronze_object_id uuid REFERENCES catalog.bronze_object(id) ON DELETE RESTRICT,
    ADD CONSTRAINT publication_revision_one_provenance_anchor_check
        CHECK (num_nonnulls(source_record_id, bronze_object_id) = 1);

COMMENT ON COLUMN catalog.publication_revision.bronze_object_id IS
    'The collected Bronze object this revision was published from, for a unit whose source is a collected file. Exactly one of this and source_record_id is set.';

-- The geometry rows follow their revision. The column is renamed rather than added beside the old
-- one: it held exactly one fact — which object these polygons came from — and root ADR-0044 is that
-- a column named for a fact must hold that fact. `source_object_key` stays, because it is the key
-- every exported row cites and `validate_row` compares against, and the publisher checks the two
-- agree before it writes either.
ALTER TABLE serving_postgis.industrial_complex_boundary_publication
    DROP CONSTRAINT complex_boundary_publication_source_record_fkey;
ALTER TABLE serving_postgis.industrial_complex_boundary_publication
    RENAME COLUMN source_record_id TO bronze_object_id;
ALTER TABLE serving_postgis.industrial_complex_boundary_publication
    ADD CONSTRAINT complex_boundary_publication_bronze_object_fkey
        FOREIGN KEY (bronze_object_id)
        REFERENCES catalog.bronze_object(id) ON DELETE RESTRICT;

COMMENT ON COLUMN serving_postgis.industrial_complex_boundary_publication.bronze_object_id IS
    'The collected Bronze object the Silver boundary was decoded from. Its checksum is compared against the object the publish names before any geometry is written.';
