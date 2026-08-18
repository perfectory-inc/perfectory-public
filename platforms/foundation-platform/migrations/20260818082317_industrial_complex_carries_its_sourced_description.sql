-- The canonical complex row carries what the source says about the complex, not just its identity.
--
-- Everything `catalog.industrial_complex` said about a complex was its official code, name, kind,
-- legal-dong code, and area; the rest of the table is record lifecycle. `GET /catalog/v1/complexes`
-- reads that table directly, so those five were the whole public answer to "what is this industrial
-- complex". The address, the lifecycle status, the designation
-- and completion dates, and the managing and developing organizations all existed in
-- `silver.industrial_complexes` and had nowhere to land: root ADR-0040's loader recorded them in its
-- run summary as `gold_columns_not_loaded` precisely because this table had no column for them.
--
-- Every column is nullable. The profile source leaves cells blank, and a blank cell is `NULL` — the
-- loader skips no row over a missing agency name and substitutes nothing for a missing date.
--
-- What is *not* nullable-by-omission is emptiness. `''` claims the source stated a value while
-- carrying none, which is the one failure mode that turns "we do not know" into "we know it is
-- nothing" silently. The non-blank CHECKs below make that unrepresentable rather than merely
-- discouraged; they evaluate to NULL for a NULL value, which Postgres accepts, so absence still
-- passes. Same construction as the shape CHECK on `primary_bjdong_code` (root ADR-0035).
--
-- `status` gets a value-domain CHECK for the same reason `kind` already has one on this table: the
-- domain is owned by `catalog_domain::IndustrialComplexStatus`, and
-- `lakehouse_domain::INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES` is pinned to that enum by a test, but
-- neither can reach a hand-written `UPDATE`. The constraint is what makes an off-domain value
-- impossible for *every* writer instead of only for the ones that go through the Rust write path.
-- The cost is a third spelling of the six values; the table already pays that cost for `kind` and
-- the alternative is a column whose reader fails on rows the writer was allowed to create.
--
-- `sido_code` and `sigungu_code` get shape CHECKs matching `primary_bjdong_code`: a Korean legal
-- division code is two digits for a province and five for a city/county/district, and a code of the
-- wrong width in a column named for one of them is a lie the type system cannot catch.
--
-- `lakehouse_complex_id` is identity rather than description, and it is here because without it a
-- canonical row cannot name its own Gold artifact. `id` is minted locally with `Uuid::now_v7()`
-- when the row is created; the lakehouse derives its own `complex_id` as a UUIDv5 in the
-- Bronze-to-Silver job, and the Gold profile object key is
-- `gold/industrial-complex/profiles/{uuidv5(ns, '{iceberg_snapshot_id}:{complex_id}')}.json`.
-- Neither id is computable from the other — for `111010` the canonical row is
-- `01a0136d-…-7e61-…` and the lakehouse row is `7df3859c-…-51fa-…` — so a row carrying only the
-- first addresses no object in R2.
--
-- The value is deterministic, not a snapshot artifact: a UUIDv5 over the same source identity is
-- the same UUID after every Gold re-run, so this column does not churn when the projection is
-- rebuilt. Its shape CHECK pins the version nibble to 5, which is what makes the two identities
-- impossible to confuse: a locally minted `now_v7()` value written here is rejected rather than
-- stored as an object key that resolves to nothing. (`uuid_extract_version` is PostgreSQL 18; this
-- database is 17, so the check is written against the text form.)
--
-- Unique where present, as a partial index. `gold.complex_catalog` admits one row per `complex_id`
-- and the Spark job asserts it, so two canonical rows claiming one lakehouse complex is a defect
-- rather than a state to represent. Partial because the many rows with no lakehouse identity are
-- not in conflict with each other.
--
-- No data changes. Every existing row takes NULL in every new column, so no CHECK can fail here and
-- nothing is backfilled — there is no source from which a value could be guessed for a row this
-- migration did not load.
--
-- Rollback: `ALTER TABLE catalog.industrial_complex DROP COLUMN ...` as a new forward migration
-- (ADR-0001 §7 — migration files are immutable once merged). Dropping the columns discards loaded
-- source values, which the next canonical load restores from the Gold snapshot.

ALTER TABLE catalog.industrial_complex
    ADD COLUMN lakehouse_complex_id uuid,
    ADD COLUMN status text,
    ADD COLUMN sido_code text,
    ADD COLUMN sigungu_code text,
    ADD COLUMN address_text text,
    ADD COLUMN management_agency_name text,
    ADD COLUMN developer_name text,
    ADD COLUMN designated_date date,
    ADD COLUMN completion_date date,
    ADD CONSTRAINT industrial_complex_status_check
        CHECK (status = ANY (ARRAY[
            'planned'::text,
            'developing'::text,
            'operating'::text,
            'changed'::text,
            'abolished'::text,
            'unknown'::text
        ])),
    ADD CONSTRAINT industrial_complex_sido_code_shape
        CHECK (sido_code ~ '^[0-9]{2}$'),
    ADD CONSTRAINT industrial_complex_sigungu_code_shape
        CHECK (sigungu_code ~ '^[0-9]{5}$'),
    ADD CONSTRAINT industrial_complex_address_text_non_blank
        CHECK (btrim(address_text) <> ''),
    ADD CONSTRAINT industrial_complex_management_agency_name_non_blank
        CHECK (btrim(management_agency_name) <> ''),
    ADD CONSTRAINT industrial_complex_developer_name_non_blank
        CHECK (btrim(developer_name) <> ''),
    ADD CONSTRAINT industrial_complex_lakehouse_complex_id_is_uuid_v5
        CHECK (lakehouse_complex_id::text ~
               '^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$');

CREATE UNIQUE INDEX industrial_complex_lakehouse_complex_id_idx
    ON catalog.industrial_complex (lakehouse_complex_id)
    WHERE lakehouse_complex_id IS NOT NULL;
