-- Root ADR-0226: an industrial-complex target_id is the lakehouse complex id (lowercase UUIDv5),
-- the id the complex tile publishes and the panel opens on. The Catalog row id (UUIDv7) is rejected.

-- The one place this database spells the form; the same pattern as
-- shared_kernel::lakehouse_complex_id and apps/web/lib/identity/patterns.ts.
CREATE DOMAIN lakehouse_complex_id AS text
    CHECK (VALUE ~ '^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$');

-- A CHECK cannot change a column type per row, so it casts: a wrong form raises the domain's error.
ALTER TABLE bookmark_external
    ADD CONSTRAINT bookmark_external_complex_target_is_lakehouse_id
    CHECK (target_kind <> 'industrial_complex' OR target_id::lakehouse_complex_id IS NOT NULL);

ALTER TABLE featured_content
    ADD CONSTRAINT featured_content_complex_target_is_lakehouse_id
    CHECK (target_kind <> 'industrial_complex' OR target_id::lakehouse_complex_id IS NOT NULL);
