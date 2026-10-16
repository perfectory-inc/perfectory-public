-- A staff edit of an industrial complex becomes a row in the Catalog edit ledger (ADR-0264).
--
-- `PATCH /catalog/v1/complexes/{id}` overwrites `name` and `area_m2` and keeps the previous value
-- only in an outbox event, which is drained, and never records who edited. The ledger built for
-- exactly this (ADR-0023) admits only `parcel`, a kind that no longer has a writer (ADR-0194).
-- The update now writes a `catalog.catalog_edit` row with its before and after snapshot and the
-- verified principal in the same transaction, so the vocabulary gains the kind that is written.

ALTER TABLE catalog.catalog_edit
    DROP CONSTRAINT catalog_edit_target_kind_check;

ALTER TABLE catalog.catalog_edit
    ADD CONSTRAINT catalog_edit_target_kind_check
    CHECK (target_kind IN ('parcel', 'industrial_complex'));
