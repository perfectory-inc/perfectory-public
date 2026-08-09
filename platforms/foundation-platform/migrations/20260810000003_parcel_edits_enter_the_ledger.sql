-- A Catalog edit becomes a row in a Catalog-owned edit ledger.
--
-- ADR-0006 says the PostGIS projection is reconstructible from the selected Iceberg snapshot plus
-- an audited edit ledger. The one parcel edit this repository has does not produce one:
-- `PATCH /catalog/v1/parcels/{id}/kind` locks the row, checks the version, UPDATEs, emits an outbox
-- event, and records nothing. The previous value survives only in that event, which is a delivery
-- channel built to be drained, not a ledger. See ADR-0023.
--
-- `catalog.normalization_application` has exactly the right shape and is deliberately NOT reused.
-- Normalization is a separate bounded context: `package_boundary.rs` asserts that only
-- `foundation-normalization-infrastructure` touches `normalization_*` tables, and a second test
-- exists solely to stop that ownership returning to Catalog. Catalog does not depend on the
-- Normalization crates and this migration does not make it. Unifying the two ledgers is a decision
-- for whoever writes the projection rebuilder, which reads both (ADR-0023 남은 부채 3).

CREATE TABLE catalog.catalog_edit (
    id uuid NOT NULL,
    command_type text NOT NULL,
    target_kind text NOT NULL,
    target_id uuid NOT NULL,
    expected_version bigint NOT NULL,
    before_snapshot jsonb NOT NULL,
    after_snapshot jsonb NOT NULL,
    applied_by_principal_id uuid NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    -- Reverting is a new row naming the one it reverts, never a deletion — the same shape
    -- `normalization_application.rollback_of` uses, arrived at for the same reason: a ledger that
    -- can lose rows cannot rebuild anything.
    rollback_of uuid,
    CONSTRAINT catalog_edit_pkey PRIMARY KEY (id),
    CONSTRAINT catalog_edit_target_kind_check CHECK (target_kind IN ('parcel')),
    CONSTRAINT catalog_edit_command_type_check CHECK (btrim(command_type) <> ''),
    CONSTRAINT catalog_edit_expected_version_check CHECK (expected_version > 0),
    CONSTRAINT catalog_edit_before_object_check CHECK (jsonb_typeof(before_snapshot) = 'object'),
    CONSTRAINT catalog_edit_after_object_check CHECK (jsonb_typeof(after_snapshot) = 'object'),
    CONSTRAINT catalog_edit_no_self_rollback_check CHECK (rollback_of IS NULL OR rollback_of <> id),
    -- A revert states what it reverts and nothing else may claim to. One row can be reverted once;
    -- a second revert of the same edit would be two answers to "is this edit in effect".
    CONSTRAINT catalog_edit_rollback_of_key UNIQUE (rollback_of),
    -- Nothing in the Normalization context writes here. A `*.normalization.*` command in this table
    -- would mean that boundary was crossed by spelling rather than by dependency, which is the
    -- failure this table exists to avoid.
    CONSTRAINT catalog_edit_not_a_normalization_command_check
        CHECK (command_type NOT LIKE '%.normalization.%')
);

ALTER TABLE catalog.catalog_edit
    ADD CONSTRAINT catalog_edit_rollback_of_fkey
    FOREIGN KEY (rollback_of) REFERENCES catalog.catalog_edit(id) ON DELETE RESTRICT;

-- The ledger accumulates; it is never rewritten. Same trigger the temporal fact tables use.
CREATE TRIGGER catalog_edit_append_only
BEFORE UPDATE OR DELETE ON catalog.catalog_edit
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();

CREATE INDEX catalog_edit_target_idx
    ON catalog.catalog_edit (target_kind, target_id, applied_at);

COMMENT ON TABLE catalog.catalog_edit IS
    'Audited ledger of Catalog edits applied outside the Normalization pipeline, and half of what ADR-0006 rebuilds a serving projection from; catalog.normalization_application is the other half and belongs to a different bounded context (ADR-0023).';
