-- A Gold pointer that names an object key but no address is not consumable (root ADR-0037).
--
-- `profile_object_key` says which object the pointer refers to; nothing in the contract said how a
-- client turns that key into a URL. The address becomes a publication input, recorded next to the
-- key, the way `catalog.vector_tile_manifest.tiles_url_template` already records it for tiles.
--
-- The column is mandatory: an address cannot be invented for a row that never carried one. This
-- migration refuses to run against a populated table rather than backfilling a guessed URL. No
-- producer for `gold/industrial-complex/profiles/{artifact_id}.json` existed before this change, so
-- no pointer row can legitimately exist yet.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM catalog.industrial_complex_gold_pointer) THEN
        RAISE EXCEPTION
            'catalog.industrial_complex_gold_pointer has rows; profile_url_template cannot be backfilled without inventing an address';
    END IF;
END
$$;

ALTER TABLE catalog.industrial_complex_gold_pointer
    ADD COLUMN profile_url_template text NOT NULL,
    -- Emptiness is the only rule restated in SQL. The full template contract (exactly one
    -- {object_key} placeholder, absolute HTTPS except loopback, no query or fragment) lives in
    -- `foundation_shared_kernel::ObjectUrlTemplate`; restating it here is how two copies of one
    -- rule drift apart.
    ADD CONSTRAINT industrial_complex_gold_pointer_profile_url_template_check
        CHECK (profile_url_template <> '');
