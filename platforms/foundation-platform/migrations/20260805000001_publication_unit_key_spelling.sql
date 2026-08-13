-- One spelling rule for a publication unit key, in the language that stores it.
--
-- `catalog.vector_tile_publication_unit.unit_key` is not just a name. It becomes a Martin source id
-- and the stem of an immutable PMTiles object key, so what this column admits decides what is
-- addressable in object storage.
--
-- The column and the publisher disagreed in three directions at once. The column admitted upper
-- case and refused `.`; `r2_layout.rs` refused upper case, admitted `.`, and let a key start with a
-- digit or a separator. ADR-0013 남은 부채 1 recorded only the first of those, as the publisher
-- being "stricter" — it was not a subset in either direction, so no unit could rely on either rule.
--
-- This narrows the column to the intersection, which is the set the derivations can actually carry:
--
--   lower case  — object keys are case-sensitive, so `Parcels` and `parcels` would be two prefixes
--                 for one unit;
--   no `.`      — the derived name is a filename stem with the extension appended, and Martin
--                 discovers static sources by splitting exactly there;
--   letter first — a key opening with a digit or a separator reads as a fragment, and nothing in
--                 use needs it.
--
-- `catalog_domain::is_publication_unit_key` is the same rule in Rust, and
-- `a_publication_unit_key_is_spelled_the_same_way_in_both_languages` compares the two against a
-- shared candidate set rather than trusting that they were written to agree.

BEGIN;

-- Every unit that exists is already inside the narrower set (`parcels`, `complex`, `buildings`,
-- `admin`). Proving it here rather than assuming it: a row outside the new rule would otherwise make
-- the constraint fail to attach with a message about validation rather than about the row.
DO $function$
DECLARE
    offending text;
BEGIN
    SELECT string_agg(unit_key, ', ' ORDER BY unit_key)
      INTO offending
      FROM catalog.vector_tile_publication_unit
     WHERE unit_key !~ '^[a-z][a-z0-9_-]{0,127}$';
    IF offending IS NOT NULL THEN
        RAISE EXCEPTION
            'publication unit keys outside the addressable spelling: %. Rename them before this '
            'migration can narrow the constraint; the key is part of every derived object address.',
            offending;
    END IF;
END;
$function$;

ALTER TABLE catalog.vector_tile_publication_unit
    DROP CONSTRAINT vector_tile_publication_unit_key_check;
ALTER TABLE catalog.vector_tile_publication_unit
    ADD CONSTRAINT vector_tile_publication_unit_key_check
    CHECK (unit_key ~ '^[a-z][a-z0-9_-]{0,127}$');

COMMENT ON COLUMN catalog.vector_tile_publication_unit.unit_key IS
    'Addressable identity of a publication unit: lower-case, letter-first, [a-z0-9_-]. It becomes '
    'the Martin source id and the PMTiles object-key stem, so the spelling is a storage contract, '
    'not a label. catalog_domain::is_publication_unit_key states the same rule in Rust.';

COMMIT;
