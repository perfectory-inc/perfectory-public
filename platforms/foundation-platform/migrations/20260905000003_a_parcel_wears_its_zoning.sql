-- A parcel wears its zoning (root ADR-0083 §5).
--
-- One row is one (parcel, zone) verdict from the land-use plan ledger: the zone reached an
-- anchor in the LMIS code tree and the designation was 포함(1) or 저촉(2) — 접함(3) never
-- enters this table because being adjacent to a zone is not the parcel's own use. The loader
-- (`load-parcel-zoning-catalog-projection`) is the only writer; the anchor vocabulary is its
-- decision to enforce, not this table's to restate.
--
-- No FK to catalog.parcel: the plan ledger legitimately names parcels the boundary source has
-- not delivered yet (measured in Sejong: 207,840 plan parcels vs 207,699 boundary parcels),
-- and a zoning fact should not be dropped because the boundary row arrives later.
CREATE TABLE catalog.parcel_zoning (
    pnu character(19) NOT NULL,
    zone_code text NOT NULL,
    zone_name text,
    anchor_code text NOT NULL,
    inclusion_code text NOT NULL,
    source_snapshot_id text NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT parcel_zoning_pkey PRIMARY KEY (pnu, zone_code),
    CONSTRAINT parcel_zoning_pnu_check CHECK (pnu ~ '^[0-9]{19}$'::text),
    CONSTRAINT parcel_zoning_zone_code_check CHECK (btrim(zone_code) <> ''::text),
    CONSTRAINT parcel_zoning_anchor_code_check CHECK (btrim(anchor_code) <> ''::text),
    -- 포함(1) and 저촉(2) are the only designations that mean "this parcel's use";
    -- the source's third value, 접함(3), is excluded upstream by decision.
    CONSTRAINT parcel_zoning_inclusion_check CHECK (inclusion_code = ANY (ARRAY['1'::text, '2'::text]))
);

COMMENT ON TABLE catalog.parcel_zoning IS
    'Per-parcel land-use zoning verdicts projected from silver.land_use_plan via the LMIS code tree walk (root ADR-0083).';
