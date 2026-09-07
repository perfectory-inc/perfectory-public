-- Root ADR-0088: forest-ledger attributes arrive independently from parcel boundaries.
CREATE TABLE catalog.parcel_forest_ledger (
    pnu character(19) NOT NULL,
    land_category text,
    area_m2 numeric NOT NULL,
    ownership_kind text,
    co_owner_count integer,
    source_snapshot_id text NOT NULL,
    source_vintage date NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT parcel_forest_ledger_pkey PRIMARY KEY (pnu),
    CONSTRAINT parcel_forest_ledger_pnu_check CHECK (pnu ~ '^[0-9]{10}[1289][0-9]{8}$'),
    CONSTRAINT parcel_forest_ledger_area_check CHECK (
        area_m2 > 0 AND area_m2 < 'Infinity'::numeric
    ),
    CONSTRAINT parcel_forest_ledger_co_owner_count_check CHECK (
        co_owner_count IS NULL OR co_owner_count >= 0
    ),
    CONSTRAINT parcel_forest_ledger_snapshot_check CHECK (btrim(source_snapshot_id) <> '')
);

COMMENT ON TABLE catalog.parcel_forest_ledger IS
    'Per-parcel newest AL_D003 CSV forest-ledger attributes from silver.land_forest_ledger (root ADR-0088).';
COMMENT ON COLUMN catalog.parcel_forest_ledger.ownership_kind IS
    'Provider ownership_kind_code category; this is not an owner identity.';
