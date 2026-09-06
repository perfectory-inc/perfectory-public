-- Root ADR-0087: source attributes, independently arriving from parcel boundaries.
-- Price is deliberately absent: catalog.parcel_price owns the serving price fact.
CREATE TABLE catalog.parcel_characteristic (
    pnu character(19) NOT NULL,
    land_category text,
    area_m2 numeric NOT NULL,
    land_use_situation text,
    terrain_height text,
    terrain_shape text,
    road_contact text,
    source_snapshot_id text NOT NULL,
    source_vintage date NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT parcel_characteristic_pkey PRIMARY KEY (pnu),
    CONSTRAINT parcel_characteristic_pnu_check CHECK (pnu ~ '^[0-9]{10}[1289][0-9]{8}$'),
    CONSTRAINT parcel_characteristic_area_check CHECK (
        area_m2 > 0 AND area_m2 < 'Infinity'::numeric
    ),
    CONSTRAINT parcel_characteristic_snapshot_check CHECK (btrim(source_snapshot_id) <> '')
);

COMMENT ON TABLE catalog.parcel_characteristic IS
    'Per-parcel newest AL_D195 CSV attributes from silver.land_characteristic (root ADR-0087).';
