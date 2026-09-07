-- Root ADR-0090: unit-level land rights remain independent of parcel boundary arrival.
CREATE TABLE catalog.parcel_land_right (
    pnu character(19) NOT NULL,
    right_serial_no text NOT NULL,
    building_name text,
    dong_name text,
    floor_name text,
    ho_name text,
    room_name text,
    right_ratio text,
    closure_kind_code text,
    closure_kind text,
    source_snapshot_id text NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT parcel_land_right_pkey PRIMARY KEY (pnu, right_serial_no),
    CONSTRAINT parcel_land_right_pnu_check CHECK (pnu ~ '^[0-9]{10}[1289][0-9]{8}$'),
    CONSTRAINT parcel_land_right_serial_check CHECK (right_serial_no ~ '^[0-9]+$'),
    CONSTRAINT parcel_land_right_snapshot_check CHECK (btrim(source_snapshot_id) <> '')
);

COMMENT ON TABLE catalog.parcel_land_right IS
    'AL_D006 registered unit-level land rights from silver.land_right_registration (root ADR-0090).';
COMMENT ON COLUMN catalog.parcel_land_right.right_serial_no IS
    'Provider serial preserved as digit text; duplicate keys in one vintage refuse the entire load.';
COMMENT ON COLUMN catalog.parcel_land_right.closure_kind IS
    'Verbatim provider closure-kind name; closed registrations remain queryable.';
