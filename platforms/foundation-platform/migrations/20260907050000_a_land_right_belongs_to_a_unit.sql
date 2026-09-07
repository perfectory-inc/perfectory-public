-- Root ADR-0093: a land right's identity is the unit, not the parcel serial.
UPDATE catalog.parcel_land_right
SET dong_name = COALESCE(dong_name, ''),
    floor_name = COALESCE(floor_name, ''),
    ho_name = COALESCE(ho_name, ''),
    room_name = COALESCE(room_name, '');

ALTER TABLE catalog.parcel_land_right
    ALTER COLUMN dong_name SET DEFAULT '',
    ALTER COLUMN dong_name SET NOT NULL,
    ALTER COLUMN floor_name SET DEFAULT '',
    ALTER COLUMN floor_name SET NOT NULL,
    ALTER COLUMN ho_name SET DEFAULT '',
    ALTER COLUMN ho_name SET NOT NULL,
    ALTER COLUMN room_name SET DEFAULT '',
    ALTER COLUMN room_name SET NOT NULL;

ALTER TABLE catalog.parcel_land_right
    DROP CONSTRAINT parcel_land_right_pkey,
    ADD CONSTRAINT parcel_land_right_pkey
        PRIMARY KEY (pnu, right_serial_no, dong_name, floor_name, ho_name, room_name);

COMMENT ON COLUMN catalog.parcel_land_right.right_serial_no IS
    'Provider per-parcel serial preserved as digit text; unit designation columns complete the key (root ADR-0093).';
COMMENT ON COLUMN catalog.parcel_land_right.dong_name IS
    'Unit dong designation; empty string when the provider row leaves it blank.';
