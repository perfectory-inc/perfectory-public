-- Root ADR-0089: an event timeline, independent of the parcel boundary load.
CREATE TABLE catalog.parcel_transfer_event (
    pnu character(19) NOT NULL,
    transfer_history_seq bigint NOT NULL,
    reason_code text,
    reason text,
    moved_at text,
    erased_at text,
    land_category text,
    area_m2 numeric,
    closure_seq text,
    source_snapshot_id text NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT parcel_transfer_event_pkey PRIMARY KEY (pnu, transfer_history_seq),
    CONSTRAINT parcel_transfer_event_pnu_check CHECK (pnu ~ '^[0-9]{10}[1289][0-9]{8}$'),
    CONSTRAINT parcel_transfer_event_area_check CHECK (
        area_m2 IS NULL OR (area_m2 > '-Infinity'::numeric AND area_m2 < 'Infinity'::numeric)
    ),
    CONSTRAINT parcel_transfer_event_snapshot_check CHECK (btrim(source_snapshot_id) <> '')
);

COMMENT ON TABLE catalog.parcel_transfer_event IS
    'Append-only AL_D157 cadastral events from silver.land_transfer_history (root ADR-0089).';
COMMENT ON COLUMN catalog.parcel_transfer_event.transfer_history_seq IS
    'Provider event sequence within a PNU; duplicate keys in one vintage must refuse the entire load.';
COMMENT ON COLUMN catalog.parcel_transfer_event.erased_at IS
    'Verbatim provider cancellation date; cancelled and closed events remain in the timeline.';
