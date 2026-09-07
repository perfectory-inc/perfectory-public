-- Root ADR-0090: the first national projection refused on a duplicate (pnu,
-- transfer_history_seq) — measured against 121M rows, the provider reuses that pair
-- across distinct rows that differ by 토지이력순번. The event identity is three
-- sequences. The table is empty (the refusal fired before any insert), so this is a
-- key change, not a data migration.
ALTER TABLE catalog.parcel_transfer_event
    ADD COLUMN parcel_history_seq text NOT NULL;

ALTER TABLE catalog.parcel_transfer_event
    DROP CONSTRAINT parcel_transfer_event_pkey;

ALTER TABLE catalog.parcel_transfer_event
    ADD CONSTRAINT parcel_transfer_event_pkey
    PRIMARY KEY (pnu, transfer_history_seq, parcel_history_seq);

COMMENT ON COLUMN catalog.parcel_transfer_event.transfer_history_seq IS
    'Provider event sequence within a PNU; unique only together with parcel_history_seq (root ADR-0090).';
COMMENT ON COLUMN catalog.parcel_transfer_event.parcel_history_seq IS
    '토지이력순번 — disambiguates rows sharing one 이동이력순번 (e.g. 등록사항 회복 pairs).';
