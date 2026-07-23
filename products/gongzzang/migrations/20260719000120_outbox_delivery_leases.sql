-- Prevent concurrent outbox publisher instances from claiming the same row.
-- The lease is deliberately separate from published_at: delivery remains
-- at-least-once and a crashed worker can be retried after lease expiry.

ALTER TABLE outbox_event
    ADD COLUMN IF NOT EXISTS lease_owner varchar(30),
    ADD COLUMN IF NOT EXISTS lease_until timestamptz;

CREATE INDEX IF NOT EXISTS outbox_claimable_idx
    ON outbox_event(created_at)
    WHERE published_at IS NULL;
