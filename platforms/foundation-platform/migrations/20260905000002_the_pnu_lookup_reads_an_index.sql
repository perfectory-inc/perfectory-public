-- The by-PNU lookup reads an index instead of the country.
--
-- Measured 2026-09-05: catalog.parcel grew to 39.8M rows and the identifier lookup view
-- fallback arm compares btrim((pnu)::text), an expression no plain index serves — every
-- protected parcel lookup read ~451k pages (5.7s) and died on the 2.5s statement timeout,
-- which the serving gateway then surfaced as 502 on every parcel panel. The expression
-- index matches the view expression exactly: 5.7s to 86ms measured.
CREATE INDEX IF NOT EXISTS parcel_pnu_btrim_idx ON catalog.parcel (btrim((pnu)::text));
