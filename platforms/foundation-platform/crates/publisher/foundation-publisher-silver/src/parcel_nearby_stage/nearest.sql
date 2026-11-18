-- The nearest facility of kind {kind} to every parcel of `sp` (one 시도), exactly, on the WGS84 spheroid.
-- Inputs: sp(pnu, lon, lat, cx, cy, fx, fy) and f_{kind}(row_key, lat, lon, {fields}).
-- Parameters: {coarse}, {fine} (cell sizes in degrees), {fields_of_f}, {struct_fields}, {fields_order}.
--
-- Candidates by the triangle inequality: every parcel p of a cell with centre o has
-- d(o, f*) <= d(p, f*) + d(o, p) <= d(p, f0) + d(o, p) <= d(o, f0) + 2 d(o, p) <= d(o, f0) + diagonal
-- for its nearest facility f* and any facility f0, so a cell's candidates are the facilities within
-- (the one nearest its centre) + (its diagonal) of its centre, plus 2 m for the spheroid's rounding.
-- Coarse cells first; a fine cell takes its candidates from its coarse cell's. Every facility at the
-- nearest distance is a candidate, so the tie rule below sees all of them.
--
-- The tie rule (root ADR-0559 exception ⓑ, owner 2026-11-12): the nearest by distance, then the
-- facility's stable id (`row_key`). Sedona's ST_KNN (k = 1) broke an equal-distance tie arbitrarily.
-- The words of the facility close the order only for the rows of one key that tie as current rows.
CREATE OR REPLACE TABLE n_{kind} AS
WITH cc AS (
  SELECT DISTINCT cx, cy, (cx + 0.5) * {coarse} AS lon, (cy + 0.5) * {coarse} AS lat FROM sp
),
cd AS (
  SELECT cc.cx, cc.cy, f.row_key, nearby_spheroid_m(cc.lon, cc.lat, f.lon, f.lat) AS d,
         nearby_spheroid_m(cc.cx * {coarse}, cc.cy * {coarse}, (cc.cx + 1) * {coarse}, (cc.cy + 1) * {coarse}) AS diagonal
  FROM cc CROSS JOIN f_{kind} f
),
ccand AS (
  SELECT cx, cy, row_key FROM (SELECT *, min(d) OVER (PARTITION BY cx, cy) AS d0 FROM cd)
  WHERE d <= d0 + diagonal + 2
),
fc AS (
  SELECT DISTINCT cx, cy, fx, fy, (fx + 0.5) * {fine} AS lon, (fy + 0.5) * {fine} AS lat FROM sp
),
fd AS (
  SELECT fc.fx, fc.fy, f.row_key, nearby_spheroid_m(fc.lon, fc.lat, f.lon, f.lat) AS d,
         nearby_spheroid_m(fc.fx * {fine}, fc.fy * {fine}, (fc.fx + 1) * {fine}, (fc.fy + 1) * {fine}) AS diagonal
  FROM fc JOIN ccand USING (cx, cy) JOIN f_{kind} f USING (row_key)
),
fcand AS (
  SELECT DISTINCT fx, fy, row_key FROM (SELECT *, min(d) OVER (PARTITION BY fx, fy) AS d0 FROM fd)
  WHERE d <= d0 + diagonal + 2
),
pd AS (
  SELECT p.pnu, f.row_key, {fields_of_f}, nearby_spheroid_m(p.lon, p.lat, f.lon, f.lat) AS d
  FROM sp p JOIN fcand USING (fx, fy) JOIN f_{kind} f USING (row_key)
)
SELECT pnu, {{struct_fields}, 'distance_m': CAST(round(d) AS BIGINT)} AS {kind}
FROM (SELECT *, row_number() OVER (PARTITION BY pnu ORDER BY d, row_key, {fields_order}) AS nearest FROM pd)
WHERE nearest = 1;
