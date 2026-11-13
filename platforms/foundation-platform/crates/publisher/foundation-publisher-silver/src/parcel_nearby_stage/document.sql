-- One 시도's rows of the nearby Gold (parcel_nearby_silver_to_gold.gold_rows and place_id_join.with_place_ids,
-- root ADR-0465, ADR-0483), appended to `built`.
-- Inputs: sp (the 시도's parcels), n_<kind> per nearest kind (nearest.sql), stores(cell, n), highway(sigungu,
-- highway_access_points), place_ids(pnu, place_id).
-- Parameters: {ring} (H3 rings around a parcel's cell), {joins} (LEFT JOIN n_<kind> USING (pnu), each kind),
-- {document} (the section's fields in the Gold's order), {source_snapshot_id}, {published_at_utc}.
--
-- `nearby_json` is Spark's `to_json(struct, ignoreNullFields=false)`: every key, a missing value written
-- null. `row_digest` is `sha2(to_json(struct(pnu, nearby_json, attached_via, place_id)), 256)` with
-- Spark's default that leaves a null field out (attached_via is always null here).
CREATE OR REPLACE TABLE ring AS
SELECT r.pnu, sum(s.n)::BIGINT AS store_count
FROM (SELECT pnu, unnest(h3_grid_disk(cell, {ring})) AS cell FROM sp) r JOIN stores s USING (cell)
GROUP BY r.pnu;

INSERT INTO built
WITH d AS (
  SELECT sp.pnu, to_json({document})::VARCHAR AS nearby_json
  FROM sp {joins}
  LEFT JOIN ring r USING (pnu)
  LEFT JOIN highway h ON h.sigungu = substr(sp.pnu, 1, 5)
)
SELECT d.pnu, d.nearby_json, CAST(NULL AS VARCHAR) AS attached_via, i.place_id,
       sha256('{"pnu":' || to_json(d.pnu) || ',"nearby_json":' || to_json(d.nearby_json)
              || coalesce(',"place_id":' || to_json(i.place_id), '') || '}') AS row_digest,
       '{source_snapshot_id}' AS source_snapshot_id, '{published_at_utc}' AS published_at_utc
FROM d LEFT JOIN place_ids i USING (pnu);
