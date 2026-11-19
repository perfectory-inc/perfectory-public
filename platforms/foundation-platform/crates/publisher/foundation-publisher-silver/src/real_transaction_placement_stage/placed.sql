-- Every deal with the PNU it is placed on (NULL for none), the rules of `placed_deals` in
-- `real_transaction_silver_to_gold.py` and `placed` / `servable` in `onbid_public_auction_silver_to_gold.py`:
--
-- - a masked lot takes the inference's link (`inferred`: row_id, pnu, pnu_group, place_identity_grade;
--   only the deals it linked);
-- - a deal the 본번 group level links is a row on each current parcel of its group, with the group's
--   key and size;
-- - a deal on a current parcel stays there (`attached_via` NULL); one on a retired parcel follows the
--   lineage (`carry`: successor_pnu, source_pnu, hop, path, walked by `lineage_carry`) to every current
--   successor, else the merge it went into (`merges`, `merged:<grade>`); one no path leaves
--   (`lineage_missing`) is not servable and stays unplaced, as does a deal with no PNU at all.
CREATE OR REPLACE TABLE placed AS
WITH linked AS (
  SELECT r.* EXCLUDE (pnu, place_identity_grade),
         coalesce(r.pnu, i.pnu) AS pnu,
         coalesce(i.place_identity_grade, r.place_identity_grade) AS place_identity_grade,
         i.pnu_group AS _group
  FROM resolved r LEFT JOIN inferred i ON i.row_id = r.row_id
),
keyed AS (
  SELECT * EXCLUDE (_group, pnu),
         CASE WHEN _group IS NOT NULL THEN substr(_group[1], 1, 15) END AS main_lot_group_key,
         CAST(CASE WHEN _group IS NOT NULL THEN len(_group) END AS INTEGER) AS main_lot_group_size,
         unnest(coalesce(_group, [pnu])) AS pnu
  FROM linked
),
sales AS (
  SELECT * EXCLUDE (pnu, pnu_candidate), coalesce(pnu, pnu_candidate) AS source_pnu FROM keyed
),
current AS (SELECT DISTINCT pnu FROM parcels),
known AS (
  SELECT s.*, c.pnu IS NOT NULL AS _current
  FROM sales s LEFT JOIN current c ON c.pnu = s.source_pnu
  WHERE s.source_pnu IS NOT NULL
),
own AS (
  SELECT * EXCLUDE (_current), source_pnu AS pnu, NULL::VARCHAR AS attached_via FROM known WHERE _current
),
retired AS (SELECT * EXCLUDE (_current) FROM known WHERE NOT _current),
followed AS (
  SELECT r.*, c.successor_pnu AS pnu, c.path AS attached_via
  FROM retired r
  JOIN carry c ON c.source_pnu = r.source_pnu
  SEMI JOIN current p ON p.pnu = c.successor_pnu
),
merged_into AS (
  SELECT DISTINCT m.predecessor_pnu AS source_pnu, m.successor_pnu AS pnu, concat('merged:', m.grade) AS attached_via
  FROM merges m SEMI JOIN current p ON p.pnu = m.successor_pnu
  WHERE m.grade NOT IN ('needs_review', 'pending')
),
merge_followed AS (
  SELECT r.*, i.pnu, i.attached_via
  FROM (SELECT * FROM retired ANTI JOIN (SELECT DISTINCT source_pnu FROM followed) f USING (source_pnu)) r
  JOIN merged_into i ON i.source_pnu = r.source_pnu
),
every_followed AS (SELECT * FROM followed UNION ALL BY NAME SELECT * FROM merge_followed),
servable AS (
  SELECT * FROM own
  UNION ALL BY NAME SELECT * FROM every_followed
)
SELECT * EXCLUDE (source_pnu) FROM servable
UNION ALL BY NAME
SELECT * EXCLUDE (source_pnu), NULL::VARCHAR AS pnu, NULL::VARCHAR AS attached_via
FROM sales ANTI JOIN (SELECT DISTINCT row_id FROM servable) p USING (row_id);
