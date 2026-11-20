-- The next step of `silver.parcel_registry` for one region, as joins (root ADR-0113 §2·§3·§7(b),
-- ADR-0490 §4a). It was `parcel_registry_frames.py` over Spark frames: `parcel_identity.fold`,
-- `advance` and `upgrade`, and `lineage_review_queue.steward_resolved`'s two set rules.
--
-- Inputs (temp tables the stage makes):
--   registry_region(parcel_id, pnu, status, valid_from, valid_to, redirect_to, to_snapshot_id): the
--     registry as of the edition (its rows and the ones before it), of the region's 시도;
--   registry_after(pnu): the edition's parcels of the region, distinct;
--   lineage_pair(predecessor_pnu, successor_pnu, relation, grade, evidence_kind, evidence_ref,
--     derived_at_utc, from_snapshot_id, to_snapshot_id): the lineage rows of the edition pair.
-- Parts, split by `-- step:` lines; the stage runs the ones its mode needs.
-- Parameters: {to_date} (YYYY-MM-DD, the day the edition reflects).

-- step: macros
-- `parcel_identity.fold`'s order: the row's date, a current row after the others on one date.
CREATE OR REPLACE TEMP MACRO registry_key(valid_from, valid_to) AS
  coalesce(nullif(valid_from, ''), valid_to, '');

-- `parcel_identity.parcel_id_for(pnu, first_seen)`: uuid5 (RFC 4122 §4.3) under NAMESPACE_URL, the
-- SHA-1 of the namespace's bytes and the name, with the version and variant bits set.
CREATE OR REPLACE TEMP MACRO parcel_uuid5_hex(name) AS
  sha1(from_hex('6ba7b8119dad11d180b400c04fd430c8') || encode(name));
CREATE OR REPLACE TEMP MACRO parcel_uuid5_of(h) AS
  substr(h, 1, 8) || '-' || substr(h, 9, 4) || '-5' || substr(h, 14, 3) || '-'
  || lower(hex((CAST(('0x' || substr(h, 17, 1)) AS INTEGER) & 3) | 8)) || substr(h, 18, 3) || '-' || substr(h, 21, 12);
CREATE OR REPLACE TEMP MACRO parcel_id_for(pnu, first_seen) AS
  parcel_uuid5_of(parcel_uuid5_hex('parcel:' || pnu || ':' || first_seen));

-- `fold(...).current` of a registry relation: (pnu, parcel_id) of every current id. Per PNU the id
-- its last `current` row opened (by the fold's order, then the id), unless a later `historic` or
-- `redirected` row closes that same id.
CREATE OR REPLACE TEMP MACRO registry_current_ids(source) AS TABLE
  WITH rows AS (
    SELECT pnu, parcel_id, status, registry_key(valid_from, valid_to) AS k FROM query_table(source)
  ), last AS (
    SELECT pnu, arg_max(k, (k, parcel_id)) AS k, arg_max(parcel_id, (k, parcel_id)) AS parcel_id
    FROM rows WHERE status = 'current' GROUP BY pnu
  )
  SELECT l.pnu, l.parcel_id FROM last l
  WHERE NOT EXISTS (
    SELECT 1 FROM rows c
    WHERE c.status IN ('historic', 'redirected') AND c.pnu = l.pnu AND c.parcel_id = l.parcel_id AND c.k > l.k);

-- step: current
CREATE OR REPLACE TEMP TABLE registry_current AS SELECT * FROM registry_current_ids('registry_region');

-- step: closed
-- `fold(...).last_id_of_closed`: (pnu, parcel_id) of each PNU whose last opening or closing row, in
-- the fold's order, is `historic` (a `redirected` row leaves it as it is).
CREATE OR REPLACE TEMP TABLE registry_closed AS
WITH rows AS (
  SELECT pnu, parcel_id, status, registry_key(valid_from, valid_to) AS k,
         CASE WHEN status = 'current' THEN 1 ELSE 0 END AS o
  FROM registry_region WHERE status IN ('current', 'historic')
), last AS (
  SELECT pnu, arg_max(parcel_id, (k, o, parcel_id, status)) AS parcel_id,
         arg_max(status, (k, o, parcel_id, status)) AS status
  FROM rows GROUP BY pnu
)
SELECT pnu, parcel_id FROM last WHERE status = 'historic';

-- step: lineage
-- The lineage rows as every consumer reads them, the part that is a set rule: of the derived rows,
-- only the newest derivation run's for each (edition pair, successor); then, of one predecessor's
-- `same_lot_paired_dong` links, only those of the best-graded pair source (root ADR-0329 §3).
-- A successor a steward row names is resolved by the stage (`lineage_stewarded`), not here.
CREATE OR REPLACE TEMP TABLE lineage_newest AS
SELECT * EXCLUDE (derived, newest) FROM (
  SELECT *, coalesce(evidence_kind, '') <> 'steward' AS derived,
         max(CASE WHEN coalesce(evidence_kind, '') <> 'steward' THEN derived_at_utc END)
           OVER (PARTITION BY from_snapshot_id, to_snapshot_id, successor_pnu) AS newest
  FROM lineage_pair
) WHERE NOT derived OR derived_at_utc IS NULL OR derived_at_utc = newest;

CREATE OR REPLACE TEMP TABLE lineage_standing AS
WITH scored AS (
  SELECT *,
         coalesce(evidence_kind, '') = 'same_lot_paired_dong' AND coalesce(predecessor_pnu, '') <> '' AS ranked,
         CASE WHEN starts_with(coalesce(evidence_ref, ''), 'steward:') THEN 0
              WHEN starts_with(coalesce(evidence_ref, ''), 'official:') THEN 1
              WHEN starts_with(coalesce(evidence_ref, ''), 'derived:code-go-kr:date+name:') THEN 2
              WHEN starts_with(coalesce(evidence_ref, ''), 'derived:parcel-jibun:') THEN 3
              WHEN starts_with(coalesce(evidence_ref, ''), 'derived:children') THEN 4
              ELSE 5 END AS source_rank
  FROM lineage_newest
), best AS (
  SELECT predecessor_pnu, min(source_rank) AS best FROM scored WHERE ranked GROUP BY predecessor_pnu
)
SELECT s.* EXCLUDE (ranked, source_rank)
FROM scored s LEFT JOIN best b ON b.predecessor_pnu = s.predecessor_pnu
WHERE NOT s.ranked OR s.source_rank = b.best;

-- The successors a steward row names, every row of theirs: the stage resolves them.
CREATE OR REPLACE TEMP TABLE lineage_stewarded AS
SELECT * FROM lineage_standing
WHERE successor_pnu IN (SELECT successor_pnu FROM lineage_standing WHERE evidence_kind = 'steward');

-- Every other successor's rows; the stage appends the resolved ones of `lineage_stewarded`.
CREATE OR REPLACE TEMP TABLE lineage_resolved AS
SELECT * FROM lineage_standing
WHERE successor_pnu NOT IN (SELECT successor_pnu FROM lineage_stewarded WHERE successor_pnu IS NOT NULL)
   OR successor_pnu IS NULL;

-- step: carry
-- `parcel_identity.carrying_links` over `parcel_lineage.effective_links`: per successor its
-- best-graded link (ties by relation, then predecessor) carries when it is an identity relation
-- with a predecessor at evidence_strong or stronger; a predecessor two successors claim carries
-- neither. `lineage_conflicts`: the successors two identity predecessors claim.
CREATE OR REPLACE TEMP TABLE lineage_ranked AS
SELECT successor_pnu, relation, coalesce(predecessor_pnu, '') AS predecessor_pnu,
       CASE grade WHEN 'official' THEN 0 WHEN 'code_derived' THEN 1 WHEN 'evidence_strong' THEN 2
                  WHEN 'evidence_weak' THEN 3 WHEN 'needs_review' THEN 4 WHEN 'pending' THEN 5 ELSE 6 END AS grade_rank,
       relation IN ('code_change', 'jurisdiction_transfer', 'registration_conversion')
         AND coalesce(predecessor_pnu, '') <> '' AS identity
FROM lineage_resolved;

CREATE OR REPLACE TEMP TABLE lineage_conflicts AS
SELECT successor_pnu FROM lineage_ranked WHERE identity
GROUP BY successor_pnu HAVING count(DISTINCT predecessor_pnu) > 1;

CREATE OR REPLACE TEMP TABLE registry_carry AS
WITH best AS (
  SELECT successor_pnu,
         arg_min(grade_rank, (grade_rank, relation, predecessor_pnu)) AS grade_rank,
         arg_min(relation, (grade_rank, relation, predecessor_pnu)) AS relation,
         arg_min(predecessor_pnu, (grade_rank, relation, predecessor_pnu)) AS predecessor_pnu
  FROM lineage_ranked GROUP BY successor_pnu
), candidates AS (
  SELECT successor_pnu, predecessor_pnu FROM best
  WHERE relation IN ('code_change', 'jurisdiction_transfer', 'registration_conversion')
    AND predecessor_pnu <> '' AND grade_rank <= 2
)
SELECT successor_pnu, predecessor_pnu FROM candidates
WHERE predecessor_pnu IN (SELECT predecessor_pnu FROM candidates GROUP BY predecessor_pnu HAVING count(*) = 1);

-- step: bootstrap
-- The first snapshot: every parcel gets its id, current from that date.
CREATE OR REPLACE TEMP TABLE registry_rows AS
SELECT parcel_id_for(pnu, '{to_date}') AS parcel_id, pnu, 'current' AS status, '{to_date}' AS valid_from,
       NULL::VARCHAR AS valid_to, NULL::VARCHAR AS redirect_to
FROM registry_after;

-- step: advance
-- `parcel_identity.advance`: the PNUs both editions hold write nothing; a new PNU a carrying link
-- joins to a vanished one takes its id (the old row closes, the new opens), every other new PNU
-- gets a fresh id, every other vanished PNU closes.
CREATE OR REPLACE TEMP TABLE registry_new AS
SELECT pnu FROM registry_after WHERE pnu NOT IN (SELECT pnu FROM registry_current);
CREATE OR REPLACE TEMP TABLE registry_gone AS
SELECT pnu, parcel_id FROM registry_current WHERE pnu NOT IN (SELECT pnu FROM registry_after);
CREATE OR REPLACE TEMP TABLE registry_carried AS
SELECT c.successor_pnu, c.predecessor_pnu, g.parcel_id
FROM registry_carry c JOIN registry_gone g ON g.pnu = c.predecessor_pnu
WHERE c.successor_pnu IN (SELECT pnu FROM registry_new);
CREATE OR REPLACE TEMP TABLE registry_issued AS
SELECT pnu, parcel_id_for(pnu, '{to_date}') AS parcel_id FROM registry_new
WHERE pnu NOT IN (SELECT successor_pnu FROM registry_carried);
CREATE OR REPLACE TEMP TABLE registry_retired AS
SELECT pnu, parcel_id FROM registry_gone WHERE pnu NOT IN (SELECT predecessor_pnu FROM registry_carried);
CREATE OR REPLACE TEMP TABLE registry_rows AS
SELECT parcel_id, predecessor_pnu AS pnu, 'historic' AS status, NULL::VARCHAR AS valid_from,
       '{to_date}' AS valid_to, NULL::VARCHAR AS redirect_to FROM registry_carried
UNION ALL SELECT parcel_id, successor_pnu, 'current', '{to_date}', NULL, NULL FROM registry_carried
UNION ALL SELECT parcel_id, pnu, 'current', '{to_date}', NULL, NULL FROM registry_issued
UNION ALL SELECT parcel_id, pnu, 'historic', NULL, '{to_date}', NULL FROM registry_retired;
CREATE OR REPLACE TEMP TABLE registry_counts AS
SELECT (SELECT count(*) FROM registry_after WHERE pnu IN (SELECT pnu FROM registry_current)) AS kept,
       (SELECT count(*) FROM registry_carried) AS carried,
       (SELECT count(*) FROM registry_issued) AS issued,
       (SELECT count(*) FROM registry_retired) AS retired,
       0::BIGINT AS redirected;

-- step: upgrade
-- `parcel_identity.upgrade`: a current parcel whose carrying link now points at a closed PNU takes
-- that PNU's id back, its interim id redirected (never deleted).
CREATE OR REPLACE TEMP TABLE registry_pairs AS
SELECT c.successor_pnu AS pnu, cur.parcel_id AS interim, closed.parcel_id AS original
FROM registry_carry c
JOIN registry_current cur ON cur.pnu = c.successor_pnu
JOIN registry_closed closed ON closed.pnu = c.predecessor_pnu
WHERE cur.parcel_id <> closed.parcel_id;
CREATE OR REPLACE TEMP TABLE registry_rows AS
SELECT interim AS parcel_id, pnu, 'redirected' AS status, '{to_date}' AS valid_from, NULL::VARCHAR AS valid_to,
       original AS redirect_to FROM registry_pairs
UNION ALL SELECT original, pnu, 'current', '{to_date}', NULL, NULL FROM registry_pairs;
CREATE OR REPLACE TEMP TABLE registry_counts AS
SELECT (SELECT count(*) FROM registry_pairs) AS redirected;

-- step: check
-- `check_every_parcel_has_one_current_id` over the registry with the rows appended: every parcel
-- of the edition must carry exactly one current id. At most twenty of each problem.
CREATE OR REPLACE TEMP TABLE registry_folded AS
SELECT parcel_id, pnu, status, valid_from, valid_to, redirect_to FROM registry_region
UNION ALL SELECT parcel_id, pnu, status, valid_from, valid_to, redirect_to FROM registry_rows;
CREATE OR REPLACE TEMP TABLE registry_final AS SELECT * FROM registry_current_ids('registry_folded');
CREATE OR REPLACE TEMP TABLE registry_problems AS
(SELECT 'no current id: ' || pnu AS problem, 0 AS kind, pnu AS at FROM registry_after
 WHERE pnu NOT IN (SELECT pnu FROM registry_final) ORDER BY pnu LIMIT 20)
UNION ALL
(SELECT 'id on two current PNUs: ' || parcel_id, 1, parcel_id FROM registry_final
 WHERE pnu IN (SELECT pnu FROM registry_after) GROUP BY parcel_id HAVING count(*) > 1 ORDER BY parcel_id LIMIT 20);
