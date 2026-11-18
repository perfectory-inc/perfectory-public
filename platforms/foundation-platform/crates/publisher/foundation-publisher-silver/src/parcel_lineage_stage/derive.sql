-- Parcel lineage for one pair of editions and one group of sido prefixes (root ADR-0113 §1·§4·§5).
-- Inputs: before_raw(pnu), after_raw(pnu), code_changes(kind, old_code, new_code, level,
-- effective_date, source), history(pnu, reason_code, reason, moved_at, erased_at,
-- land_category_code, area_m2, transfer_history_seq): the current rows of the pinned land movement history.
-- Parameters: {from_date}, {to_date} (YYYY-MM-DD), {from_day}, {to_day} (YYYYMMDD), {ws}: the
-- characters Python's str.strip() and \s take for white space, as a regular expression class body.

CREATE OR REPLACE MACRO lineage_ws() AS
  chr(9) || chr(10) || chr(11) || chr(12) || chr(13) || chr(28) || chr(29) || chr(30) || chr(31) || chr(32) || chr(133) || chr(160)
  || chr(5760) || chr(8192) || chr(8193) || chr(8194) || chr(8195) || chr(8196) || chr(8197) || chr(8198) || chr(8199)
  || chr(8200) || chr(8201) || chr(8202) || chr(8232) || chr(8233) || chr(8239) || chr(8287) || chr(12288);

CREATE OR REPLACE TEMP TABLE before_pnus AS SELECT pnu FROM before_raw WHERE regexp_full_match(pnu, '[0-9]{19}');
CREATE OR REPLACE TEMP TABLE after_pnus AS SELECT pnu FROM after_raw WHERE regexp_full_match(pnu, '[0-9]{19}');

-- The pairs in force (legal_dong_code_change_views.pair_rows): a pair a supersession withdraws is
-- left out unless a steward recorded it; one row per (old, new, source), a dated row before an undated.
CREATE OR REPLACE TEMP TABLE pair_rows AS
WITH superseded AS (
  SELECT DISTINCT old_code, new_code FROM code_changes
  WHERE kind = 'supersession' AND coalesce(old_code, '') <> '' AND coalesce(new_code, '') <> ''
)
SELECT c.old_code, c.new_code, coalesce(c.source, '') AS source,
       coalesce(min(nullif(c.effective_date, '')), '') AS effective_date,
       count(DISTINCT nullif(c.effective_date, '')) AS dated_variants,
       arg_min(coalesce(c.level, ''), coalesce(nullif(c.effective_date, ''), '~')) AS level
FROM code_changes c
WHERE c.kind = 'pair' AND coalesce(c.old_code, '') <> '' AND coalesce(c.new_code, '') <> ''
  AND NOT (EXISTS (SELECT 1 FROM superseded s WHERE s.old_code = c.old_code AND s.new_code = c.new_code)
           AND NOT starts_with(coalesce(c.source, ''), 'steward:'))
GROUP BY c.old_code, c.new_code, coalesce(c.source, '');

-- leaf_pairs within the window (both days included), then dong_pairing_from_changes.
CREATE OR REPLACE TEMP TABLE leaf_pairs AS
SELECT old_code, new_code, min(source) AS source
FROM pair_rows
WHERE level IN ('eupmyeondong', 'ri') AND old_code <> new_code
  AND replace(effective_date, '-', '') <> ''
  AND replace(effective_date, '-', '') >= '{from_day}' AND replace(effective_date, '-', '') <= '{to_day}'
GROUP BY old_code, new_code;

CREATE OR REPLACE TEMP TABLE dong_pairs AS
SELECT old_code, any_value(new_code) AS new_code, any_value(source) AS how
FROM leaf_pairs GROUP BY old_code HAVING count(*) = 1;

CREATE OR REPLACE TEMP TABLE dong_split AS
SELECT old_code FROM leaf_pairs GROUP BY old_code HAVING count(*) <> 1;

-- The number the same lot had at the earlier snapshot: the last old code, in code order, a new code came from.
CREATE OR REPLACE TEMP TABLE dong_reverse AS
SELECT new_code, max(old_code) AS old_code FROM dong_pairs GROUP BY new_code;

CREATE OR REPLACE TEMP TABLE dong_unrecorded AS
SELECT DISTINCT substr(b.pnu, 1, 10) AS code FROM before_pnus b
WHERE substr(b.pnu, 1, 10) NOT IN (SELECT substr(pnu, 1, 10) FROM after_pnus)
  AND substr(b.pnu, 1, 10) NOT IN (SELECT old_code FROM dong_pairs)
  AND substr(b.pnu, 1, 10) NOT IN (SELECT old_code FROM dong_split);

-- carry_over: a lot that survives under the paired dong.
CREATE OR REPLACE TEMP TABLE carried AS
SELECT b.pnu AS before_pnu, coalesce(p.new_code, substr(b.pnu, 1, 10)) || substr(b.pnu, 11) AS after_pnu,
       coalesce(p.how, '') AS how
FROM before_pnus b LEFT JOIN dong_pairs p ON p.old_code = substr(b.pnu, 1, 10)
WHERE coalesce(p.new_code, substr(b.pnu, 1, 10)) || substr(b.pnu, 11) IN (SELECT pnu FROM after_pnus);

CREATE OR REPLACE TEMP TABLE vanished AS SELECT pnu FROM before_pnus WHERE pnu NOT IN (SELECT before_pnu FROM carried);
CREATE OR REPLACE TEMP TABLE appeared AS SELECT pnu FROM after_pnus WHERE pnu NOT IN (SELECT after_pnu FROM carried);

-- The window's events the rules read, as the Spark job's MovementEvent set: nulls read as ''.
CREATE OR REPLACE TEMP TABLE window_events AS
SELECT pnu, coalesce(reason_code, '') AS reason_code, coalesce(reason, '') AS reason, coalesce(moved_at, '') AS moved_at,
       coalesce(erased_at, '') AS erased_at, coalesce(land_category_code, '') AS land_category_code, area_m2,
       max(transfer_history_seq) AS seq
FROM history
WHERE moved_at >= '{from_date}' AND moved_at < '{to_date}'
  AND (reason_code = '52' OR reason LIKE '%합병되어%말소%' OR reason LIKE '%번에서%분할%' OR reason LIKE '%번에서%등록전환%')
GROUP BY ALL;

-- history_links: what the history text states. One spelling per end: a predecessor as the earlier
-- snapshot numbers it, a successor as the later one does.
CREATE OR REPLACE TEMP TABLE history_links AS
WITH texts AS (
  SELECT DISTINCT pnu, moved_at, trim(reason, lineage_ws()) AS text FROM window_events
), parsed AS (
  SELECT pnu, moved_at, text,
    regexp_extract(text, '^(산[{ws}]*[0-9]+(?:-[0-9]+)?|[0-9]+(?:-[0-9]+)?)[{ws}]*번과[{ws}]*합병되어[{ws}]*말소', 1) AS merged,
    regexp_extract(text, '^(산[{ws}]*[0-9]+(?:-[0-9]+)?|[0-9]+(?:-[0-9]+)?)[{ws}]*번에서[{ws}]*분할', 1) AS split,
    regexp_extract(text, '^(산[{ws}]*[0-9]+(?:-[0-9]+)?)[{ws}]*번에서[{ws}]*등록전환', 1) AS converted
  FROM texts
), chosen AS (
  -- The first text that matches decides, as the rule's if/elif does, even when its lot is unusable.
  SELECT pnu, moved_at, text,
    CASE WHEN merged <> '' THEN 'merge' WHEN split <> '' THEN 'split' ELSE 'registration_conversion' END AS relation,
    trim(CASE WHEN merged <> '' THEN merged WHEN split <> '' THEN split ELSE converted END, lineage_ws()) AS ref
  FROM parsed WHERE merged <> '' OR split <> '' OR converted <> ''
), referenced AS (
  SELECT pnu, moved_at, text, relation,
    regexp_extract(ref, '^(산[{ws}]*)?([0-9]{1,4})(?:-([0-9]{1,4}))?$', ['mountain', 'main', 'sub']) AS lot,
    regexp_full_match(ref, '(산[{ws}]*)?([0-9]{1,4})(?:-([0-9]{1,4}))?') AS usable
  FROM chosen
), ends AS (
  SELECT moved_at, text, relation, pnu AS event_pnu,
    substr(pnu, 1, 10) || CASE WHEN lot.mountain <> '' THEN '2' ELSE '1' END
      || lpad(CAST(CAST(lot.main AS INTEGER) AS VARCHAR), 4, '0')
      || lpad(CAST(CAST(CASE WHEN lot.sub = '' THEN '0' ELSE lot.sub END AS INTEGER) AS VARCHAR), 4, '0') AS other_pnu
  FROM referenced WHERE usable
), spelled AS (
  SELECT moved_at, text, relation,
    CASE WHEN relation = 'merge' THEN event_pnu ELSE other_pnu END AS predecessor,
    CASE WHEN relation = 'merge' THEN other_pnu ELSE event_pnu END AS successor
  FROM ends WHERE other_pnu <> event_pnu
)
SELECT DISTINCT
  coalesce(r.old_code, substr(s.predecessor, 1, 10)) || substr(s.predecessor, 11) AS predecessor_pnu,
  coalesce(p.new_code, substr(s.successor, 1, 10)) || substr(s.successor, 11) AS successor_pnu,
  s.relation, s.text AS evidence_ref, s.moved_at AS effective_date
FROM spelled s
LEFT JOIN dong_reverse r ON r.new_code = substr(s.predecessor, 1, 10)
LEFT JOIN dong_pairs p ON p.old_code = substr(s.successor, 1, 10);

-- New parcels the history marks as moved in by a jurisdiction change, among those that appeared.
CREATE OR REPLACE TEMP TABLE moved_in AS
SELECT DISTINCT e.pnu FROM window_events e WHERE e.reason_code = '52' AND e.pnu IN (SELECT pnu FROM appeared);

-- The attribute-evidence pool: vanished parcels no official link accounts for.
CREATE OR REPLACE TEMP TABLE pool AS
SELECT pnu FROM vanished WHERE pnu NOT IN (SELECT predecessor_pnu FROM history_links);

-- The pre-window events of the pool, spelled as the earlier snapshot numbers them.
CREATE OR REPLACE TEMP TABLE old_events AS
SELECT coalesce(r.old_code, substr(h.pnu, 1, 10)) || substr(h.pnu, 11) AS pnu, coalesce(h.moved_at, '') AS moved_at,
       coalesce(h.land_category_code, '') AS land_category_code, h.area_m2, max(h.transfer_history_seq) AS seq
FROM history h JOIN pool q ON q.pnu = h.pnu
LEFT JOIN dong_reverse r ON r.new_code = substr(h.pnu, 1, 10)
WHERE h.moved_at < '{from_date}'
GROUP BY ALL;

-- The latest facts of each. Several events of one day are ordered by the history's own sequence
-- (the Spark job took whichever its unordered read gave last); `ties` counts the different facts
-- that last day holds.
CREATE OR REPLACE TEMP TABLE old_facts AS
SELECT pnu, land_category_code, area_m2, printf('%.1f', area_m2) AS area, ties FROM (
  SELECT e.*, row_number() OVER (PARTITION BY pnu ORDER BY moved_at DESC, seq DESC NULLS LAST, area_m2 DESC NULLS LAST, land_category_code DESC) AS n,
         count(*) OVER (PARTITION BY pnu, moved_at) AS ties
  FROM old_events e WHERE e.pnu IN (SELECT pnu FROM pool)
) WHERE n = 1;

CREATE OR REPLACE TEMP TABLE new_facts AS
SELECT pnu, land_category_code, area_m2, printf('%.1f', area_m2) AS area, ties FROM (
  SELECT f.*, row_number() OVER (PARTITION BY pnu ORDER BY moved_at DESC, seq DESC NULLS LAST, area_m2 DESC NULLS LAST, land_category_code DESC) AS n,
         count(*) OVER (PARTITION BY pnu, moved_at) AS ties
  FROM (SELECT pnu, moved_at, land_category_code, area_m2, max(seq) AS seq FROM window_events
        WHERE pnu IN (SELECT pnu FROM moved_in) GROUP BY ALL) f
) WHERE n = 1;

-- attribute_links without ownership facts: area and category unique on both sides is evidence_weak;
-- every other moved-in parcel is listed as pending.
CREATE OR REPLACE TEMP TABLE attribute_links AS
WITH old_keys AS (
  SELECT area, land_category_code, any_value(pnu) AS pnu, count(*) AS n FROM old_facts WHERE area IS NOT NULL GROUP BY 1, 2
), new_keys AS (
  SELECT area, land_category_code, any_value(pnu) AS pnu, count(*) AS n FROM new_facts WHERE area IS NOT NULL GROUP BY 1, 2
), paired AS (
  SELECT o.pnu AS predecessor_pnu, n.pnu AS successor_pnu
  FROM new_keys n JOIN old_keys o ON o.area = n.area AND o.land_category_code = n.land_category_code
  WHERE n.n = 1 AND o.n = 1
)
SELECT predecessor_pnu, successor_pnu, 'evidence_weak' AS grade, 'area+category' AS evidence_kind FROM paired
UNION ALL
SELECT '', pnu, 'pending', 'no unique attribute match' FROM moved_in WHERE pnu NOT IN (SELECT successor_pnu FROM paired);

CREATE OR REPLACE TEMP TABLE links AS
SELECT before_pnu AS predecessor_pnu, after_pnu AS successor_pnu, 'code_change' AS relation, 'code_derived' AS grade,
       'same_lot_paired_dong' AS evidence_kind, how AS evidence_ref, '' AS effective_date
FROM carried WHERE after_pnu <> before_pnu
UNION ALL
SELECT predecessor_pnu, successor_pnu, relation, 'official', 'history_text', evidence_ref, effective_date FROM history_links
UNION ALL
SELECT predecessor_pnu, successor_pnu, 'jurisdiction_transfer', grade, evidence_kind, '', '' FROM attribute_links;

-- The rows, in the table's column order; cardinality counts links, not distinct pairs.
CREATE OR REPLACE TEMP TABLE lineage_rows AS
WITH real AS (SELECT * FROM links WHERE grade <> 'pending' AND predecessor_pnu <> ''),
outs AS (SELECT predecessor_pnu, count(*) AS n FROM real GROUP BY 1),
ins AS (SELECT successor_pnu, count(*) AS n FROM real GROUP BY 1)
SELECT nullif(l.predecessor_pnu, '') AS predecessor_pnu, l.successor_pnu, l.relation,
  CASE WHEN l.grade = 'pending' OR l.predecessor_pnu = '' THEN NULL
       WHEN o.n > 1 AND i.n > 1 THEN 'N:M' WHEN o.n > 1 THEN '1:N' WHEN i.n > 1 THEN 'N:1' ELSE '1:1' END AS cardinality,
  nullif(l.effective_date, '') AS effective_date, l.grade, l.evidence_kind, nullif(l.evidence_ref, '') AS evidence_ref
FROM links l LEFT JOIN outs o ON o.predecessor_pnu = l.predecessor_pnu LEFT JOIN ins i ON i.successor_pnu = l.successor_pnu;
