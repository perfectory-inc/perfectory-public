-- What the one place-identity resolver reads once a run (root ADR-0288 §1), as DuckDB tables: the code
-- crosswalk of the legal-dong code pairs in force, the earlier forms of each code a pair touches, and
-- the address readings of the newest 법정동 snapshot. It was `place_identity_resolver.crosswalk_of`,
-- `served_forms` and `address_pnu.sido_prefixes` / `legal_dong_index` (Python over collected rows).
--
-- Input: place_code_changes(kind, old_code, new_code, level, effective_date, source): every row of
-- reference.legal_dong_code_change at the run's pin; place_dong_names(region_cd, full_name, status):
-- every row of the newest snapshot of reference.legal_dong_code_snapshot.
-- Output: place_remap(old_code, current_code), place_ambiguous(code), place_forms(code10, form10, rank),
-- place_sido_prefixes(prefix, sido), place_dongs(sido, place, code).

-- legal_dong_code_change_views.pair_rows: the pairs in force. A supersession withdraws a derived pair;
-- a steward's pair is never withdrawn.
CREATE OR REPLACE TEMP TABLE place_pairs AS
SELECT DISTINCT p.old_code, p.new_code
FROM place_code_changes p
WHERE p.kind = 'pair' AND coalesce(p.old_code, '') <> '' AND coalesce(p.new_code, '') <> ''
  AND (starts_with(coalesce(p.source, ''), 'steward:') OR NOT EXISTS (
        SELECT 1 FROM place_code_changes s
        WHERE s.kind = 'supersession' AND s.old_code = p.old_code AND s.new_code = p.new_code));

-- code_crosswalk: follow old -> new to the current code; a split (one old, many new) or a cycle is
-- ambiguous. A code recorded as changing to itself is no change.
CREATE OR REPLACE TEMP TABLE place_next AS
SELECT old_code AS node, count(DISTINCT new_code) AS successors, min(new_code) AS successor
FROM place_pairs WHERE old_code <> new_code GROUP BY old_code;

CREATE OR REPLACE TEMP TABLE place_walk AS
WITH RECURSIVE walk(start, node, path, outcome) AS (
  SELECT node, node, [node], NULL::VARCHAR FROM place_next
  UNION ALL
  SELECT w.start,
         CASE WHEN n.successors = 1 THEN n.successor ELSE w.node END,
         list_append(w.path, coalesce(n.successor, w.node)),
         CASE WHEN n.node IS NULL THEN 'current'
              WHEN n.successors > 1 THEN 'ambiguous'
              WHEN list_contains(w.path, n.successor) THEN 'ambiguous' END
  FROM walk w LEFT JOIN place_next n ON n.node = w.node
  WHERE w.outcome IS NULL
)
SELECT start, node, outcome FROM walk WHERE outcome IS NOT NULL;

CREATE OR REPLACE TEMP TABLE place_remap AS
SELECT start AS old_code, node AS current_code FROM place_walk WHERE outcome = 'current' AND node <> start;
CREATE OR REPLACE TEMP TABLE place_ambiguous AS
SELECT start AS code FROM place_walk WHERE outcome = 'ambiguous';

-- served_forms: (code, an earlier form of it, rank) for every code a recorded pair touches: rank 1
-- the code itself when it is superseded, rank 2 every other code the pairs carry to the same current.
CREATE OR REPLACE TEMP TABLE place_forms AS
WITH codes AS (
  SELECT old_code AS code FROM place_remap UNION SELECT current_code FROM place_remap
), currents AS (
  SELECT c.code, coalesce(r.current_code, c.code) AS current_code
  FROM codes c LEFT JOIN place_remap r ON r.old_code = c.code
)
SELECT code AS code10, code AS form10, 1 AS rank FROM currents WHERE current_code <> code
UNION ALL
SELECT c.code, r.old_code, 2 FROM currents c JOIN place_remap r ON r.current_code = c.current_code
WHERE r.old_code <> c.code;

-- Python's str.split() of a name: its words, split at runs of whitespace, none blank.
CREATE OR REPLACE MACRO place_words(t) AS
  list_filter(regexp_split_to_array(coalesce(t, ''), '[' || py_ws() || ']+'), lambda w: w <> '');

-- address_pnu.without_gu: the place names without a 구 word first or after a 시 word.
CREATE OR REPLACE MACRO place_without_gu(place) AS
  array_to_string(list_filter(place_words(place), lambda w, i:
    NOT (suffix(w, '구') AND (i = 1 OR suffix(place_words(place)[i - 1], '시')))), ' ');

-- region_pairs(rows, digits): the (old, new) regions a pair moves a code between.
CREATE OR REPLACE TEMP TABLE place_sido_moves AS
SELECT DISTINCT left(old_code, 2) AS old_code, left(new_code, 2) AS new_code FROM place_pairs
WHERE left(old_code, 2) <> left(new_code, 2);
CREATE OR REPLACE TEMP TABLE place_dong_moves AS
SELECT DISTINCT left(old_code, 10) AS old_code, left(new_code, 10) AS new_code FROM place_pairs
WHERE left(old_code, 10) <> left(new_code, 10);

CREATE OR REPLACE TEMP TABLE place_names AS
SELECT region_cd AS code, place_words(full_name) AS words, status FROM place_dong_names
WHERE length(coalesce(region_cd, '')) = 10;

-- sido_prefixes: {address word prefix: current 시도}, from every row of the newest snapshot and the
-- 시도 pairs in force.
CREATE OR REPLACE MACRO place_stem(name) AS
  CASE WHEN suffix(name, '특별자치시') THEN left(name, length(name) - 5)
       WHEN suffix(name, '특별자치도') THEN left(name, length(name) - 5)
       WHEN suffix(name, '특별시') THEN left(name, length(name) - 3)
       WHEN suffix(name, '광역시') THEN left(name, length(name) - 3)
       WHEN suffix(name, '직할시') THEN left(name, length(name) - 3)
       WHEN suffix(name, '도') THEN left(name, length(name) - 1)
       ELSE '' END;

CREATE OR REPLACE MACRO place_sido_aliases(name) AS
  list_distinct(list_filter([
    name,
    CASE WHEN length(place_stem(name)) >= 2 THEN place_stem(name) END,
    CASE WHEN length(place_stem(name)) = 3 THEN substr(place_stem(name), 1, 1) || substr(place_stem(name), 3, 1) END
  ], lambda a: a IS NOT NULL));

CREATE OR REPLACE TEMP TABLE place_current_sido AS
SELECT DISTINCT left(code, 2) AS sido FROM place_names WHERE status = '존재';

CREATE OR REPLACE TEMP TABLE place_sido_names AS
SELECT DISTINCT left(code, 2) AS sido, words[1] AS name FROM place_names WHERE len(words) > 0;

-- current_successors over the 시도 moves: the current 시도 a 시도 is, itself or through its moves.
CREATE OR REPLACE TEMP TABLE place_sido_successors AS
WITH RECURSIVE reach(start, node, path) AS (
  SELECT DISTINCT sido, sido, [sido] FROM place_sido_names
  UNION ALL
  SELECT r.start, m.new_code, list_append(r.path, m.new_code)
  FROM reach r JOIN place_sido_moves m ON m.old_code = r.node
  WHERE r.node NOT IN (SELECT sido FROM place_current_sido) AND NOT list_contains(r.path, m.new_code)
)
SELECT DISTINCT start AS sido, node AS successor FROM reach WHERE node IN (SELECT sido FROM place_current_sido);

CREATE OR REPLACE TEMP TABLE place_current_stems AS
SELECT DISTINCT n.sido, unnest(place_sido_aliases(n.name)) AS stem
FROM place_sido_names n WHERE n.sido IN (SELECT sido FROM place_current_sido);

CREATE OR REPLACE TEMP TABLE place_sido_claims AS
WITH named AS (
  SELECT sido, name, place_sido_aliases(name) AS aliases FROM place_sido_names
), found AS (
  -- A 시도 the record carries leads where its moves do; one it does not is the current 시도 whose
  -- name stem it shares.
  SELECT n.name, n.aliases, s.successor AS target FROM named n JOIN place_sido_successors s ON s.sido = n.sido
  UNION ALL
  SELECT n.name, n.aliases, t.sido FROM named n JOIN place_current_stems t
    ON list_contains(list_filter(n.aliases, lambda a: a <> n.name), t.stem)
  WHERE NOT EXISTS (SELECT 1 FROM place_sido_successors s WHERE s.sido = n.sido)
)
SELECT DISTINCT unnest(aliases) AS prefix, target FROM found;

-- A prefix that would name two current 시도 is left out.
CREATE OR REPLACE TEMP TABLE place_sido_prefixes AS
SELECT prefix, min(target) AS sido FROM place_sido_claims GROUP BY prefix HAVING count(DISTINCT target) = 1;

-- legal_dong_index: (시도, the names after the 시도) -> 법정동 code; a name two codes share is left
-- out; the names without the 구 where that still names one code; an abolished name read as the one
-- current code its pairs lead to. A current name wins over a looser and a former one.
CREATE OR REPLACE TEMP TABLE place_exact AS
SELECT left(code, 2) AS sido, array_to_string(words[2:], ' ') AS place, code
FROM place_names WHERE status = '존재' AND len(words) >= 2 AND regexp_full_match(code, '[0-9]{10}');

CREATE OR REPLACE TEMP TABLE place_loose AS
SELECT DISTINCT sido, place_without_gu(place) AS place, code FROM place_exact
WHERE place_without_gu(place) <> place;

CREATE OR REPLACE TEMP TABLE place_dong_successors AS
WITH RECURSIVE reach(start, node, path) AS (
  SELECT DISTINCT code, code, [code] FROM place_names
  WHERE status <> '존재' AND len(words) >= 2 AND regexp_full_match(code, '[0-9]{10}')
  UNION ALL
  SELECT r.start, m.new_code, list_append(r.path, m.new_code)
  FROM reach r JOIN place_dong_moves m ON m.old_code = r.node
  WHERE r.node NOT IN (SELECT code FROM place_exact) AND NOT list_contains(r.path, m.new_code)
)
SELECT DISTINCT start AS code, node AS successor FROM reach WHERE node IN (SELECT code FROM place_exact);

CREATE OR REPLACE TEMP TABLE place_former AS
SELECT DISTINCT left(s.successor, 2) AS sido, n.place, s.successor AS code
FROM (SELECT code, array_to_string(words[2:], ' ') AS place FROM place_names
      WHERE status <> '존재' AND len(words) >= 2 AND regexp_full_match(code, '[0-9]{10}')) f
JOIN place_dong_successors s ON s.code = f.code,
LATERAL (SELECT unnest(list_distinct([f.place, place_without_gu(f.place)])) AS place) n;

CREATE OR REPLACE TEMP TABLE place_dongs AS
WITH exact AS (SELECT sido, place, min(code) AS code, count(DISTINCT code) AS codes FROM place_exact GROUP BY ALL),
     loose AS (SELECT sido, place, min(code) AS code, count(DISTINCT code) AS codes FROM place_loose GROUP BY ALL),
     former AS (SELECT sido, place, min(code) AS code, count(DISTINCT code) AS codes FROM place_former GROUP BY ALL)
SELECT sido, place, code FROM exact WHERE codes = 1
UNION ALL
SELECT l.sido, l.place, l.code FROM loose l
WHERE l.codes = 1 AND NOT EXISTS (SELECT 1 FROM exact e WHERE e.sido = l.sido AND e.place = l.place)
UNION ALL
SELECT f.sido, f.place, f.code FROM former f
WHERE f.codes = 1 AND NOT EXISTS (SELECT 1 FROM exact e WHERE e.sido = f.sido AND e.place = f.place)
  AND NOT EXISTS (SELECT 1 FROM loose l WHERE l.sido = f.sido AND l.place = f.place);
