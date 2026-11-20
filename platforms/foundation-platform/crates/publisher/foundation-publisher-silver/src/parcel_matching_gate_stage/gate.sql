-- The parcel matching gate (root ADR-0113 §7) over one cadastral edition, as DuckDB tables. It was
-- `parcel_matching_gate.check_parcels` and `parcel_registry_frames.current_ids` (Spark joins).
--
-- Input: gate_edition_source(pnu): the edition's current rows of silver.parcel_boundaries;
-- gate_sido(prefix): the 시도 checked; gate_official(region_cd, status): the newest snapshot of the code
-- list; gate_admin(canonical_code): the served administrative boundaries; gate_registry(pnu, parcel_id,
-- status, valid_from, valid_to): the registry as of the edition (empty when check (b) does not run);
-- renumbering(src, dst): legal_dong_code_change_stage's 1:1 renumberings into the existing codes.
--
-- A well-shaped PNU (19 digits) is held as a UBIGINT: forty million of them fit the gold half's 2 GiB
-- unit, where the strings do not. The width is fixed, so the number orders as the text did and
-- lpad(n, 19, '0') gives the text back.

-- One scan of the edition: every distinct PNU once, as a number when well shaped, as text otherwise,
-- and one row for a NULL PNU (Spark's distinct counts it; it reaches no check).
CREATE OR REPLACE TEMP TABLE gate_scan AS
SELECT DISTINCT
  CASE WHEN regexp_full_match(pnu, '[0-9]{19}') THEN CAST(pnu AS UBIGINT) END AS n,
  CASE WHEN pnu IS NOT NULL AND NOT regexp_full_match(pnu, '[0-9]{19}') THEN pnu END AS bad
FROM gate_edition_source;

CREATE OR REPLACE TEMP TABLE gate_well AS
SELECT n FROM gate_scan
WHERE n IS NOT NULL AND lpad(CAST(n // 100000000000000000 AS VARCHAR), 2, '0') IN (SELECT prefix FROM gate_sido);

CREATE OR REPLACE TEMP TABLE gate_malformed AS
SELECT bad AS pnu FROM gate_scan WHERE bad IS NOT NULL AND substr(bad, 1, 2) IN (SELECT prefix FROM gate_sido);

-- (a) The legal dongs the newest list holds, and the 1:1 renumberings into them.
CREATE OR REPLACE TEMP TABLE gate_existing AS
SELECT DISTINCT CAST(region_cd AS UBIGINT) AS dong FROM gate_official
WHERE status = '존재' AND regexp_full_match(region_cd, '[0-9]{10}');

CREATE OR REPLACE TEMP TABLE gate_successor AS
SELECT CAST(src AS UBIGINT) AS src, CAST(dst AS UBIGINT) AS dst FROM renumbering
WHERE regexp_full_match(src, '[0-9]{10}') AND regexp_full_match(dst, '[0-9]{10}')
  AND CAST(dst AS UBIGINT) IN (SELECT dong FROM gate_existing);

CREATE OR REPLACE TEMP TABLE gate_emd AS
SELECT DISTINCT CAST(canonical_code AS UBIGINT) AS emd FROM gate_admin
WHERE regexp_full_match(canonical_code, '[0-9]{10}');

-- Each well-shaped parcel: its legal dong, whether the list holds it, and its successor if renumbered.
CREATE OR REPLACE TEMP TABLE gate_placed AS
SELECT w.n, w.n // 1000000000 AS dong, s.dst,
       (w.n // 1000000000) IN (SELECT dong FROM gate_existing) AS listed
FROM gate_well w LEFT JOIN gate_successor s ON s.src = w.n // 1000000000;

-- A parcel whose dong the list does not hold and no renumbering explains has no place (a stray); one a
-- renumbering explains is placed under its successor. A placed parcel's 읍면동 needs a boundary.
CREATE OR REPLACE TEMP TABLE gate_stray AS
SELECT n FROM gate_placed WHERE NOT listed AND dst IS NULL;

CREATE OR REPLACE TEMP TABLE gate_moved AS
SELECT n FROM gate_placed WHERE NOT listed AND dst IS NOT NULL;

CREATE OR REPLACE TEMP TABLE gate_no_boundary AS
SELECT n FROM gate_placed
WHERE (listed OR dst IS NOT NULL)
  AND coalesce(dst, dong) // 100 * 100 NOT IN (SELECT emd FROM gate_emd);

-- (b) The current id of each PNU (`parcel_identity.fold(...).current`): the id its last `current` row
-- opened, by (valid_from or valid_to, then the id), unless a later `historic` or `redirected` row closes
-- that same id.
CREATE OR REPLACE TEMP TABLE gate_registry_rows AS
SELECT CAST(pnu AS UBIGINT) AS n, parcel_id, status,
       coalesce(nullif(valid_from, ''), valid_to, '') AS k
FROM gate_registry WHERE regexp_full_match(pnu, '[0-9]{19}');

CREATE OR REPLACE TEMP TABLE gate_last AS
SELECT n, parcel_id, k FROM gate_registry_rows WHERE status = 'current'
QUALIFY row_number() OVER (PARTITION BY n ORDER BY k DESC, parcel_id DESC NULLS LAST) = 1;

CREATE OR REPLACE TEMP TABLE gate_current AS
SELECT l.n, l.parcel_id FROM gate_last l
WHERE NOT EXISTS (
  SELECT 1 FROM gate_registry_rows c
  WHERE c.status IN ('historic', 'redirected') AND c.n = l.n AND c.parcel_id = l.parcel_id AND c.k > l.k);

CREATE OR REPLACE TEMP TABLE gate_held AS
SELECT w.n, c.parcel_id FROM gate_well w LEFT JOIN gate_current c USING (n);

CREATE OR REPLACE TEMP TABLE gate_no_current AS
SELECT n FROM gate_held WHERE parcel_id IS NULL;

CREATE OR REPLACE TEMP TABLE gate_shared_id AS
SELECT parcel_id FROM gate_held WHERE parcel_id IS NOT NULL GROUP BY parcel_id HAVING count(*) > 1;
