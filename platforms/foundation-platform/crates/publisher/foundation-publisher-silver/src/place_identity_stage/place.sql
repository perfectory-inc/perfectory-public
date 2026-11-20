-- Places each row on a served parcel (root ADR-0288 §1, ADR-0482): the provider's code first, carried
-- old -> current and kept only when the served edition holds it (or holds it under an earlier form of
-- its code); then, for what that leaves, the address. It was `place_identity_resolver.resolve` +
-- `held_forms`, `address_pnu.resolve` and `data_go_kr_onbid.placed_items` (Spark joins).
--
-- Input: place_rows, place_offers (offers.sql), place_remap, place_ambiguous, place_forms
-- (reference.sql), place_parcels(pnu): the served parcels among place_asked, place_titles(pnu,
-- road_address_raw): the building register's current titles on the roads place_roads names.
-- Output: place_result(_row, pnu, pnu_basis, pnu_unresolved_reason).

-- held_forms: for each asked PNU whose current form is not served, the served parcel an earlier form of
-- its code names, the lowest rank first; ambiguous when that rank holds more than one.
CREATE OR REPLACE MACRO place_held_forms(asked_table) AS TABLE
SELECT asked, CASE WHEN n = 1 THEN form END AS served_form, n > 1 AS served_ambiguous
FROM (
  SELECT asked, arg_min(form, (rank, n, form)) AS form, arg_min(n, (rank, n, form)) AS n
  FROM (
    SELECT a.asked, f.rank, count(*) AS n, min(f.form10 || substr(a.asked, 11)) AS form
    FROM (SELECT DISTINCT asked FROM query_table(asked_table) WHERE asked IS NOT NULL) a
    JOIN place_forms f ON f.code10 = left(a.asked, 10)
    WHERE f.form10 || substr(a.asked, 11) IN (SELECT pnu FROM place_parcels)
    GROUP BY a.asked, f.rank
  )
  GROUP BY asked
);

-- The code path (resolve).
CREATE OR REPLACE TEMP TABLE place_by_code AS
SELECT r._row, r.pnu AS source_pnu,
       r.pnu IS NOT NULL AND left(r.pnu, 10) IN (SELECT code FROM place_ambiguous) AS ambiguous,
       m.current_code IS NOT NULL AS superseded,
       CASE WHEN r.pnu IS NOT NULL AND left(r.pnu, 10) NOT IN (SELECT code FROM place_ambiguous)
            THEN coalesce(m.current_code, left(r.pnu, 10)) || substr(r.pnu, 11) END AS candidate
FROM place_rows r LEFT JOIN place_remap m ON m.old_code = left(r.pnu, 10);

CREATE OR REPLACE TEMP TABLE place_code_asked AS
SELECT b.source_pnu AS asked FROM place_by_code b
WHERE NOT b.ambiguous AND (b.candidate IS NULL OR b.candidate NOT IN (SELECT pnu FROM place_parcels));

CREATE OR REPLACE TEMP TABLE place_code_forms AS SELECT * FROM place_held_forms('place_code_asked');

CREATE OR REPLACE TEMP TABLE place_code_result AS
SELECT b._row,
       CASE WHEN b.held THEN b.candidate WHEN x.served THEN f.served_form END AS pnu,
       CASE WHEN x.served THEN 'code_served_edition'
            WHEN NOT b.held AND b.superseded AND NOT b.ambiguous THEN 'code_superseded_parcel_absent'
            WHEN b.held AND b.superseded THEN 'code_superseded'
            WHEN b.held THEN 'code_exact'
            ELSE 'unresolved' END AS grade,
       CASE WHEN b.held OR x.served THEN NULL
            WHEN b.source_pnu IS NULL THEN 'no_source_code'
            WHEN b.ambiguous OR coalesce(f.served_ambiguous, false) THEN 'code_subdivision_ambiguous'
            ELSE 'parcel_absent' END AS pnu_unresolved_reason
FROM (
  SELECT b.*, coalesce(b.candidate IN (SELECT pnu FROM place_parcels), false) AS held FROM place_by_code b
) b
LEFT JOIN place_code_forms f ON f.asked = CASE WHEN NOT b.held AND NOT b.ambiguous THEN b.source_pnu END,
LATERAL (SELECT NOT b.held AND f.served_form IS NOT NULL AS served) x;

-- The address path (address_pnu.resolve), for the rows the code path left.
CREATE OR REPLACE TEMP TABLE place_jibun AS
SELECT o._row, o.jibun_pnu, o.jibun_pnu IN (SELECT pnu FROM place_parcels) AS held
FROM place_offers o JOIN place_code_result c ON c._row = o._row AND c.pnu IS NULL
WHERE o.jibun_pnu IS NOT NULL;

CREATE OR REPLACE TEMP TABLE place_jibun_asked AS SELECT jibun_pnu AS asked FROM place_jibun WHERE NOT held;
CREATE OR REPLACE TEMP TABLE place_jibun_forms AS SELECT * FROM place_held_forms('place_jibun_asked');

CREATE OR REPLACE TEMP TABLE place_road_titles AS
SELECT road_sido, road_sigungu, road_name, road_main_no, road_sub_no,
       count(DISTINCT pnu) AS pnus, min(pnu) AS road_pnu
FROM (
  SELECT left(pnu, 2) AS road_sido,
         regexp_extract(road_address_raw, place_register_road(), 1) AS road_sigungu,
         regexp_extract(road_address_raw, place_register_road(), 2) AS road_name,
         TRY_CAST(regexp_extract(road_address_raw, place_register_road(), 3) AS INTEGER) AS road_main_no,
         coalesce(TRY_CAST(regexp_extract(road_address_raw, place_register_road(), 4) AS INTEGER), 0) AS road_sub_no,
         pnu
  FROM place_titles WHERE pnu IS NOT NULL AND road_address_raw IS NOT NULL AND road_address_raw <> ''
)
GROUP BY ALL;

CREATE OR REPLACE TEMP TABLE place_address_result AS
SELECT o._row,
       CASE WHEN x.jibun_ok THEN x.jibun WHEN x.road_ok THEN t.road_pnu END AS pnu,
       CASE WHEN x.jibun_ok THEN 'jibun_address' WHEN x.road_ok THEN 'road_address' END AS pnu_basis,
       CASE WHEN x.jibun_ok OR x.road_ok THEN NULL
            WHEN o.reason IS NOT NULL THEN o.reason
            WHEN x.jibun IS NOT NULL THEN 'parcel_absent'
            WHEN t.pnus > 1 THEN 'road_ambiguous'
            ELSE 'road_not_found' END AS reason,
       -- pnu_candidate_count: one for a lot found; for a road address, the PNUs its titles name.
       CASE WHEN x.jibun_ok THEN 1 WHEN o.road_name IS NOT NULL THEN CAST(coalesce(t.pnus, 0) AS INTEGER) END AS candidate_count
FROM place_offers o
JOIN place_code_result c ON c._row = o._row AND c.pnu IS NULL
LEFT JOIN place_jibun j ON j._row = o._row
LEFT JOIN place_jibun_forms f ON f.asked = CASE WHEN NOT j.held THEN j.jibun_pnu END
LEFT JOIN place_road_titles t
  ON o.road_name IS NOT NULL AND t.road_sido = o.road_sido AND t.road_sigungu = o.road_sigungu
 AND t.road_name = o.road_name AND t.road_main_no = o.road_main_no AND t.road_sub_no = o.road_sub_no,
LATERAL (SELECT coalesce(f.served_form, j.jibun_pnu) AS jibun,
                coalesce(f.served_form, j.jibun_pnu) IS NOT NULL AND (coalesce(j.held, false) OR f.served_form IS NOT NULL) AS jibun_ok,
                coalesce(t.pnus = 1, false) AS road_ok) x;

-- placed_items: the code path's grade names its basis; the address's reason replaces the code path's
-- only where the provider gave no code and the address said something (not "no address", "unparsed").
CREATE OR REPLACE TEMP TABLE place_result AS
SELECT c._row,
       coalesce(c.pnu, a.pnu) AS pnu,
       coalesce(CASE WHEN c.pnu IS NOT NULL THEN
                  CASE c.grade WHEN 'code_exact' THEN 'legal_dong_code'
                               WHEN 'code_superseded' THEN 'legal_dong_code_superseded'
                               WHEN 'code_served_edition' THEN 'legal_dong_code_served_edition' END END,
                a.pnu_basis) AS pnu_basis,
       CASE WHEN coalesce(c.pnu, a.pnu) IS NOT NULL THEN NULL
            WHEN a.reason IS NOT NULL AND a.reason NOT IN ('no_address', 'unparsed')
                 AND c.pnu_unresolved_reason = 'no_source_code' THEN a.reason
            ELSE c.pnu_unresolved_reason END AS pnu_unresolved_reason
FROM place_code_result c LEFT JOIN place_address_result a ON a._row = c._row;
