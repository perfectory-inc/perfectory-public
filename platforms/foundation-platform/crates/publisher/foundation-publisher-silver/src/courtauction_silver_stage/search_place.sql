-- The placement of a 물건 search's rows (courtauction_go_kr.offer and link, root ADR-0288 §1, ADR-0382),
-- in three parts the caller runs around the one place-identity resolver (place_identity_stage):
--
-- ca_place_first: each row of ca_search_rows numbered, its source code PNU (none for the sale results,
-- whose table keeps no code: the Spark link took the code path only where the table has
-- source_code_pnu_raw) and its address as the names with the representative lot, the jibun form.
-- After offers over it, ca_place_second: the road form (the first two names, the road name and the
-- building number) for a row whose jibun form gave no PNU, named a 법정동 and has a road name;
-- the jibun form otherwise. After offers, the served parcels and place_offered over that,
-- ca_place: the code path's PNU first (its grade naming the basis, one candidate, its 법정동 its own),
-- else the address path's, with the address path's reason and candidate count.

-- @first
CREATE OR REPLACE TEMP TABLE ca_place_first AS
SELECT row_number() OVER (ORDER BY court_office_code, case_no, property_seq, item_seq) AS _row,
       court_office_code, case_no, property_seq, item_seq, {code_pnu} AS pnu,
       list_filter([address_sido_name, address_sigungu_name, address_dong_name, address_ri_name], lambda x: x IS NOT NULL)
         AS names,
       road_name, building_no_raw, representative_lot_raw
FROM ca_search_rows;
CREATE OR REPLACE TEMP TABLE place_rows AS
SELECT _row, pnu, array_to_string(list_concat(names, [coalesce(representative_lot_raw, '')]), ' ') AS address
FROM ca_place_first;

-- @second
CREATE OR REPLACE TEMP TABLE ca_place_second AS
SELECT f._row,
       CASE WHEN o.jibun_pnu IS NULL AND o.reason IS DISTINCT FROM 'no_legal_dong' AND f.road_name IS NOT NULL
            THEN array_to_string(list_filter([f.names[1], f.names[2], f.road_name, coalesce(f.building_no_raw, '')],
                                             lambda x: x IS NOT NULL AND x <> ''), ' ')
            ELSE array_to_string(list_concat(f.names, [coalesce(f.representative_lot_raw, '')]), ' ') END AS address
FROM ca_place_first f JOIN place_offers o USING (_row);
CREATE OR REPLACE TEMP TABLE place_rows AS
SELECT f._row, f.pnu, s.address FROM ca_place_first f JOIN ca_place_second s USING (_row);

-- @place
CREATE OR REPLACE TEMP TABLE ca_place AS
SELECT f.court_office_code, f.case_no, f.property_seq, f.item_seq,
       coalesce(c.pnu, a.pnu) AS pnu,
       CASE WHEN c.pnu IS NOT NULL THEN
              CASE c.grade WHEN 'code_superseded' THEN 'legal_dong_code_superseded'
                           WHEN 'code_served_edition' THEN 'legal_dong_code_served_edition'
                           ELSE 'legal_dong_code' END
            ELSE a.pnu_basis END AS pnu_basis,
       CASE WHEN c.pnu IS NOT NULL THEN NULL ELSE a.reason END AS pnu_unresolved_reason,
       CASE WHEN c.pnu IS NOT NULL THEN 1 ELSE a.candidate_count END AS pnu_candidate_count,
       CASE WHEN c.pnu IS NOT NULL THEN substr(c.pnu, 1, 10) ELSE o.jibun_code END AS legal_dong_code
FROM ca_place_first f
LEFT JOIN place_code_result c ON c._row = f._row
LEFT JOIN place_address_result a ON a._row = f._row
LEFT JOIN place_offers o ON o._row = f._row;
