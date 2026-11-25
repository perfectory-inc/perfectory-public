-- The industrial-complex catalog's rows (gold.complex_catalog, root ADR-0426), as
-- industrial_complex_silver_to_gold.py built them: each complex's current Silver row, its 시도 and
-- 시군구 carried to the current codes through the pairs in force, and the current 시도's name.
-- reference.sql runs first.
--
-- Input: complex_current: the current rows of silver.industrial_complexes (the contract's rule);
-- place_remap(old_code, current_code), place_ambiguous(code), place_names(code, words, status).
-- Output: built, the replaced table's columns then row_digest.

-- place_identity_resolver.current_region: each level read as its 10-digit code; a code whose chain
-- splits keeps no code at that level; the finest level that resolves names the levels above it.
CREATE OR REPLACE TEMP TABLE complex_levels AS
SELECT DISTINCT sido_code, sigungu_code, primary_bjdong_code,
       CASE WHEN length(sido_code) = 2 THEN sido_code || '00000000' END AS sido10,
       CASE WHEN length(sigungu_code) = 5 THEN sigungu_code || '00000' END AS sigungu10,
       CASE WHEN length(primary_bjdong_code) = 10 THEN primary_bjdong_code END AS bjdong10
FROM complex_current;

CREATE OR REPLACE TEMP TABLE complex_carried AS
SELECT l.sido_code, l.sigungu_code, l.primary_bjdong_code,
       CASE WHEN l.sido10 IS NULL OR l.sido10 IN (SELECT code FROM place_ambiguous) THEN NULL
            ELSE left(coalesce(rs.current_code, l.sido10), 2) END AS sido_now,
       CASE WHEN l.sigungu10 IS NULL OR l.sigungu10 IN (SELECT code FROM place_ambiguous) THEN NULL
            ELSE left(coalesce(rg.current_code, l.sigungu10), 5) END AS sigungu_now,
       CASE WHEN l.bjdong10 IS NULL OR l.bjdong10 IN (SELECT code FROM place_ambiguous) THEN NULL
            ELSE coalesce(rb.current_code, l.bjdong10) END AS bjdong_now
FROM complex_levels l
LEFT JOIN place_remap rs ON rs.old_code = l.sido10
LEFT JOIN place_remap rg ON rg.old_code = l.sigungu10
LEFT JOIN place_remap rb ON rb.old_code = l.bjdong10;

CREATE OR REPLACE TEMP TABLE complex_region AS
SELECT sido_code, sigungu_code, primary_bjdong_code,
       CASE WHEN coalesce(sigungu_final, '') <> '' THEN left(sigungu_final, 2) ELSE sido_now END AS sido_now,
       sigungu_final AS sigungu_now
FROM (SELECT *, CASE WHEN coalesce(bjdong_now, '') <> '' THEN left(bjdong_now, 5) ELSE sigungu_now END
                AS sigungu_final
      FROM complex_carried);

-- place_identity_resolver.sido_names: the first word of the full name of the newest snapshot's
-- current rows under each 시도, the 시도 row (xx00000000) first and otherwise the smallest code.
CREATE OR REPLACE TEMP TABLE complex_sido_names AS
SELECT left(code, 2) AS sido,
       arg_min(words[1], CASE WHEN substr(code, 3) = '00000000' THEN '0' ELSE '1' END || code) AS name
FROM place_names WHERE status = '존재' AND len(words) > 0 GROUP BY 1;

CREATE OR REPLACE TABLE built AS
SELECT *,
       sha256(to_json({'complex_id': complex_id, 'official_complex_code': official_complex_code,
         'name': name, 'kind': kind, 'status': status, 'development_stage': development_stage,
         'kind_raw': kind_raw, 'status_raw': status_raw, 'sido_code': sido_code,
         'sigungu_code': sigungu_code, 'sido_name': sido_name, 'address_text': address_text,
         'management_agency_name': management_agency_name, 'developer_name': developer_name,
         'designated_date': designated_date, 'construction_start_date': construction_start_date,
         'completion_date': completion_date, 'official_area_sqm': official_area_sqm,
         'development_progress_percent': development_progress_percent,
         'lot_sales_status': lot_sales_status, 'lot_sales_status_raw': lot_sales_status_raw,
         'business_period_raw': business_period_raw,
         'business_period_start_month': business_period_start_month,
         'business_period_end_month': business_period_end_month,
         'designation_basis_law_raw': designation_basis_law_raw,
         'development_method_raw': development_method_raw,
         'development_purpose_raw': development_purpose_raw,
         'invited_industries_raw': invited_industries_raw, 'calculated_area_sqm': calculated_area_sqm,
         'parcel_count': parcel_count, 'boundary_object_key': boundary_object_key})::VARCHAR) AS row_digest
FROM (
  SELECT c.complex_id, c.official_complex_code, c.complex_name AS name, c.complex_kind AS kind, c.status,
         c.development_stage, c.complex_kind_raw AS kind_raw, c.status_raw,
         r.sido_now AS sido_code, r.sigungu_now AS sigungu_code, n.name AS sido_name,
         c.address_text, c.management_agency_name, c.developer_name,
         CAST(c.designated_date AS DATE) AS designated_date,
         CAST(c.construction_start_date AS DATE) AS construction_start_date,
         CAST(c.completion_date AS DATE) AS completion_date,
         CAST(c.official_area_sqm AS DECIMAL(18, 2)) AS official_area_sqm,
         CAST(c.development_progress_percent AS DECIMAL(5, 2)) AS development_progress_percent,
         c.lot_sales_status, c.lot_sales_status_raw, c.business_period_raw,
         c.business_period_start_month, c.business_period_end_month, c.designation_basis_law_raw,
         c.development_method_raw, c.development_purpose_raw, c.invited_industries_raw,
         -- The Spark job's constants: no calculated area, no parcel count, no boundary artifact (the
         -- served table holds none on any of its 1,442 rows, 2026-11-22).
         CAST(NULL AS DECIMAL(18, 2)) AS calculated_area_sqm, CAST(0 AS BIGINT) AS parcel_count,
         CAST(NULL AS VARCHAR) AS boundary_object_key,
         c.source_snapshot_id, '@iceberg_snapshot_id@' AS iceberg_snapshot_id,
         CAST('@published_at_utc@' AS TIMESTAMP) AS published_at_utc
  FROM complex_current c
  LEFT JOIN complex_region r
    ON r.sido_code IS NOT DISTINCT FROM c.sido_code
   AND r.sigungu_code IS NOT DISTINCT FROM c.sigungu_code
   AND r.primary_bjdong_code IS NOT DISTINCT FROM c.primary_bjdong_code
  LEFT JOIN complex_sido_names n ON n.sido = r.sido_now
);
