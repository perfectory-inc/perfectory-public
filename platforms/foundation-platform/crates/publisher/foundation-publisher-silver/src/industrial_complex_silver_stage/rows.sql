-- One export of `bronze.industrial_complexes_raw_jsonl` as `silver.industrial_complexes` rows (root
-- ADR-0033, ADR-0559). It was `industrial_complex_bronze_to_silver.build_silver_frame` (Spark); each
-- column is read the way that frame read it, so the rows are the rows Spark wrote:
--
-- - `trim` is Spark's `trim`: spaces only, a null stays null, and `""` stays `""` so the empty-string
--   gate can refuse a producer that filled a column with nothing (root ADR-0035).
-- - `complex_ic_text_or_null` is Spark's `trim_to_null`, for the three free-text columns it used it on.
-- - Dates are `yyyy-MM-dd` and anything else is null (`to_date` with a pattern); decimals are cast to
--   the contract's precision and an unreadable one is null (Spark's non-ANSI `cast`).
-- - The two instants are the exporter's RFC 3339 text (`notice_utc`, `python_values.sql`, runs first).
--
-- Input: complex_lines_json (`lines.sql`, which defines the readers used here) and
-- complex_ids(official_complex_code VARCHAR, complex_id VARCHAR), the UUIDv5 of each trimmed code
-- (`ComplexId::for_official_complex_code`, the shared kernel's derivation). Parameter: {checksum}, the
-- row checksum over the other columns, rendered from the contract by the caller.
-- Output: complex_rows, the table's columns in contract order.

CREATE OR REPLACE TEMP TABLE complex_unchecked AS
SELECT
  coalesce(complex_ic_text_or_null(complex_ic_field(l.j, 'complex_id')), i.complex_id) AS complex_id,
  trim(complex_ic_field(l.j, 'official_complex_code')) AS official_complex_code,
  trim(complex_ic_field(l.j, 'complex_name')) AS complex_name,
  lower(trim(regexp_replace(trim(complex_ic_field(l.j, 'complex_name')), '\s+', ' ', 'g'))) AS complex_name_normalized,
  trim(complex_ic_field(l.j, 'complex_kind')) AS complex_kind,
  trim(complex_ic_field(l.j, 'status')) AS status,
  trim(complex_ic_field(l.j, 'development_stage')) AS development_stage,
  trim(complex_ic_field(l.j, 'complex_kind_raw')) AS complex_kind_raw,
  trim(complex_ic_field(l.j, 'status_raw')) AS status_raw,
  trim(complex_ic_field(l.j, 'sido_code')) AS sido_code,
  trim(complex_ic_field(l.j, 'sigungu_code')) AS sigungu_code,
  trim(complex_ic_field(l.j, 'primary_bjdong_code')) AS primary_bjdong_code,
  complex_ic_text_or_null(complex_ic_field(l.j, 'address_text')) AS address_text,
  complex_ic_text_or_null(complex_ic_field(l.j, 'management_agency_name')) AS management_agency_name,
  complex_ic_text_or_null(complex_ic_field(l.j, 'developer_name')) AS developer_name,
  CAST(try_strptime(complex_ic_field(l.j, 'designated_date'), '%Y-%m-%d') AS DATE) AS designated_date,
  CAST(try_strptime(complex_ic_field(l.j, 'construction_start_date'), '%Y-%m-%d') AS DATE) AS construction_start_date,
  CAST(try_strptime(complex_ic_field(l.j, 'completion_date'), '%Y-%m-%d') AS DATE) AS completion_date,
  TRY_CAST(complex_ic_field(l.j, 'official_area_sqm') AS DECIMAL(18, 2)) AS official_area_sqm,
  TRY_CAST(complex_ic_field(l.j, 'development_progress_percent') AS DECIMAL(5, 2)) AS development_progress_percent,
  trim(complex_ic_field(l.j, 'lot_sales_status')) AS lot_sales_status,
  trim(complex_ic_field(l.j, 'lot_sales_status_raw')) AS lot_sales_status_raw,
  trim(complex_ic_field(l.j, 'business_period_raw')) AS business_period_raw,
  trim(complex_ic_field(l.j, 'business_period_start_month')) AS business_period_start_month,
  trim(complex_ic_field(l.j, 'business_period_end_month')) AS business_period_end_month,
  trim(complex_ic_field(l.j, 'designation_basis_law_raw')) AS designation_basis_law_raw,
  trim(complex_ic_field(l.j, 'development_method_raw')) AS development_method_raw,
  trim(complex_ic_field(l.j, 'development_purpose_raw')) AS development_purpose_raw,
  trim(complex_ic_field(l.j, 'invited_industries_raw')) AS invited_industries_raw,
  trim(complex_ic_field(l.j, 'source_record_id')) AS source_record_id,
  trim(complex_ic_field(l.j, 'source_snapshot_id')) AS source_snapshot_id,
  notice_utc(json_extract(l.j, '$.valid_from_utc')) AS valid_from_utc,
  CAST(NULL AS TIMESTAMPTZ) AS valid_to_utc,
  notice_utc(json_extract(l.j, '$.ingested_at_utc')) AS ingested_at_utc,
  l.line_number
FROM complex_lines_json l
LEFT JOIN complex_ids i ON i.official_complex_code = trim(complex_ic_field(l.j, 'official_complex_code'));

CREATE OR REPLACE TEMP TABLE complex_rows AS
SELECT * EXCLUDE (line_number), {checksum} AS row_checksum_sha256
FROM complex_unchecked
ORDER BY line_number;
