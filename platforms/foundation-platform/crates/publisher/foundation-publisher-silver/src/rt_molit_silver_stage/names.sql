-- One export's rows with their names, and the lines not loaded (root ADR-0349 §2, ADR-0364): the rules of
-- `rt_molit_real_transaction.py` `rows_of`, `identity` and `build_row`.
--
-- Reads `records.sql`'s tables and `rt_line_rejects_{i}` (record, reason): the records the row-number
-- walk refused. A record is a row when it is not blank, not refused and names its export's month in
-- `계약년월` (a record without that column does). Its fields are the header's names with its cells
-- (a name the header repeats takes its last cell). Its name is the sha256 (first 32 hex) of
-- `{dataset}|{month}|` and the JSON of its fields less {drop}, keys sorted, as `json.dumps(…,
-- ensure_ascii=False, sort_keys=True, separators=(",", ":"))` writes it; rows of one name are ranked by
-- the cells of {mutable} (a missing one is ''), then by their order in the export.
--
-- `rt_parsed_{i}`: the rows in the Silver columns the export fills. `rt_rejected_lines_{i}`
-- (source_line_number, raw_line, reason): the records not loaded.
CREATE OR REPLACE TEMP TABLE rt_fields_{i} AS
WITH names AS (SELECT name, min(idx) AS position, max(idx) AS at FROM rt_header_{i} GROUP BY name)
SELECT c.record, n.name, n.position, c.value
FROM rt_cells_{i} c
JOIN names n ON n.at = c.idx
WHERE c.record IN (SELECT record FROM rt_records_{i} WHERE NOT blank)
  AND c.record NOT IN (SELECT record FROM rt_line_rejects_{i});

CREATE OR REPLACE TEMP TABLE rt_month_rejects_{i} AS
SELECT r.record
FROM (SELECT DISTINCT record FROM rt_fields_{i}) r
LEFT JOIN rt_fields_{i} f ON f.record = r.record AND f.name = '계약년월'
WHERE coalesce(rt_strip(f.value), '{month}') <> '{month}';

CREATE OR REPLACE TEMP TABLE rt_rejected_lines_{i} AS
SELECT r.source_line_number, r.raw_line, x.reason
FROM rt_line_rejects_{i} x JOIN rt_records_{i} r USING (record)
UNION ALL
SELECT r.source_line_number, r.raw_line, 'deal_month_outside_export'
FROM rt_month_rejects_{i} m JOIN rt_records_{i} r USING (record);

CREATE OR REPLACE TEMP TABLE rt_kept_{i} AS
SELECT * FROM rt_fields_{i} WHERE record NOT IN (SELECT record FROM rt_month_rejects_{i});

CREATE OR REPLACE TEMP TABLE rt_named_{i} AS
WITH rows AS (SELECT DISTINCT record FROM rt_kept_{i}),
stable AS (
  SELECT record, '{' || string_agg(rt_json_text(name) || ':' || rt_json_text(value), ',' ORDER BY name) || '}' AS fields
  FROM rt_kept_{i} WHERE name NOT IN (SELECT unnest({drop})) GROUP BY record
),
provider AS (
  SELECT record, '{' || string_agg(rt_json_text(name) || ':' || rt_json_text(value), ',' ORDER BY name) || '}' AS fields
  FROM rt_kept_{i} WHERE name <> '{row_number_column}' GROUP BY record
),
mutable AS (
  SELECT r.record, list(coalesce(f.value, '') ORDER BY m.position) AS cells
  FROM rows r
  CROSS JOIN (SELECT unnest({mutable}) AS name, generate_subscripts({mutable}, 1) AS position) m
  LEFT JOIN rt_kept_{i} f ON f.record = r.record AND f.name = m.name
  GROUP BY r.record
),
named AS (
  SELECT r.record,
         substr(sha256('{dataset}|{month}|' || coalesce(s.fields, '{}')), 1, 32) AS name,
         coalesce(p.fields, '{}') AS provider_fields_json,
         m.cells AS mutable_cells
  FROM rows r
  LEFT JOIN stable s USING (record)
  LEFT JOIN provider p USING (record)
  LEFT JOIN mutable m USING (record)
)
SELECT record, provider_fields_json,
       name || '-' || row_number() OVER (PARTITION BY name ORDER BY mutable_cells, record) AS row_key
FROM named;

-- Each Silver column from the last header that fills it.
CREATE OR REPLACE TEMP TABLE rt_mapped_{i} AS
SELECT record, target, area_kind, value
FROM (
  SELECT k.record, c.target, c.area_kind, k.value,
         row_number() OVER (PARTITION BY k.record, c.target ORDER BY k.position DESC) AS last_header
  FROM rt_kept_{i} k JOIN rt_csv_columns c ON c.header = k.name
)
WHERE last_header = 1;

CREATE OR REPLACE TEMP TABLE rt_parsed_{i} AS
WITH typed AS (
  SELECT record,
         max(rt_text(value)) FILTER (WHERE target = 'address_raw') AS address_raw,
         max(rt_text(value)) FILTER (WHERE target = 'jibun_raw') AS jibun_raw,
         max(rt_text(value)) FILTER (WHERE target = 'bonbun') AS bonbun,
         max(rt_text(value)) FILTER (WHERE target = 'bubun') AS bubun,
         max(rt_text(value)) FILTER (WHERE target = 'building_name') AS building_name,
         max(rt_text(value)) FILTER (WHERE target = 'building_type') AS building_type,
         max(rt_text(value)) FILTER (WHERE target = 'house_type') AS house_type,
         max(rt_text(value)) FILTER (WHERE target = 'building_use') AS building_use,
         max(rt_text(value)) FILTER (WHERE target = 'land_use') AS land_use,
         max(rt_text(value)) FILTER (WHERE target = 'jimok') AS jimok,
         max(rt_text(value)) FILTER (WHERE target = 'road_condition') AS road_condition,
         max(rt_text(value)) FILTER (WHERE target = 'road_address') AS road_address,
         max(rt_whole(value)) FILTER (WHERE target = 'deal_amount_10k_krw') AS deal_amount_10k_krw,
         max(rt_whole(value)) FILTER (WHERE target = 'deposit_10k_krw') AS deposit_10k_krw,
         max(rt_whole(value)) FILTER (WHERE target = 'monthly_rent_10k_krw') AS monthly_rent_10k_krw,
         max(rt_whole(value)) FILTER (WHERE target = 'pre_deposit_10k_krw') AS pre_deposit_10k_krw,
         max(rt_whole(value)) FILTER (WHERE target = 'pre_monthly_rent_10k_krw') AS pre_monthly_rent_10k_krw,
         max(rt_text(value)) FILTER (WHERE target = 'rent_kind') AS rent_kind,
         max(rt_text(value)) FILTER (WHERE target = 'contract_term') AS contract_term,
         max(rt_text(value)) FILTER (WHERE target = 'contract_type') AS contract_type,
         max(rt_text(value)) FILTER (WHERE target = 'use_rr_right') AS use_rr_right,
         max(rt_text(value)) FILTER (WHERE target = 'apt_dong') AS apt_dong,
         max(rt_text(value)) FILTER (WHERE target = 'floor') AS floor,
         max(rt_whole(value)) FILTER (WHERE target = 'build_year') AS build_year,
         max(rt_text(value)) FILTER (WHERE target = 'buyer_gbn') AS buyer_gbn,
         max(rt_text(value)) FILTER (WHERE target = 'seller_gbn') AS seller_gbn,
         max(rt_text(value)) FILTER (WHERE target = 'dealing_gbn') AS dealing_gbn,
         max(rt_text(value)) FILTER (WHERE target = 'estate_agent_sgg_nm') AS estate_agent_sgg_nm,
         max(rt_text(value)) FILTER (WHERE target = 'share_dealing_type') AS share_dealing_type,
         max(rt_text(value)) FILTER (WHERE target = 'ownership_gbn') AS ownership_gbn,
         max(rt_day(value)) FILTER (WHERE target = 'cancel_date') AS cancel_date,
         max(rt_day(value)) FILTER (WHERE target = 'registration_date') AS registration_date,
         max(rt_decimal(value)) FILTER (WHERE target = 'building_area_m2') AS building_area_m2,
         max(CASE WHEN rt_decimal(value) IS NOT NULL THEN area_kind END)
           FILTER (WHERE target = 'building_area_m2') AS building_area_kind,
         max(rt_decimal(value)) FILTER (WHERE target = 'land_area_m2') AS land_area_m2,
         max(CASE WHEN rt_decimal(value) IS NOT NULL THEN area_kind END)
           FILTER (WHERE target = 'land_area_m2') AS land_area_kind
  FROM rt_mapped_{i}
  GROUP BY record
),
day AS (
  SELECT record, TRY_CAST(rt_strip(value) AS INTEGER) AS day
  FROM rt_kept_{i} WHERE name = '계약일' AND regexp_full_match(rt_strip(value), '[+-]?[0-9]+')
)
SELECT '{dataset}' AS dataset_slug, '{month}' AS deal_ym, n.row_key,
       TRY_CAST(printf('%s-%s-%02d', substr('{month}', 1, 4), substr('{month}', 5, 2), d.day) AS DATE) AS deal_date,
       t.* EXCLUDE (record),
       t.cancel_date IS NOT NULL AS cancelled,
       contains(coalesce(t.jibun_raw, ''), '*') AS jibun_masked,
       n.provider_fields_json,
       '{object_key}' AS source_record_id,
       r.source_line_number
FROM rt_named_{i} n
JOIN rt_records_{i} r USING (record)
LEFT JOIN typed t USING (record)
LEFT JOIN day d USING (record)
ORDER BY n.record;
