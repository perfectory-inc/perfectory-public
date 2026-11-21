-- The parts of the 실거래 documents keyed by a 법정동, built once for the whole build: its comparables
-- and its region statistics (document.sql reads them, one 시군구 at a time). The rules and the JSON
-- text are document.sql's.
-- Comparables: one row per deal (the lowest PNU it is placed on, if any), newest first. A 본번 group
-- deal is one comparable: its group's key and size, no one PNU (root ADR-0405).
CREATE OR REPLACE TEMP TABLE rt_comparables AS
WITH by_deal AS (
  SELECT * FROM placed QUALIFY row_number() OVER (PARTITION BY row_id ORDER BY pnu ASC NULLS LAST) = 1
),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY legal_dong ORDER BY deal_date DESC, row_id) AS _n
  FROM by_deal
  WHERE dataset_slug = 'real_transaction_industrial_trade' AND legal_dong IS NOT NULL
    AND NOT coalesce(cancelled, false)
    AND deal_date >= DATE '{comparables_from}' AND deal_date < DATE '{next_month}'
)
  SELECT legal_dong, '[' || string_agg({comparable}, ',' ORDER BY _n) || ']' AS comparables
  FROM ranked WHERE _n <= {comparable_count} GROUP BY legal_dong;

-- Region: the 법정동's 공장창고 and 토지 rows of the trailing 12 months, m² prices in 만원.
CREATE OR REPLACE TEMP TABLE rt_region_doc AS
  SELECT region_code AS legal_dong,
         '[' || string_agg('{"dataset_slug":' || to_json(dataset_slug)::VARCHAR
           || ',"deal_count":' || deal_count || ',"cancelled_count":' || cancelled_count
           || ',"land_price_per_m2_10k_krw":' || CASE WHEN land_price_per_m2_median IS NOT NULL
                THEN '{"p25":' || spark_double_text(land_price_per_m2_p25)
                  || ',"median":' || spark_double_text(land_price_per_m2_median)
                  || ',"p75":' || spark_double_text(land_price_per_m2_p75) || '}' ELSE 'null' END
           || ',"building_price_per_m2_10k_krw":' || CASE WHEN dataset_slug = 'real_transaction_industrial_trade'
                  AND building_price_per_m2_median IS NOT NULL
                THEN '{"p25":' || spark_double_text(building_price_per_m2_p25)
                  || ',"median":' || spark_double_text(building_price_per_m2_median)
                  || ',"p75":' || spark_double_text(building_price_per_m2_p75) || '}' ELSE 'null' END
           || '}', ',' ORDER BY dataset_slug) || ']' AS datasets
  FROM region
  WHERE period_kind = 'trailing_12_months' AND region_level = 'legal_dong'
    AND dataset_slug IN ('real_transaction_industrial_trade', 'real_transaction_land_trade')
  GROUP BY region_code;

-- The 시군구s to build: every one a served parcel or a placed deal is in.
CREATE OR REPLACE TEMP TABLE rt_chunks AS
  SELECT DISTINCT substr(pnu, 1, 5) AS chunk FROM parcels
  UNION SELECT DISTINCT substr(pnu, 1, 5) FROM placed WHERE pnu IS NOT NULL;

CREATE OR REPLACE TABLE gold_rows (pnu VARCHAR, real_transaction_json VARCHAR, transaction_total BIGINT,
  attached_via VARCHAR, place_id VARCHAR, main_lot_group_place_ids VARCHAR, row_digest VARCHAR);
