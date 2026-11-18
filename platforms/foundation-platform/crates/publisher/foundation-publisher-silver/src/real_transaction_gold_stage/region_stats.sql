-- The 실거래 region statistics (root ADR-0350 §4, ADR-0371 §4) of the deals in `deals`: per
-- (dataset, 법정동 or 시군구, contract month or the trailing 12 months before the build month) the
-- deal count, the cancelled count and the m² price quartiles in 만원 of the uncancelled deals, by
-- land area and, but for 토지, by building area. The rules of `region_rows` and
-- `region_gold_rows` in `real_transaction_silver_to_gold.py`, written so the rows and their
-- `row_digest` are byte for byte what that job writes (judged nationally against the served
-- `gold.real_transaction_region_stats`, 2026-11-14: 3,204,170 rows, every digest equal).
--
-- `deals`: dataset_slug, legal_dong (the deal's CURRENT 법정동, or NULL), deal_date (DATE),
-- deal_amount_10k_krw (BIGINT), land_area_m2, building_area_m2 (DOUBLE), cancelled (BOOLEAN).
-- `{as_of}` is the build month's first day, `{trailing_from}` the month REGION_MONTHS before it,
-- `{as_of_month}` the build month as YYYY-MM. `spark_percentile` and `spark_double_text` are the
-- macros `real_transaction_gold_stage.rs` defines.
WITH base AS (
  SELECT dataset_slug, legal_dong, deal_date, NOT coalesce(cancelled, false) AS live,
         CASE WHEN NOT coalesce(cancelled, false) AND land_area_m2 > 0
              THEN deal_amount_10k_krw / land_area_m2 END AS land_price,
         CASE WHEN NOT coalesce(cancelled, false) AND building_area_m2 > 0
                   AND dataset_slug <> 'real_transaction_land_trade'
              THEN deal_amount_10k_krw / building_area_m2 END AS building_price
  FROM deals
  WHERE legal_dong IS NOT NULL AND deal_date IS NOT NULL
),
periods AS (
  SELECT *, 'month' AS period_kind, strftime(deal_date, '%Y-%m') AS period FROM base
  UNION ALL
  SELECT *, 'trailing_12_months', '{as_of_month}' FROM base
  WHERE deal_date >= DATE '{trailing_from}' AND deal_date < DATE '{as_of}'
),
levels AS (
  SELECT *, 'legal_dong' AS region_level, legal_dong AS region_code FROM periods
  UNION ALL
  SELECT *, 'sigungu', substr(legal_dong, 1, 5) || '00000' FROM periods
),
grouped AS (
  SELECT dataset_slug, region_level, region_code, period_kind, period,
         count(*)::BIGINT AS deal_count,
         sum(CASE WHEN NOT live THEN 1 ELSE 0 END)::BIGINT AS cancelled_count,
         list(land_price ORDER BY land_price) FILTER (WHERE land_price IS NOT NULL) AS land_prices,
         list(building_price ORDER BY building_price) FILTER (WHERE building_price IS NOT NULL) AS building_prices
  FROM levels
  GROUP BY ALL
),
quartiles AS (
  SELECT dataset_slug, region_level, region_code, period_kind, period, deal_count, cancelled_count,
         spark_percentile(land_prices, 0.25) AS land_price_per_m2_p25,
         spark_percentile(land_prices, 0.5) AS land_price_per_m2_median,
         spark_percentile(land_prices, 0.75) AS land_price_per_m2_p75,
         spark_percentile(building_prices, 0.25) AS building_price_per_m2_p25,
         spark_percentile(building_prices, 0.5) AS building_price_per_m2_median,
         spark_percentile(building_prices, 0.75) AS building_price_per_m2_p75
  FROM grouped
)
SELECT *,
       -- Spark's to_json of the content columns in contract order, nulls left out.
       sha256('{"dataset_slug":"' || dataset_slug || '","region_level":"' || region_level
              || '","region_code":"' || region_code || '","period_kind":"' || period_kind
              || '","period":"' || period || '","deal_count":' || deal_count
              || ',"cancelled_count":' || cancelled_count
              || coalesce(',"land_price_per_m2_p25":' || spark_double_text(land_price_per_m2_p25), '')
              || coalesce(',"land_price_per_m2_median":' || spark_double_text(land_price_per_m2_median), '')
              || coalesce(',"land_price_per_m2_p75":' || spark_double_text(land_price_per_m2_p75), '')
              || coalesce(',"building_price_per_m2_p25":' || spark_double_text(building_price_per_m2_p25), '')
              || coalesce(',"building_price_per_m2_median":' || spark_double_text(building_price_per_m2_median), '')
              || coalesce(',"building_price_per_m2_p75":' || spark_double_text(building_price_per_m2_p75), '')
              || '}') AS row_digest
FROM quartiles
