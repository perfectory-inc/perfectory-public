-- The deals whose lot the provider masked (`3**`), as the inference reads them: the rules of
-- `masked_deals` in `real_transaction_silver_to_gold.py`. 지분 거래 are left out: their area is the
-- share's, not the parcel's (root ADR-0351 §1). `lot_low`..`lot_high` is the 본번 range the mask
-- allows (`3**` 300–399); `zone` is the provider's 용도지역 in the land register's words.
CREATE OR REPLACE TABLE masked_all AS
WITH lots AS (
  SELECT *, trim(coalesce(jibun_raw, '')) AS _raw, trim(coalesce(land_use, '')) AS _zone
  FROM deals
),
masks AS (
  SELECT *,
         length(regexp_extract(_raw, '^(산\s*)?(\d+)(\*+)$', 3)) AS _stars,
         TRY_CAST(regexp_extract(_raw, '^(산\s*)?(\d+)(\*+)$', 2) AS INTEGER) AS _digits
  FROM lots
  WHERE regexp_matches(_raw, '^(산\s*)?(\d+)(\*+)$') AND legal_dong IS NOT NULL
    AND coalesce(share_dealing_type, '') IS DISTINCT FROM '지분'
)
SELECT row_id, dataset_slug, legal_dong AS legal_dong_code,
       starts_with(_raw, '산') AS lot_mountain,
       _digits * CAST(pow(10, _stars) AS INTEGER) AS lot_low,
       _digits * CAST(pow(10, _stars) AS INTEGER) + CAST(pow(10, _stars) AS INTEGER) - 1 AS lot_high,
       CASE WHEN _zone IN ('', '-') THEN NULL
            WHEN suffix(_zone, '지역') OR suffix(_zone, '구역') THEN _zone
            ELSE concat(_zone, '지역') END AS zone,
       CASE WHEN trim(jimok) <> '' THEN trim(jimok) END AS jimok,
       land_area_m2, building_area_m2 AS floor_area_m2, build_year AS building_year, deal_date
FROM masks;
