-- How the provider's export columns become Silver columns (root ADR-0349 §2), once a run: the rules of
-- `rt_molit_real_transaction.py` (`COLUMNS`, `AREAS`, `LAND_CONTRACT_AREA`, `KNOWN_ELSEWHERE` and the
-- readers `text_or_none`, `amount`, `decimal`, `day`).
--
-- `rt_csv_columns` maps a header name to the Silver column it fills and, for an area, the kind its
-- kind column takes when the area reads. Two headers that fill one column (`번지`/`지번`,
-- `매수자`/`매수`) are never both in one export; if they were, the later header wins, as a Python
-- dict filled in header order. How a column reads its text is the column's (`names.sql`), not the
-- header's: every header of one column reads alike.

-- Python's `str.strip()`: every character `str.isspace()` holds, at both ends.
CREATE OR REPLACE MACRO rt_strip(v) AS
  regexp_replace(v, '^[\s\x{0B}\x{1C}-\x{1F}\x{85}\pZ]+|[\s\x{0B}\x{1C}-\x{1F}\x{85}\pZ]+$', '', 'g');
-- `text_or_none`: stripped, and nothing for a blank or `-`.
CREATE OR REPLACE MACRO rt_text(v) AS
  CASE WHEN rt_strip(v) IN ('', '-') THEN NULL ELSE rt_strip(v) END;
-- `amount`: a whole number once its thousands commas go, else nothing.
CREATE OR REPLACE MACRO rt_whole(v) AS
  CASE WHEN regexp_full_match(replace(coalesce(rt_text(v), ''), ',', ''), '-?[0-9]+')
       THEN TRY_CAST(replace(rt_text(v), ',', '') AS BIGINT) END;
-- `decimal`: a number once its thousands commas go, else nothing.
CREATE OR REPLACE MACRO rt_decimal(v) AS TRY_CAST(replace(coalesce(rt_text(v), ''), ',', '') AS DOUBLE);
-- `day`: `20261028`, `26.10.28`, `2026.10.28` or `2026-10-28`, else nothing.
CREATE OR REPLACE MACRO rt_day(v) AS
  CAST(try_strptime(coalesce(rt_text(v), ''), ['%Y%m%d', '%y.%m.%d', '%Y.%m.%d', '%Y-%m-%d']) AS DATE);
-- A text as `json.dumps(..., ensure_ascii=False)` writes it. DuckDB's writer differs only in the case of
-- the hex digits of nine control characters (measured over U+0000..U+001F, U+007F and the separators).
CREATE OR REPLACE MACRO rt_json_text(v) AS
  CASE WHEN regexp_matches(v, '[\x{0B}\x{0E}\x{0F}\x{1A}-\x{1F}]')
       THEN replace(replace(replace(replace(replace(replace(replace(replace(replace(CAST(to_json(v) AS VARCHAR),
              '\u000B', '\u000b'), '\u000E', '\u000e'), '\u000F', '\u000f'), '\u001A', '\u001a'),
              '\u001B', '\u001b'), '\u001C', '\u001c'), '\u001D', '\u001d'), '\u001E', '\u001e'),
              '\u001F', '\u001f')
       ELSE CAST(to_json(v) AS VARCHAR) END;

CREATE OR REPLACE TABLE rt_csv_columns AS
SELECT * FROM (VALUES
  ('시군구', 'address_raw', NULL),
  ('번지', 'jibun_raw', NULL),
  ('지번', 'jibun_raw', NULL),
  ('본번', 'bonbun', NULL),
  ('부번', 'bubun', NULL),
  ('단지명', 'building_name', NULL),
  ('건물명', 'building_name', NULL),
  ('유형', 'building_type', NULL),
  ('주택유형', 'house_type', NULL),
  ('건축물주용도', 'building_use', NULL),
  ('용도지역', 'land_use', NULL),
  ('지목', 'jimok', NULL),
  ('도로조건', 'road_condition', NULL),
  ('도로명', 'road_address', NULL),
  ('거래금액(만원)', 'deal_amount_10k_krw', NULL),
  ('보증금(만원)', 'deposit_10k_krw', NULL),
  ('월세금(만원)', 'monthly_rent_10k_krw', NULL),
  ('종전계약 보증금(만원)', 'pre_deposit_10k_krw', NULL),
  ('종전계약 월세(만원)', 'pre_monthly_rent_10k_krw', NULL),
  ('전월세구분', 'rent_kind', NULL),
  ('계약기간', 'contract_term', NULL),
  ('계약구분', 'contract_type', NULL),
  ('갱신요구권 사용', 'use_rr_right', NULL),
  ('동', 'apt_dong', NULL),
  ('층', 'floor', NULL),
  ('건축년도', 'build_year', NULL),
  ('매수자', 'buyer_gbn', NULL),
  ('매수', 'buyer_gbn', NULL),
  ('매도자', 'seller_gbn', NULL),
  ('매도', 'seller_gbn', NULL),
  ('거래유형', 'dealing_gbn', NULL),
  ('중개사소재지', 'estate_agent_sgg_nm', NULL),
  ('지분구분', 'share_dealing_type', NULL),
  ('분/입주권', 'ownership_gbn', NULL),
  ('해제사유발생일', 'cancel_date', NULL),
  ('등기일자', 'registration_date', NULL),
  -- Areas: 계약면적 is the land's in the 토지 export and the house's in the 단독다가구 전월세 export.
  ('전용면적(㎡)', 'building_area_m2', 'exclusive'),
  ('전용/연면적(㎡)', 'building_area_m2', 'exclusive_or_total_floor'),
  ('연면적(㎡)', 'building_area_m2', 'total_floor'),
  ('계약면적(㎡)', 'building_area_m2', 'contract'),
  ('대지면적(㎡)', 'land_area_m2', 'plottage'),
  ('대지권면적(㎡)', 'land_area_m2', 'land_right'),
  ('계약면적', 'land_area_m2', 'deal')
) AS t(header, target, area_kind);

-- Header names the load reads for something else than a column: the row number, the month and the day.
CREATE OR REPLACE TABLE rt_known_elsewhere AS SELECT * FROM (VALUES ('NO'), ('계약년월'), ('계약일')) AS t(header);
