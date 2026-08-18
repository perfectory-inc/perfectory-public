-- The canonical complex row carries the ten columns the profile source states and nothing read.
--
-- `TB_IRSTT_BASS_HIST.xlsx` has twenty header columns. Ten reached this table; eight more state
-- facts about the complex and were never decoded, and the two that remain are excluded on measured
-- evidence (root ADR-0044). Those eight become ten columns here because one of them,
-- `bsms_pd`, is carried both verbatim and as the two months a parse recovers from it.
--
-- Source fill rates over the whole 202506 table (1,442 rows), measured before this change:
--
--   lttot_sttus_nm    1442/1442  100.00%  three distinct labels
--   appn_basis_law    1442/1442  100.00%  48 distinct spellings
--   make_procs_rt     1442/1442  100.00%  0.0 .. 100.0
--   bsms_pd           1441/1442   99.93%  1,440 parse as YYYY-MM~YYYY-MM, one does not
--   devlop_mth        1441/1442   99.93%  232 distinct spellings
--   invite_upj        1440/1442   99.86%  longest 485 characters
--   make_purps_cn     1439/1442   99.79%  longest 521 characters
--   strwrk_de         1438/1442   99.72%  yyyyMMdd
--
-- Every column is nullable, for the same reason the columns the previous migration added are:
-- a blank source cell is `NULL`, and no fill rate is 100% of every future snapshot. `''` is a
-- different claim — that the source stated a value and the value was nothing — and the non-blank
-- CHECKs below make it unrepresentable rather than merely discouraged. They evaluate to NULL for a
-- NULL value, which Postgres accepts, so absence still passes.
--
-- `lot_sales_status` gets a value-domain CHECK for the reason `kind` and `status` already have one
-- on this table: the domain is owned by `catalog_domain::IndustrialComplexLotSalesStatus` and
-- `lakehouse_domain::INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES` is pinned to that enum by a
-- test, but neither can reach a hand-written `UPDATE`. Three values rather than six, because that
-- is the whole observed domain and there is no `unknown` member: a fourth label is a fact about the
-- source worth failing over, not a bucket to hide it in.
--
-- The four `_raw` columns have no value-domain CHECK and will not get one. `devlop_mth` holds 232
-- distinct values across 1,442 rows and the distinctness is spelling, not meaning: `공영개발`,
-- `공영개발방식`, and `공영개발 방식` are three of them, and `appn_basis_law` has the same shape with
-- 48. Folding those onto codes would assert a classification nobody published. The `_raw` suffix is
-- the contract — this column holds what the source wrote — and normalizing is a separate decision
-- that needs its own evidence. They are `text` and unbounded: the longest observed value is 521
-- characters, and a length limit that truncated would turn a stated fact into a shorter, false one.
--
-- `development_progress_percent` is `numeric(5,2)` with a range CHECK. Not a float: `59.9` has no
-- exact binary representation, and a progress figure that came back as `59.899999999999999` would
-- be one no source stated. This workspace has adopted no Rust decimal type, so the reader projects
-- the column as `::text` and carries the exact digits Postgres renders. `0.00` is a real value here
-- and not an absence — `준비중` and `보상중` complexes report exactly zero progress — which is why
-- there is no `> 0` gate of the kind `official_area_sqm` has.
--
-- `business_period_start_month` and `business_period_end_month` are `text` of shape `YYYY-MM`.
-- There is no date type for a month, and a `date` column would have to pick a day the source never
-- wrote. Their CHECK requires them to be null together or present together: one boundary without
-- the other describes a period the source did not bound. One of the 1,441 stated periods reads
-- `2020-~2024-` — years, no months — and that complex keeps `business_period_raw` intact with both
-- derived columns null. There is deliberately no `start <= end` CHECK: nobody has measured whether
-- every stated period is ordered, and a constraint asserted without measurement is a constraint
-- that rejects real rows.
--
-- No data changes. Every existing row takes NULL in every new column, so no CHECK can fail here and
-- nothing is backfilled — there is no source from which a value could be guessed for a row this
-- migration did not load.
--
-- Rollback: `ALTER TABLE catalog.industrial_complex DROP COLUMN ...` as a new forward migration
-- (ADR-0001 §7 — migration files are immutable once merged). Dropping the columns discards loaded
-- source values, which the next canonical load restores from the Gold snapshot.

ALTER TABLE catalog.industrial_complex
    ADD COLUMN construction_start_date date,
    ADD COLUMN development_progress_percent numeric(5,2),
    ADD COLUMN lot_sales_status text,
    ADD COLUMN business_period_raw text,
    ADD COLUMN business_period_start_month text,
    ADD COLUMN business_period_end_month text,
    ADD COLUMN designation_basis_law_raw text,
    ADD COLUMN development_method_raw text,
    ADD COLUMN development_purpose_raw text,
    ADD COLUMN invited_industries_raw text,
    ADD CONSTRAINT industrial_complex_lot_sales_status_check
        CHECK (lot_sales_status = ANY (ARRAY[
            'planned'::text,
            'in_progress'::text,
            'completed'::text
        ])),
    ADD CONSTRAINT industrial_complex_development_progress_percent_range
        CHECK (development_progress_percent >= 0 AND development_progress_percent <= 100),
    ADD CONSTRAINT industrial_complex_business_period_raw_non_blank
        CHECK (btrim(business_period_raw) <> ''),
    ADD CONSTRAINT industrial_complex_business_period_start_month_shape
        CHECK (business_period_start_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    ADD CONSTRAINT industrial_complex_business_period_end_month_shape
        CHECK (business_period_end_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    ADD CONSTRAINT industrial_complex_business_period_months_together
        CHECK ((business_period_start_month IS NULL) = (business_period_end_month IS NULL)),
    ADD CONSTRAINT industrial_complex_designation_basis_law_raw_non_blank
        CHECK (btrim(designation_basis_law_raw) <> ''),
    ADD CONSTRAINT industrial_complex_development_method_raw_non_blank
        CHECK (btrim(development_method_raw) <> ''),
    ADD CONSTRAINT industrial_complex_development_purpose_raw_non_blank
        CHECK (btrim(development_purpose_raw) <> ''),
    ADD CONSTRAINT industrial_complex_invited_industries_raw_non_blank
        CHECK (btrim(invited_industries_raw) <> '');
