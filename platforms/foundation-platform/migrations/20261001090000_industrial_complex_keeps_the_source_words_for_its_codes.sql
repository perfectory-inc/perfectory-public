-- The industrial complex keeps the source's own words beside its three coded columns
-- (root ADR-0117 §5).
--
-- The Bronze plan maps VWorld's `lrstt_ty`, `make_sttus_nm` and `lttot_sttus_nm` labels to the
-- `kind`, `status` and `lot_sales_status` codes and, until now, dropped the words. Two words share
-- one code (`준비중` and `보상중` are both `planned`), so the code cannot say which the source
-- stated, and a screen that shows the code has to invent a word for it. These columns hold the
-- trimmed label the mapping matched; the codes stay the filter.
--
-- Free text, so no value-domain check: an unknown label already fails loudly in the Bronze plan.
-- Null until the next Gold snapshot is loaded, and null for complexes registered through the API,
-- which have no source row. Never blank.
--
-- Rollback: `ALTER TABLE catalog.industrial_complex DROP COLUMN ...` as a new forward migration
-- (ADR-0001 §7). Dropping the columns discards loaded source words, which the next canonical load
-- restores from the Gold snapshot.

ALTER TABLE catalog.industrial_complex
    ADD COLUMN kind_raw text,
    ADD COLUMN status_raw text,
    ADD COLUMN lot_sales_status_raw text,
    ADD CONSTRAINT industrial_complex_kind_raw_non_blank
        CHECK (btrim(kind_raw) <> ''),
    ADD CONSTRAINT industrial_complex_status_raw_non_blank
        CHECK (btrim(status_raw) <> ''),
    ADD CONSTRAINT industrial_complex_lot_sales_status_raw_non_blank
        CHECK (btrim(lot_sales_status_raw) <> '');
