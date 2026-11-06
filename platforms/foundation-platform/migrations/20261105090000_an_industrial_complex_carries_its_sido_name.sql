-- An industrial complex carries the current name of its 시도 code (root ADR-0426).
--
-- `sido_code` and `sigungu_code` are carried to the current codes through the recorded 법정동 code
-- pairs (`reference.legal_dong_code_change`) when gold.complex_catalog is built, and re-placed for
-- the complexes a code change reaches. `sido_name` is that code's name in the newest 법정동 code
-- snapshot, written by the same load, so the 시도 filter of every screen reads its options and their
-- names from the API instead of a hand-written list (2026-11-04: three such lists still offered the
-- 29 and 46 abolished on 2026-07-01 and not 12).
--
-- Nullable: a row loaded before this column, or a complex whose 시도 the table does not name, has
-- none; the facet then shows the code. No data changes here; the next canonical load fills it.
-- Rollback: a forward migration dropping the column (ADR-0001 §7).

ALTER TABLE catalog.industrial_complex
    ADD COLUMN sido_name text CHECK (sido_name IS NULL OR btrim(sido_name) <> '');
