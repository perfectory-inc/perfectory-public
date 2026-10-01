-- The industrial complex's development stage, one value per site-formation word the source states
-- (root ADR-0121).
--
-- `status` merged the source's `준비중` and `보상중` into `planned` and called `조성완료` `operating`,
-- which the source does not say. This column names each measured word after itself:
-- 조성완료 site_completed, 조성중 site_in_progress, 준비중 preparing, 보상중 compensating. No value
-- stands for a word nobody has seen; a new word stops the Bronze export before it reaches here.
--
-- Nullable: rows loaded before the column existed carry none until the next canonical load, and a
-- complex registered through the API has no source row. `status` stays until its consumers have
-- moved to this column (expand, then contract).
--
-- Rollback: `ALTER TABLE catalog.industrial_complex DROP COLUMN development_stage` as a new forward
-- migration (ADR-0001 §7).

ALTER TABLE catalog.industrial_complex
    ADD COLUMN development_stage text,
    ADD CONSTRAINT industrial_complex_development_stage_check
        CHECK (development_stage = ANY (ARRAY[
            'site_completed'::text,
            'site_in_progress'::text,
            'preparing'::text,
            'compensating'::text
        ]));
