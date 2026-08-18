-- A canonical column may only be required when something produces it (root ADR-0040).
--
-- `catalog.industrial_complex.primary_bjdong_code` was `NOT NULL`, and no producer fills it. The
-- 1,442 sourced industrial complexes carry an administrative code of sigungu granularity at best
-- (root ADR-0034), and one carries none at all; root ADR-0035 already dropped the requirement on
-- the lakehouse side and left this column named as the remaining open item. Requiring it here has
-- exactly two outcomes: a ten-digit code is invented for rows that have none, or the canonical
-- table stays empty. It has been the second one.
--
-- The shape CHECK is deliberately left in place. `primary_bjdong_code ~ '^[0-9]{10}$'` evaluates to
-- NULL for a NULL value, and Postgres accepts a row whose CHECK is NULL, so the constraint keeps
-- rejecting a malformed code without rejecting an absent one. "Unknown" and "malformed" stay
-- different facts.
--
-- No data changes. The six rows already in the table all carry a ten-digit code and keep it; this
-- migration only widens what a future row is allowed to be.
--
-- Rollback: `ALTER TABLE catalog.industrial_complex ALTER COLUMN primary_bjdong_code SET NOT NULL;`
-- as a new forward migration (ADR-0001 §7 — migration files are immutable once merged). That
-- statement fails while any NULL row exists, so reverting means first sourcing a legal-dong code
-- for every loaded complex or archiving those rows. The revert cost is the source, not the schema.

ALTER TABLE catalog.industrial_complex
    ALTER COLUMN primary_bjdong_code DROP NOT NULL;
