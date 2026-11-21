-- Each deal's source PNU carried to the CURRENT parcel (root ADR-0288 §1, ADR-0482 §1): the rules of
-- `place_identity_resolver.resolve(…, keep_candidate=True)` and `held_forms`.
--
-- `deals` (deals.sql), `crosswalk` (old_code → current_code, `place_identity_stage` reference.sql `place_remap`),
-- `ambiguous` (amb_code: old codes whose chain splits), `served_forms` (_code10, _form10, _rank:
-- `place_identity_stage` reference.sql `place_forms`), `parcels` (pnu: the served edition's parcels).
--
-- `pnu` becomes the current parcel or NULL; `pnu_candidate` keeps the current form a deal names when
-- it is not served (the lineage may place it); `place_identity_grade` and `pnu_unresolved_reason` say how.
CREATE OR REPLACE TABLE resolved AS
WITH coded AS (
  SELECT d.*, substr(d.pnu, 1, 10) AS _code10, x.current_code,
         coalesce(a.amb_code IS NOT NULL, false) AS _amb
  FROM deals d
  LEFT JOIN crosswalk x ON x.old_code = substr(d.pnu, 1, 10)
  LEFT JOIN ambiguous a ON a.amb_code = substr(d.pnu, 1, 10)
),
candidates AS (
  SELECT *,
         CASE WHEN pnu IS NOT NULL AND NOT _amb
              THEN concat(coalesce(current_code, _code10), substr(pnu, 11, 9)) END AS _candidate
  FROM coded
),
held AS (
  SELECT c.*, p.pnu IS NOT NULL AS _held
  FROM candidates c
  LEFT JOIN (SELECT DISTINCT pnu FROM parcels) p ON p.pnu = c._candidate
),
asked AS (
  SELECT DISTINCT pnu AS _asked FROM held WHERE NOT _held AND NOT _amb AND pnu IS NOT NULL
),
forms AS (
  SELECT a._asked, f._rank, concat(f._form10, substr(a._asked, 11, 9)) AS _form
  FROM asked a JOIN served_forms f ON f._code10 = substr(a._asked, 1, 10)
),
found_forms AS (
  SELECT f._asked, f._rank, count(*) AS _n, min(f._form) AS _form
  FROM forms f SEMI JOIN (SELECT DISTINCT pnu FROM parcels) p ON p.pnu = f._form
  GROUP BY f._asked, f._rank
),
best_form AS (
  SELECT _asked, arg_min(_form, _rank) AS _form, arg_min(_n, _rank) AS _n
  FROM found_forms GROUP BY _asked
),
served AS (
  SELECT h.*,
         coalesce(b._n > 1, false) AS _served_ambiguous,
         CASE WHEN NOT coalesce(b._n > 1, false) THEN b._form END AS _served_form
  FROM held h
  LEFT JOIN best_form b ON b._asked = (CASE WHEN NOT h._held AND NOT h._amb THEN h.pnu END)
),
graded AS (
  SELECT *,
         NOT _held AND _served_form IS NOT NULL AS _served,
         _held OR (NOT _held AND _served_form IS NOT NULL) AS _placed
  FROM served
)
SELECT * EXCLUDE (pnu, _code10, current_code, _amb, _candidate, _held, _served_form, _served_ambiguous,
                  _served, _placed),
       CASE WHEN _served THEN 'code_served_edition'
            WHEN NOT _held AND current_code IS NOT NULL AND NOT _amb THEN 'code_superseded_parcel_absent'
            WHEN _held AND current_code IS NOT NULL THEN 'code_superseded'
            WHEN _held THEN 'code_exact'
            ELSE 'unresolved' END AS place_identity_grade,
       CASE WHEN _placed THEN NULL
            WHEN pnu IS NULL THEN 'no_source_code'
            WHEN _amb OR _served_ambiguous THEN 'code_subdivision_ambiguous'
            ELSE 'parcel_absent' END AS pnu_unresolved_reason,
       CASE WHEN _held THEN _candidate WHEN _served THEN _served_form END AS pnu,
       CASE WHEN NOT _placed THEN _candidate END AS pnu_candidate
FROM graded;
