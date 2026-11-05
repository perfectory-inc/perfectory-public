-- Rendered by render_current_rows.py from the lakehouse contracts; do not edit (root ADR-0286 §4).
-- No foundation source declares a current key that must be unique (each keeps ties or has none).
select 'none' as source, '' as current_key, 0 as rows_of_key where false
