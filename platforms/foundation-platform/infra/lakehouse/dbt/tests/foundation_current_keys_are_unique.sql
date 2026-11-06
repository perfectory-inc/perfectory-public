-- Rendered by render_current_rows.py from the lakehouse contracts; do not edit (root ADR-0286 §4).
select 'unit_official_price_versions' as source, cast(mgm_bldrgst_pk as varchar) || '/' || cast(base_date as varchar) as current_key, count(*) as rows_of_key
from {{ foundation_current_rows('unit_official_price_versions') }}
group by mgm_bldrgst_pk, base_date
having count(*) > 1
