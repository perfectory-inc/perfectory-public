{# Rendered by render_current_rows.py from the lakehouse contracts; do not edit (root ADR-0267, ADR-0275). #}
{% macro foundation_current_rows(name) %}
{%- set rules = {
    'building_register_unit': none,
    'building_register_unit_area': none,
    'land_right_registration': none,
    'unit_official_price_versions': {'predicate': none, 'key': ['mgm_bldrgst_pk', 'base_date'], 'newest_first': ['vintage', 'ingested_at_utc'], 'removal': 'row_state'},
} -%}
{%- set relation = source('foundation', name) -%}
{%- set rule = rules[name] -%}
{%- if rule is none -%}
{{ relation }}
{%- else -%}
(
    select * from (
        select *{% if rule['key'] %}, rank() over (
            partition by {{ rule['key'] | join(', ') }}
            order by {% for column in rule['newest_first'] %}{{ column }} desc nulls last{% if not loop.last %}, {% endif %}{% endfor %}
        ) as _current_row_rank{% endif %}
        from {{ relation }}
        {% if rule['predicate'] %}where {{ rule['predicate'] }}{% endif %}
    ) as ranked
    {% if rule['key'] %}where _current_row_rank = 1{% endif %}
    {% if rule['removal'] %}and ({{ rule['removal'] }} is null or {{ rule['removal'] }} <> 'removed'){% endif %}
) as {{ name }}
{%- endif -%}
{% endmacro %}
