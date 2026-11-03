{# Rendered by render_current_rows.py from the lakehouse contracts; do not edit (root ADR-0267, ADR-0275, ADR-0286). #}
{% macro foundation_current_rows(name) %}
{%- set rules = {
    'building_register_unit': {'predicate': none, 'key': ['mgm_bldrgst_pk'], 'newest_first': ['valid_from_utc', 'ingested_at_utc'], 'removal': 'row_state'},
    'building_register_unit_area': {'predicate': none, 'key': ['mgm_bldrgst_pk'], 'newest_first': ['valid_from_utc', 'ingested_at_utc'], 'removal': 'row_state'},
    'land_right_registration': {'predicate': none, 'key': ['pnu', 'right_serial_no'], 'newest_first': ['source_snapshot_id', 'ingested_at_utc'], 'removal': 'row_state'},
    'unit_official_price': {'predicate': none, 'key': none, 'newest_first': ['vintage'], 'removal': none, 'edition': 'source_snapshot_id', 'per': []},
} -%}
{%- set relation = source('foundation', name) -%}
{%- set rule = rules[name] -%}
{%- if rule is none -%}
{{ relation }}
{%- elif rule['edition'] -%}
(
    select r.* from {{ relation }} as r
    where exists (
        select 1 from (
            select {% for column in rule['per'] %}{{ column }}, {% endfor %}{{ rule['edition'] }}, rank() over (
                {% if rule['per'] %}partition by {{ rule['per'] | join(', ') }} {% endif %}order by {% for column in rule['newest_first'] %}max({{ column }}) desc nulls last{% if not loop.last %}, {% endif %}{% endfor %}
            ) as _edition_rank
            from {{ relation }}
            group by {% for column in rule['per'] %}{{ column }}, {% endfor %}{{ rule['edition'] }}
        ) as e
        where e._edition_rank = 1 and e.{{ rule['edition'] }} = r.{{ rule['edition'] }}{% for column in rule['per'] %} and e.{{ column }} is not distinct from r.{{ column }}{% endfor %}
    )
    {% if rule['predicate'] %}and ({{ rule['predicate'] }}){% endif %}
) as {{ name }}
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
