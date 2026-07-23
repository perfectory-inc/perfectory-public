{% macro foundation_unit_number_from_designation(column_expr) -%}
    try_cast(nullif(regexp_extract({{ column_expr }}, '([0-9]+)[^0-9]*$', 1), '') as integer)
{%- endmacro %}

{% macro foundation_floor_designation_hint_from_designation(column_expr) -%}
    nullif(regexp_extract({{ column_expr }}, '(지하\\s*[0-9]+층|[0-9]+층)', 1), '')
{%- endmacro %}
