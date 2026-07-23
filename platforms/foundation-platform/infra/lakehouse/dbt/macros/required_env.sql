{% macro required_env_var_sql_literal(name) -%}
    {%- set value = env_var(name, '') -%}
    {%- if value | trim == '' -%}
        {{ exceptions.raise_compiler_error("Missing required environment variable: " ~ name) }}
    {%- endif -%}
    '{{ value | replace("'", "''") }}'
{%- endmacro %}
