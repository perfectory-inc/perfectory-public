{#-
  호 이름 보수 정규형 (ADR-0106 결정 3) — 대장·등기 양쪽이 같은 규칙으로
  정규화될 때만 교차확증이 성립하므로, 정규형의 SSOT는 이 매크로 한 곳이다.

  순서(측정 스크립트와 동일): 공백 제거 → 괄호주석 제거 → 동 접두 제거 →
  앞 `제` 제거 → 뒤 `호` 제거 → 층 접두 제거 → 지층/지하층 → B →
  한글 음역 6종 영문화 → 구분자(·.,) 제거 → 선행 0 제거.
  붙임표는 보존한다: `6-2`와 `62`는 다른 호다 (2026-09-25 실측 근거).
-#}
{% macro foundation_normalized_unit_name(name_expr, dong_expr="cast(null as varchar)") -%}
    {%- set s0 = "replace(upper(" ~ name_expr ~ "), ' ', '')" -%}
    {%- set s1 = "regexp_replace(" ~ s0 ~ ", '[(][^)]*[)]', '')" -%}
    {%- set d0 = "nullif(nullif(replace(upper(coalesce(" ~ dong_expr ~ ", '')), ' ', ''), ''), '0000')" -%}
    {%- set s2 = "if(" ~ d0 ~ " is not null and starts_with(" ~ s1 ~ ", " ~ d0 ~ "), substr(" ~ s1 ~ ", length(" ~ d0 ~ ") + 1), " ~ s1 ~ ")" -%}
    {%- set s3 = "regexp_replace(" ~ s2 ~ ", '^제', '')" -%}
    {%- set s4 = "regexp_replace(" ~ s3 ~ ", '호$', '')" -%}
    {%- set s5 = "regexp_replace(" ~ s4 ~ ", '^(지하|지상)?[0-9]{1,2}층', '')" -%}
    {%- set s6 = "regexp_replace(" ~ s5 ~ ", '^(지층|지하층)', 'B')" -%}
    {%- set s7 = "replace(replace(replace(replace(replace(replace(" ~ s6 ~ ", '에이치', 'H'), '에프', 'F'), '에이', 'A'), '비', 'B'), '씨', 'C'), '디', 'D')" -%}
    {%- set s8 = "regexp_replace(" ~ s7 ~ ", '[·.,]', '')" -%}
    {%- set s9 = "regexp_replace(" ~ s8 ~ ", '(^|[^0-9])0+([0-9])', '$1$2')" -%}
    nullif({{ s9 }}, '')
{%- endmacro %}

{#-
  문자열 규칙(1~3차)으로 이미 깨끗하게 읽히는 호 표기인지 (ADR-0106 문맥의
  전수조사 사다리). 교차확증 대상 = 이 판정이 거짓인 표기 + proposal_required.
  입력은 공백 압축된 unit_designation 을 전제한다.
-#}
{% macro foundation_unit_name_is_string_clean(column_expr) -%}
    (
        regexp_like({{ column_expr }}, '^[0-9]+호?$')
        or regexp_like({{ column_expr }}, '^(B|비|지하?[0-9]*층?|[0-9]+층)-?[0-9]+호?$')
        or regexp_like({{ column_expr }}, '^제[0-9]+호?$')
        or regexp_like({{ column_expr }}, '^[가-힣A-Za-z0-9]{1,4}동[A-Za-z]?[0-9]+호?$')
        or regexp_like({{ column_expr }}, '^[A-Za-z]{1,3}-?[0-9]+호?$')
        or regexp_like({{ column_expr }}, '^[0-9]+-[0-9]+호?$')
        or regexp_like({{ column_expr }}, '^(상가|근생|사무소?|점포)[A-Za-z]?-?[0-9]+호?$')
        or regexp_like({{ column_expr }}, '^(일|이|삼|사|오|육|칠|팔|구|십)?층[0-9]+호?$')
    )
{%- endmacro %}
