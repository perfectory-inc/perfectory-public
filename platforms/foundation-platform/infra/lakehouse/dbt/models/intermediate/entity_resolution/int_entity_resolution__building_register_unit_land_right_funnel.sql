{{ config(materialized='table', tags=['full_quality']) }}

{#-
  교차확증 깔때기 (ADR-0106). 대상 = proposal_required + 문자열 규칙 밖 표기
  (후보 모델과 같은 범위 — 조건의 SSOT 는 foundation_unit_name_is_string_clean).
  대조 기준(2026-09-25, 운영 silver 를 SELECT 로 직접 검산): 대상 270,315 →
  등기 필지 위 243,047(89.9%) → 이름 유일 확증 101,271(37.5%) → 층 회수
  +5,516. 카탈로그 투영 전수조사(잔여 270,979, 확증률 36.2%)와 일치. 운영
  실행 수치가 이 자릿수에서 크게 벗어나면 정규형 매크로나 범위 조건이
  어긋난 것이다.
-#}

with building_unit as (
    select * from {{ ref('stg_foundation__building_register_unit') }}
),

land_right as (
    select * from {{ ref('stg_foundation__land_right_registration') }}
),

candidates as (
    select * from {{ ref('int_entity_resolution__building_register_unit_land_right_candidates') }}
),

unit_scope as (
    select
        building_unit_id,
        pnu,
        {{ foundation_normalized_unit_name('coalesce(unit_designation, unit_name_raw)', 'dong_name') }} as normalized_name
    from building_unit
    where nullif(pnu, '') is not null
      and (
        normalization_status = 'proposal_required'
        or not {{ foundation_unit_name_is_string_clean("coalesce(unit_designation, '')") }}
      )
),

right_parcels as (
    select distinct pnu from land_right
),

right_names as (
    select
        pnu,
        {{ foundation_normalized_unit_name('ho_name', 'dong_name') }} as normalized_name
    from land_right
),

matched_pairs as (
    select
        unit_scope.building_unit_id,
        count(*) as matching_right_rows
    from unit_scope
    join right_names
      on unit_scope.pnu = right_names.pnu
     and unit_scope.normalized_name = right_names.normalized_name
    where unit_scope.normalized_name is not null
    group by 1
),

units_in_corroboration_scope as (
    select
        1 as stage_order,
        'units_in_corroboration_scope' as diagnostic_stage,
        count(*) as affected_row_count
    from unit_scope
),

on_parcels_with_land_rights as (
    select
        2 as stage_order,
        'on_parcels_with_land_rights' as diagnostic_stage,
        count(*) as affected_row_count
    from unit_scope
    join right_parcels
      on unit_scope.pnu = right_parcels.pnu
),

uniquely_corroborated as (
    select
        3 as stage_order,
        'uniquely_corroborated' as diagnostic_stage,
        count(*) as affected_row_count
    from candidates
    where match_path = 'PNU_NORMALIZED_UNIT_NAME_UNIQUE'
),

floor_recovered as (
    select
        4 as stage_order,
        'floor_recovered' as diagnostic_stage,
        count(*) as affected_row_count
    from candidates
    where match_path = 'PNU_FLOOR_NORMALIZED_UNIT_NAME_UNIQUE'
),

ambiguous_matches as (
    select
        5 as stage_order,
        'ambiguous_matches' as diagnostic_stage,
        count(*) as affected_row_count
    from matched_pairs
    where matching_right_rows > 1
)

select * from units_in_corroboration_scope
union all
select * from on_parcels_with_land_rights
union all
select * from uniquely_corroborated
union all
select * from floor_recovered
union all
select * from ambiguous_matches
order by stage_order
