{{ config(materialized='table', tags=['full_quality']) }}

{#-
  교차확증 깔때기 (ADR-0106). 운영 실행이 2026-09-25 측정치를 재현하는지
  대조하는 것이 첫 검증이다: 꼬리 108,054 → 등기 필지 위 94,737(87.7%) →
  유일 일치 39,084(36.2%), 모호 3,865.
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
    where normalization_status = 'proposal_required'
      and nullif(pnu, '') is not null
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

units_needing_proposal as (
    select
        1 as stage_order,
        'units_needing_proposal' as diagnostic_stage,
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
),

ambiguous_matches as (
    select
        4 as stage_order,
        'ambiguous_matches' as diagnostic_stage,
        count(*) as affected_row_count
    from matched_pairs
    where matching_right_rows > 1
)

select * from units_needing_proposal
union all
select * from on_parcels_with_land_rights
union all
select * from uniquely_corroborated
union all
select * from ambiguous_matches
order by stage_order
