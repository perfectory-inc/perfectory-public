{{ config(materialized='table') }}

with court_auction as (
    select * from {{ ref('stg_gongzzang__court_auction_observation') }}
),

building_unit as (
    select * from {{ ref('stg_foundation__building_register_unit') }}
),

court_pnu as (
    select distinct pnu
    from court_auction
    where nullif(pnu, '') is not null
),

building_relevant as (
    select building_unit.*
    from building_unit
    join court_pnu
      on building_unit.pnu = court_pnu.pnu
),

unit_number_scope_rollup as (
    select
        pnu,
        coalesce(dong_name, '') as dong_name_key,
        unit_number,
        count(distinct building_unit_id) as distinct_target_count,
        count(distinct coalesce(unit_designation, '<null>')) as distinct_designation_count
    from building_relevant
    where unit_number is not null
    group by 1, 2, 3
),

safe_unit_number_scope as (
    select
        pnu,
        dong_name_key,
        unit_number,
        distinct_target_count,
        distinct_designation_count,
        'safe' as unit_number_scope_state
    from unit_number_scope_rollup
    where unit_number_scope_rollup.distinct_target_count = 1
      and unit_number_scope_rollup.distinct_designation_count = 1
),

collision_unit_number_scope as (
    select
        pnu,
        dong_name_key,
        unit_number,
        distinct_target_count,
        distinct_designation_count,
        'collision' as unit_number_scope_state
    from unit_number_scope_rollup
    where not (
        unit_number_scope_rollup.distinct_target_count = 1
        and unit_number_scope_rollup.distinct_designation_count = 1
    )
),

classified_unit_number_scope as (
    select * from safe_unit_number_scope
    union all
    select * from collision_unit_number_scope
),

label_area_candidate_pairs as (
    select
        court_auction.source_observation_id,
        court_auction.source_system,
        court_auction.source_record_id,
        court_auction.source_snapshot_id,
        building_relevant.building_unit_id as target_entity_id,
        building_relevant.target_snapshot_id,
        building_relevant.mgm_bldrgst_pk,
        building_relevant.pnu,
        court_auction.exclusive_area_sqm,
        'building_unit' as target_entity_type,
        'deterministic' as match_method,
        'PNU_DONG_UNIT_LABEL_AREA' as match_path,
        0.99 as confidence_score,
        'high' as confidence_band,
        court_auction.lineage_run_id
    from court_auction
    join building_relevant
      on court_auction.pnu = building_relevant.pnu
     and coalesce(court_auction.dong_name, '') = coalesce(building_relevant.dong_name, '')
     and coalesce(court_auction.unit_label, '') = coalesce(building_relevant.unit_label, '')
     and court_auction.exclusive_area_sqm is not null
),

unit_number_area_candidate_pairs as (
    select
        court_auction.source_observation_id,
        court_auction.source_system,
        court_auction.source_record_id,
        court_auction.source_snapshot_id,
        building_relevant.building_unit_id as target_entity_id,
        building_relevant.target_snapshot_id,
        building_relevant.mgm_bldrgst_pk,
        building_relevant.pnu,
        court_auction.exclusive_area_sqm,
        'building_unit' as target_entity_type,
        'deterministic' as match_method,
        'PNU_DONG_UNIT_NUMBER_AREA' as match_path,
        case
            when classified_unit_number_scope.unit_number_scope_state = 'safe' then 0.98
            else 0.80
        end as confidence_score,
        case
            when classified_unit_number_scope.unit_number_scope_state = 'safe' then 'high'
            else 'medium'
        end as confidence_band,
        classified_unit_number_scope.unit_number_scope_state,
        court_auction.lineage_run_id
    from court_auction
    join building_relevant
      on court_auction.pnu = building_relevant.pnu
     and coalesce(court_auction.dong_name, '') = coalesce(building_relevant.dong_name, '')
     and court_auction.unit_number is not null
     and building_relevant.unit_number is not null
     and court_auction.unit_number = building_relevant.unit_number
     and court_auction.exclusive_area_sqm is not null
    join classified_unit_number_scope
      on court_auction.pnu = classified_unit_number_scope.pnu
     and coalesce(court_auction.dong_name, '') = classified_unit_number_scope.dong_name_key
     and court_auction.unit_number = classified_unit_number_scope.unit_number
),

candidate_area_keys as (
    select distinct mgm_bldrgst_pk, pnu
    from label_area_candidate_pairs
    union
    select distinct mgm_bldrgst_pk, pnu
    from unit_number_area_candidate_pairs
),

exclusive_areas as (
    select
        unit_area.mgm_bldrgst_pk,
        unit_area.pnu,
        sum(unit_area.area_m2) as exclusive_area_sqm
    from {{ source('foundation', 'building_register_unit_area') }} as unit_area
    join candidate_area_keys
      on unit_area.mgm_bldrgst_pk = candidate_area_keys.mgm_bldrgst_pk
     and unit_area.pnu = candidate_area_keys.pnu
    where unit_area.area_kind = 'exclusive'
    group by 1, 2
),

label_area_candidates as (
    select
        label_area_candidate_pairs.source_observation_id,
        label_area_candidate_pairs.source_system,
        label_area_candidate_pairs.source_record_id,
        label_area_candidate_pairs.source_snapshot_id,
        label_area_candidate_pairs.target_entity_id,
        label_area_candidate_pairs.target_snapshot_id,
        label_area_candidate_pairs.target_entity_type,
        label_area_candidate_pairs.match_method,
        label_area_candidate_pairs.match_path,
        label_area_candidate_pairs.confidence_score,
        label_area_candidate_pairs.confidence_band,
        label_area_candidate_pairs.lineage_run_id
    from label_area_candidate_pairs
    join exclusive_areas
      on label_area_candidate_pairs.mgm_bldrgst_pk = exclusive_areas.mgm_bldrgst_pk
     and label_area_candidate_pairs.pnu = exclusive_areas.pnu
    where abs(label_area_candidate_pairs.exclusive_area_sqm - exclusive_areas.exclusive_area_sqm) <= 0.1
),

unit_number_area_candidates as (
    select
        unit_number_area_candidate_pairs.source_observation_id,
        unit_number_area_candidate_pairs.source_system,
        unit_number_area_candidate_pairs.source_record_id,
        unit_number_area_candidate_pairs.source_snapshot_id,
        unit_number_area_candidate_pairs.target_entity_id,
        unit_number_area_candidate_pairs.target_snapshot_id,
        unit_number_area_candidate_pairs.target_entity_type,
        unit_number_area_candidate_pairs.match_method,
        unit_number_area_candidate_pairs.match_path,
        unit_number_area_candidate_pairs.confidence_score,
        unit_number_area_candidate_pairs.confidence_band,
        unit_number_area_candidate_pairs.lineage_run_id
    from unit_number_area_candidate_pairs
    join exclusive_areas
      on unit_number_area_candidate_pairs.mgm_bldrgst_pk = exclusive_areas.mgm_bldrgst_pk
     and unit_number_area_candidate_pairs.pnu = exclusive_areas.pnu
    where abs(unit_number_area_candidate_pairs.exclusive_area_sqm - exclusive_areas.exclusive_area_sqm) <= 0.1
),

unit_number_candidate_pairs as (
    select
        court_auction.source_observation_id,
        court_auction.source_system,
        court_auction.source_record_id,
        court_auction.source_snapshot_id,
        building_relevant.building_unit_id as target_entity_id,
        building_relevant.target_snapshot_id,
        'building_unit' as target_entity_type,
        'deterministic' as match_method,
        'PNU_DONG_UNIT_NUMBER_UNIQUE' as match_path,
        0.90 as confidence_score,
        'medium' as confidence_band,
        court_auction.lineage_run_id
    from court_auction
    join building_relevant
      on court_auction.pnu = building_relevant.pnu
     and coalesce(court_auction.dong_name, '') = coalesce(building_relevant.dong_name, '')
     and court_auction.unit_number is not null
     and building_relevant.unit_number is not null
     and court_auction.unit_number = building_relevant.unit_number
    join safe_unit_number_scope
      on court_auction.pnu = safe_unit_number_scope.pnu
     and coalesce(court_auction.dong_name, '') = safe_unit_number_scope.dong_name_key
     and court_auction.unit_number = safe_unit_number_scope.unit_number
),

unit_number_candidate_source_counts as (
    select
        source_observation_id,
        count(distinct target_entity_id) as distinct_target_count
    from unit_number_candidate_pairs
    group by 1
),

unit_number_unique_candidates as (
    select unit_number_candidate_pairs.*
    from unit_number_candidate_pairs
    join unit_number_candidate_source_counts
      on unit_number_candidate_pairs.source_observation_id = unit_number_candidate_source_counts.source_observation_id
    where unit_number_candidate_source_counts.distinct_target_count = 1
)

select * from label_area_candidates
union all
select * from unit_number_area_candidates
union all
select * from unit_number_unique_candidates
