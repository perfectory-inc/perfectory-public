{{ config(materialized='table', tags=['full_quality']) }}

with pnu_status as (
    select * from {{ ref('int_entity_resolution__court_auction_pnu_recovery_status') }}
),

status_summary as (
    select
        count(*) as source_total_count,
        sum(case when nullif(pnu, '') is not null then 1 else 0 end) as source_with_pnu_count,
        sum(case when nullif(pnu, '') is null then 1 else 0 end) as source_without_pnu_count,
        sum(case when nullif(pnu, '') is not null and exact_unit_count > 0 then 1 else 0 end) as source_pnu_exact_match_count,
        sum(case when nullif(pnu, '') is not null and exact_unit_count > 0 then exact_unit_count else 0 end) as source_pnu_exact_match_pairs,
        sum(case when nullif(pnu, '') is not null and exact_unit_count = 0 then 1 else 0 end) as source_pnu_exact_missing_count,
        sum(case when nullif(pnu, '') is not null and exact_unit_count = 0 and flipped_unit_count > 0 then 1 else 0 end) as source_pnu_11th_flip_possible_count,
        sum(case when nullif(pnu, '') is not null and exact_unit_count = 0 and flipped_unit_count > 0 then flipped_unit_count else 0 end) as source_pnu_11th_flip_possible_pairs,
        sum(case when flipped_dong_match_count > 0 then 1 else 0 end) as source_pnu_11th_flip_with_dong_count,
        sum(case when flipped_dong_match_count > 0 then flipped_dong_match_count else 0 end) as source_pnu_11th_flip_with_dong_pairs,
        sum(case when flipped_unit_number_match_count > 0 then 1 else 0 end) as source_pnu_11th_flip_with_unit_number_count,
        sum(case when flipped_unit_number_match_count > 0 then flipped_unit_number_match_count else 0 end) as source_pnu_11th_flip_with_unit_number_pairs,
        sum(case when flipped_dong_unit_number_match_count > 0 then 1 else 0 end) as source_pnu_11th_flip_with_dong_unit_number_count,
        sum(case when flipped_dong_unit_number_match_count > 0 then flipped_dong_unit_number_match_count else 0 end) as source_pnu_11th_flip_with_dong_unit_number_pairs,
        sum(case when nullif(pnu, '') is null and nullif(dong_name, '') is not null then 1 else 0 end) as source_without_pnu_with_dong_name_count,
        sum(case when nullif(pnu, '') is null and unit_number is not null then 1 else 0 end) as source_without_pnu_with_unit_number_count,
        sum(case when nullif(pnu, '') is null and exclusive_area_sqm is not null then 1 else 0 end) as source_without_pnu_with_area_count,
        sum(
            case
                when nullif(pnu, '') is null
                 and nullif(dong_name, '') is not null
                 and unit_number is not null
                 and exclusive_area_sqm is not null then 1
                else 0
            end
        ) as source_without_pnu_with_dong_unit_number_area_count,
        sum(case when nullif(pnu, '') is null and has_address_evidence = 1 then 1 else 0 end) as source_without_pnu_with_any_address_count,
        sum(case when nullif(pnu, '') is null and has_coordinate_evidence = 1 then 1 else 0 end) as source_without_pnu_with_coordinates_count,
        sum(
            case
                when nullif(pnu, '') is null
                 and (has_address_evidence = 1 or has_coordinate_evidence = 1) then 1
                else 0
            end
        ) as source_without_pnu_with_address_or_coordinate_count,
        sum(
            case
                when nullif(pnu, '') is null
                 and has_address_evidence = 0
                 and has_coordinate_evidence = 0 then 1
                else 0
            end
        ) as source_without_pnu_without_address_or_coordinate_count
    from pnu_status
)

select
    1 as stage_order,
    'source_total' as recovery_stage,
    source_total_count as source_observation_count,
    source_total_count as candidate_pair_count
from status_summary
union all
select
    2 as stage_order,
    'source_with_pnu' as recovery_stage,
    source_with_pnu_count as source_observation_count,
    source_with_pnu_count as candidate_pair_count
from status_summary
union all
select
    3 as stage_order,
    'source_without_pnu' as recovery_stage,
    source_without_pnu_count as source_observation_count,
    source_without_pnu_count as candidate_pair_count
from status_summary
union all
select
    4 as stage_order,
    'source_pnu_exact_match' as recovery_stage,
    source_pnu_exact_match_count as source_observation_count,
    source_pnu_exact_match_pairs as candidate_pair_count
from status_summary
union all
select
    5 as stage_order,
    'source_pnu_exact_missing' as recovery_stage,
    source_pnu_exact_missing_count as source_observation_count,
    source_pnu_exact_missing_count as candidate_pair_count
from status_summary
union all
select
    6 as stage_order,
    'source_pnu_11th_flip_possible' as recovery_stage,
    source_pnu_11th_flip_possible_count as source_observation_count,
    source_pnu_11th_flip_possible_pairs as candidate_pair_count
from status_summary
union all
select
    7 as stage_order,
    'source_pnu_11th_flip_with_dong' as recovery_stage,
    source_pnu_11th_flip_with_dong_count as source_observation_count,
    source_pnu_11th_flip_with_dong_pairs as candidate_pair_count
from status_summary
union all
select
    8 as stage_order,
    'source_pnu_11th_flip_with_unit_number' as recovery_stage,
    source_pnu_11th_flip_with_unit_number_count as source_observation_count,
    source_pnu_11th_flip_with_unit_number_pairs as candidate_pair_count
from status_summary
union all
select
    9 as stage_order,
    'source_pnu_11th_flip_with_dong_unit_number' as recovery_stage,
    source_pnu_11th_flip_with_dong_unit_number_count as source_observation_count,
    source_pnu_11th_flip_with_dong_unit_number_pairs as candidate_pair_count
from status_summary
union all
select
    10 as stage_order,
    'source_without_pnu_with_dong_name' as recovery_stage,
    source_without_pnu_with_dong_name_count as source_observation_count,
    source_without_pnu_with_dong_name_count as candidate_pair_count
from status_summary
union all
select
    11 as stage_order,
    'source_without_pnu_with_unit_number' as recovery_stage,
    source_without_pnu_with_unit_number_count as source_observation_count,
    source_without_pnu_with_unit_number_count as candidate_pair_count
from status_summary
union all
select
    12 as stage_order,
    'source_without_pnu_with_area' as recovery_stage,
    source_without_pnu_with_area_count as source_observation_count,
    source_without_pnu_with_area_count as candidate_pair_count
from status_summary
union all
select
    13 as stage_order,
    'source_without_pnu_with_dong_unit_number_area' as recovery_stage,
    source_without_pnu_with_dong_unit_number_area_count as source_observation_count,
    source_without_pnu_with_dong_unit_number_area_count as candidate_pair_count
from status_summary
union all
select
    14 as stage_order,
    'source_without_pnu_with_any_address' as recovery_stage,
    source_without_pnu_with_any_address_count as source_observation_count,
    source_without_pnu_with_any_address_count as candidate_pair_count
from status_summary
union all
select
    15 as stage_order,
    'source_without_pnu_with_coordinates' as recovery_stage,
    source_without_pnu_with_coordinates_count as source_observation_count,
    source_without_pnu_with_coordinates_count as candidate_pair_count
from status_summary
union all
select
    16 as stage_order,
    'source_without_pnu_with_address_or_coordinate' as recovery_stage,
    source_without_pnu_with_address_or_coordinate_count as source_observation_count,
    source_without_pnu_with_address_or_coordinate_count as candidate_pair_count
from status_summary
union all
select
    17 as stage_order,
    'source_without_pnu_requires_address_or_coordinate_evidence' as recovery_stage,
    source_without_pnu_without_address_or_coordinate_count as source_observation_count,
    cast(0 as bigint) as candidate_pair_count
from status_summary
