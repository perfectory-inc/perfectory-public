{{ config(materialized='table', tags=['full_quality']) }}

with court_auction as (
    select * from {{ ref('stg_gongzzang__court_auction_observation') }}
),

building_unit as (
    select * from {{ ref('stg_foundation__building_register_unit') }}
),

court_flipped_pnu as (
    select
        court_auction.*,
        case
            when nullif(pnu, '') is not null
             and length(pnu) >= 11
             and substr(pnu, 11, 1) in ('1', '2')
                then concat(
                    substr(pnu, 1, 10),
                    case substr(pnu, 11, 1)
                        when '1' then '2'
                        when '2' then '1'
                    end,
                    substr(pnu, 12)
                )
        end as flipped_pnu
    from court_auction
),

court_probe_pnu as (
    select pnu
    from court_flipped_pnu
    where nullif(pnu, '') is not null

    union

    select flipped_pnu as pnu
    from court_flipped_pnu
    where nullif(flipped_pnu, '') is not null
),

building_relevant as (
    select building_unit.*
    from building_unit
    join court_probe_pnu
      on building_unit.pnu = court_probe_pnu.pnu
),

building_pnu_rollup as (
    select
        pnu,
        count(*) as unit_count
    from building_relevant
    group by 1
),

court_pnu_status as (
    select
        court_flipped_pnu.source_observation_id,
        court_flipped_pnu.source_record_id,
        court_flipped_pnu.pnu,
        court_flipped_pnu.flipped_pnu,
        court_flipped_pnu.dong_name,
        court_flipped_pnu.unit_label,
        court_flipped_pnu.unit_number,
        court_flipped_pnu.exclusive_area_sqm,
        court_flipped_pnu.lot_address_raw,
        court_flipped_pnu.road_address_raw,
        court_flipped_pnu.print_addr_raw,
        court_flipped_pnu.x_coordinate,
        court_flipped_pnu.y_coordinate,
        case
            when nullif(court_flipped_pnu.lot_address_raw, '') is not null
              or nullif(court_flipped_pnu.road_address_raw, '') is not null
              or nullif(court_flipped_pnu.print_addr_raw, '') is not null then 1
            else 0
        end as has_address_evidence,
        case
            when court_flipped_pnu.x_coordinate is not null
             and court_flipped_pnu.y_coordinate is not null then 1
            else 0
        end as has_coordinate_evidence,
        coalesce(exact_pnu.unit_count, 0) as exact_unit_count,
        coalesce(flipped_pnu.unit_count, 0) as flipped_unit_count
    from court_flipped_pnu
    left join building_pnu_rollup as exact_pnu
      on court_flipped_pnu.pnu = exact_pnu.pnu
    left join building_pnu_rollup as flipped_pnu
      on court_flipped_pnu.flipped_pnu = flipped_pnu.pnu
),

flipped_match_rollup as (
    select
        court_pnu_status.source_observation_id,
        count(*) as flipped_candidate_pair_count,
        sum(
            case
                when coalesce(court_pnu_status.dong_name, '') = coalesce(building_relevant.dong_name, '') then 1
                else 0
            end
        ) as flipped_dong_match_count,
        sum(
            case
                when court_pnu_status.unit_number is not null
                 and building_relevant.unit_number is not null
                 and court_pnu_status.unit_number = building_relevant.unit_number then 1
                else 0
            end
        ) as flipped_unit_number_match_count,
        sum(
            case
                when coalesce(court_pnu_status.dong_name, '') = coalesce(building_relevant.dong_name, '')
                 and court_pnu_status.unit_number is not null
                 and building_relevant.unit_number is not null
                 and court_pnu_status.unit_number = building_relevant.unit_number then 1
                else 0
            end
        ) as flipped_dong_unit_number_match_count
    from court_pnu_status
    join building_relevant
      on court_pnu_status.flipped_pnu = building_relevant.pnu
    where nullif(court_pnu_status.pnu, '') is not null
      and court_pnu_status.exact_unit_count = 0
      and court_pnu_status.flipped_unit_count > 0
    group by 1
)

select
    court_pnu_status.source_observation_id,
    court_pnu_status.source_record_id,
    court_pnu_status.pnu,
    court_pnu_status.flipped_pnu,
    court_pnu_status.dong_name,
    court_pnu_status.unit_label,
    court_pnu_status.unit_number,
    court_pnu_status.exclusive_area_sqm,
    court_pnu_status.lot_address_raw,
    court_pnu_status.road_address_raw,
    court_pnu_status.print_addr_raw,
    court_pnu_status.x_coordinate,
    court_pnu_status.y_coordinate,
    court_pnu_status.has_address_evidence,
    court_pnu_status.has_coordinate_evidence,
    court_pnu_status.exact_unit_count,
    court_pnu_status.flipped_unit_count,
    coalesce(flipped_match_rollup.flipped_candidate_pair_count, 0) as flipped_candidate_pair_count,
    coalesce(flipped_match_rollup.flipped_dong_match_count, 0) as flipped_dong_match_count,
    coalesce(flipped_match_rollup.flipped_unit_number_match_count, 0) as flipped_unit_number_match_count,
    coalesce(flipped_match_rollup.flipped_dong_unit_number_match_count, 0) as flipped_dong_unit_number_match_count
from court_pnu_status
left join flipped_match_rollup
  on court_pnu_status.source_observation_id = flipped_match_rollup.source_observation_id
