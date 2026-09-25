{{ config(materialized='table') }}

{#-
  세대 공시가격 이름을 셋째 증인으로 쓴 교차확증 (ADR-0106 과 같은 원리).
  가격공시는 아파트 위주라 등기보다 좁지만 표기가 달라, 등기가 못 잡은 호를
  추가로 확증한다 — 운영 silver 검산(2026-09-25): 유일 확증 19,603, 그중
  이름 티어 밖 12,096. 최신 기준일 한 판만 쓴다(연도별 중복 제거).
  가격 이름은 대장과 계보가 일부 겹칠 수 있어 등기(0.95)보다 한 단 낮은
  0.93/high 로 낸다. 모호 일치는 후보가 아니다(양방향 유일).
-#}

with building_unit as (
    select * from {{ ref('stg_foundation__building_register_unit') }}
),

unit_price as (
    select * from {{ ref('stg_foundation__unit_official_price') }}
),

unit_scope as (
    select
        building_unit_id,
        target_snapshot_id,
        pnu,
        {{ foundation_normalized_unit_name('coalesce(unit_designation, unit_name_raw)', 'dong_name') }} as normalized_name
    from building_unit
    where nullif(pnu, '') is not null
      and (
        normalization_status = 'proposal_required'
        or not {{ foundation_unit_name_is_string_clean("coalesce(unit_designation, '')") }}
      )
),

latest_base_date as (
    select max(base_date) as base_date from unit_price
),

price_names as (
    select
        unit_price.source_record_id,
        unit_price.source_system,
        unit_price.source_snapshot_id,
        unit_price.pnu,
        unit_price.lineage_run_id,
        {{ foundation_normalized_unit_name('unit_price.ho_name', 'unit_price.dong_name') }} as normalized_name
    from unit_price
    join latest_base_date on unit_price.base_date = latest_base_date.base_date
),

unit_name_counts as (
    select pnu, normalized_name, count(*) as unit_count
    from unit_scope where normalized_name is not null group by 1, 2
),

price_name_counts as (
    select pnu, normalized_name, count(*) as price_count
    from price_names where normalized_name is not null group by 1, 2
)

select
    price_names.source_record_id as source_observation_id,
    price_names.source_system,
    price_names.source_record_id,
    price_names.source_snapshot_id,
    unit_scope.building_unit_id as target_entity_id,
    unit_scope.target_snapshot_id,
    'building_unit' as target_entity_type,
    'deterministic' as match_method,
    'PNU_NORMALIZED_UNIT_NAME_UNIQUE' as match_path,
    0.93 as confidence_score,
    'high' as confidence_band,
    price_names.lineage_run_id
from unit_scope
join price_names
  on unit_scope.pnu = price_names.pnu
 and unit_scope.normalized_name = price_names.normalized_name
join unit_name_counts
  on unit_scope.pnu = unit_name_counts.pnu
 and unit_scope.normalized_name = unit_name_counts.normalized_name
 and unit_name_counts.unit_count = 1
join price_name_counts
  on price_names.pnu = price_name_counts.pnu
 and price_names.normalized_name = price_name_counts.normalized_name
 and price_name_counts.price_count = 1
