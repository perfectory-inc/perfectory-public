{{ config(materialized='table') }}

{#-
  세대 공시가격을 증인으로 쓴 교차확증 (ADR-0106 과 같은 원리) — 열쇠 기반.

  가격공시 원천은 대장 관리번호(mgm_bldrgst_pk)를 자체 보유하므로 이름 대조
  없이 열쇠로 연결된다. 2026-09-25 운영 검산: 대상 호 중 열쇠 조인 24,736,
  99.8%가 단일 표기; 이름 대조 검증에서는 이름 일치 19,389건 중 99.45%가
  열쇠와 일치해 정규형 이름 대조 방식 자체가 열쇠로 입증됐다.

  두 밴드:
  - MGM_KEY_AND_NORMALIZED_NAME: 열쇠 + 정규형 이름까지 일치 — 0.97/high.
  - MGM_KEY_ONLY: 열쇠는 맞는데 표기가 다름 — 가격공시가 더 깨끗하게 적어둔
    사전 후보. 표기 불일치는 사람이 봐야 하므로 0.85/medium(needs_review).
  이름은 맞는데 열쇠가 다른 쌍은 어느 밴드에도 들어가지 않는다(운영 106건).
  최신 기준일 한 판만 쓰고, 열쇠·pnu 동시 일치 + 유닛당 단일 표기만 낸다.
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
        mgm_bldrgst_pk,
        pnu,
        {{ foundation_normalized_unit_name('coalesce(unit_designation, unit_name_raw)', 'dong_name') }} as normalized_name
    from building_unit
    where nullif(pnu, '') is not null
      and nullif(mgm_bldrgst_pk, '') is not null
      and (
        normalization_status = 'proposal_required'
        or not {{ foundation_unit_name_is_string_clean("coalesce(unit_designation, '')") }}
      )
),

latest_base_date as (
    select max(base_date) as base_date from unit_price
),

price_latest as (
    select
        unit_price.source_record_id,
        unit_price.source_system,
        unit_price.source_snapshot_id,
        unit_price.pnu,
        unit_price.mgm_bldrgst_pk,
        unit_price.lineage_run_id,
        {{ foundation_normalized_unit_name('unit_price.ho_name', 'unit_price.dong_name') }} as normalized_name
    from unit_price
    join latest_base_date on unit_price.base_date = latest_base_date.base_date
    where nullif(unit_price.mgm_bldrgst_pk, '') is not null
),

key_rollup as (
    select
        mgm_bldrgst_pk,
        pnu,
        count(distinct coalesce(normalized_name, '<null>')) as distinct_names
    from price_latest
    group by 1, 2
),

key_pairs as (
    select
        unit_scope.building_unit_id,
        unit_scope.target_snapshot_id,
        unit_scope.normalized_name as unit_name,
        price_latest.source_record_id,
        price_latest.source_system,
        price_latest.source_snapshot_id,
        price_latest.normalized_name as price_name,
        price_latest.lineage_run_id,
        row_number() over (
            partition by unit_scope.building_unit_id
            order by price_latest.source_record_id
        ) as pair_rank
    from unit_scope
    join price_latest
      on price_latest.mgm_bldrgst_pk = unit_scope.mgm_bldrgst_pk
     and price_latest.pnu = unit_scope.pnu
    join key_rollup
      on key_rollup.mgm_bldrgst_pk = unit_scope.mgm_bldrgst_pk
     and key_rollup.pnu = unit_scope.pnu
     and key_rollup.distinct_names = 1
)

select
    source_record_id as source_observation_id,
    source_system,
    source_record_id,
    source_snapshot_id,
    building_unit_id as target_entity_id,
    target_snapshot_id,
    'building_unit' as target_entity_type,
    'deterministic' as match_method,
    case
        when unit_name is not null and unit_name = price_name
            then 'MGM_KEY_AND_NORMALIZED_NAME'
        else 'MGM_KEY_ONLY'
    end as match_path,
    case
        when unit_name is not null and unit_name = price_name then 0.97
        else 0.85
    end as confidence_score,
    case
        when unit_name is not null and unit_name = price_name then 'high'
        else 'medium'
    end as confidence_band,
    lineage_run_id
from key_pairs
where pair_rank = 1
