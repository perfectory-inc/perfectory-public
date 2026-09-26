{{ config(materialized='table') }}

{#-
  경매 물건을 정규형 호명으로 대장 호에 꽂는다 (ADR-0107 정규형, ADR-0106 원리).

  기존 court_auction 후보 모델은 unit_number·면적으로 맞추는데, 마지막-숫자런
  unit_number 는 같은 건물 안에서 충돌하고(812,298행 실측) `4층404호`류 표기를
  놓친다. 이 모델은 양쪽을 같은 정규형(SSOT 매크로, 경매는 스테이징에서·대장은
  실버 엔진이 계산)으로 청소해, 같은 PNU 안에서 양방향 유일 일치할 때만 낸다.

  운영 실측(2026-09-26): pnu 맞는 경매 호단위 4,190 중 3,152(75.2%)가 유일 매칭.
-#}

with court_auction as (
    select * from {{ ref('stg_gongzzang__court_auction_observation') }}
),

building_unit as (
    select * from {{ ref('stg_foundation__building_register_unit') }}
),

auction_scope as (
    select
        source_observation_id,
        source_system,
        source_record_id,
        source_snapshot_id,
        pnu,
        unit_designation_normalized as normalized_name,
        lineage_run_id
    from court_auction
    where nullif(pnu, '') is not null
      and nullif(unit_designation_normalized, '') is not null
),

unit_scope as (
    select
        building_unit_id,
        target_snapshot_id,
        pnu,
        unit_designation_normalized as normalized_name
    from building_unit
    where nullif(pnu, '') is not null
      and nullif(unit_designation_normalized, '') is not null
),

unit_name_counts as (
    select pnu, normalized_name, count(*) as unit_count
    from unit_scope group by 1, 2
),

auction_name_counts as (
    select pnu, normalized_name, count(*) as auction_count
    from auction_scope group by 1, 2
)

select
    auction_scope.source_observation_id,
    auction_scope.source_system,
    auction_scope.source_record_id,
    auction_scope.source_snapshot_id,
    unit_scope.building_unit_id as target_entity_id,
    unit_scope.target_snapshot_id,
    'building_unit' as target_entity_type,
    'deterministic' as match_method,
    'PNU_NORMALIZED_UNIT_NAME_UNIQUE' as match_path,
    0.95 as confidence_score,
    'high' as confidence_band,
    auction_scope.lineage_run_id
from auction_scope
join unit_scope
  on unit_scope.pnu = auction_scope.pnu
 and unit_scope.normalized_name = auction_scope.normalized_name
join unit_name_counts
  on unit_name_counts.pnu = auction_scope.pnu
 and unit_name_counts.normalized_name = auction_scope.normalized_name
 and unit_name_counts.unit_count = 1
join auction_name_counts
  on auction_name_counts.pnu = auction_scope.pnu
 and auction_name_counts.normalized_name = auction_scope.normalized_name
 and auction_name_counts.auction_count = 1
