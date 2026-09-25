{{ config(materialized='table') }}

{#-
  등기 대지권 이름으로 대장 호 정규화를 교차확증한다 (ADR-0106).

  대상은 `proposal_required` 행 + 문자열 규칙(1~3차)으로 깨끗하지 않은 표기
  전부다. 옛 엔진은 지저분한 표기 대부분을 헐거운 추출로 '확정' 처리했기
  때문에(2026-09-25 실측: proposal_required 24,715행뿐, 유일 확증 1,219행)
  딱지만 보면 교차확증이 필요한 행을 놓친다.

  두 단계 후보 (모호 일치는 어느 단계에서도 후보가 아니다):
  1) 이름 유일: 같은 PNU 안에서 보수 정규형이 양방향 유일 — 0.95/high
     (운영 silver 검산 101,271행).
  2) 층 회수: 이름만으로 모호하지만 (PNU, 층, 정규형) 3중키가 양방향
     유일 — 서로 다른 두 등기 칸(이름·층)이 함께 맞아야 하므로 0.92/high
     (운영 silver 검산 +5,516행). 1단계에 이미 잡힌 호는 제외한다.
-#}

with building_unit as (
    select * from {{ ref('stg_foundation__building_register_unit') }}
),

land_right as (
    select * from {{ ref('stg_foundation__land_right_registration') }}
),

unit_scope as (
    select
        building_unit_id,
        target_snapshot_id,
        pnu,
        floor_index,
        {{ foundation_normalized_unit_name('coalesce(unit_designation, unit_name_raw)', 'dong_name') }} as normalized_name
    from building_unit
    where nullif(pnu, '') is not null
      and (
        normalization_status = 'proposal_required'
        or not {{ foundation_unit_name_is_string_clean("coalesce(unit_designation, '')") }}
      )
),

right_names as (
    select
        source_record_id,
        source_system,
        source_snapshot_id,
        pnu,
        floor_no,
        lineage_run_id,
        {{ foundation_normalized_unit_name('ho_name', 'dong_name') }} as normalized_name
    from land_right
),

unit_name_counts as (
    select
        pnu,
        normalized_name,
        count(*) as unit_count
    from unit_scope
    where normalized_name is not null
    group by 1, 2
),

right_name_counts as (
    select
        pnu,
        normalized_name,
        count(*) as right_count
    from right_names
    where normalized_name is not null
    group by 1, 2
),

name_unique_candidates as (
    select
        right_names.source_record_id as source_observation_id,
        right_names.source_system,
        right_names.source_record_id,
        right_names.source_snapshot_id,
        unit_scope.building_unit_id as target_entity_id,
        unit_scope.target_snapshot_id,
        'building_unit' as target_entity_type,
        'deterministic' as match_method,
        'PNU_NORMALIZED_UNIT_NAME_UNIQUE' as match_path,
        0.95 as confidence_score,
        'high' as confidence_band,
        right_names.lineage_run_id
    from unit_scope
    join right_names
      on unit_scope.pnu = right_names.pnu
     and unit_scope.normalized_name = right_names.normalized_name
    join unit_name_counts
      on unit_scope.pnu = unit_name_counts.pnu
     and unit_scope.normalized_name = unit_name_counts.normalized_name
     and unit_name_counts.unit_count = 1
    join right_name_counts
      on right_names.pnu = right_name_counts.pnu
     and right_names.normalized_name = right_name_counts.normalized_name
     and right_name_counts.right_count = 1
),

unit_triple_counts as (
    select
        pnu,
        floor_index,
        normalized_name,
        count(*) as unit_count
    from unit_scope
    where normalized_name is not null and floor_index is not null
    group by 1, 2, 3
),

right_triple_counts as (
    select
        pnu,
        floor_no,
        normalized_name,
        count(*) as right_count
    from right_names
    where normalized_name is not null and floor_no is not null
    group by 1, 2, 3
),

floor_recovered_candidates as (
    select
        right_names.source_record_id as source_observation_id,
        right_names.source_system,
        right_names.source_record_id,
        right_names.source_snapshot_id,
        unit_scope.building_unit_id as target_entity_id,
        unit_scope.target_snapshot_id,
        'building_unit' as target_entity_type,
        'deterministic' as match_method,
        'PNU_FLOOR_NORMALIZED_UNIT_NAME_UNIQUE' as match_path,
        0.92 as confidence_score,
        'high' as confidence_band,
        right_names.lineage_run_id
    from unit_scope
    join right_names
      on unit_scope.pnu = right_names.pnu
     and unit_scope.normalized_name = right_names.normalized_name
     and right_names.floor_no = unit_scope.floor_index
    join unit_triple_counts
      on unit_scope.pnu = unit_triple_counts.pnu
     and unit_scope.floor_index = unit_triple_counts.floor_index
     and unit_scope.normalized_name = unit_triple_counts.normalized_name
     and unit_triple_counts.unit_count = 1
    join right_triple_counts
      on right_names.pnu = right_triple_counts.pnu
     and right_names.floor_no = right_triple_counts.floor_no
     and right_names.normalized_name = right_triple_counts.normalized_name
     and right_triple_counts.right_count = 1
    where unit_scope.building_unit_id not in (
        select target_entity_id from name_unique_candidates
    )
)

select * from name_unique_candidates
union all
select * from floor_recovered_candidates
