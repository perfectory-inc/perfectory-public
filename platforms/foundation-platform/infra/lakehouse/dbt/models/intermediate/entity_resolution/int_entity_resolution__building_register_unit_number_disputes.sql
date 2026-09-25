{{ config(materialized='table', tags=['full_quality']) }}

{#-
  호번호 분쟁 작업목록 (ADR-0106 의 채점 결과물).

  엔진이 헐거운 마지막-숫자런 추출로 '확정'해 둔 unit_number 를, 같은 필지에서
  정규형 이름이 양방향 유일하게 일치하는 등기 대지권의 숫자 표기로 채점한다.
  두 번호가 다르면 어느 한쪽이 틀린 것이고, 대부분 엔진의 오추출이다
  (2026-09-25 운영 검산: 채점 가능 10,099 중 어긋남 556, 5.5%).

  이 모델은 후보를 내지 않는다 — 검토자가 볼 분쟁 명단이다. 행이 줄어드는
  것이 엔진 추출 규칙 개선의 성과 지표다.
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
        pnu,
        unit_number,
        unit_name_raw,
        {{ foundation_normalized_unit_name('coalesce(unit_designation, unit_name_raw)', 'dong_name') }} as normalized_name
    from building_unit
    where nullif(pnu, '') is not null
      and unit_number is not null
      and not {{ foundation_unit_name_is_string_clean("coalesce(unit_designation, '')") }}
),

right_names as (
    select
        source_record_id,
        pnu,
        ho_name,
        {{ foundation_normalized_unit_name('ho_name', 'dong_name') }} as normalized_name
    from land_right
),

unit_name_counts as (
    select pnu, normalized_name, count(*) as unit_count
    from unit_scope where normalized_name is not null group by 1, 2
),

right_name_counts as (
    select pnu, normalized_name, count(*) as right_count
    from right_names where normalized_name is not null group by 1, 2
)

select distinct
    unit_scope.building_unit_id,
    unit_scope.pnu,
    unit_scope.unit_name_raw,
    unit_scope.unit_number as engine_unit_number,
    try_cast(right_names.normalized_name as integer) as witness_unit_number,
    right_names.ho_name as witness_ho_name,
    right_names.source_record_id as witness_source_record_id
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
where regexp_like(right_names.normalized_name, '^[0-9]{1,5}$')
  and try_cast(right_names.normalized_name as integer) <> unit_scope.unit_number
