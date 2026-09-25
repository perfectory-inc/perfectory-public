-- 골든 벡터 교차 시험 (ADR-0107 결정 2): dbt 매크로가 벡터의 정답과 다르면
-- 행이 나오고 시험이 실패한다. 러스트 쪽은 같은 seed 를
-- foundation-normalization-domain 의 단위시험이 채점한다 — 한쪽만 고치면
-- 반대쪽이 깨진다.
select
    raw_name,
    dong_name,
    expected,
    {{ foundation_normalized_unit_name("coalesce(raw_name, '')", "coalesce(dong_name, '')") }} as actual
from {{ ref('unit_name_normalization_vectors') }}
where {{ foundation_normalized_unit_name("coalesce(raw_name, '')", "coalesce(dong_name, '')") }}
      is distinct from nullif(expected, '')
