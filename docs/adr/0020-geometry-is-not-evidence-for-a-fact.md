---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-09
---

# ADR 0020: 도형은 사실의 근거가 아니다

- Status: Accepted
- Date: 2026-08-09
- 관련: [ADR-0019 소속은 한쪽의 컬럼이 아니라 기간을 가진 사실이다](./0019-membership-is-a-dated-fact-not-a-column.md) (§Decision 1의 어휘를 이 ADR이 대체한다), [ADR-0018 두 언어가 같은 어휘를 적으면 대조한다](./0018-a-vocabulary-written-in-two-languages-is-compared.md)
- 마이그레이션: `20260809000001_parcel_complex_membership.sql` (ADR-0019 1단계와 같은 파일. 아래 §이행 참조)

## Context

[ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md)는 소속을 기간을 가진 사실로 옮기기로
했고 그 판단은 유효하다. 그러나 §Decision 1이 정한 **어휘**는 틀렸다.

```sql
CHECK (membership_kind IN ('inside', 'intersects', 'candidate', 'excluded'))
CHECK (source_method  IN ('official_list', 'geometry_overlay', 'manual_review'))
overlap_ratio numeric(9,6)
```

`inside`와 `intersects`는 **도형에 대한 주장**이다. 필지 다각형이 산단 경계 다각형 안에
들어가는가, 걸치는가. `geometry_overlay`는 그 판정을 우리가 계산해서 얻었다는 뜻이고,
`overlap_ratio`는 몇 퍼센트 겹쳤는지다. 세 값이 모두 같은 전제 위에 있다 — **도형을 겹쳐 보면
소속을 알 수 있다.**

알 수 없다. 출처마다 다각형의 정확도·기준시점·좌표계·일반화 수준이 다르고, 경계선 근처의 필지는
어느 쪽 파일을 쓰느냐에 따라 안에 있기도 하고 밖에 있기도 하다. 그렇게 얻은 판정이 값 하나로
저장되면 **틀렸다는 사실조차 남지 않는다.** 산업단지 경계만의 문제가 아니라 모든 다각형이 그렇다.

소속은 자료가 말한다. 정부가 목록에 `(산단코드, PNU)`를 적어 두면 그것이 근거이고, 우리가 도형을
겹쳐 계산한 결과는 근거가 아니다. 그 자료는 이미 카탈로그에 등록되어 있다 —
`public-source-endpoint-catalog.v1.json`의 VWorld `sandan_parcel`이
`identity_policy.canonical_keys`로 `industrial_complex_code`와 `pnu`를 선언한다. 아직 수집기가
없을 뿐, **어디서 오는지는 이미 적혀 있다.**

### 어떻게 여기까지 왔는가

ADR-0019 §Decision 1은 이 어휘를 **발명하지 않았다는 것을 근거로** 채택했다 — "어휘 네 값·세 값은
발명하지 않았다. `industrial-complex-lakehouse-poc.md:217-218`이 이미 소유하고 있고, 여기서는 같은
값을 Postgres 쪽에 적는 것뿐이다."

SSOT를 지키려는 판단이었고, 그래서 더 위험했다. **어휘를 베끼면 그 어휘가 담은 가정까지 들어온다.**
레이크하우스 계약이 도형 기반 판정을 전제하고 만들어졌다는 사실은 값의 목록만 봐서는 보이지 않았고,
"이미 있는 것을 쓴다"는 규칙이 그것을 검토 없이 통과시켰다. 같은 목록을 두 곳에 적는 것이 결함인
것과 별개로, **틀린 목록을 옮겨 적는 것도 결함이다.**

## Decision

### 1. 도형은 사실 판정의 입력이 아니다

다각형 포함·교차 관계로 소속·귀속·소재를 판정하지 않는다. 산업단지에 한정된 규칙이 아니라 이
저장소의 모든 다각형에 적용된다. 도형은 표시·시각화·면적 산출에 쓰고, **어느 실체가 어느 실체에
속하는가는 그렇게 진술한 자료가 있을 때만 기록한다.**

도형이 자료와 어긋나는 것을 발견하는 일은 가치가 있다. 그것은 **품질 검사**이지 소속의 근거가
아니며, 검사 결과를 소속 사실로 승격시키지 않는다.

### 2. 소속 표는 어휘를 잃고 단순해진다

ADR-0019 §Decision 1의 표에서 `membership_kind`, `source_method`, `overlap_ratio` 세 컬럼을
제거하고 `asserted_by` 하나를 둔다.

```sql
CREATE TABLE catalog.parcel_complex_membership (
    id uuid NOT NULL,
    parcel_id uuid NOT NULL,
    complex_id uuid NOT NULL,
    asserted_by text NOT NULL,
    effective_period daterange NOT NULL,
    data_revision uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT parcel_complex_membership_pkey PRIMARY KEY (id),
    CONSTRAINT parcel_complex_membership_asserted_by_check
        CHECK (asserted_by IN ('official_list', 'manual_review')),
    CONSTRAINT parcel_complex_membership_period_check
        CHECK (NOT isempty(effective_period) AND lower_inc(effective_period)),
    CONSTRAINT parcel_complex_membership_one_complex_excl EXCLUDE USING gist
        (parcel_id WITH =, effective_period WITH &&)
);
```

`membership_kind`가 사라지는 이유는 값이 줄어서가 아니라 **물을 것이 없어져서다.** 도형 판정을
하지 않으면 남는 종류는 하나뿐이고, 값이 하나인 컬럼은 아무것도 구분하지 않는다. **행이 존재한다는
것 자체가 주장이다.**

`asserted_by`가 남는 이유는 `official_list`와 `manual_review`가 같은 행에 대한 **서로 다른 종류의
주장**이기 때문이다. 정부 파일이 그렇게 적었다는 것과 사람이 보고 그렇게 판단했다는 것은 신뢰
근거가 다르고, 나중에 둘이 어긋날 때 어느 쪽을 남길지 결정하려면 구분이 필요하다.
`source_record_id`는 어느 레코드에서 왔는지를 가리킬 뿐 **누가 주장했는지**를 말하지 않는다.

### 3. EXCLUDE는 하나로 합쳐지고 조건이 사라진다

ADR-0019는 두 개를 뒀다 — 한 쌍·한 종류당 하나(`_pair_excl`), 그리고 `inside`에만 걸리는
"담는 산단은 최대 하나"(`_one_container_excl`). 종류가 사라지면 둘은 같은 명제가 된다.

```sql
EXCLUDE USING gist (parcel_id WITH =, effective_period WITH &&)
```

**한 필지는 한 시점에 최대 하나의 산업단지에 속한다.**

이것이 참인지는 실물 자료로 확인하지 못했다. `sandan_parcel`은 아직 수집되지 않으므로, 정부 목록이
한 PNU를 두 산단코드 아래 적는 경우가 있는지 알 수 없다. **모르는 채로 강제하기로 한다.** 이유는
두 방향의 비용이 대칭이 아니기 때문이다 — 제약이 있으면 위반하는 자료가 들어올 때 이름 붙은 오류로
**시끄럽게** 멈추고, 그때 제약을 푸는 것은 마이그레이션 한 줄이다. 제약이 없으면 한 필지가 두 산단
아래 조용히 쌓이고, 그 결과는 산단별 집계가 이중으로 세어지는 형태로 한참 뒤에 나타나며, 그때는
어느 행이 옳은지 판정할 근거가 남아 있지 않다.

### 4. 소속은 덮어쓰지 않고 쌓는다 — 그리고 이것은 이 표만의 규칙이 아니다

사실이 바뀌면 `effective_period`의 상한을 닫고 새 행을 넣는다. UPDATE로 값을 갈아 끼우지 않는다.
`parcel_complex_membership_append_only` 트리거가 발행 경로 밖의 UPDATE·DELETE를 42501로 거부한다.

이 방향은 소속에 한정되지 않는다. 이 저장소의 사실 표는 모두 이 형태로 가야 하며, 형판은
`20260727000001_administrative_boundary_identity.sql`에 이미 있다. 다만 기존의 덮어쓰기형 표
(`updated_at`·`version`만 가진 `catalog.parcel` 등)를 전환하는 것은 한 증분이 아니라 **프로그램**이며,
이 ADR은 그 방향만 기록하고 범위에 넣지 않는다.

## 기각한 대안

### `geometry_overlay`를 남기되 쓰지 않기로 한다

어휘는 그대로 두고 운영 규율로 그 값을 쓰지 않는 안. 가장 작고, 레이크하우스 계약과의 대조도
유지된다.

기각한 이유는 이 저장소가 이미 여러 번 지불한 비용이다 — **스키마가 허용하는 것은 언젠가 쓰인다.**
`administrative_boundary_revision.status`의 `published`는 writer가 없는 채로 남아 있다가
[ADR-0017](./0017-a-data-revision-belongs-to-the-unit-it-revises.md)에서 접혔고, 그 전까지 다른
곳이 소유한 사실을 두 번째로 진술하고 있었다. 값을 남겨 두면 규율은 문서에만 있고 제약에는 없다.
CHECK가 거부하면 규율이 필요 없다.

### 소속 표를 레이크하우스 계약에 맞춰 두고 그쪽을 먼저 고친다

정본이 레이크하우스이니 그쪽 어휘부터 고치고 Postgres는 따라가자는 안. 방향은 맞다.

기각한 이유는 순서다. `silver.complex_parcel_memberships`에는 **생산자도 소비자도 없다** — Rust가
Iceberg 행을 읽지 못하고(워크스페이스에 클라이언트가 없다), 그 표를 쓰는 코드도 없다. 지금 그
계약을 고치는 것은 아무도 지키지 않는 문서를 고치는 것이고, 그동안 Postgres에는 틀린 어휘가 실물로
남는다. 실물을 먼저 바로잡고, 레이크하우스 계약 정정은 그 표에 생산자가 생기는 증분이 함께 한다.

### 도형 판정을 `candidate`로 기록해 두고 사람이 승격시킨다

도형 겹침을 후보로 남기고 사람이 확인하면 소속으로 올리는 안. 도형을 근거로 삼지 않으면서 활용은
한다는 점에서 매력적이다.

기각한 이유는 사용자가 후보 자체를 원하지 않는다고 명시했기 때문이고, 설계상으로도 후보 행은 **소속
표에 있을 이유가 없다.** 그것은 아직 소속이 아니며, 소속 표에 두면 모든 읽기가 "후보는 빼고"라는
조건을 반복해서 달아야 한다. 도형과 자료의 불일치는 품질 보고서의 산출물이지 소속 원장의 행이 아니다.

## Consequences

- **ADR-0019 §Decision 1의 어휘가 대체된다.** 나머지 결정 — 소속이 기간을 가진 사실이라는 것,
  `parcel.complex_id`를 제거한다는 것, 건물↔필지와 필지 전이의 모양, 착수 순서 — 는 그대로 유효하다.
- **소속 표를 읽는 모든 질의가 단순해진다.** 종류를 거를 필요가 없으므로 2단계의 읽기 이전은
  `EXISTS (… WHERE parcel_id = … AND effective_period @> CURRENT_DATE)` 한 형태다. 조사가 지목한
  "같은 쌍에 여러 종류가 있으면 단순 JOIN이 중복 카운트를 만든다"는 위험은 **표현 불가가 되어**
  사라진다.
- **`overlap_ratio`가 사라지면서 면적 근거도 사라진다.** 겹친 면적이 필요해지면 그것은 소속 사실이
  아니라 별도의 계산 결과이며, 별도의 자리를 갖는다.
- **Rust 어휘가 하나 줄고 하나는 두 값이 된다.** `ParcelComplexMembershipKind`는 삭제되고,
  `MembershipSourceMethod`는 `MembershipAssertedBy`(두 값)가 된다.
  [ADR-0018](./0018-a-vocabulary-written-in-two-languages-is-compared.md)의 대조 테스트 등록도
  그에 맞춰 하나가 된다.
- **G1 지표는 ADR-0019가 적은 그대로다.** 표 개수가 변하지 않으므로 생산자 없는 표 21개와 분모
  59는 유지된다.

## 이행

`20260809000001_parcel_complex_membership.sql`은 **다시 쓴다.** 뒤에 정정 마이그레이션을 덧붙이지
않는다. 이 브랜치는 한 번도 푸시된 적이 없고 그 표에는 어떤 데이터베이스에도 행이 없으므로,
append-only 마이그레이션 규칙이 보호하려는 대상 — 이미 적용된 스키마 — 이 존재하지 않는다. 틀린
표를 만들고 바로 고치는 두 파일을 영구히 남기는 것은 이 저장소를 읽는 사람에게 아무것도 주지 않는다.

되돌린 커밋은 `backup/membership-geometry-vocabulary` 태그로 남긴다.

## 남은 부채

1. **`sandan_parcel` 수집기가 없다.** 소속의 유일한 정당한 생산자이며, 그것이 생기기 전까지 이
   표를 채우는 것은 1단계 백필뿐이다. 백필은 `parcel.complex_id`가 이미 주장하던 것을 옮겨 적을 뿐
   새 근거를 만들지 않는다.
2. **"한 필지 한 산단"을 실물로 확인하지 못했다.** §Decision 3. 자료가 이 제약을 위반하면
   그것이 첫 실측이며, 그때 제약을 푸는 판단을 별도로 한다.
3. **레이크하우스 계약은 아직 도형 어휘를 갖고 있다.** `silver.complex_parcel_memberships`의
   `membership_kind`·`source_method`·`overlap_ratio`는 그대로다. 생산자가 생기는 증분이 함께
   정정한다. 그때까지 두 층의 어휘는 **의도적으로** 다르며, ADR-0018의 대조 테스트는 Postgres와
   Rust만 비교하므로 이 차이를 빨갛게 만들지 않는다.
4. **덮어쓰기형 표의 전환이 남아 있다.** §Decision 4. 범위와 순서를 정하는 별도 ADR이 필요하다.
