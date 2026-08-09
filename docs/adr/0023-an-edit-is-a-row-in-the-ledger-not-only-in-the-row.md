---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-10
---

# ADR 0023: 편집은 원장의 행이지, 고쳐진 행에만 남는 것이 아니다

- Status: Accepted
- Date: 2026-08-10
- 관련: [ADR-0006 기준 데이터는 객체 저장소 우선](./0006-object-storage-first-serving.md) (§투영 재구성), [ADR-0019 소속은 기간을 가진 사실이다](./0019-membership-is-a-dated-fact-not-a-column.md), [ADR-0022 "현재"는 오늘이다](./0022-current-means-today-and-one-view-says-so.md)
- 마이그레이션: `20260810000003_parcel_edits_enter_the_ledger.sql`

## Context

[ADR-0006](./0006-object-storage-first-serving.md)은 Postgres를 정본으로 두지 않는다고 결정했다.
필지·건물 같은 기준 데이터는 R2에서 제공하고, PostGIS는 **방금 승인된 편집을 즉시 보이기 위한 warm
투영**이다. 그 결정문은 재구성 방법까지 적었다.

> PostGIS는 warm 상태의 완전한 projection으로 유지하지만, 이는 선택된 R2/Iceberg Silver SCD2
> snapshot과 **감사된 편집 ledger**에서 재구성한 serving projection이다.

`FP-ADR-0006`도 같은 문장을 쓴다 — mirror가 깨지면 스냅샷과 편집 원장에서 되살린다.

**그 원장은 존재한다.** `catalog.normalization_application`이 정확히 그 모양이다.

| 컬럼 | 담는 것 |
|---|---|
| `before_snapshot` / `after_snapshot` | 무엇이 어떻게 바뀌었나 |
| `applied_by_principal_id`, `applied_at` | 누가, 언제 |
| `rollback_of` | 되돌리기는 **행을 지우지 않고 새 행**으로 |
| `expected_version` | 낙관적 동시성 |

그리고 재생 메커니즘도 있다. `building_register_unit/active_overrides.sql`은 재귀 CTE로 편집
체인을 걸어가며 롤백된 것을 걸러내고, 체인이 선형인지(뿌리 하나, 전부 도달 가능, 분기 없음)까지
검증한 뒤 현재 유효한 override를 돌려준다. **투영 = 스냅샷 + 원장은 이미 한 종류에 구현되어 있다.**

### 필지는 그 원장에 들어가지 않는다

두 가지가 막는다.

```sql
CONSTRAINT normalization_application_target_kind_check
    CHECK (target_kind IN ('industrial_complex', 'building_register_floor', 'building_register_unit'))
```

`parcel`이 없다. 그리고 `proposal_id uuid NOT NULL` — **모든 편집이 LLM 제안을 거쳐야 한다.**

그런데 저장소에 실재하는 유일한 필지 편집은 제안을 거치지 않는다.
`PATCH /catalog/v1/parcels/{id}/kind`는 직원이 직접 하는 조작이며, `update_parcel_kind`는 이렇게
동작한다 — 행을 `FOR UPDATE`로 잠그고, 버전을 대조하고, `UPDATE`하고, outbox 이벤트를 넣고, 끝난다.

**원장에는 아무것도 남지 않는다.** 이전 값이 남는 유일한 곳은 outbox 이벤트인데, 그것은 소비자에게
알리는 **전달 통로**이지 원장이 아니다. 발행되고 나면 지워지도록 만들어진 자리다.

그래서 지금 `catalog.parcel`은 ADR-0006이 말한 투영이 아니라 **정본처럼 동작한다.** 그 행을 잃으면
편집도 함께 잃는다.

## Decision

### 1. 필지 편집은 원장에 행을 남긴다

`update_parcel_kind`가 필지 행을 고치는 **같은 트랜잭션에서** 원장 행을 넣는다. 둘 중 하나만
성공하는 상태가 없어야 하므로 커밋은 하나다.

`before_snapshot`과 `after_snapshot`은 편집된 필드와 그 필드를 식별하는 것만 담는다 — `parcel_id`,
`kind`, `version`. 행 전체를 담지 않는 이유는 그것이 스냅샷의 역할이 아니기 때문이다. 원장은
**무엇이 바뀌었는지**를 적고, 바뀌지 않은 값의 정본은 R2 스냅샷이다.

라우트는 누가 편집하는지를 **이미 받고 있었고 버리고 있었다** — `Extension(_principal)`.
`applied_by_principal_id`가 필수인 이유가 그것이다. 책임질 사람이 없는 편집은 원장이 배제하려는 바로
그것이다.

### 2. 원장은 `catalog.catalog_edit`이며, `normalization_application`을 재사용하지 않는다

`catalog.normalization_application`은 정확히 필요한 모양이다 — before/after, 적용 주체,
`rollback_of`. **그럼에도 쓰지 않는다.**

이 ADR의 초안은 그것을 재사용하기로 했고, `proposal_id`를 nullable로 만들어 그 유무를 출처의
구분으로 삼으려 했다. 저장소가 그 결정을 거부했다.

```
FAILED  normalization_tables_are_owned_only_by_normalization_infrastructure
```

정규화는 카탈로그에서 **의도적으로 분리된** 컨텍스트다. `package_boundary.rs`는 `normalization_*`
표를 `foundation-normalization-infrastructure`만 건드릴 수 있다고 단언하고,
`moved_normalization_ownership_cannot_return_to_catalog`는 오직 그 소유권이 카탈로그로 되돌아오는
것을 막기 위해 존재한다. 카탈로그 크레이트는 정규화에 의존하지 않으며, 이 결정은 그 의존을 만들지
않는다.

초안이 틀린 지점은 재사용 자체가 아니라 **결정하기 전에 경계를 확인하지 않은 것**이다. 표의 모양이
맞는다는 것과 그 표를 쓸 자격이 있다는 것은 다른 문제다.

새 표는 `normalization_application`의 형태를 따르되 컨텍스트 안에 둔다. 되돌리기는 새 행이고,
append-only 트리거가 붙고, `rollback_of`에 UNIQUE를 걸어 한 편집이 두 번 취소되지 않게 한다 —
두 번 취소되면 "이 편집이 유효한가"에 답이 둘이 된다.

그리고 CHECK 하나가 경계를 스키마에도 적는다: **이 표에는 `*.normalization.*` 명령이 들어올 수
없다.** 경계를 철자로 넘는 일을 데이터베이스가 거부한다.

### 3. 재생(replay)은 이 증분에 넣지 않는다

`active_overrides.sql`에 해당하는 필지용 질의는 만들지 않는다.

이유는 **아직 재생할 것이 없기 때문이다.** 필지 편집은 `kind` 하나뿐이고, 그 값은 R2 스냅샷이 아니라
Postgres 행에서만 왔다. 재생이 의미를 갖는 것은 "스냅샷에 있는 값을 편집이 덮는" 구조가 생긴
뒤이며, 그것은 필지 지오메트리 편집 경로가 생기는 시점이다. 그 경로는 지금 없다(§남은 부채 1).

이 증분이 하는 일은 **그때 재생할 원본을 지금부터 남기는 것**이다. 원장이 비어 있으면 나중에
재생기를 만들어도 되살릴 과거가 없다.

## 기각한 대안

### `normalization_application`을 넓혀 재사용한다

이 ADR의 초안이었다. 표 하나를 아끼고, "지금 유효한 편집이 무엇인가"에 한 곳이 답한다.

기각한 이유는 §Decision 2다 — 저장소가 그 경계를 테스트로 지키고 있고, 그 테스트는 정규화가
카탈로그에서 **나간** 뒤 돌아오지 못하게 하려고 만들어졌다. 두 원장을 합치는 비용은 실재하지만, 그
비용은 투영 재구성기를 쓰는 사람이 **한 번** 지불한다. 경계를 무르는 비용은 그 뒤 모든 변경이
지불한다.

### 카탈로그가 정규화 포트를 호출해 기록하게 한다

경계를 지키면서 원장 하나를 유지하는 안. 카탈로그가 정규화 크레이트에 의존하고, 편집을 기록해 달라고
부른다.

기각한 이유는 의존 방향이다. 정규화는 LLM 파이프라인이고, 직원이 필지 종류를 바꾸는 일은 그것과
무관하다. 무관한 것에 의존을 만들면 카탈로그를 빌드하는 데 정규화 도메인이 필요해지고, 그 방향은
`package_boundary.rs`가 막고 있는 것과 같은 종류의 결합이다.

### 직원 편집에 합성 제안(synthetic proposal)을 만들어 준다

`proposal_id NOT NULL`을 유지한 채, 직접 편집마다 제안 행을 하나 지어내는 안. 스키마를 안 건드린다.

기각한 이유는 그 제안이 **거짓이기 때문이다.** `normalization_proposal`은 `confidence`, `evidence`,
`model_id`, `policy_id`, `trace_id`를 요구한다. 직원이 종류를 바꾼 사건에는 그런 것이 없고, 채워
넣으면 모델이 제안하지 않은 것을 제안했다고 적는 셈이다. 이 저장소는 없는 출처를 지어내지 않는다 —
`20260727000001`의 `legacy:` 소스 레코드가 지어낸 값이 아니라 **명시적으로 마이그레이션이 발급한
것임을 밝히는** 형태인 이유와 같다.

### outbox 이벤트를 원장으로 삼는다

`ParcelKindChanged`가 이미 이전·이후 종류를 담고 있으니 그것을 읽으면 된다는 안. 새 쓰기가 없다.

기각한 이유는 outbox가 **전달 통로**이기 때문이다. 발행된 이벤트는 지워지도록 설계된 자리이고,
보존 기간·재발행·소비자 재생은 원장의 계약이 아니다. 사실을 옮기는 것과 사실을 보관하는 것은 다른
일이며, 하나가 둘을 겸하면 큐를 비우는 운영 작업이 역사를 지우게 된다.

## Consequences

- **필지 편집이 되돌릴 수 있게 된다** — 정확히는, 되돌리는 데 필요한 기록이 남기 시작한다. 되돌리는
  코드는 이 증분에 없다(§Decision 3).
- **`catalog.parcel`이 투영에 한 걸음 가까워진다.** 그 행을 잃어도 편집이 원장에 남는다. 다만
  `kind`의 원본이 R2에 없으므로 아직 완전한 재구성은 불가능하다(§남은 부채 2).
- **편집 원장이 둘이 된다.** 컨텍스트별로 하나씩이며, 투영 재구성기는 둘 다 읽어야 한다. 그 합치는
  일을 누가 어떻게 하는지는 재구성기가 생기는 증분이 정한다(§남은 부채 3).
- **G1 지표의 분모가 하나 는다.** 새 표에는 생산자가 있으므로(같은 증분의 `update_parcel_kind`)
  생산자 없는 표 수는 그대로다.

## 남은 부채

1. **필지 지오메트리 편집 경로가 없다.** 편집 가능한 것은 `kind`뿐이다. ADR-0006이 말한 "승인된
   편집이 있는 단위"는 지오메트리를 뜻하는데, 그것을 바꾸는 명령이 저장소에 없다. 재생기(§Decision 3)는
   그 경로와 함께 온다.
2. **`kind`의 원본이 R2에 없다.** 완전한 재구성은 "스냅샷의 값 + 편집"인데 `parcel.kind`는
   `silver.parcel_boundaries` 계약에 없다. 그 값이 어디서 오는지가 정해지기 전까지 이 원장은 편집만
   보존하고 기준값은 보존하지 못한다.
3. **편집 원장이 둘이고 합치는 코드가 없다.** `catalog.catalog_edit`과
   `catalog.normalization_application`이 각자의 컨텍스트에서 "적용된 편집"을 적는다. ADR-0006이
   말한 재구성은 둘 다 읽어야 하고, 그 재구성기는 아직 없다. 만드는 사람이 합치는 방법을 정한다 —
   읽는 쪽에서 합칠지, 컨텍스트 위의 뷰를 둘지, 아니면 둘을 하나로 옮길지.
4. **되돌리기 코드가 없다.** 표에 `rollback_of`가 있고 UNIQUE로 한 번만 취소되도록 막혀 있지만,
   그 행을 쓰는 명령은 없다. 필지 지오메트리 편집(§남은 부채 1)과 같은 증분에 온다.
5. **`catalog_edit.target_kind`는 `parcel` 하나뿐이다.** 산업단지·건물 편집도 같은 성격이지만 이
   증분은 실재하는 편집 경로 하나만 덮는다. 값을 미리 넣으면 writer 없는 어휘가 된다.
