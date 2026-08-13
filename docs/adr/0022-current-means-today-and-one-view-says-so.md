---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-10
---

# ADR 0022: "현재"는 오늘이고, 그것을 말하는 뷰는 하나다

- Status: Accepted
- Date: 2026-08-10
- 관련: [ADR-0019 소속은 한쪽의 컬럼이 아니라 기간을 가진 사실이다](./0019-membership-is-a-dated-fact-not-a-column.md) (§이행 순서 2), [ADR-0021 아무도 읽지 않는 표면은 옮기지 않고 지운다](./0021-an-unread-surface-is-deleted-not-migrated.md) (남은 부채 1을 이 ADR이 닫는다)
- 마이그레이션: `20260810000001_parcel_current_complex.sql`

## Context

[ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md) 2단계는 산단 스코프 읽기를 소속
표로 옮기는 일이고, [ADR-0021](./0021-an-unread-surface-is-deleted-not-migrated.md)이 대상을 넷에서
하나로 줄였다 — `/catalog/v1/complexes/{id}/anchor-summary`. 그 ADR의 남은 부채 1은 옮길 때 "현재"의
기준일 계약을 정해야 한다고 적었고, 정하지 않았다.

소속이 기간을 갖게 되면 "이 산단의 필지"는 시점 없이는 답할 수 없는 질문이 된다. 답하는 방법은 둘이다 —
서버가 오늘로 고정하거나, 부르는 쪽이 날짜를 준다.

### 이미 한 번 정해져 있었다

`20260727000001`이 만든 `catalog.parcel_current_identifier`는 필지의 현재 PNU를 이렇게 고른다.

```sql
AND pi.effective_period @> CURRENT_DATE
```

저장소 전체에서 `CURRENT_DATE`를 쓰는 곳은 여기 하나뿐이다. **PNU 조회는 이미 오늘 기준이고,
그 결정은 어느 문서에도 적혀 있지 않다.** 소속을 다르게 정하면 같은 필지에 대해 두 가지 시간 규칙이
생긴다.

### 지금은 이력이 없다

1단계 백필은 필지마다 상한이 열린 구간 하나를 만든다. 소속이 실제로 바뀌려면
[ADR-0020](./0020-geometry-is-not-evidence-for-a-fact.md) 남은 부채 1이 말하는 `sandan_parcel`
수집기가 있어야 하고, 그것은 없다. 따라서 **지금 날짜 파라미터를 만들면 어떤 날짜를 물어도 같은
답이 나온다.**

## Decision

### 1. "현재"는 `CURRENT_DATE`이고, 파라미터는 두지 않는다

`anchor-summary`는 오늘 유효한 소속만 센다. `as_of` 같은 입력을 받지 않는다.

나중에 선택 파라미터로 추가하는 것은 하위 호환이며, **그것을 가능하게 하는 것은 API의 모양이 아니라
저장 방식이다.** 소속 표는 append-only이므로 API가 오늘만 보여주는 동안에도 이력은 쌓인다. 파라미터가
필요해지는 시점에 꺼낼 것이 이미 있다.

### 2. "오늘 유효한 소속"의 정의는 뷰 하나가 소유한다

```sql
CREATE VIEW catalog.parcel_current_complex AS
SELECT parcel_id, complex_id, asserted_by, effective_period, data_revision
  FROM catalog.parcel_complex_membership
 WHERE effective_period @> CURRENT_DATE;
```

`parcel_current_identifier`와 같은 자리, 같은 형태다. 읽는 쪽은 술어를 다시 적지 않는다 — 같은
술어를 두 곳에 적는 것이 이 저장소가 반복해 지불한 결함이고, 그중 하나가 시간 규칙이면 두 답이 언제
갈라졌는지 알 방법이 없다.

뷰가 `SECURITY INVOKER`(기본)인 것은 의도적이다. 소속 표의 읽기 권한을 우회하는 자리가 되면 안 된다.

### 3. `catalog.parcel.complex_id`는 발행 경로 밖에서 바뀌지 않는다

`protect_parcel_pnu_projection`과 같은 형태의 트리거를 건다. `UPDATE`에서 값이 실제로 달라질 때만
42501로 거부하고, `INSERT`는 막지 않는다 — 그 컬럼은 3단계까지 `NOT NULL`이고 필지를 만드는 모든
경로가 여전히 값을 넣어야 한다.

이것이 2단계에서 필요한 이유: 읽기가 소속 표로 옮겨간 뒤에도 컬럼은 남아 있고, **두 곳이 서로 다른
답을 갖게 되는 유일한 경로가 그 컬럼에 대한 UPDATE다.** 트리거가 그 경로를 닫으면 컬럼은 3단계까지
과거의 스냅샷으로만 남는다.

### 4. 옮기기 전에 두 경로가 같은 답을 내는 것을 확인한다

구 술어(`p.complex_id = $1`)와 신 술어(뷰 경유)가 같은 필지 집합을 고르는지 대조하는 테스트를 먼저
둔다. 백필이 두 경로를 일치시켰다는 것이 1단계의 주장이고, **그 주장은 아직 검사된 적이 없다.**

## 기각한 대안

### `as_of` 파라미터를 지금 받는다

과거 재현이 가능해지고, 서버 시간대에 의존하지 않게 된다. 산단 지정·해제는 법적 효력이 있는 사건이라
언젠가 필요해질 가능성이 높다.

기각한 이유는 **지금 그것이 아무 일도 하지 않기 때문이다.** 이력이 없으므로 모든 날짜가 같은 답을
낸다. 검증할 수 없는 기능은 틀려도 드러나지 않고, 미래·범위 밖 날짜 정책과 캐시 파편화라는 비용은
즉시 발생한다. 그리고 선언된 소비자가 이것을 요구한 적이 없다 — 요구하지 않은 표면을 만드는 것이
[ADR-0021](./0021-an-unread-surface-is-deleted-not-migrated.md)이 오늘 세 개를 지운 이유다.

### 술어를 뷰가 아니라 각 질의에 직접 적는다

뷰 하나를 아끼고, 질의 계획이 단순해진다.

기각한 이유는 읽는 곳이 하나뿐인 지금이 아니라 둘째가 생기는 시점이다. 그때 술어를 복사하면 "현재"의
정의가 두 벌이 되고, 둘 중 하나만 고쳐지는 사고는 이 저장소에 이미 있었다. 뷰는 그 복사가 일어날
자리를 없앤다.

### `CURRENT_DATE` 대신 애플리케이션이 날짜를 계산해 바인딩한다

시간대를 Rust 쪽에서 정할 수 있다.

기각한 이유는 SQL로 직접 쓰는 호출자가 있기 때문이다 — 시드와 픽스처가 그렇고, 앞으로의 진단 질의도
그렇다. 애플리케이션만 아는 규칙은 그들에게 적용되지 않는다. 시간대 문제는 실재하지만(§남은 부채 1)
그것은 `parcel_current_identifier`가 이미 가진 문제이며, 한 곳에서 함께 고칠 일이지 이 결정이 우회할
일이 아니다.

## Consequences

- **`anchor-summary`가 소속 표를 읽는다.** `catalog.parcel.complex_id`를 읽던 마지막 프로덕션 경로가
  사라진다. 남은 읽기는 `ParcelResponse`가 그 값을 실어 나르는 것뿐이며, 그것은
  [ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md) §Decision 3의 3단계다.
- **집계의 중복 위험이 없다.** 이 질의는 `AVG`/`MIN`/`MAX`/`COUNT`이고, 한 필지가 소속 행을 여러 개
  가지면 중복으로 세어진다. [ADR-0020](./0020-geometry-is-not-evidence-for-a-fact.md) §Decision 3의
  단일 EXCLUDE가 그것을 **표현 불가**로 만들었으므로, 이 이전은 그 위험을 새로 만들지 않는다.
  `EXISTS`를 쓰는 이유는 중복 방지가 아니라 의도를 적기 위해서다.
- **응답에 유효기간을 싣지 않는다.** 처음 검토에서 "부르는 쪽이 시점 개념을 알게 하자"는 안이 있었으나,
  이 라우트는 앵커들의 **집계**라 보고할 단일 기간이 없다. 소속 행을 그대로 돌려주는 라우트가 생기면
  그때 다시 볼 일이다.
- **시간대가 결정에 포함되지 않았다.** §남은 부채 1.

## 남은 부채

1. **`CURRENT_DATE`의 시간대가 지정되지 않았다.** 저장소 어디에도 데이터베이스 시간대 설정이 없어
   UTC로 동작하며, 한국 시간 09:00에 날짜가 넘어간다. 오늘 발효되는 소속이 오전 내내 어제 상태로
   보인다는 뜻이다. 이 ADR은 그 문제를 **만들지 않고 하나 더 늘린다** —
   `parcel_current_identifier`가 이미 같은 상태다. 둘을 함께 고쳐야 하며, 그 결정은 여기 범위가 아니다.
2. **`as_of` 파라미터는 열려 있다.** §기각한 대안 1. `sandan_parcel` 수집기가 생겨 이력이 쌓이면
   다시 판단한다.
3. **`parcel.complex_id`의 INSERT는 여전히 자유롭다.** §Decision 3의 트리거는 UPDATE만 막는다.
   3단계에서 컬럼이 사라지면 함께 해소된다.
