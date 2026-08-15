# ADR 0005 — 결과 조립: 출처별 상한과 이웃 문맥

- 상태: Accepted
- 날짜: 2026-08-15
- 소유자: Intelligence Platform
- 관련: [ADR-0004](./0004-retriever-fusion-over-a-single-scorer.md) · [사례 레퍼런스](../../../../docs/reference/knowledge-search-industry-cases.md)

## 배경

[ADR-0004](./0004-retriever-fusion-over-a-single-scorer.md)가 순위를 여러 신호의 합의로
정하게 만들었다. 그러나 순위는 **조각 하나하나가 얼마나 맞는지**만 본다. 사용자가 실제로
받는 것은 조각 하나가 아니라 **결과 묶음**인데, 그 묶음이 쓸모 있는지는 아무도 보지 않았다.

두 가지가 비어 있었다.

**하나, 한 문서가 결과를 독식한다.** 긴 고시 하나가 다섯 절에서 걸리면 그 다섯 조각이
상위 다섯 칸을 다 차지하고 다른 고시는 아예 보이지 않는다. 고시문은 길어서 이 상황이
예외가 아니라 기본값에 가깝다.

**둘, 조각이 홀로 온다.** 청킹은 제목 아래 문단을 잘라 놓는다. 앞 조각에 있던 정의나 전제,
뒤 조각에 있던 단서("다만 ~은 제외한다")가 사라진 채 한 문단만 남는다. 고시문에서 단서가
빠진 조문은 틀린 정보에 가깝다.

조사한 사례는 융합 직후 이 둘을 모두 처리한다
([사례 레퍼런스](../../../../docs/reference/knowledge-search-industry-cases.md)).

> *"we merge duplicate chunks back to one source, **cap how many results each file can
> contribute**, and end up with a more diverse top twenty."*
>
> *"if we match a wiki section **we pull in the two neighboring sections** so the heading,
> preconditions, and caveats that chunking split apart aren't lost."*
> — Cerebras

## 결정

**융합과 자르기 사이에 결과 조립 단계를 둔다.**

```text
리트리버 3종 → RRF(k=60) → 출처별 상한 → limit으로 자르기 → 이웃과 함께 읽기
                            ← 여기          ← 여기
```

### 출처별 상한은 자르기 **전에** 건다

`MAX_CHUNKS_PER_SOURCE = 3`.

자른 뒤에 걸면 이미 한 출처가 자리를 다 차지한 뒤라 다양성이 회복되지 않는다 — 걸러낸
자리를 채울 후보가 남아 있지 않기 때문이다. 그래서 순서가 규칙의 일부다.

**3인 이유:** 1이면 실제로 관련 있는 다른 절을 버리게 된다. 크면 상한의 의미가 없다.
3이면 `limit`이 10일 때 서로 다른 문서가 최소 4개는 보인다.
**측정 근거는 없다** — 평가 세트가 생기면 그 자료로 정한다. 지금 이 숫자를 튜닝하는 것은
근거 없는 조정이다.

### 상한도 도메인의 순수 함수다

`knowledge-domain::fusion::cap_per_group`. 융합과 같은 이유로 여기 있다 — 목록이 들어가고
목록이 나오며, 어댑터마다 다르게 구현되면 같은 질의가 저장소에 따라 다른 묶음을 낸다.

상한 0은 전부 버린다. 호출부가 실수로 0을 넘기면 **결과가 사라져 바로 드러나야** 하기
때문이다. 조용히 무제한으로 해석하면 상한이 없는 것과 구분되지 않는다.

### 문맥은 본문에 섞지 않는다

`SearchHit`에 `context_before`·`context_after`를 **별도 필드로** 둔다.

`body`에 이어 붙이면 무엇이 맞았고 무엇이 곁에 있었는지를 구분할 수 없다. 인용을 만들거나
하이라이트를 칠 때 그 구분이 사라지면 되돌릴 방법이 없다.

### 이웃은 같은 질의에서 함께 읽는다

수화 시점에 `ordinal-1, ordinal, ordinal+1`을 한 번의 `unnest` 질의로 가져온다.
결과당 질의를 따로 내면 N+1이 된다.

## 재발 방지 — 각 테스트가 무엇을 증명하는지

두 계약 테스트 모두 **기능을 끄면 실패하도록** 데이터를 짰고, 실제로 꺼서 실패를 확인한 뒤
되돌렸다.

| 테스트 | 끄는 방법 | 끄면 나오는 것 |
|---|---|---|
| `postgres_index_caps_how_much_one_source_can_fill` | `MAX_CHUNKS_PER_SOURCE`를 크게 | 상위 5칸이 전부 `long-notice`, `short-notice`는 사라짐 |
| `postgres_index_returns_neighbouring_context` | 수화에서 이웃 ordinal 제거 | `context_before`·`context_after`가 `None` |

도메인 쪽은 `one_source_cannot_fill_the_whole_result`가 같은 성질을 순수 함수 수준에서 잡는다.

## 결과

- 수화 질의가 읽는 행이 최대 3배가 된다(매치 + 앞 + 뒤). 기본 키 조회이며 한 번의 질의다.
- `SearchHit`에 필드 둘이 늘었다. 인메모리 어댑터는 `None`을 채운다 — 테스트와 loopback
  개발용이며 문맥 복원은 Postgres 스위트가 본다.
- 상한 때문에 관련 있는 조각이 잘릴 수 있다. 다양성과 맞바꾼 것이며, 평가 세트가 생기면
  그 값이 맞는지 재야 한다.

## 아직 하지 않은 것

| 항목 | 왜 지금 아닌가 |
|---|---|
| **재순위(rerank)** | 사례들은 조립 뒤 리랭커 모델로 20→10을 한다. 모델 선택은 ADR-0002의 provider 규율에 걸린다 |
| **이웃을 두 칸까지** | 지금은 한 칸. 늘릴 근거가 없다 — 고시 조문 길이를 보고 정한다 |
| **중복 본문 병합** | 같은 본문을 가진 조각이 실제로 나오는지 확인되지 않았다 |
| **age decay** | 문서 날짜 칼럼이 없다 |

## 재검토 트리거

| 조건 | 검토 대상 |
|---|---|
| 평가 세트에서 상한 3이 재현율을 해치는 것으로 **측정**됨 | `MAX_CHUNKS_PER_SOURCE` |
| 조문 하나가 앞뒤 한 칸으로 안 채워지는 사례가 반복 | 이웃 범위 확대 |
| 수화 질의가 지연의 주원인으로 **측정**됨 | 이웃 읽기를 선택적으로 |

**측정 없이 트리거를 당기지 않는다.**
