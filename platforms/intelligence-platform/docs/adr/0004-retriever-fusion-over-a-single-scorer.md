# ADR 0004 — 검색은 신호별 리트리버를 두고 RRF로 합친다

- 상태: Accepted
- 날짜: 2026-08-14
- 소유자: Intelligence Platform
- 관련: [ADR-0002](./0002-canonical-release-rag-design.md) · [ADR-0003](./0003-korean-morphology-in-rust.md) · [사례 레퍼런스](../../../../docs/reference/knowledge-search-industry-cases.md)

## 배경

색인은 형태소 토큰과 원문을 `search_text` 한 칸에 섞어 tsvector 하나로 만들고 있었다.
순위를 매기는 자가 하나뿐이라는 뜻이다.

그 구조의 문제는 **신호가 서로를 가린다**는 것이다.

- 제목이 정확히 맞은 문서가 긴 본문에 희석된다. `ts_rank`는 문서 길이로 정규화한다.
- 원문 일치와 형태소 일치를 구분할 수 없다. 둘은 서로 다른 실패를 보완하는데
  ([ADR-0003](./0003-korean-morphology-in-rust.md)) 한 칸에 있으면 그 보완이 점수 하나로 뭉개진다.
- 그 하나가 틀렸을 때 대안이 없다.

조사한 프로덕션 사례 중 단일 스코어러로 끝내는 곳이 하나도 없었다
([사례 레퍼런스](../../../../docs/reference/knowledge-search-industry-cases.md) §교차 관찰).

> *"No single scorer is trusted on its own."* — Cerebras

## 결정

**신호마다 별도의 리트리버를 두고, 각자의 순위 목록을 Reciprocal Rank Fusion으로 합친다.**

```text
질의
 ├─ morpheme  search_vector_morph    형태소 질의   조사가 붙은 명사
 ├─ raw       search_vector_raw      원문 질의     사전에 없는 단어
 └─ heading   search_vector_heading  원문 질의     제목 일치
        ↓
   RRF (k = 60)
        ↓
   상위 N만 본문과 함께 읽음(hydrate)
```

### 융합은 도메인에 둔다

`knowledge-domain::fusion::reciprocal_rank_fusion`. 순수 함수이며 I/O도 사전도 필요 없다.

어댑터에 두면 어댑터마다 다르게 구현될 수 있고, 그러면 **같은 질의가 저장소에 따라 다른
순서를 낸다.** 도메인에 두면 규칙이 한 곳에만 있다.

### 완충 상수는 60

원 논문([Cormack, Clarke, Büttcher, SIGIR 2009](https://dl.acm.org/doi/10.1145/1571941.1572114))의
값이며 조사한 구현들이 쓰는 값이다.

```text
score(d) = Σ  1 / (60 + rank_l(d))
          목록 l
```

상수가 클수록 상위 순위 간 점수 차가 줄어 **한 리트리버의 1등보다 여러 리트리버의 합의가
이기기 쉬워진다.** 그것이 융합의 목적이다. 이 성질은
`a_smaller_smoothing_constant_lets_a_single_first_place_win` 테스트가 고정한다.

### 가중치는 전부 1.0

리트리버별 가중치를 두지 않는다. **측정할 자가 없는 상태에서 튜닝하지 않는다.**
평가 세트가 생기면 그때 근거를 갖고 조정한다.

### 후보는 최종 개수보다 깊이 가져온다

`max(limit × 4, 20)`. 융합은 순위를 보고 합의를 찾는 것이라 후보가 얕으면 합의할 거리가 없다.
사례들도 융합 뒤 상위 20을 만들고 재순위로 10을 남긴다.

### 포트는 바뀌지 않는다

`KnowledgeIndexPort::search(scope, query, limit)` 그대로다. 리트리버 구성은 어댑터의 내부
사정이며, 인메모리 어댑터는 계속 단순 포함 검사를 쓴다. **포트를 미리 넓히지 않는다.**

## 왜 이 구조인가 — 벡터가 들어올 자리

벡터 리트리버 추가는 `RETRIEVERS` 표에 **한 줄 더하는 것**이 된다. 융합·수화·포트·호출부는
바뀌지 않는다. 엔진 교체를 쉽게 만드는 것이 이 구조의 값이다.

이것은 [ADR-0002](./0002-canonical-release-rag-design.md)가 금지하는 "승인 전 provider 고정"이
아니다. 벡터 provider를 고르지 않았고 의존성도 없다 —
`scripts/guard/intelligence-no-vector-provider.sh`가 계속 강제한다.

## 재발 방지 — 이 테스트가 무엇을 증명하는지

`postgres_index_lets_consensus_across_signals_win`은 **리트리버를 하나로 줄이면 실패하도록**
데이터를 짰다.

| 문서 | 형태소 | 원문 | 제목 |
|---|---|---|---|
| `josa-heavy` | **1등** (`건폐율` 3회) | ✗ 원문 토큰은 `건폐율을` | ✗ 제목이 `부칙` |
| `consensus` | 2등 (1회) | ✓ | ✓ |

리트리버 1개: `josa-heavy` 승 → 실패
리트리버 3개 + RRF: `consensus` 승 → 통과

**실제로 리트리버를 1개로 줄여 실패를 확인한 뒤 되돌렸다.** 첫 판본은 1개로도 통과해서
아무것도 증명하지 못했고, 그 사실을 발견해 데이터를 다시 짰다.

도메인 쪽은 `consensus_beats_a_single_strong_vote`가 같은 성질을 순수 함수 수준에서 잡는다.

## 결과

- 질의당 SQL이 1회에서 **4회**(리트리버 3 + 수화 1)로 늘었다. 각 리트리버는 GIN 인덱스만
  읽고 본문을 가져오지 않으므로 가볍다. 지금 규모에서 문제가 되지 않으며, 되면 측정 후
  병렬 실행이나 단일 CTE로 합칠 수 있다.
- GIN 인덱스가 3개가 된다.
- `search_text` 한 칼럼이 `search_text_morph`·`search_text_raw` 둘로 나뉜다. 토큰화 규칙이나
  리트리버 구성을 바꾸면 **새 `release_id`로 재색인**해야 한다. 원본은 Bronze에 있다.
- 융합 결과는 어느 리트리버가 올렸는지(`FusedItem::retrievers`)를 갖는다. 지금은 쓰지 않지만
  "왜 이게 1등인가"를 답할 재료다.

## 아직 하지 않은 것

| 항목 | 왜 지금 아닌가 |
|---|---|
| **재순위(rerank) 단계** | 사례들은 융합 뒤 리랭커 모델로 20→10을 한다. 모델 선택이 필요하고 ADR-0002의 provider 규율에 걸린다 |
| **age decay** | 문서 날짜 칼럼이 없다. 고시 코퍼스가 `noti_dtm`을 가져오면 그때 리트리버로 추가한다 |
| **리트리버별 가중치** | 측정할 평가 세트가 없다 |
| **문맥 복원(이웃 청크)** | 융합·재순위가 자리 잡은 뒤 |
| **벡터 리트리버** | ADR-0002 — 근거와 별도 ADR이 먼저다 |

## 재검토 트리거

| 조건 | 검토 대상 |
|---|---|
| 질의당 SQL 4회가 지연의 주원인으로 **측정**됨 | 단일 CTE 통합 또는 병렬 실행 |
| 평가 세트에서 특정 신호가 일관되게 해로움 | 해당 리트리버 제거 또는 가중치 |
| 정밀도가 문제 — 합의가 엉뚱한 문서를 올림 | 재순위 단계 도입 |

**측정 없이 트리거를 당기지 않는다.**
