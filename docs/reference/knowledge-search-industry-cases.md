---
status: current
owner: repository-maintainers
doc_type: reference
last_reviewed: 2026-08-14
---

# 지식 검색·RAG 사례 레퍼런스

사내 지식 검색과 RAG를 직접 설계하기 위해 조사한 **실제 프로덕션 사례의 공개 기록**이다.
[AGENTS.md 해결 접근 순서](../../AGENTS.md) 3번(검증된 사례 조사, 블로그 요약보다 1차 자료 우선)의
근거 자료다.

**이 문서는 채택 결정이 아니다.** 무엇을 취하고 버릴지는 결정 시점에 ADR로 남긴다. 이미 내린
결정은 [Intelligence ADR-0002](../../platforms/intelligence-platform/docs/adr/0002-canonical-release-rag-design.md)와
[ADR-0003](../../platforms/intelligence-platform/docs/adr/0003-korean-morphology-in-rust.md)에 있다.

사례의 복잡성을 그대로 복사하지 않고, 우리 규모와 제약에 맞는 보장만 가져오는 것이 원칙이다.

## 조사 대상

| 사례 | 소속 | 자료 등급 | 조사일 |
|---|---|---|---|
| [Cerebras Knowledge](https://www.cerebras.ai/blog/how-we-built-our-knowledge-base) | Cerebras | **1차** 공식 엔지니어링 블로그 (2026-07-15) | 2026-08-14 |
| [Improving Agent with Semantic Search](https://cursor.com/blog/semsearch) | Cursor | **1차** 공식 블로그 | 2026-08-14 |
| [Enterprise search, secure and private](https://slack.engineering/how-we-built-enterprise-search-to-be-secure-and-private/) | Slack | **1차** slack.engineering | 2026-08-14 |
| [How Slack AI Processes Billions of Messages](https://engineering.salesforce.com/how-slack-ai-processes-billions-of-messages-to-reduce-information-overload-with-ai-powered-search-and-summarization/) | Salesforce/Slack | **1차** engineering.salesforce.com | 2026-08-14 |
| Enhanced Agentic-RAG | Uber | **1차** 공식 블로그 | 2026-08-13 |
| [Introducing Contextual Retrieval](https://www.anthropic.com/news/contextual-retrieval) | Anthropic | **1차** 공식 블로그 | 2026-08-13 |
| KG-RAG for customer service | LinkedIn | **1차** SIGIR 2024 논문 | 2026-08-13 |
| LLM 기반 지원 자동화 | DoorDash | **2차** — 원문이 HTTP 403, ZenML LLMOps DB 요약으로 대체 | 2026-08-13 |
| [LARAG](https://arxiv.org/abs/2605.07517) | 학계 (Rulex 문서 대상) | **1차** arXiv, 다만 프로덕션 아님 | 2026-08-13 |

조사 대상을 추가할 때는 `## <사례명>` 절을 같은 형식으로 덧붙이고 위 표와 `## 교차 관찰`을 함께 갱신한다.

---

## 교차 관찰 — 여러 사례가 독립적으로 같은 결론에 도달한 것

서로 다른 도메인(반도체 사내 위키, 코드 에이전트, 배달 지원, 채용 플랫폼)에서 출발해 같은 답에
도달한 항목이다. 단일 사례보다 근거의 무게가 크다.

### 1. 벡터 검색만 쓰는 곳이 하나도 없다

조사한 프로덕션 사례 **전부**가 하이브리드다. Cerebras는 이것을 직접 시험하고 기록했다.

> *"We initially tested whether simple embeddings over raw text performed well enough.
> We quickly realized that **vector search alone was insufficient** for matching all relevant data."*
> — Cerebras

> *"Our agent makes heavy use of grep as well as semantic search, and **the combination of these
> two leads to the best outcomes**."* — Cursor

정확 토큰(에러 문자열·플래그명·조문 번호)은 어휘 유사도가 이길 수 없다는 것이 공통 근거다.

### 2. 여러 리트리버를 RRF로 합친다

Cerebras는 **리트리버 6개를 병렬로** 돌리고 Reciprocal Rank Fusion으로 합친다.

```text
score(d) = Σ  weight / (k + rank_l(d))      k = 60, 기본 weight 1.0
          리스트 l
```

> *"The smoothing constant makes **consensus matter more than a single strong vote**."*

원전은 [Cormack, Clarke, Büttcher, SIGIR 2009](https://dl.acm.org/doi/10.1145/1571941.1572114).

**중요:** Cerebras의 6개 리트리버 중 벡터는 일부다(vector, FTS, thread summaries, graph,
wiki vector, Slack FTS). **임베딩 없이도 리트리버를 늘리고 RRF로 합칠 수 있다.**

### 3. 재순위는 융합과 별개의 단계다

Cerebras: RRF로 합친 뒤 중복 병합 → 파일당 기여 상한 → 상위 20 → **리랭커 모델이 0~10점** →
상위 10. 융합만으로 끝내지 않는다.

### 4. 원문을 그대로 임베딩하지 않는다

두 사례가 같은 말을 한다.

- Cerebras **distillation**: LLM이 스레드에서 `question / summary / resolution / systems /
  code_refs`를 뽑아 정규화한 문서를 임베딩한다. **원본 transcript는 임베딩하지 않는다.**
  > *"accuracy increased significantly when the thread was normalized into a consistent format."*
- Anthropic **contextual retrieval**: 청크 앞에 문서 맥락을 붙여 임베딩. 검색 실패 −49%,
  리랭킹까지 더하면 −67%.
- Uber: 청크마다 LLM 요약·FAQ·키워드를 붙임.

### 5. 평가 세트를 먼저 만든다

Uber(SME 골든질문 100+), DoorDash(5개 축 채점 + 회귀 게이트), Anthropic(실패율 5.7%→1.9%),
LinkedIn(MRR). **채점표 없이 개선을 주장한 사례가 없다.**

### 6. 스코프 분리는 코퍼스가 커지면 반드시 온다

> *"As the corpus grew, **'search everything everywhere' rapidly stopped being useful**.
> Engineers on compiler teams did not want infrastructure runbooks in their results."* — Cerebras

Cerebras는 `Project`(데이터소스 묶음)를 도입하고 온보딩 때 기본 프로젝트를 고르게 한다.

### 7. Postgres 한 대로 충분한 규모가 넓다

Cerebras는 **하루 15,000 질문**을 **단일 Postgres 테이블**로 받는다. Elasticsearch가 아니다.

### 8. 권한은 인덱스가 아니라 경계에서 푼다

Slack 엔터프라이즈 검색은 **외부 데이터를 아예 색인하지 않는다.**

> *"We never store data from external sources in our databases."*
> *"Slack AI only operates on the data that the user can already see."*

OAuth read scope만 요청하고, 검색 시점에 파트너 API를 호출한다. 색인에 ACL을 박는 대신
**저장하지 않는 것**으로 해결한 반대 방향의 답이다.

---

## Cerebras Knowledge

사내 지식베이스. 출시 3개월 만에 **하루 15,000 질문**, 사람·자동화·에이전트가 함께 쓴다.

### 파이프라인

```text
SOURCES        Slack · Wiki · Code · Incidents · Netlists · 팀별 커스텀 DB
   ↓
DISTILLATION   LLM 추출기 — question/summary/resolution/systems/code_refs
   ↓
EMBEDDINGS     pgvector · 3,072차원 · HNSW · 단일 테이블
   ↓
RETRIEVAL      6개 리스트 병렬
   ↓
FUSION+RERANK  RRF(k=60) → LLM 리랭커 → 상위 10
   ↓
SYNTHESIS      답변 + 인용
```

### 설계 선택

| 항목 | 선택 | 근거로 든 것 |
|---|---|---|
| 저장소 | **단일 Postgres 테이블** — 모든 소스가 같은 임베딩 행 형식 | 커스텀 커넥터를 다른 팀이 PR로 추가 가능 |
| 수집 | 소스별 커넥터. Slack은 Socket Mode WebSocket | rate limit 소진 회피 |
| Slack 단위 | 메시지가 아니라 **스레드 전체를 한 행으로** 재수집 | 참여자·최종활동시각이 항상 완전한 대화를 반영 |
| 키워드 | 원문에 **Postgres full-text (GIN) 인덱스** | 도착 즉시 검색 가능 |
| 코드 | CocoIndex — 언어별 정규식 경계를 거친 것부터 시도(클래스→메서드→블록) | 저장소 일부가 40GB 초과, 변경분만 재임베딩 |
| 노출 | MCP 도구를 **검색 프리미티브 단위로**, 가능한 한 LLM 없이 | Claude Code 등 에이전트가 오케스트레이터가 됨 |

### Slack 스레드에 쓰는 4개 신호

| 신호 | 잡는 것 | 인용 |
|---|---|---|
| Full-text | 에러 문자열·플래그명·호스트명 | *"no amount of semantic similarity should outrank it"* |
| Embedding | 말바꿈 — "restore hangs"와 "checkpoint stalls" | |
| **IDF** | 희귀 토큰이 짧은 메시지를 살림 | *"sounds good, thanks!"*는 IDF로 0점에 가까워짐 |
| **Age decay** | 6개월 전 답은 이제 없는 인프라를 설명할 수 있음 | 동점이면 최신이 이김 |

> *"No single scorer is trusted on its own."*

### Bursting

같은 작성자의 연속 메시지 묶음(burst)을 **스레드 주제를 앞에 붙여** 별도로 임베딩한다.
긴 스레드에서 요약에 안 잡히는 곁가지 답을 살리기 위함. 임베딩 전 임계값:

- 코퍼스 기준 **IDF ≥ 4.0**인 희귀 토큰 포함
- 합쳐서 **200자 이상**
- 메시지 중 하나 이상에 **리액션**이 있을 것(사회적 가점)

### 문맥 복원

재순위가 끝난 뒤 **이웃 두 섹션을 다시 붙인다.** 청킹이 쪼개버린 제목·전제·주의사항을 잃지 않게.

---

## Cursor — 코드 시맨틱 검색

측정치가 가장 구체적인 사례다.

| 지표 | 결과 |
|---|---|
| 질의응답 정확도 | **평균 +12.5%** (모델별 6.5~23.5%) |
| 코드 유지율 | +0.3%, **1,000파일 이상 저장소에서 +2.6%** |
| 시맨틱 검색 제거 시 | **불만족 후속 요청 +2.2%** |

### 훈련 방법 — 우리 평가 세트에 응용 가능한 발상

에이전트 세션 트레이스를 훈련 데이터로 쓴다. 에이전트가 여러 번 검색하고 파일을 열다 결국
맞는 코드를 찾으면, **사후에 "이것이 더 일찍 나왔어야 했다"를 역산**한다. LLM이 각 단계에서
가장 도움이 됐을 내용을 순위 매기고, 그 순위에 맞도록 임베딩 모델을 훈련한다.

일반적인 코드 유사도가 아니라 **실제 에이전트 행동에서 나온 피드백 루프**다.

### 기타

- 벡터는 Turbopuffer, **원본 소스코드는 로컬에만** 두고 서버에 저장하지 않는다.
- 수만 파일 저장소는 순진하게 색인하면 몇 시간이 걸리고, 80% 완료 전에는 시맨틱 검색을 못 쓴다.
- 조직 내 같은 코드베이스 사본은 평균 **92% 유사**하다.

---

## Slack — 두 개의 다른 답

### 엔터프라이즈 검색: 색인하지 않는다

외부 소스(Google Drive, GitHub)를 **검색 시점에 파트너 API로 직접 조회**한다. 색인을 만들지
않으므로 신선도 문제도 ACL 동기화 문제도 생기지 않는다. 대신 외부 API 의존과 지연을 받는다.

### Slack AI: 비용을 계층으로 나눈다

- Recap은 **야간 배치** — 피크 시간에 LLM을 돌리지 않고 아침에 신선한 요약을 제공
- 실시간은 **동시성 슬롯**으로 고가치 질의만
- 커스텀 랭킹이 Slack 내부 검색 신호와 AI 추론을 결합
- 모델은 AWS escrow VPC에 두어 **모델 제공자가 고객 데이터에 접근하지 못하게** 함
- 저장은 MySQL·Redis, 검색은 Elasticsearch, 오래된 메시지는 S3로 계층화

---

## Uber · Anthropic · LinkedIn · DoorDash

| 사례 | 한 것 | 결과 |
|---|---|---|
| **Uber** Enhanced Agentic-RAG | 하이브리드(벡터 ∪ BM25), 청크별 LLM 요약·FAQ·키워드, 표 인식 청킹, Query Optimizer·Source Identifier 에이전트, SME 골든질문 100+ 로 LLM-as-Judge | 수용 가능한 답변 **+27%**, 틀린 조언 **−60%** |
| **Anthropic** Contextual Retrieval | 청크 앞에 문서 맥락을 LLM으로 생성해 붙인 뒤 색인. BM25 병행 | 검색 실패 **−49%**, 리랭킹까지 **−67%** (5.7%→1.9%) |
| **LinkedIn** KG-RAG (SIGIR 2024) | 티켓의 내부 구조와 티켓 간 관계를 지식그래프로 보존한 뒤 검색 | MRR **+77.6%**, 중앙값 해결시간 **−28.6%** |
| **DoorDash** | 2단계 가드레일 — 값싼 의미유사도 검사로 거르고 걸린 것만 LLM 판정기. 5개 축 채점 + 회귀 게이트 | 환각 **−90%**, 심각 컴플라이언스 이슈 **−99%** |

**LARAG**(arXiv 2605.07517, 2026-05)은 문서에 이미 있는 하이퍼링크를 청크 메타데이터로 넣어
"명시적 그래프 구축 없이" 그래프 같은 검색을 만든다. 다만 **학술 연구이고 전문가 질문 20개
규모**이므로 프로덕션 근거로 쓰지 않는다.

---

## 우리 현재 상태와의 대조

조사 시점 기준 우리 색인은 `platforms/intelligence-platform` 안의 Postgres 전문검색 하나이며,
코퍼스는 비계(`tenant:scaffold`)뿐이다.

| 축 | 우리 현재 | 사례들의 공통 전제 |
|---|---|---|
| 저장소 | Postgres 단일 테이블 | ✅ 같음 (Cerebras) |
| 키워드 색인 | Postgres FTS + GIN | ✅ 같음 (Cerebras) |
| 스코프 | `TenantScope`(tenant + product) | ✅ 같음 (Cerebras Projects) |
| 정확 토큰 우선 | 형태소 토큰 + 원문 토큰 병행 | ✅ 같은 문제의식 |
| **리트리버 수** | **3개** (형태소·원문·제목) — [ADR-0004](../../platforms/intelligence-platform/docs/adr/0004-retriever-fusion-over-a-single-scorer.md) | Cerebras 6개 |
| **RRF 융합** | ✅ k=60, 도메인 순수 함수 | 표준 |
| **재순위 단계** | 없음 | 표준 |
| **원문 정규화(distillation)** | 없음 | Cerebras·Anthropic·Uber |
| **age decay** | 없음 | Cerebras |
| **평가 세트** | 없음 | 전 사례 |
| 벡터 | 없음(의도적 — ADR-0002가 근거 없는 provider 고정 금지) | pgvector 3072 HNSW 등 |
| MCP 도구 노출 | 없음 | Cerebras |

**바닥 여섯 줄은 이미 같다.** 남은 네 줄 중 재순위·age decay는 임베딩 없이 만들 수 있고,
age decay는 코퍼스가 문서 날짜를 가져와야 한다.

리트리버 분리와 RRF는 이 조사의 직접적 산물이다 — 조사 전에는 신호 하나를 한 칼럼에 섞어
쓰고 있었고, 그것이 서로를 가린다는 것을 사례들이 알려 줬다.

---

## 가져오지 않기로 한 것

| 사례의 선택 | 우리가 안 가져오는 이유 |
|---|---|
| Uber의 Langfx/LangGraph/Michelangelo 스택 | Rust 단일 정본 규칙과 충돌하고 우리 규모에 과하다. 보장만 가져오고 복잡성은 두고 온다 |
| Elasticsearch + Nori | 우리 코퍼스 규모가 통용 전환선(약 50만 행)의 한참 아래다. 전환 조건은 [ADR-0003](../../platforms/intelligence-platform/docs/adr/0003-korean-morphology-in-rust.md) 재검토 트리거에 적었다 |
| 문서 링크 그래프 시각화 | 조사한 프로덕션 사례 중 만든 곳이 없다. LinkedIn의 그래프는 시각화가 아니라 검색용이고 대상도 문서가 아닌 티켓이었다 |
| Slack식 무색인 연합 검색 | 우리 코퍼스는 공공 고시문이라 원본 보존(`bronze_raw_preservation_required`)이 규칙이다. 저장하지 않는 선택지가 없다 |

---

## 관련 문서

- [Intelligence ADR-0002 — canonical release를 읽는 RAG 설계 경계](../../platforms/intelligence-platform/docs/adr/0002-canonical-release-rag-design.md)
- [Intelligence ADR-0003 — 한국어 형태소 분석](../../platforms/intelligence-platform/docs/adr/0003-korean-morphology-in-rust.md)
- [기술 스택 §2 Search 행](../technology-stack.md)
- [Intelligence 아키텍처](../../platforms/intelligence-platform/docs/architecture.md)
