# AGENTS.md — intelligence-platform

Intelligence Platform에서 작업하는 AI 에이전트 공용 진입점. 모노레포 공통 규칙은
[루트 AGENTS.md](../../AGENTS.md) →
[루트 ADR-0001](../../docs/adr/0001-monorepo-governance-and-conventions.md)이 SSOT이며,
이 파일은 그 위에 영역 규칙을 추가한다.

## 영역 정체 — proposal-only

- LLM 기반 정규화(normalization) **제안** 엔진. Rust workspace가 canonical 구현이다
  ([ADR-0001](./docs/adr/0001-canonical-implementation-rust.md) — Python 프로토타입 2026-07-08 은퇴).
- **Foundation 쓰기 권한 없음**: evidence·confidence·lineage·idempotency key를 갖춘 제안을
  Foundation API로 제출만 한다. 승인/적용은 Foundation 커맨드 전용 — 이 영역에서 canonical
  데이터를 변경하는 코드를 만들지 말 것 (`docs/architecture.md` Cross-Platform Contract).

## 경계 (라우팅·상태)

- 네이티브 API는 `/intelligence/v1/*` (normalization 4종). **OpenAI-호환 표면**
  (`/v1/chat/completions` · `/v1/models`)은 루트 ADR-0001 §6에 기록된 예외로
  생태계 표준 경로를 유지한다 — `/intelligence/v1` 밑으로 옮기지 말 것.
- crates: normalization 3계층(intelligence-normalization-*) + knowledge 3계층 +
  `messaging-infrastructure` + `intelligence-contracts`. services: `intelligence-api` ·
  `intelligence-worker`(바이너리 4종 — drain worker, floor/unit normalization, knowledge consumer).
- **C0-C1**(인바운드 인증·admission control·durable outbox) 구현 완료. **C2** 이벤트 백본은
  코드(Kafka/Karapace 어댑터, Avro 스키마, `foundation_knowledge_consumer`)가 있으나
  **선택적이며 prod 발행 미배선**: submission-requested 토픽을 발행하는 프로덕션 코드 없음,
  Foundation 측 knowledge.source 프로듀서 부재(기본 토픽은 fixture 상수).
  C2를 "가동 중"으로 서술하지 말 것. `/metrics` 분리 리스너는 C3로 연기.
- knowledge retrieval은 **비계 수직 슬라이스만 구현**: Postgres 전문검색 색인(`ip_knowledge_chunk`),
  `KnowledgeIndexPort`, `tenant:scaffold` 전용 CLI 2종. HTTP route 없음.
  vector/embedding은 **여전히 미구현**이며 [ADR-0002](./docs/adr/0002-canonical-release-rag-design.md)가
  승인 전 provider·index 고정을 금지한다 — `scripts/guard/intelligence-no-vector-provider.sh`가 강제한다.

## 절대 규칙

- **한국어 출력 정책**: `/v1/chat/completions`가 ko-KR 답변 정책 주입 + 출력 검증 + 1회
  repair를 수행한다. `gemma-ko` 류 숨은 모델 별칭에 의존 금지 — 한국어 동작은
  정책/검증기/repair 흐름 소관.
- **fail-closed env 원칙**: non-loopback 바인드는 shared-token 인증 없이 기동 거부;
  Foundation base URL 설정 시 workload 토큰 파일 필수(부재 시 fail-fast); drain worker는
  in-memory outbox 거부; 미구성 생성/제출 엔드포인트는 501. 이 게이트를 완화하지 말 것.
- 워크스페이스 린트 `unwrap_used`·`expect_used` = deny, `unsafe_code` = forbid (`Cargo.toml`).
  헬스는 `/healthz`·`/readyz`·`/metrics`.
- 앱/제품 코드는 모델 런타임(Ollama/Open WebUI)을 직접 호출하지 않는다 — 이 플랫폼 API만.

## 검증 명령

```bash
# 모노레포 루트에서 — CI와 동일한 fmt+clippy+test (Docker 필요)
bash scripts/verify/cargo-verify.sh platforms/intelligence-platform

# 이 디렉토리에서 — 스키마 계약 테스트 / C2 라이브 검증(선택, compose 기동 후)
cargo test -p intelligence-normalization-application --test event_schema_contract
docker compose -f docker/c2-event-backbone.compose.yml up -d
cargo test -p messaging-infrastructure --test live_kafka_karapace -- --nocapture
```

## 지식 검색을 건드리기 전에

이 영역에서 가장 자주 되돌려지는 자리다. 순서대로 읽을 것.

| 무엇을 하려는가 | 먼저 볼 것 |
|---|---|
| 검색 엔진·벡터 DB·임베딩 provider를 고르려 함 | [ADR-0002](./docs/adr/0002-canonical-release-rag-design.md) — **승인 전 고정 금지**. 가드가 기계로 막는다 |
| "형태소 분석 없이 `simple`로 충분하지 않나" | [ADR-0003](./docs/adr/0003-korean-morphology-in-rust.md) — 실측으로 아니라고 확인했다 |
| Elasticsearch로 옮기려 함 | [사례 레퍼런스](../../docs/reference/knowledge-search-industry-cases.md) — Cerebras는 하루 15,000 질문을 Postgres 한 테이블로 받는다. 전환 조건은 ADR-0003 재검토 트리거 |
| 리트리버를 하나로 되돌리려 함 | [ADR-0004](./docs/adr/0004-retriever-fusion-over-a-single-scorer.md) — 신호를 한 칼럼에 섞으면 서로를 가린다. 계약 테스트가 1개로 줄이면 실패한다 |
| 검색 품질을 올리려 함 | [사례 레퍼런스](../../docs/reference/knowledge-search-industry-cases.md) §교차 관찰 — 다음은 **재순위**이고, 그 전에 평가 세트가 있어야 한다 |
| 무엇을 색인할지 정하려 함 | 코퍼스는 산업단지 고시다. 수집은 Foundation 소관 |

**지금 없는 것을 있다고 쓰지 말 것:** 재순위·age decay·평가 세트·벡터는 없다.
있는 것은 신호 3종(형태소·원문·제목)과 RRF(k=60) 융합까지다.

## 문서 라우팅

- [README.md](./README.md) — env 레퍼런스 전체 (C0-C1 fail-closed 규칙·엔드포인트·모델 런타임)
- [docs/architecture.md](./docs/architecture.md) — 모듈 경계 + Cross-Platform Contract
- [docs/adr/](./docs/adr/README.md) — 영역 결정 기록 (0001 = Rust canonical)
- [schemas/README.md](./schemas/README.md) — Avro 스키마 진화 규율 + C2 라이브 검증 절차
- [지식 검색·RAG 사례 레퍼런스](../../docs/reference/knowledge-search-industry-cases.md) — 외부 프로덕션 사례의 측정치와 우리 대조표
