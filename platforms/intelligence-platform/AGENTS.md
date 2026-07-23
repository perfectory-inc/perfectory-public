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
- knowledge retrieval·vector/RAG는 **미구현**(소유권 선언만 존재) — 착수 전 신규 설계 문서 필요.

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

## 문서 라우팅

- [README.md](./README.md) — env 레퍼런스 전체 (C0-C1 fail-closed 규칙·엔드포인트·모델 런타임)
- [docs/architecture.md](./docs/architecture.md) — 모듈 경계 + Cross-Platform Contract
- [docs/adr/](./docs/adr/README.md) — 영역 결정 기록 (0001 = Rust canonical)
- [schemas/README.md](./schemas/README.md) — Avro 스키마 진화 규율 + C2 라이브 검증 절차
