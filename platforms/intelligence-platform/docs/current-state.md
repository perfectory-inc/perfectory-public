---
status: current
owner: intelligence-platform
doc_type: documentation
last_reviewed: 2026-11-11
---

# Intelligence Platform 현황

**무엇이 코드로 있고, 무엇이 ai-server 에서 돌고, Foundation 과 무엇을 주고받는지**의 단일 출처다.
다른 문서는 이 표를 다시 적지 않고 여기를 가리킨다. 표를 바꾸는 PR 은 근거(코드 경로·측정 명령)를 함께 바꾼다.

결론부터: **코드의 대부분은 시험 안에서만 돈다.** ai-server 에서 실제로 도는 것은 모델 런타임(Ollama·LiteLLM·Open WebUI)뿐이고,
intelligence-api·worker·지식 색인은 배포된 적이 없다. Foundation 데이터를 읽는 경로는 문서 텍스트 작업
([루트 ADR-0477](../../../docs/adr/0477-foundation-hands-intelligence-documents-and-place-ids-and-takes-back-cited-facts-in-tables-only-intelligence-writes.md))이 처음이다.

## 1. 구성 요소

측정: 2026-11-11, ai-server 읽기만(`systemctl list-units`, `docker ps`, `curl localhost:11434/api/tags`, `curl localhost:8010/healthz`).

| 구성 요소 | 코드 | 상태 | ai-server | 결정 |
|---|---|---|---|---|
| 정규화 제안(건축물대장 층·단위) | `crates/normalization/*`, `services/intelligence-worker/src/bin/building_register_{floor,unit}_normalization.rs` | 생성→검증→outbox→Foundation 제출까지 구현, dry-run 지원 | **미배포**(유닛·컨테이너 없음) | [ADR-0001](./adr/0001-canonical-implementation-rust.md) |
| HTTP API(`/intelligence/v1/*`, `/v1/chat/completions`) | `services/intelligence-api` | C0–C1(인바운드 인증·admission·durable outbox) 구현 | **미배포**(`:8010` 응답 없음) | [루트 ADR-0216](../../../docs/adr/0216-intelligence-callers-are-identity-workloads-decided-by-identity.md) |
| 지식 색인(Postgres 전문검색) | `crates/knowledge/*`, `migrations/2026081*` | `ip_knowledge_chunk`·`KnowledgeIndexPort`, `tenant:scaffold` CLI 2종만, HTTP route 없음 | **미배포**(색인 DB 없음) | [ADR-0002](./adr/0002-canonical-release-rag-design.md)(벡터·운영 RAG 금지) |
| 한국어 형태소 | `knowledge-infrastructure` 색인기, `lindera` + mecab-ko-dic | 색인은 형태소+원문, 질의는 형태소 | 미배포(색인과 같이) | [ADR-0003](./adr/0003-korean-morphology-in-rust.md) |
| 리트리버 융합 | `knowledge-domain` | 형태소·원문·제목 3신호 → RRF(k=60) | 미배포 | [ADR-0004](./adr/0004-retriever-fusion-over-a-single-scorer.md) |
| 결과 조립 | `knowledge-domain` | 출처별 상한 3, 이웃 문맥 | 미배포 | [ADR-0005](./adr/0005-result-assembly-diversity-and-context.md) |
| 이벤트 백본(C2) | `crates/messaging`, `crates/intelligence-contracts`, `schemas/*.avsc` | Kafka/Karapace 어댑터·`foundation_knowledge_consumer` 코드 있음 | **미배선**: Foundation 쪽 `knowledge.source-upserted` 생산자 없음 | [루트 ADR-0215](../../../docs/adr/0215-a-topics-avro-schema-belongs-to-its-producer-and-consumers-only-look-it-up.md) |
| **문서 텍스트(첫 조각)** | `jobs/document_text`, `config/document-extraction.contract.json` | PDF 텍스트 층 + 쪽 단위 OCR, HWP/HWPX→PDF, 쪽·위치 보존, 새 해시만; 사실은 규칙 먼저·로컬 모델 나머지 | **작업 등록, 첫 run 으로 켜짐(ADR-0457)**: `intelligence_document_text`(`documents` 풀)·`intelligence_document_fact`(spark 풀 전부, 하루 한 번), 이미지는 릴리스가 CI 에서 짓는다. 입력 수집과 함께 켜진다(§3). 운영 절차는 `platforms/foundation-platform/docs/runbooks/intelligence-documents.md` | [루트 ADR-0477](../../../docs/adr/0477-foundation-hands-intelligence-documents-and-place-ids-and-takes-back-cited-facts-in-tables-only-intelligence-writes.md) |
| **문서 사실 v1** | `jobs/document_text/document_text/fact_run.py`·`facts.py`·`model_facts.py`, `config/ksic-divisions.json` | 규칙 먼저(토지이용계획 용지별 면적·유치업종·토지조서 지번·분양가), 규칙이 못 닫은 토지이용계획 표만 로컬 모델(`MODEL_RUNTIME_*`), OCR 쪽 값은 표 합계 검산 또는 비전 재독; 검산 통과·인용 유효한 값만 씀 | **미배포**(입력 수집 대기). 손 확인 정밀도는 계약 `accuracy_gate.measured` | [루트 ADR-0477](../../../docs/adr/0477-foundation-hands-intelligence-documents-and-place-ids-and-takes-back-cited-facts-in-tables-only-intelligence-writes.md) §5 |
| 재순위·distillation·평가 세트·벡터 | — | **없다** | — | ADR-0002 승인 조건 |

## 2. ai-server 의 모델 런타임

| 무엇 | 상태(2026-11-11) |
|---|---|
| `ollama.service` | active. 모델: `Qwen3.6-35B-A3B`(UD-Q4_K_M, MoE — 기본, [ADR-0006](./adr/0006-default-model-follows-measured-accuracy.md)), `Qwen3.6-27B`(Q4_K_M), `qwen2.5vl:7b`(비전), `qwen2.5:7b`, `gemma2:9b`·`gemma-ko`(판정 역할 제외, ADR-0006) |
| `litellm.service` | active — OpenAI 호환 게이트웨이 |
| `open-webui.service` | active — 개발 UI, 운영 계약 아님 |
| GPU | RTX 3060 8 GiB, 측정 시 9 MiB 사용·0 %(유휴). 35B MoE 는 CPU 분담 |
| 호스트 | 20 코어, 메모리 62 GiB(가용 51) — Spark 풀·by-PNU 굽기와 공유([루트 ADR-0455](../../../docs/adr/0455-one-job-cannot-take-the-host-every-unit-declares-its-memory-and-batch-is-killed-first.md)) |
| 문서 도구 | 호스트에 LibreOffice·Tesseract 없음 — 문서 텍스트 작업은 자기 이미지(`jobs/document_text/Dockerfile`)에 담는다 |

## 3. Foundation 과의 경계

정본은 [루트 ADR-0477](../../../docs/adr/0477-foundation-hands-intelligence-documents-and-place-ids-and-takes-back-cited-facts-in-tables-only-intelligence-writes.md)과
그 계약 파일 [`config/document-extraction.contract.json`](../config/document-extraction.contract.json)이다.

| 방향 | 무엇 | 어떻게 | 오늘 |
|---|---|---|---|
| Foundation → Intelligence | 정규화 대상 레코드 | 작업 env 로 받은 입력 | 미가동 |
| Intelligence → Foundation | 정규화 제안 | `POST /internal/normalization/proposals`, workload token 파일, `Idempotency-Key` | 미가동 |
| 호출자 → Intelligence | 인증·권한 | Identity workload token, 결정은 Identity(`intelligence.*` capability) | 미배포 |
| Foundation → Intelligence | 고시 첨부 바이트 | 공용 내용주소 저장소 `…/sha256/<해시>.<형식>`(루트 ADR-0298·0456) | 수집 작업이 꺼져 있다(ADR-0456 §6) |
| Foundation → Intelligence | 첨부 색인 | `silver.industrial_complex_notice_attachment`(Iceberg, 읽기만) | 표 계약만(`contract_only`) |
| Foundation → Intelligence | 필지 | `gold.place_id_registry`·`gold.place_id_bridge`(루트 ADR-0288 §5) — intelligence 는 PNU 를 만들지 않는다 | 읽는 코드 없음 |
| Intelligence → Foundation | 텍스트·사실 | `intelligence.document_text_*`·`intelligence.document_fact*`(intelligence 만 쓰는 append-only Iceberg 표; 열은 계약의 열 + `run_id`, 레이크하우스 계약 `industrial_complex_lakehouse_contracts.json`) | 이미지는 로컬 파일만 읽고 쓰고, 표는 그 작업의 Spark 단계(`infra/lakehouse/spark/jobs/intelligence_documents.py`)만 쓴다. 첫 run 이 아직 없다 |

R2·Iceberg·by-PNU 를 직접 읽는 intelligence 코드는 문서 텍스트 작업 전에는 없었다(`git grep` 기준: `trino`·`place_id`·`by_pnu` 0건).
