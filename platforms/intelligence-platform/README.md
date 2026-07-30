---
status: current
owner: intelligence-platform
doc_type: README
last_reviewed: 2026-07-28
---

# Intelligence Platform

기업용 Intelligence Platform의 Rust 우선 구현 경로입니다.

## 책임 트리

```text
intelligence-platform/
├── crates/normalization/       LLM 정규화 제안
├── crates/knowledge/           지식 계약·처리
├── crates/messaging/           Kafka·Avro·Karapace 어댑터
├── services/intelligence-api   HTTP API
├── services/intelligence-worker  비동기 워커
└── schemas/                    이벤트 스키마
```

문서 지도: [Intelligence docs](./docs/README.md) ·
[전체 문서 색인](../../docs/document-catalog.md)

이 Rust workspace가 API·검증·출처·멱등성·outbox 상태·adapter·Foundation Platform 제출을
포함한 플랫폼 경계의 **정본 구현**입니다. 이전 Python prototype은 2026-07-08에 폐기되었고
(`docs/adr/0001-canonical-implementation-rust.md`), 더 이상 배포 대상이나 계약 참고 집합에
포함되지 않습니다. Foundation Platform wire 계약은 이 영역과 `schemas/`에서만 정의합니다.

## 구성

- `crates/intelligence-contracts`: wire·식별자 공통 계약
- `crates/knowledge/knowledge-domain`: 지식 검증과 도메인 타입
- `crates/knowledge/knowledge-application`: 지식 유스케이스와 port
- `crates/knowledge/knowledge-infrastructure`: 지식 저장 adapter
- `crates/normalization/intelligence-normalization-domain`: 정규화 규칙과 도메인 타입
- `crates/normalization/intelligence-normalization-application`: 정규화 유스케이스와 port
- `crates/normalization/intelligence-normalization-infrastructure`: 모델·Foundation·상태·rate-limit adapter
- `crates/messaging/messaging-infrastructure`: Kafka·Avro·schema registry adapter
- `services/intelligence-api`: Axum HTTP API 경계
- `services/intelligence-worker`: 백그라운드 작업·이벤트 소비·outbox 전달

앱은 intelligence-platform API를 호출해야 합니다. Open WebUI나 model server에 직접
연결하지 않습니다. Open WebUI는 모델 개발 UI로 사용할 수 있지만 production backend
계약은 아닙니다.

운영 코드는 Open WebUI 애플리케이션 endpoint가 아니라 model runtime 또는 gateway endpoint에
연결한다. 현재 local 구성에서 `<model-runtime-host>:8080`은 UI/API 인증이 필요한 Open WebUI이고,
`<model-runtime-host>:11434`는 OpenAI 호환 API를 제공하는 Ollama runtime이다.
`<model-runtime-host>`는 운영자의 로컬 모델 머신을 뜻한다. 실제 hostname/IP는 local env의
`MODEL_RUNTIME_BASE_URL`에만 두고 커밋하지 않는다.

## 로컬 포트

- Rust API scaffold: `127.0.0.1:8010`
- 현재 Open WebUI 개발 UI: `<model-runtime-host>:8080`
- 현재 Ollama 모델 runtime: `<model-runtime-host>:11434`

`INTELLIGENCE_API_BIND=0.0.0.0:8010`은 로컬 개발 중 Open WebUI를 정책 gateway에 붙이는 등
일시적인 LAN 접근에만 사용한다. 단일 머신 개발의 기본값은 `127.0.0.1:8010`으로 유지한다.

## 기업용 runtime C0-C1

이 절은 C0-C1 기반 계획에서 추가한 운영 수준 설정을 설명한다. 모든 설정은 환경변수로 주입하며,
기본값은 loopback 전용이라 추가 변수 없이도 단일 머신 개발에서 안전하다.

### 인바운드 인증(fail-closed)

loopback이 아닌 주소에 bind하려면 인바운드 인증이 필수다. `INTELLIGENCE_API_BIND`가 non-loopback인데
`INTELLIGENCE_INBOUND_AUTH_MODE`가 `shared-token`이 아니면 프로세스는 시작하지 않는다.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `INTELLIGENCE_API_BIND` | 아니오 | `127.0.0.1:8010` | 수신 주소. non-loopback이면 인증 필요 |
| `INTELLIGENCE_INBOUND_AUTH_MODE` | 조건부 | `disabled` | non-loopback에서 `shared-token` 지정 |
| `INTELLIGENCE_INBOUND_SERVICE_TOKEN` | `shared-token`일 때 | — | 호출자가 보낼 Bearer token. 아래 workload identity에 묶임 |
| `INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID` | `shared-token`일 때 | — | token이 대표하는 고정 서비스 주체. 요청 헤더로 바꿀 수 없음 |
| `INTELLIGENCE_INBOUND_SERVICE_TENANT_ID` | `shared-token`일 때 | — | 고정 tenant 범위 |
| `INTELLIGENCE_INBOUND_SERVICE_PRODUCT_ID` | `shared-token`일 때 | — | 고정 product 범위 |
| `INTELLIGENCE_INBOUND_SERVICE_ACTIONS` | `shared-token`일 때 | — | 쉼표로 나열한 허용 동작(예: `submit_normalization_proposal`) |
| `INTELLIGENCE_CORS_ALLOWED_ORIGINS` | 아니오 | *(없음 — 교차 출처 거부)* | 허용 origin 목록 |

```dotenv
INTELLIGENCE_API_BIND=0.0.0.0:8010
INTELLIGENCE_INBOUND_AUTH_MODE=shared-token
INTELLIGENCE_INBOUND_SERVICE_TOKEN=replace-with-a-strong-random-secret
INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID=service:intelligence-client
INTELLIGENCE_INBOUND_SERVICE_TENANT_ID=tenant:production
INTELLIGENCE_INBOUND_SERVICE_PRODUCT_ID=foundation-platform
INTELLIGENCE_INBOUND_SERVICE_ACTIONS=submit_normalization_proposal
INTELLIGENCE_CORS_ALLOWED_ORIGINS=https://app.example.com
```

### Admission control

세 가지 설정값이 부하 차단·본문 크기·요청별 deadline을 제어한다.
Health endpoint(`/healthz`, `/readyz`, `/metrics`)는 admission 계층 밖에 있어 부하 차단이나 동시성
제한의 대상이 아니다.

| Variable | Default | Description |
|----------|---------|-------------|
| `INTELLIGENCE_MAX_BODY_BYTES` | `1048576` (1 MiB) | 초과 요청은 413 |
| `INTELLIGENCE_REQUEST_TIMEOUT_SECONDS` | `30` | wall-clock 초과 요청은 504 |
| `INTELLIGENCE_MAX_CONCURRENCY` | `128` | semaphore가 차면 503 |

Overload response semantics:

| Status | Condition |
|--------|-----------|
| 401 | Missing or wrong `Authorization: Bearer` token |
| 413 | Body exceeds `INTELLIGENCE_MAX_BODY_BYTES` |
| 422 | Idempotency key reused with a different payload |
| 429 | Per-tenant/subject route rate limit exceeded; includes `Retry-After` |
| 503 | Global concurrency cap (`INTELLIGENCE_MAX_CONCURRENCY`) saturated |
| 504 | Request exceeded `INTELLIGENCE_REQUEST_TIMEOUT_SECONDS` |

### 영속 상태

`DATABASE_URL`이 있으면 정규화 outbox와 감사 로그가 Postgres adapter를 사용하고 접속 시 마이그레이션을
자동 실행한다. 없으면 API는 프로세스 내부 메모리 저장소로 대체된다.

**메모리 대체 경로는 loopback 개발 전용이다. 여러 replica를 실행하면 각 프로세스가 별도 저장소를
가지므로 안전하지 않다.**

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | *(absent — in-memory fallback)* | Postgres connection string. |
| `DATABASE_TIMEOUT_SECONDS` | `10` | Connection and query timeout in seconds. |
| `DATABASE_MAX_CONNECTIONS` | `10` | Maximum pool connections (must be > 0). |

```dotenv
DATABASE_URL=postgres://user:pass@db.internal:5432/intelligence
DATABASE_TIMEOUT_SECONDS=10
DATABASE_MAX_CONNECTIONS=10
```

### Outbox drain worker

drain worker는 대기 중인 outbox를 claim해 Foundation Platform으로 보내는 별도 binary다.
`DATABASE_URL`이 있을 때 API와 함께 실행한다. **`DATABASE_URL`은 필수**이며 메모리 outbox에서는
시작을 거부한다.

```bash
cargo run -p intelligence-worker --bin normalization_outbox_drain_worker
```

| Variable | Default | Description |
|----------|---------|-------------|
| `NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE` | `4` | Records claimed per drain cycle. |
| `NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS` | `60` | Delivery lease duration per claimed record. |
| `NORMALIZATION_OUTBOX_MAX_ATTEMPTS` | `8` | Maximum delivery attempts before dead-lettering. |
| `NORMALIZATION_OUTBOX_DRAIN_IDLE_SECONDS` | `2` | Sleep between polls when the outbox is empty. |

**Lease와 batch 불변식:**
`NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE * FOUNDATION_PLATFORM_TIMEOUT_SECONDS` stays
`NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE * FOUNDATION_PLATFORM_TIMEOUT_SECONDS`가
`NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS`보다 충분히 작아야 한다. batch 끝부분이 lease를 넘기면
다른 worker가 레코드를 재획득해 중복 전달을 시도한다. Foundation의 `Idempotency-Key` 중복 제거는
최후의 방어선이지만 batch를 작게, lease를 넉넉하게 잡으면 경합 자체를 피할 수 있다.

### Other worker binaries

`intelligence-worker`는 drain worker 외에 다음 세 binary를 제공한다.

```bash
cargo run -p intelligence-worker --bin building_register_floor_normalization
cargo run -p intelligence-worker --bin building_register_unit_normalization
cargo run -p intelligence-worker --bin foundation_knowledge_consumer
```

- `building_register_floor_normalization` — 건축물대장 **층** 정규화 제안 작업(generate → validate →
  Foundation submitter 제출). env로 dry-run을 지원한다.
- `building_register_unit_normalization` — 건축물대장 **단위** 정규화 제안 작업.
- `foundation_knowledge_consumer` — Foundation 지식 원천 이벤트(Kafka·Karapace schema, DLQ)를
  Postgres 지식 원천 registry로 소비한다. 현재 Foundation 쪽 producer는 없으며 기본 topic은 fixture 상수다.

### Observability

| Endpoint | Auth required | Admission | Notes |
|----------|---------------|-----------|-------|
| `GET /healthz` | 없음 | 제외 — 2초 timeout, 1 KiB 본문 | 프로세스 생존 확인. 실행 중이면 항상 200 |
| `GET /readyz` | 없음 | 제외 — 2초 timeout, 1 KiB 본문 | 설정 기반 준비 상태. model gateway나 Foundation submitter가 없으면 503 |
| `GET /metrics` | `shared-token`이면 bearer 필요 | 부하/동시성 제외 — 2초 timeout, 1 KiB 본문 | Prometheus 형식. LLM 지연 bucket에 30초·60초 포함 |

`/metrics`는 부하 차단·동시성 계층 밖의 주 포트에서 제공하므로 포화 중에도 Prometheus scrape가
동작한다. 별도 loopback listener로 옮기는 일은 C3로 미룬다.

## Commands

workspace는 루트 `rust-toolchain.toml`로 Rust `1.96.0`을 고정한다(영역 내부 toolchain 파일은
루트 guard가 금지한다).

Rust를 로컬에 설치한 뒤 다음을 실행한다.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p intelligence-api
```

## Current Endpoints

- `GET /healthz`
- `GET /readyz`
- `GET /metrics` (bearer-authenticated in `shared-token` mode; see Observability)
- `GET /v1/models`
- `POST /v1/chat/completions`
- `POST /intelligence/v1/normalization/validate-proposal`
- `POST /intelligence/v1/normalization/generate-and-validate`
- `POST /intelligence/v1/normalization/generate-validate-submit`
- `POST /intelligence/v1/normalization/submit-proposal`

플랫폼 전용 route는 루트 ADR-0001 §6에 따라 `/intelligence/v1/...` 아래에 둔다.
OpenAI 호환 표면(`/v1/models`, `/v1/chat/completions`)은 생태계가 요구하는 경로이므로 예외로 기록한다.

`/v1/chat/completions`는 정책이 적용된 chat 경계다. OpenAI 호환 비스트리밍 요청을 받고
`ko-KR` 답변 정책을 주입한 뒤 모델 출력을 검증한다. 첫 답변이 한글 출력 validator를 통과하지
못하면 한 번만 보정 호출한다. 앱은 Open WebUI를 직접 호출하지 말고 이 endpoint를 사용한다.

model proposal generator가 설정되기 전까지 generation endpoint는 `501`을 반환한다.
Foundation Platform submitter가 설정되기 전까지 submission endpoint도 `501`을 반환한다.

## Foundation Platform Submission

Configure these environment variables before starting the Rust API:

```dotenv
FOUNDATION_PLATFORM_BASE_URL=http://127.0.0.1:18080
FOUNDATION_PLATFORM_NORMALIZATION_PATH=/internal/normalization/proposals
FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE=/run/secrets/foundation-workload-token
```

token 파일에는 Intelligence Platform runtime용으로 발급된 Zitadel workload bearer가 있어야 한다.
Rust API는 시작할 때 읽어 bearer authorization header만 보낸다. 정적 service token이나 workload가
아닌 자격증명은 받지 않는다. Foundation Platform base URL을 설정하면 token 파일도 필수이며,
없거나 읽을 수 없을 때 즉시 시작을 실패한다.

제출 흐름은 제안을 먼저 검증하고 잘못된 제안은 건너뛴다. idempotency key로 enqueue한 뒤 Foundation에
전송하고 이미 전송된 레코드는 중복 제거한다.

모든 proposal POST는 outbox idempotency key(`{tenant_id}:{target_kind}:{raw_record_id}:{schema_version}`)와
같은 값을 가진 `Idempotency-Key` header를 보낸다. Foundation Platform은 IETF Idempotency-Key draft에
따라 이 header를 서버 측 exactly-once 수신 중복 제거에 사용할 수 있다. outbox drain worker의 재전송도
같은 key를 쓰므로 Intelligence 쪽에 proposal 상태를 저장하지 않고도 Foundation이 retry를 안전하게
중복 제거한다.

## Model Runtime

Configure these environment variables to enable AI proposal generation:

```dotenv
INTELLIGENCE_API_BIND=0.0.0.0:8010
INTELLIGENCE_INBOUND_AUTH_MODE=shared-token
INTELLIGENCE_INBOUND_SERVICE_TOKEN=replace-with-a-strong-random-secret
INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID=service:intelligence-client
INTELLIGENCE_INBOUND_SERVICE_TENANT_ID=tenant:production
INTELLIGENCE_INBOUND_SERVICE_PRODUCT_ID=foundation-platform
INTELLIGENCE_INBOUND_SERVICE_ACTIONS=submit_normalization_proposal
MODEL_RUNTIME_BASE_URL=http://<model-runtime-host>:11434
MODEL_RUNTIME_CHAT_PATH=/v1/chat/completions
MODEL_RUNTIME_DEFAULT_MODEL=gemma2:9b
MODEL_RUNTIME_PROFILE_ID=normalization-ko
MODEL_RUNTIME_API_KEY=optional-token
# For reasoning-first models such as Qwen 3.6, set this so message.content is
# populated with the final answer instead of spending the response on reasoning.
MODEL_RUNTIME_REASONING_EFFORT=none
```

로컬 예시 profile은 `config/local-ollama.env.example`에 있다.
Load it through the deployment environment or secret/config mechanism; it is
configuration data, not an executable production wrapper. Any
non-loopback bind requires the two `INTELLIGENCE_INBOUND_*` auth variables; see
the **Enterprise Runtime C0-C1** section for the full fail-closed guard rules.

runtime은 OpenAI 호환 chat completions 형태를 사용한다. base URL은 Ollama·vLLM 또는 다른
호환 model gateway를 가리킬 수 있지만 앱은 여전히
`intelligence-platform`, not the model runtime. `MODEL_GATEWAY_*` names are
still accepted as a deprecated compatibility alias, but new deployments should
use `MODEL_RUNTIME_*`.

`MODEL_RUNTIME_REASONING_EFFORT` is optional. Use `none` for reasoning-first
models when the application expects the final JSON or text in
`choices[].message.content`.

Example policy-enforced chat call after starting `cargo run -p intelligence-api`:

```bash
curl --fail-with-body \
  --request POST \
  --url http://127.0.0.1:8010/v1/chat/completions \
  --header "Authorization: Bearer ${INTELLIGENCE_INBOUND_SERVICE_TOKEN}" \
  --header 'Content-Type: application/json' \
  --data @- <<'JSON'
{
  "model": "gemma2:9b",
  "messages": [{"role": "user", "content": "짧게 자기소개해 주세요."}],
  "temperature": 0.2,
  "max_tokens": 256
}
JSON
```

`gemma-ko` 같은 숨겨진 한국어 별칭을 운영 동작의 기준으로 삼지 않는다.
Korean behavior belongs to the chat policy, validator, and repair flow exposed by
the intelligence platform.

### Temporary Open WebUI Connection

현재 로컬 Open WebUI(`http://<model-runtime-host>:8080`)에는 Ollama를 직접 가리키지 않고
intelligence platform을 가리키는 OpenAI-compatible connection을 설정한다.
`<intelligence-api-host>`는 `intelligence-api`가 실행 중인 machine의 LAN address다.

```text
Base URL: http://<intelligence-api-host>:8010/v1
API Key: local-dev
Model: gemma2:9b
```

이 설정은 임시 bridge로만 사용한다. 최종 제품 UI는 `intelligence-platform`을 직접 호출하고
Open WebUI는 개발 도구로 남긴다.

## LangChain And LangGraph

LangChain과 LangGraph는 이 Rust 플랫폼의 런타임 의존성이 아니다. LangChain은 LLM 앱과
agent를 빠르게 조합할 때 유용하고 LangGraph는 내구성·상태 보존·사람 검토형 agent 실행의
참고 모델로 유용하다. Rust 플랫폼은 명시적 계약, outbox 상태, 멱등성, 검토 경계로 해당
아키텍처 아이디어만 채택한다.
