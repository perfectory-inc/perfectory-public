---
status: current
owner: intelligence-platform
doc_type: README
last_reviewed: 2026-11-05
---

# schemas/

인텔리전스 플랫폼이 발행하는 이벤트의 Avro 스키마 파일(`.avsc`)을 둔다.

## 포함 내용

버전이 붙은 이벤트 토픽마다 파일 하나:

| 파일 | 토픽 | Kafka 키 |
|------|-------|-----------|
| `intelligence.normalization-proposal.submission-requested.v1.avsc` | `intelligence.normalization-proposal.submission-requested.v1` | `aggregate_id` (= `idempotency_key`) |
| `intelligence.dead-letter.v1.avsc` | `intelligence.dead-letter.v1` | 실패한 원본 메시지의 키 |

토픽 이름 상수는 `crates/intelligence-contracts/src/event_envelope.rs`가 정본이다.

## 스키마 변경 규칙 — 이전 판 전부와 하위 호환(`BACKWARD_TRANSITIVE`)

이 디렉터리의 모든 스키마는 **`BACKWARD_TRANSITIVE`** 호환성 규칙을 따른다.

규칙:
- **버전 안에서는 더하기만 허용:** 새 필드에는 Avro `default`가 있어야 하며 기존 필드 뒤에만
  더한다. 이전 스키마 버전의 소비자는 새 필드를 조용히 무시하고 새 버전의 생산자는
  그 값을 채운다.
- **게시된 필드는 절대 바꾸지 않는다:** 스키마 파일을 `main`에 커밋한 뒤에는 필드의
  이름·삭제·타입·순서를 바꾸지 않는다.
- **호환성을 깨는 변경 = 새 파일 + 새 토픽:** 구조 변경(이름 바꾸기, 타입 바꾸기, 필드 삭제)이
  불가피하면 새 파일(예: `.v2.avsc`)과 새 토픽을 만들고, 합의한 이전 기간 동안 옛·새
  토픽을 나란히 돌린 뒤 v1 토픽을 폐기한다.

## 스키마 레지스트리(Karapace)

**`TopicNameStrategy`**로 **Karapace**(Confluent 호환 스키마 레지스트리)에 등록한다. 등록 주제
이름은 `<topic>-value`다(예: `intelligence.normalization-proposal.submission-requested.v1-value`).

생산자는 Avro 본문 앞에 5바이트 Confluent 전송 형식 접두사
(`\x00 + schema_id_int32_big_endian`)를 직렬화한다. 소비자는 같은 레지스트리로 읽을 때 스키마 ID 를
해석한다.

## 계약 시험

`crates/normalization/intelligence-normalization-application/tests/event_schema_contract.rs` 가 스키마와 코드의 호환을 다음으로 고정한다
(실패 메시지 스키마는 `crates/messaging/messaging-infrastructure/tests/codec_contract.rs`가 같은 방식으로 왕복한다):

1. 시험 때 `.avsc` 파일을 `apache_avro::Schema::parse_str` 로 해석한다.
2. `NormalizationOutboxRecord` 고정물을 직렬화 → 역직렬화로 끝까지 왕복한다.
3. 해석한 스키마의 모든 필드가 필수 필드 집합에 있거나 Avro `default`를 갖는지 확인한다.
   이것이 더하기만 하는 진화의 걸림 장치이며, `default` 없는 필드를 더하면 시험이 바로 실패한다.

실행: `cargo test -p intelligence-normalization-application --test event_schema_contract`.

## C2 실제 이벤트 백본 검증

다음 명령은 인텔리전스 플랫폼 워크스페이스 루트에서 실행한다.

Compose 기본값:

- Kafka: `127.0.0.1:19092`
- Karapace: `http://127.0.0.1:18081`

Windows 에서는 `rdkafka` 빌드를 위해 동작하는 `cmake-build` 도구 사슬이 필요하다: VS BuildTools(MSVC)와
BuildTools 의 CMake·Ninja, 또는 그에 맞먹는 구성. 지금의 모듈·플랫폼 경계는
`../docs/architecture.md` 에 있다.

로컬 의존성 시작:

```bash
docker compose -f docker/c2-event-backbone.compose.yml up -d
```

실제 이벤트 시험 실행:

```bash
INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
INTELLIGENCE_TEST_KARAPACE_URL=http://127.0.0.1:18081 \
cargo test -p messaging-infrastructure --test live_kafka_karapace -- --nocapture
```

호스트 포트가 이미 사용 중이면 `docker compose up` 전에 값을 바꾼다. 예를 들어 `18081`이
사용 중이면 다음처럼 Karapace 포트를 바꾼다.

```bash
INTELLIGENCE_TEST_KARAPACE_HOST_PORT=18082 \
docker compose -f docker/c2-event-backbone.compose.yml up -d
INTELLIGENCE_TEST_KARAPACE_URL=http://127.0.0.1:18082 \
cargo test -p messaging-infrastructure --test live_kafka_karapace -- --nocapture
```

필요하면 Kafka 포트도 바꿀 수 있다.

```dotenv
INTELLIGENCE_TEST_KAFKA_HOST_PORT=19093
INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19093
```

의존성 중지:

```bash
docker compose -f docker/c2-event-backbone.compose.yml down -v
```
