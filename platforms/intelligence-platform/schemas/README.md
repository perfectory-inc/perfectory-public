---
status: current
owner: intelligence-platform
doc_type: README
last_reviewed: 2026-07-29
---

# schemas/

intelligence-platform이 발행하는 이벤트의 Avro 스키마 파일(`.avsc`)을 둔다.

## 포함 내용

One file per versioned event topic:

| File | Topic | Kafka key |
|------|-------|-----------|
| `intelligence.normalization-proposal.submission-requested.v1.avsc` | `intelligence.normalization-proposal.submission-requested.v1` | `aggregate_id` (= `idempotency_key`) |

## 스키마 변경 규칙 — BACKWARD_TRANSITIVE

이 디렉터리의 모든 스키마는 **BACKWARD_TRANSITIVE** 호환성 규칙을 따른다.

규칙:
- **버전 안에서는 추가만 허용:** 새 field에는 Avro `default`가 있어야 하며 기존 field 뒤에만
  추가한다. 이전 schema version의 consumer는 새 field를 조용히 무시하고 새 version의 producer는
  그 값을 채운다.
- **게시된 field는 절대 변경하지 않는다:** schema 파일을 `main`에 커밋한 뒤에는 field의
  이름·삭제·타입·순서를 바꾸지 않는다.
- **호환성 파괴 변경 = 새 file + 새 topic:** 구조적 변경(rename, type change, field removal)이
  불가피하면 새 file(예: `.v2.avsc`)과 새 topic을 만들고, 합의한 migration 기간 동안 구·신
  topic을 병렬 실행한 뒤 v1 topic을 폐기한다.

## 스키마 레지스트리(C2 계획)

 C2에서는 **TopicNameStrategy**로 **Karapace**(Confluent 호환 스키마 레지스트리)에 등록한다. 등록 subject
이름은 `<topic>-value`다(예: `intelligence.normalization-proposal.submission-requested.v1-value`).

Producer는 Avro payload 앞에 5바이트 Confluent wire-format 접두사
(`\x00 + schema_id_int32_big_endian`)를 직렬화한다. Consumer는 같은 레지스트리로 읽을 때 schema ID를
해석한다.

## 계약 테스트

`crates/normalization/intelligence-normalization-application/tests/event_schema_contract.rs` pins schema-to-code compatibility by:

1. Parsing the .avsc file at test time via `apache_avro::Schema::parse_str`.
2. Performing a full serialize → deserialize round-trip of a `NormalizationOutboxRecord` fixture.
3. parse한 schema의 모든 field가 required-fields set에 있거나 Avro `default`를 갖는지 확인한다.
   이것이 additive-evolution tripwire이며 default 없는 field를 추가하면 test가 즉시 실패한다.

실행: `cargo test -p intelligence-normalization-application --test event_schema_contract`.

## C2 실 이벤트 백본 검증

다음 명령은 Intelligence Platform workspace 루트에서 실행한다.

Compose 기본값:

- Kafka on `127.0.0.1:19092`
- Karapace on `http://127.0.0.1:18081`

Windows에서는 `rdkafka` 빌드를 위해 동작하는 `cmake-build` 도구체인이 필요하다.
`rdkafka`: use VS BuildTools/MSVC plus BuildTools CMake and BuildTools Ninja,
or an equivalent setup. The current module and platform boundary is documented
in `../docs/architecture.md`.

로컬 의존성 시작:

```bash
docker compose -f docker/c2-event-backbone.compose.yml up -d
```

실 이벤트 테스트 실행:

```bash
INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
INTELLIGENCE_TEST_KARAPACE_URL=http://127.0.0.1:18081 \
cargo test -p messaging-infrastructure --test live_kafka_karapace -- --nocapture
```

호스트 포트가 이미 사용 중이면 `docker compose up` 전에 값을 바꾼다. 이 호스트에서는 `18081`이
사용 중이므로 다음처럼 Karapace 포트를 바꾼다.

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
