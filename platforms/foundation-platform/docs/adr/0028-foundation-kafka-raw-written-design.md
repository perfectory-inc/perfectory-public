# Foundation `collection.raw_written` Kafka 전달기

<!-- public-repository-safety: reviewed-public-contract -->

## Status

커밋 후 선택적으로 켜는 어댑터로 구현했다. 운영 전환은 broker·schema registry·topic/ACL·소비자
준비 증거를 통과해야 한다.

## 목표

Foundation 수집 claim-check 이벤트 `catalog.collection.raw_written.v1`을 실제 Kafka 호환 broker로
발행하되 수집 코드나 JobBus가 Kafka에 직접 의존하지 않게 한다. Postgres는 발행 이벤트 정본이고 R2는
원자료 바이트 저장소이며 기존 outbox 재시도·격리 계약이 최종 기준이다.

## 배경과 제약

Foundation에는 이미 필요한 경계가 있다:

- `OutboxRawWrittenSink`가 typed collection event를 `catalog.outbox_event`에 기록한다.
- `OutboxWorker`가 미발행 row를 claim하고 `EventBroadcaster`를 호출하며, broadcaster가
  성공을 반환한 뒤에만 row를 published로 표시한다.
- `CollectionRawWrittenV1`은 claim-check contract다. R2 object pointer, checksum, count,
  lineage만 담고 raw provider byte는 담지 않는다.
- `JobBus`는 의도적으로 transport-neutral이며 이 단계에서는 JSONL/in-memory로 남는다.
- 현재 fallback broadcaster는 webhook 또는 logging이다. `foundation-outbox`가 opt-in
  Kafka producer adapter를 소유하며 collection·JobBus crate는 Kafka에 의존하지 않는다.

이 설계는 대규모 배포의 안정적인 부분만 가져오며 전체 인프라를 복사하지 않는다.

- Zapier는 내구성 있는 local outbox, 비동기 poller, Avro/Schema Registry, at-least-once
  Kafka 전달을 문서화했다. local outbox의 scale·운영 비용을 겪은 뒤 내구성 fallback을
  S3/SQS 쪽으로 옮겼다.
- Shopify·Uber·Yelp는 partition key, schema governance, replay, materialized downstream
  view를 갖춘 CDC-to-Kafka system을 문서화했다. 이는 후속 scale 경로이지 현재
  transactional outbox를 우회할 이유가 아니다.
- Debezium Outbox Event Router는 미래 CDC adapter 목표다. 따라서 event contract와 topic
  이름은 향후 Kafka Connect migration과 호환되어야 한다.

## 검토한 대안

### A. Kafka 어댑터를 붙인 Foundation polling publisher (선택)

`foundation-outbox`에 `KafkaEventBroadcaster`를 추가한다. 기존 worker가 유일한 DB poller로
남아 claim·publish·retry·quarantine을 수행한다. adapter는 지원 event를 Kafka에 발행하고
broker acknowledgement 후에만 반환한다. 현재 repository에서 실행 가능한 가장 작은 경로이며
향후 Debezium으로 옮길 깨끗한 경계를 보존한다.

### B. 지금 Debezium/Kafka Connect 도입

Debezium connector로 `catalog.outbox_event`를 capture하고 Outbox Event Router SMT로
route한다. 이는 high-volume 운영 모델로 선호하지만, 현재 Foundation에 없는 Kafka Connect,
connector 설정, offset storage, deployment ownership, CI orchestration이 필요하다. 후속
adapter이며 이 단계의 범위가 아니다.

### C. 수집 worker의 Kafka 직접 쓰기

기각한다. DB/R2/Kafka 이중 쓰기 구간을 만들고 Bronze 수집을 broker 가용성에 묶으며 기존
`RawWrittenSink`·outbox 경계를 위반한다.

## 아키텍처

```text
Bronze/R2 write
      |
      v
collection worker --(same success path)--> catalog.outbox_event (Postgres SSOT)
                                                |
                                                v
                                      OutboxWorker claim/lease
                                                |
                                                v
                              KafkaEventBroadcaster (raw_written)
                               |                 |
                               |                 +--> Karapace schema registration
                               v
                        Kafka topic (Avro)
```

event row는 Kafka producer delivery future가 성공한 뒤에만 acknowledge한다. Kafka acknowledge
후 `published_at`을 쓰기 전에 process가 중단되면 duplicate가 생길 수 있으며 이는 의도한
at-least-once 동작이다. 안정적인 outbox `event_id`를 Kafka value에 넣고 consumer가 중복 제거한다.

### 이벤트 라우팅

- 지원 Kafka event type: `catalog.collection.raw_written.v1`.
- canonical topic: `foundation-platform.catalog.collection-raw-written.v1`.
- schema subject: Karapace TopicNameStrategy를 사용하는 `<topic>-value`.
- key: `scope_unit_id`; scope별 순서를 보존하고 기존 collection event contract와 맞춘다.
- value: Confluent Avro wire format(`0x00` magic byte + 4-byte schema id + Avro payload).
- schema artifact: `platforms/foundation-platform/schemas/foundation-platform.catalog.collection-raw-written.v1.avsc`,
  record name `CollectionRawWrittenEnvelopeV1`, namespace `kr.perfectory.foundation.catalog`.
- Avro field order and mapping are fixed for v1: `event_id` (string, canonical hyphenated UUID from the outbox envelope), `event_type` (string, default `catalog.collection.raw_written.v1`), `specversion` (string, default `1.0`), `source` (string, default `/foundation-platform/collection`), `schema_version` (int from the typed payload), `collection_snapshot_id` (string), `job_id` (string), `scope_unit_id` (string), `provider` (string), `endpoint` (string), `endpoint_slug` (string), `bronze_object_key` (string), `bronze_object_count` (long), `bronze_checksum_sha256` (string), `bronze_size_bytes` (long), `source_record_count` (long), `request_count` (long), `request_fingerprint_sha256` (string), `request_fingerprint_schema_version` (string), `license` (`["null","string"]`, default null), `srid` (`["null","string"]`, default null), `reused_bronze_object` (boolean, default false), `fetched_at_utc` (timestamp-millis long from the typed payload), `event_occurred_at` (timestamp-millis long from the typed payload), and `outbox_occurred_at` (timestamp-millis long from `EventEnvelope.occurred_at`).
- UUID는 string으로 encode한다. 모든 Rust `u64` count는 Avro 변환 전에 `i64::MAX`에
  들어가야 하며 아니면 발행을 실패시키고 기존 quarantine 경로를 따른다.
  `DateTime<Utc>`는 `timestamp_millis()`로 변환하고 typed payload timestamp와 outbox row
  timestamp를 별도 field로 보존한다.
- raw provider byte와 전체 Bronze row는 Kafka에 들어가지 않는다.

다른 catalog event type은 설정된 legacy fallback broadcaster를 계속 사용한다. 조합은
명시적이다. `CatalogEventBroadcaster`가 vector-manifest event를 계속 가로채고 fallback은
`KafkaEventBroadcaster`, 그 fallback은 기존 webhook/logging broadcaster다. `raw_written`은
Kafka를 먼저 시도한다. Kafka가 실패하면 legacy fallback을 호출하지 않고 worker가 row를
재시도한다. Kafka가 성공하고 legacy dual-publish를 켠 상태에서 legacy가 실패하면 worker는
실패를 보고하고 다음 시도에서 Kafka record가 중복 발행될 수 있다. 이는 허용된 at-least-once
동작이며 안정적인 `event_id`가 deduplication key다. dual-publish를 끄면 성공한 Kafka
발행만으로 row가 완료된다. `raw_written` 이외 event는 항상 legacy fallback만 사용한다.

### 설정과 출시 게이트

Kafka는 opt-in이며 켜면 fail-closed다.

- `FOUNDATION_PLATFORM_KAFKA_ENABLED` (default `false`).
- `FOUNDATION_PLATFORM_RUNTIME_ENV`(`local`, `ci`, `staging`, `production`)는 Kafka가 켜진
  경우 필수다. Kafka adapter가 이를 직접 enforce하므로 library caller가 publisher runtime
  경계를 우회할 수 없다.
- `FOUNDATION_PLATFORM_KAFKA_BOOTSTRAP_SERVERS` (required when enabled).
- `FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_URL` (required when enabled).
- `FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC` (default canonical topic).
- `FOUNDATION_PLATFORM_KAFKA_CLIENT_ID` (default stable service id).
- `FOUNDATION_PLATFORM_KAFKA_MESSAGE_TIMEOUT_MS` (positive) and `FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_TIMEOUT_SECONDS` (positive).
- `FOUNDATION_PLATFORM_KAFKA_SECURITY_PROTOCOL` (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`; default `PLAINTEXT`).
- `FOUNDATION_PLATFORM_KAFKA_SASL_MECHANISM`, `FOUNDATION_PLATFORM_KAFKA_SASL_USERNAME`, and `FOUNDATION_PLATFORM_KAFKA_SASL_PASSWORD` when a SASL protocol is selected.
- `FOUNDATION_PLATFORM_KAFKA_SSL_CA_LOCATION`, `FOUNDATION_PLATFORM_KAFKA_SSL_CERTIFICATE_LOCATION`, and `FOUNDATION_PLATFORM_KAFKA_SSL_KEY_LOCATION` for file-based broker TLS when required.
- `FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_USERNAME` and `FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_PASSWORD` for optional Karapace HTTP Basic Auth. HTTPS uses the host trust store; a future CA-bundle option is a separate change.
- `FOUNDATION_PLATFORM_KAFKA_DUAL_PUBLISH_LEGACY` (default `true` during rollout; set `false` after consumer cutover evidence).

Kafka topic 생성은 외부 provisioning 단계다. adapter는 `allow.auto.create.topics=false`로
설정하고 topic이 없으면 startup/첫 발행에서 실패한다. 운영 provisioning은 최소 3 partition,
broker topology가 허용하는 경우 replication factor 3, `cleanup.policy=delete`, 명시적
retention/ACL policy를 사용해야 한다. 단일 node CI broker는 partition 1개·replica 1개를
사용한다. Kafka credential은 commit하지 않으며 TLS/SASL property는 environment 설정으로
전달하고 log에 남기지 않는다.

## 구성요소와 책임

### `foundation-outbox` Kafka 어댑터

- `KafkaEventBroadcaster`, configuration validation, Karapace schema registration, Avro value
  변환, producer 전달을 소유한다.
- Kafka client는 이 infrastructure crate 안에서만 의존하며 collection domain/application
  crate는 Kafka를 알지 못한다.
- 발행 전에 잘못된 `raw_written` payload를 거부하고 `PublishError::Broadcaster`를 반환해
  기존 retry/quarantine 경로가 실패를 기록하게 한다.
- producer timeout은 제한하고 worker 경로에서 무제한 buffering을 사용하지 않는다.

### Publisher 서비스 구성

- startup에서 opt-in Kafka 설정을 parse한다.
- Kafka가 켜졌을 때만 위에서 설명한 wrapper chain을 만들고, 아니면 기존 fallback을 그대로
  유지한다.
- 기존 vector-tile manifest pointer 동작을 바꾸지 않는다.
- Kafka가 꺼진 경우 `run-publisher`와 `publish-once` 동작을 동일하게 유지한다.

### 스키마 산출물

위 path와 record identity로 `catalog.collection.raw_written.v1` append-only Avro schema 하나를
commit한다. 초기 field는 위에 적은 type/default를 그대로 사용하고 이후 field는 default를
갖는 방식으로만 추가한다. 호환성은 `BACKWARD_TRANSITIVE`다. 호환되지 않는 변경은 새
schema version과 topic이 필요하며 발행된 field를 삭제하거나 type을 바꾸지 않는다.

## 상위 ADR과의 관계

Foundation ADR-0013과 ADR-0016이 Bronze commit protocol의 권위 있는 기준이다. Kafka는
Bronze write dependency가 아니고 raw byte는 R2에 남으며 ledger가 SSOT다. 이 설계는
명시적으로 opt-in하는 **post-commit outbox adapter**이지 해당 ADR을 뒤집거나 전국 수집
rollout gate로 만드는 것이 아니다. 이 ADR은 제한된 adapter, topic/schema contract, 향후
Debezium migration trigger를 기록한다.

## 오류 처리와 전달 의미

1. Kafka가 켜졌을 때 configuration/schema registration 오류는 startup을 실패시킨다.
2. Serialization·schema·transport·delivery 오류는 broadcaster error로 반환한다.
3. `OutboxWorker`가 retry count를 늘리고 lease를 풀며 다음 tick에 재시도한다.
4. retry를 소진하면 `catalog.outbox_quarantine`에 기록하고 event를 조용히 버리지 않는다.
5. Kafka 전달 성공 뒤 DB 오류가 나면 재시도에서 중복될 수 있다. event id가 idempotency key이며
   exactly-once를 주장하지 않는다.
6. Kafka broker 장애가 commit된 Postgres outbox row를 잃게 하거나 Bronze write를 막지 않는다.

## 테스트와 운영 증거

### 단위·계약 테스트(일반 Cargo 검증)

- configuration validation: 필수 endpoint, 양수 timeout, topic/client id, 잘못된 조합.
- Avro conversion: 모든 `CollectionRawWrittenV1` field, timestamp-millis encoding, event id,
  key 선택, claim-check 규칙.
- schema subject/topic 상수와 backward-compatible fixture 검증.
- routing: Kafka는 `raw_written`만 처리하고 fallback은 다른 event type과 선택적 legacy
  dual-publish 호출을 받는다.
- 기존 outbox worker test는 recording broadcaster로 retry/quarantine 동작을 계속 검증한다.

### 실제 통합 테스트

이름을 명시한 live test 3개를 구현했다. `live_kafka_karapace.rs`는 Redpanda/Karapace에
직접 연결해 schema registration, Avro wire encoding, unique-key publish, consume, schema-id
검증, offset commit을 실행한다. `live_kafka_outbox_roundtrip.rs`는 `DATABASE_URL`이
필요하며 실제 `catalog.outbox_event` row를 insert하고 `run-publisher`와 같은 publisher
조합을 만든 뒤 `OutboxWorker::tick()`을 호출해 결과 record를 consume하고 `published_at`
설정을 확인한다. `live_kafka_outage.rs`는 연결할 수 없는 bootstrap에 실제 librdkafka
producer를 구성하고 retry, 미발행 상태, retry 한도의 quarantine을 확인한다. 세 test 모두
일반 credential-free Cargo 검증에서는 `#[ignore]`다. `FOUNDATION_TEST_KAFKA_REQUIRED=1`이나
필수 `DATABASE_URL`이 없으면 print-and-return하지 않고 빠르게 실패한다.

image digest를 복제하지 말고 `platforms/intelligence-platform/docker/c2-event-backbone.compose.yml`의
고정 Redpanda+Karapace compose 정의를 재사용한다. test 옆에 Foundation command와
environment 이름을 문서화한다.

### CI와 로컬 실행

- 일반 `cargo xtask verify foundation`은 credential-free로 유지하며 Kafka를 요구하지 않는다.
- 별도 `kafka-integration` Foundation job이 고정 compose stack을 시작하고 partition 1개인
  CI topic을 명시적으로 만든다. broker/registry 준비를 기다린 뒤
  `FOUNDATION_TEST_KAFKA_REQUIRED=1`을 설정하고 `scripts/verify/foundation-kafka-live.sh`로
  ignore된 live test 3개를 실행하며 `always` cleanup 단계에서 stack을 내린다. service가
  없거나 test가 실패하면 required workflow gate가 실패한다.
- local 개발자는 cloud credential 없이 폐기 가능한 Postgres DB로 같은 script를 실행한다.
  이후 managed Kafka 배포는 bootstrap/TLS/SASL 값만 제공한다.

## 범위 밖

- 이 Kafka relay 단계는 `PostgresJobBus`를 national executor 기본값으로 선택하거나 만들지
  않는다. adapter와 real-Postgres contract test는 ADR-0013과 migration
  `20260725000001_collection_job_bus.sql`에서 추적한다.
- national collection executor scheduling이나 direct-loop 동작을 바꾸지 않는다.
- raw R2 object를 Kafka로 복제하지 않는다.
- Silver/Gold, Trino, Spark, Iceberg, LLM normalization을 바꾸지 않는다.
- 아직 Debezium/Kafka Connect를 배포하지 않는다.
- exactly-once 전달이나 자동 consumer cutover를 주장하지 않는다.

## Acceptance criteria

1. Kafka가 꺼지면 기존 Foundation test와 publisher 동작이 변하지 않는다.
2. Redpanda+Karapace가 실행 중이고 Kafka를 켜면 commit된
   `catalog.collection.raw_written.v1` row가 올바른 key와 claim-check field를 가진
   schema-registered Avro record로 발행된다.
3. broker 장애가 발생하면 outbox row는 미발행 상태로 남고 유실 대신 retry/quarantine을
   수행한다.
4. CI가 real/emulated broker에서 live test 3개를 실행하고 broker나 schema registry가 없으면
   실패한다.
5. collection crate는 `rdkafka`를 import하지 않고 outbox infrastructure adapter만 import한다.
6. event contract와 topic은 향후 Debezium Outbox Event Router migration에서 계속 사용할 수 있다.
