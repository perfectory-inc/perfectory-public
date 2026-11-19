# ADR 0587: 발행자가 없는 Intelligence 이벤트 백본은 지우고, Kafka 는 발행자가 생길 때 그 라이브러리와 함께 돌아온다

- Status: Accepted
- Date: 2026-11-19
- Supersedes: [ADR-0215](./0215-a-topics-avro-schema-belongs-to-its-producer-and-consumers-only-look-it-up.md)
- Amends: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) §3 (Intelligence 의 Redpanda 구성을 공용 Kafka 로 옮긴다는 문장)
- Builds on: [ADR-0477](./0477-foundation-hands-intelligence-documents-and-place-ids-and-takes-back-cited-facts-in-tables-only-intelligence-writes.md)(두 플랫폼 사이는 표로만)

## Context

Intelligence 에는 Kafka 이벤트 백본("C2")이 있었다.

- `crates/messaging/messaging-infrastructure`(소스 1,008 줄): rdkafka 생산자·소비자(`kafka.rs`), 손으로 짠
  Karapace 클라이언트(`karapace.rs`), Confluent 전송 형식 코덱(`avro_codec.rs`), 지식 원천 Avro 대응
  (`foundation_knowledge_avro.rs`), 실패 메시지 발행기(`dead_letter_publisher.rs`).
- `intelligence-worker` 의 `knowledge_consumer.rs` 와 바이너리 `foundation_knowledge_consumer`(715 줄): Foundation 의
  `foundation-platform.knowledge.source-upserted.v1` 을 소비해 지식 원천 registry 에 넣는다.
- 그 시험 2,230 줄(라이브 Redpanda·Karapace 시험 포함), `intelligence-contracts` 의 토픽·CloudEvents 머리·실패 메시지
  계약, `schemas/intelligence.dead-letter.v1.avsc`, Foundation 의 `schemas/foundation-platform.knowledge.source-upserted.v1.avsc`.
- 선언만 하고 아무 데서도 가져오지 않는 `schema_registry_converter = "4.9.0"`. 이 의존성이 reqwest default-tls →
  native-tls → openssl 사슬을 끌어와 `deny.toml` 이 openssl 금지에 예외(wrapper)를 두고 있었다.

잰 것(2026-11-19):

- Foundation 어디에도 `knowledge.source-upserted` 를 내는 코드가 없다(저장소 검색). ADR-0215 가 이미 "producer 가
  없다"고 적었고, 소비자는 Foundation 이 subject 를 등록하기 전에는 기동이 실패하게 되어 있었다.
- 운영 ai-server 에는 그 소비자도 Intelligence 의 Kafka 도 없다. 도는 Kafka 는 DataHub 자기 것뿐이고, Intelligence 는
  문서 작업 둘(`intelligence-document-text`·`-fact`)만 돈다. `docs/current-state.md` 도 C2 를 "미배선"으로 적었다.
- ADR-0477 이 두 플랫폼 사이를 **표로만** 주고받기로 했고, 지금의 문서 작업은 고정 스냅숏의 표와 이미 한 집합을
  읽는다(`jobs/document_text/document_text/plan.py`). 이벤트 소비자가 할 일이 남아 있지 않다.
- 이 코드의 비용: rdkafka 의 cmake 빌드와 그 C 헤더(Intelligence 검증의 apt 의존성), Redpanda·Karapace 시험 스택,
  재발명 목록 2차의 결함 I-4~I-6, 그리고 위 openssl 예외.

## Decision

1. **지운다.** `crates/messaging` 전체, 지식 원천 소비자(모듈·바이너리·시험), `intelligence-contracts` 의 토픽·
   CloudEvents 머리·실패 메시지 계약(`event_envelope`·`dead_letter`), `intelligence.dead-letter.v1.avsc`, Foundation 의
   `foundation-platform.knowledge.source-upserted.v1.avsc`, 워크스페이스 의존성 `rdkafka`·`schema_registry_converter`,
   xtask 의 Intelligence `kafka` 라이브 레인과 rdkafka 용 apt 헤더.
2. **openssl 은 예외 없이 금지한다.** Intelligence `deny.toml` 의 wrapper 를 없애고 `native-tls` 도 금지 목록에 넣는다.
   지운 뒤의 의존성 그래프에 openssl·openssl-sys·native-tls 가 없다(`cargo tree -i`, 2026-11-19).
3. **남는 것.** 정규화 outbox 기록의 전송 모양 `intelligence.normalization-proposal.submission-requested.v1.avsc` 와 그
   계약 시험(`event_schema_contract.rs`), 그래서 `apache-avro` 도 남는다. `TraceContext` 도 정규화가 쓰므로 남는다.
4. **Foundation 의 Kafka 는 이 결정 밖이다.** Foundation outbox 의 Kafka 전달기와 그 실브로커 시험은 그대로 두고,
   그 시험이 쓰던 고정 Redpanda+Karapace compose 는 Foundation 으로 옮긴다
   (`platforms/foundation-platform/docker/kafka-live.compose.yml`, 포트 변수 `FOUNDATION_TEST_*`).
5. **Kafka 가 Intelligence 에 돌아오는 조건.** 실제 발행자가 생겨 토픽을 낼 때, 그 PR 이 같이 들인다:
   - 발행자가 토픽의 값 스키마 파일을 소유하고 subject 를 등록한다(ADR-0215 의 소유 규칙을 이 조건으로 옮긴다:
     소비자는 조회만 하고 등록·호환성 설정을 하지 않는다).
   - 스키마 레지스트리 클라이언트는 손으로 짜지 않고 `schema_registry_converter`(rustls 기능)로 쓴다. Avro 는
     `apache-avro` 의 serde 대응을 쓴다.
   - 브로커는 ADR-0118 §3 의 공용 Kafka 다. Intelligence 전용 Redpanda 구성은 다시 두지 않는다.

## Consequences

- 지운 줄: 소스 약 1.8k(백본 1,008 + 소비자 715 + 계약 모듈), 시험 약 2.4k, `Cargo.lock` 의 339 줄. Intelligence 검증은
  C 빌드 없이 돈다.
- ADR-0215 는 이 ADR 이 대신한다. 그 소유 규칙은 §5 의 복귀 조건으로 남는다.
- ADR-0118 §3 의 "Intelligence 의 Redpanda 구성은 이 Kafka 로 옮긴다"는 옮길 것이 없어져 닫힌다. 공용 Kafka 결정 자체는
  그대로다.
- 다시 켤 때 드는 것: 발행자 쪽 스키마 파일과 등록, 소비자 하나, 라이브러리 둘. 지운 코드는 git 기록에 있다.
- 기각: 코드를 두고 "선택적"으로 표시하기. 아무도 내지 않는 토픽의 소비자는 시험·빌드·보안 예외 비용만 낸다.
- 기각: `schema_registry_converter` 로 손 코드를 갈아 끼우고 백본은 두기(재발명 목록 51번). 발행자가 없으면 갈아 끼운
  코드도 쓰이지 않는다. 51번은 §5 의 복귀 조건이 된다.
