---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# Foundation Kafka outbox 실시간 계약

기존 Postgres outbox에서 Kafka로 전달하는 경로의 출시 전 증명이다. 일반
`cargo test`와 `cargo xtask verify foundation`은 의도적으로 자격증명 없이 실행한다. 실시간
게이트는 명시적으로 호출해야 하며 기본은 무시되고, 필요한 서비스가 없으면 실패한다.

## 게이트가 증명하는 것

이 게이트는 publisher 서비스가 사용하는 경로와 같은 경로를 실행한다.

1. 실제 Redpanda broker와 Karapace가 저장소의 Avro 스키마를 등록한다.
2. 일회용 스택에 정본 토픽 `foundation-platform.catalog.collection-raw-written.v1`을
   파티션 1개·복제본 1개로 명시적으로 만든다.
3. 실제 Postgres `catalog.outbox_event` 행을 `OutboxWorker`가 임대하고 Confluent-Avro
   claim-check로 전달한 뒤, 전달이 끝난 다음에만 `published_at`을 기록한다.
4. 접근할 수 없는 Kafka bootstrap은 재시도·격리되고 `published_at`은 null로 남는다.
5. Kafka 값에는 `event_id`, `scope_unit_id`, R2 객체 키·checksum·count가 들어가며,
   R2 원본 바이트는 들어가지 않는다.

전달은 최소 한 번(at-least-once)이다. 소비자는 `event_id`로 중복 제거해야 하며 exactly-once는
보장하지 않는다. Postgres outbox가 내구성 있는 정본이자 재생·롤백 수단이다.

자격증명 없이 실행하는 `kafka_broadcaster_contract` 모음은 소비자 경계도 확인한다. Avro
claim-check를 디코드하고 `bronze_object_key`로 fixture Bronze 객체를 읽어 SHA-256을 검증한 뒤,
같은 `event_id`의 재전달은 객체를 다시 읽지 않고 버린다. 이것은 계약 증명이지 운영 Silver/Gold
소비자 구현이 아니다. downstream 소유자는 실제 R2 어댑터와 내구성 있는 consumer offset·
멱등성 저장소에 같은 동작을 구현해야 한다.

## 전체 실시간 게이트 실행

사전 조건은 Docker, `curl`, `DATABASE_URL`이 설정된 일회용 Postgres/PostGIS 데이터베이스다.
Cargo/Rust 1.96이 `PATH`에 있으면 호스트에서 테스트하고, 없으면 고정된 Rust 검증 컨테이너를
자동 사용한다. 스크립트가 고정된 Redpanda/Karapace 스택의 수명과 정리를 모두 담당한다.
Postgres도 컨테이너 네트워크에서만 접근할 수 있으면
`FOUNDATION_KAFKA_TEST_CONTAINER_DATABASE_URL`에 해당 네트워크 URL을 설정한다.

```bash
export DATABASE_URL='postgres://foundation_platform:foundation_platform_dev_2026@127.0.0.1:5432/foundation_platform'
bash scripts/verify/foundation-kafka-live.sh
```

스크립트는 `platforms/intelligence-platform/docker/c2-event-backbone.compose.yml`을 시작하고,
두 서비스가 준비될 때까지 기다린 뒤 정본 토픽을 생성·조회하고 필요한 Kafka 변수를 설정한 다음
다음 ignore 테스트를 `--nocapture`로 실행한다.

- `live_kafka_karapace`
- `live_kafka_outbox_roundtrip`
- `live_kafka_outage`

다른 로컬 스택이 기본 포트·프로젝트 이름을 사용 중이면
`INTELLIGENCE_TEST_KAFKA_HOST_PORT`, `INTELLIGENCE_TEST_KARAPACE_HOST_PORT`,
`FOUNDATION_KAFKA_COMPOSE_PROJECT`를 지정한다. Docker·Postgres·broker·registry·토픽·스키마가
없거나 테스트가 실패하면 스크립트는 0이 아닌 값을 반환한다. 필수 모드 실패를 성공한 skip으로
바꾸지 않는다.

Kafka 활성화에는 `FOUNDATION_PLATFORM_RUNTIME_ENV=local|ci|staging|production`도 필요하다.
어댑터는 producer를 만들기 전에 이를 검증하고, staging·production에서는 loopback이 아닌
broker, HTTPS Schema Registry, SSL/SASL_SSL도 요구한다.

## CI

`.github/workflows/foundation-ci.yml`의 `kafka-integration` job은 고정된 PostGIS 서비스를
준비하고 Foundation 마이그레이션을 실행한 뒤 같은 스크립트를 호출하며, `if: always()` 정리
단계를 가진다. 결과는 `required/foundation`에 포함되므로 실시간 Kafka 게이트의 skip·실패는
워크플로를 막는다.

## 운영 전환 확인 목록

다음 증거가 배포 시스템에 모두 남기 전에는 staging/production에서 Kafka를 켜지 않는다.

- 관리형 Kafka bootstrap endpoint와 TLS/SASL 설정
- HTTPS Schema Registry endpoint와 인증/CA 신뢰 설정
- 정본 토픽 소유자·파티션 수·복제 계수·보존 기간·ACL
- `event_id` 중복 제거와 R2 claim-check 읽기를 증명하는 소비자 계약 테스트
- 발행 지연·오류·재시도·격리·중복·스키마/토픽/ACL 알림
- `FOUNDATION_PLATFORM_KAFKA_DUAL_PUBLISH_LEGACY=1` 관찰 기간

로컬 Redpanda와 Karapace는 테스트 인프라이며 운영 대체품이 아니다. Kafka와 Schema Registry
자격증명은 저장소에 넣지 않는다.

## 롤백

1. `FOUNDATION_PLATFORM_KAFKA_ENABLED=0`으로 설정하고 publisher를 재시작한다. 기존 Bronze/R2와
   Postgres outbox 커밋은 Kafka 없이 계속된다.
2. 대기 행이 `published_at IS NULL`로 남아 있는지 확인하고 broker·schema·topic을 복구한 뒤
   일반 outbox worker로 재생한다.
3. 전달을 다시 켜기 전에 `catalog.outbox_quarantine`을 확인한다. Kafka가 일부만 확인한 뒤에는
   소비자가 `event_id` 중복을 견뎌야 한다.
