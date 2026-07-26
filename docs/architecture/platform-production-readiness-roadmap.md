---
status: current
---

# Platform production-readiness roadmap

이 문서는 perfectory의 현재 구현을 운영 단계까지 가져가기 위한 단일 실행 목록이다. 기술
스택 이름을 늘리는 문서가 아니라, 각 단계의 완료 조건과 다음 작업의 순서를 고정한다.

## 현재 기준

- 원본 데이터는 Foundation 소유 R2 Bronze에 보존한다.
- 처리 상태와 이벤트 원장은 Foundation PostgreSQL이다.
- Kafka는 Postgres outbox에서 파생 이벤트를 전달하는 통로다.
- 로컬/CI 브로커는 Redpanda `v24.3.6`, 스키마 레지스트리는 Karapace `6.2.0`이다.
- 운영 Kafka/Schema Registry는 관리형 서비스를 사용하며, 운영 자격증명은 저장소에 넣지 않는다.
- Kafka 원장(event sourcing) 전환은 기본 계획이 아니다. 특정 파생 영역에서 필요성이 증명될 때만
  별도 ADR로 결정한다.

## Recent implementation slice

- All Foundation Bronze live-write adapters now cross one shared runtime/bucket preflight at the
  actual object-storage construction boundary. Callers still preflight before provider downloads,
  while a source guard rejects direct use of the unvalidated builders in ingest code.
- Environment-mutating publisher tests use one async-aware process-wide lock and restore ambient
  variables, so CI configuration cannot silently change their result.
- Kafka contract coverage now checks that retries preserve the same `event_id`/partition key and
  that live Avro records expose the Bronze claim-check metadata without raw object bytes.

## 우선순위 0 — 출시 전 필수 게이트

### Kafka 이벤트 전달

- [ ] 운영 Kafka와 Schema Registry 소유자·배포 대상 확정
- [ ] TLS/SASL, Schema Registry HTTPS/CA, Secret Manager 주입 검증
- [ ] `foundation-platform.catalog.collection-raw-written.v1` 토픽의 파티션·복제·보존·ACL 확정
- [ ] 소비자가 `event_id`로 중복 제거하고 R2 claim-check를 읽는 계약 테스트 추가
- [ ] 발행 지연·실패·재시도·격리·consumer lag·스키마 오류 알림 연결
- [ ] `dual_publish_legacy=1` 관찰 기간과 Kafka 비활성화 롤백 절차 증명
- [ ] GitHub `kafka-integration` 필수 게이트가 실제 보호 브랜치에서 통과하는지 확인

현재 코드와 실행 명령은
[`foundation-kafka-outbox-contract-test.md`](../../platforms/foundation-platform/docs/runbooks/foundation-kafka-outbox-contract-test.md)와
[`0028-foundation-kafka-raw-written-design.md`](../../platforms/foundation-platform/docs/adr/0028-foundation-kafka-raw-written-design.md)에 있다.

### 데이터 원장·복구

- [ ] 운영 R2/Postgres 버킷·DB를 개발/CI와 분리하고 런타임 가드로 강제
- [ ] Bronze 불변성, Postgres 백업/복구 리허설, RPO/RTO 증거 확보
- [ ] 수집 원본·ledger·outbox·quarantine의 보존 기간과 삭제 승인 절차 확정

## 우선순위 1 — 실제 파이프라인 완성

- [ ] 국가 수집 대상별 bulk/API 선택과 실제 공급자 자격증명·쿼터 검증
- [ ] Bronze → Silver → Gold를 실제 R2/Iceberg backend 자격증명으로 실행하고 결과를 검증
- [ ] dbt Gold 모델 또는 Spark Gold projection 중 하나를 정식 Gold 계약으로 확정
- [ ] Trino/Spark/Iceberg catalog 연결, snapshot 승격·롤백·재처리 증명
- [ ] LLM 정규화 provider, 비용/쿼터, proposal 승인·적용 권한과 감사 로그 확정
- [ ] production orchestrator 선택, 소유자·스케줄·재시도·취소·롤백을 ADR로 결정

## 우선순위 2 — 규모 확장용 Kafka Connect 전환

현재 직접 `OutboxWorker` 전달기는 작은 규모에서 운영 가능한 경로다. 다음 조건이 발생할
때만 Debezium/Kafka Connect 전환을 시작한다.

- outbox polling 부하 또는 backlog가 운영 목표를 넘는다;
- Kafka 소비자·sink가 여러 개로 늘어 publisher 운영이 병목이 된다;
- DB 변경을 여러 Kafka topic/sink로 표준화할 필요가 생긴다;
- CDC offset, 재시작, replay를 플랫폼 공통 기능으로 관리해야 한다.

전환 순서:

1. Outbox 행 구조와 `event_id`, partition key, Avro 호환성 계약을 동결한다.
2. 분산 Kafka Connect worker와 Debezium PostgreSQL connector를 별도 환경에 배치한다.
3. 기존 publisher와 CDC 경로를 같은 event_id 기준으로 shadow 비교한다.
4. 중복 발행을 막을 단일 production publisher 경로를 선택한다. 두 경로를 동시에 켜지 않는다.
5. connector offset/config/status, schema registry, ACL, lag, replay, 장애 복구를 검증한다.
6. 검증 후에만 기존 polling publisher를 단계적으로 끄고 rollback 경로를 유지한다.

Debezium/Kafka Connect는 Kafka를 원장으로 만들기 위한 도구가 아니다. PostgreSQL outbox를
Kafka로 안정적으로 전달하기 위한 교체 가능한 전달 계층이다. Kafka를 원장으로 하는
event-sourcing은 원본 Bronze 바이트가 아닌, 별도 파생 도메인에서만 별도 ADR로 결정한다.

## 우선순위 3 — 운영 품질과 비용 통제

- [ ] 수집·outbox·Kafka·lakehouse 전체의 trace/run id와 lineage 연결
- [ ] provider outage/quota, Kafka outage, R2 outage, DB failover, schema incompatibility 훈련
- [ ] 부하 테스트로 수집량·outbox backlog·Kafka partition 수·consumer 처리량을 측정
- [ ] 비용 대시보드: R2 저장/egress, Postgres, Kafka 보존/네트워크, LLM 호출 비용
- [ ] 모든 외부 운영 변경은 ADR·runbook·rollback 증거와 함께 반영

## 완료 판정

“운영 준비 완료”는 코드가 빌드되는 상태가 아니다. 우선순위 0의 모든 체크가 실제
운영 계정/CI에서 통과하고, 우선순위 1의 핵심 Bronze→Silver→Gold 경로가 실제 backend에서
재현되며, 장애·복구·롤백 증거가 남아 있을 때만 완료로 표시한다.
