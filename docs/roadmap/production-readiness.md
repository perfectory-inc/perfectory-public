---
status: current
owner: repository-maintainers
doc_type: roadmap
last_reviewed: 2026-07-30
---

# 운영 준비 작업 목록

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

## 운영 레이크하우스 자격증명·계정 분리 원칙

- 현재 버킷별 R2 개체 토큰은 유지한다. 개체 읽기·쓰기는 파일 API 권한이지 Iceberg 카탈로그
  권한이 아니다.
- 운영 전 카탈로그 권한은 서비스별로 분리한다. 쓰기 서비스는 카탈로그 쓰기와 운영
  레이크하우스 버킷 개체 읽기·쓰기를, 조회 서비스는 카탈로그 읽기와 버킷 개체 읽기만 갖는다.
- Cloudflare 대시보드의 `Admin Read & Write`는 계정 전체 버킷에 적용되므로 모든 서비스가
  공유하지 않는다. 사용자 지정 API 정책을 우선하고, 대시보드 어드민 토큰은 임시 대안 또는
  전용 쓰기 서비스에만 사용한다.
- 카탈로그 권한은 계정 수준이므로 계정 안의 다른 버킷과 강하게 격리해야 하면 출시 전에
  운영용 Cloudflare 계정을 별도로 둔다. 같은 로그인으로 여러 계정을 관리할 수 있지만,
  운영 중 분리하려면 새 버킷·카탈로그 생성, Iceberg 메타데이터와 객체 이전, 쓰기 동결,
  검증 후 주소·자격증명 전환과 롤백 절차가 필요하다.

## 최근 반영된 구현 조각

- Foundation Bronze 실시간 기록 어댑터는 객체 저장소를 실제로 만드는 경계에서 공통 런타임·버킷
  사전 점검을 통과한다. 호출자는 공급자 다운로드 전에 다시 점검하고, 수집 코드가 검증되지 않은
  빌더를 직접 호출하면 소스 가드가 거부한다.
- 환경변수를 바꾸는 publisher 테스트는 비동기 프로세스 전역 잠금 하나를 사용하고 원래 값을
  복원한다. 따라서 CI 환경 설정이 테스트 결과를 조용히 바꾸지 못한다.
- Kafka 계약 검사는 재시도에도 같은 `event_id`와 파티션 키가 유지되는지, 실제 Avro 레코드가
  원본 바이트가 아닌 Bronze claim-check 메타데이터를 노출하는지 확인한다.
- 자격증명 없이 실행하는 consumer 계약은 Avro claim-check를 디코드하고 Bronze checksum을
  검증하며 `event_id` 중복 전달을 버린다. 이것은 경계 증명이지 운영 Silver/Gold consumer 구현은
  아니다.
- Kafka 활성화는 어댑터 내부에서 정본 런타임 환경을 요구한다. 직접 호출자가 staging/production
  전송 경계를 생략할 수 없다.

## 우선순위 0 — 출시 전 필수 게이트

### Kafka 이벤트 전달

- [ ] 운영 Kafka와 Schema Registry 소유자·배포 대상 확정
- [ ] TLS/SASL, Schema Registry HTTPS/CA, Secret Manager 주입 검증
- [ ] `foundation-platform.catalog.collection-raw-written.v1` 토픽의 파티션·복제·보존·ACL 확정
- [x] 소비자가 `event_id`로 중복 제거하고 Bronze claim-check를 읽는 계약 테스트 추가
- [ ] 발행 지연·실패·재시도·격리·consumer lag·스키마 오류 알림 연결
- [ ] `dual_publish_legacy=1` 관찰 기간과 Kafka 비활성화 롤백 절차 증명
- [ ] GitHub `kafka-integration` 필수 게이트가 실제 보호 브랜치에서 통과하는지 확인

현재 코드와 실행 명령은
[`foundation-kafka-outbox-contract-test.md`](../../platforms/foundation-platform/docs/runbooks/foundation-kafka-outbox-contract-test.md)와
[`0028-foundation-kafka-raw-written-design.md`](../../platforms/foundation-platform/docs/adr/0028-foundation-kafka-raw-written-design.md)에 있다.

### 데이터 원장·복구

- [ ] 운영 R2/Postgres 버킷·DB를 개발/CI와 분리하고 런타임 가드로 강제
- [ ] 운영 R2 Data Catalog writer/reader 토큰을 사용자 지정 API 정책으로 분리하고, 어드민
  토큰을 공용 런타임 자격증명으로 사용하지 않도록 검증
- [ ] 계정 수준 카탈로그 격리가 필요하면 출시 전에 운영 Cloudflare 계정 분리 여부를 결정하고,
  분리 시 객체·Iceberg 메타데이터 이전과 무중단 전환 계획을 승인
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

## 우선순위 4 — 검증과 빌드 시스템

ADR-0011의 후속 작업은 발견한 테스트를 나중에 잡는 데서 멈추지 않고, 실행 경로 누락이
구조적으로 생기지 않도록 만드는 것이다.

- [ ] 라이브 테스트 자원 요구사항을 테스트 선언에서 직접 파생한다.
- [x] R2 환경변수 가드를 알려진 레거시 거부 목록에서 허용 목록으로 강화한다.
- [x] 문서 메타데이터 마이그레이션 후 문서 CI를 strict 모드로 전환한다.
- [ ] 현재 Cargo 검증을 유지하고, 두 번째 상시 엔지니어·CI 병목·원격 캐시 도입 때만
      Bazel/Buck2 재검토를 시작한다.

`AREAS`·`LiveLane`·`covers` 선언을 별도 구조화 입력으로 통합하는 안은 검토 후 기각했다.
세 선언은 이미 `tools/xtask/src/main.rs` 한 곳에 있고 셸 가드가 같은 파일을 읽는다. 중간
형식을 하나 더 두면 Rust 선언과 그 형식이 어긋날 수 있는 새 경로가 생기며, 이는 통합으로
제거하려던 문제와 같은 종류다.

## 문서 정리 상태

문서의 미완료 작업도 이 문서가 정본이다. README·ADR에 작업을 복제하지 않는다.

- [x] 루트 작업 목록을 `docs/roadmap/` 아래에 배치
- [x] 루트·영역·주요 하위 문서 폴더의 README 색인 연결
- [x] 비표준 `@` 경로 표기를 상대 Markdown 링크로 교체
- [x] 사람이 읽는 유지 서술 문장의 영문 설명을 한글 정본으로 전환 (기술 식별자·외부
      표준명·명령·계약 field는 원래 표기를 유지하며 감사 가드가 영문 문장 재발을 차단)
- [x] 유지 문서 메타데이터를 보강하고 계약·ADR·초안·법률 문서의 예외를 감사 규칙에 명시
- [x] 사용되지 않는 계획·초안 문서를 참조 검사 후 비공개 기록으로 전환

현재 제안·초안 문서는 카탈로그 색인과 현행 설계 문서에서 모두 참조되며, 감사 보고서의
유입 링크 수가 0인 초안은 없다. `audit-documentation.py --check`가 앞으로 승인 전 문서의
유입 링크 0건을 실패시킨다. 승인 전 문서는 `proposed` 또는 파일명 `.draft.` 상태를 유지하고
운영 계약으로 오인되지 않게 한다.

한글화 감사에서 예외를 제외한 유지 서술 문서의 `english` 0개와 명백한 영문 서술 문장 0개를
확인했다. 감사 보고서의 `mixed` 수는 API·제품명·schema field 같은 기술 표기를 포함한
혼합 표기 문서 수이며, 이를 번역해 식별자를 훼손하지 않는다. 계약·fixture JSON,
`AGENTS.md`/`CLAUDE.md` 라우터,
법률 고지는 원문 표기를 유지한다. 기술 식별자와 외부 제품명은 원래 표기를 보존하되,
사람이 읽는 설명은 한글로 작성한다. 상세 결과는 [`document-audit.md`](../document-audit.md)에서 확인한다.

## 완료 판정

“운영 준비 완료”는 코드가 빌드되는 상태가 아니다. 우선순위 0의 모든 체크가 실제
운영 계정/CI에서 통과하고, 우선순위 1의 핵심 Bronze→Silver→Gold 경로가 실제 backend에서
재현되며, 장애·복구·롤백 증거가 남아 있을 때만 완료로 표시한다.

## 문서 정리 완료 판정

문서 정리는 파일을 옮기거나 메타데이터를 채운 것만으로 완료하지 않는다.

- [x] 모든 문서가 정본 유형·소유자·상태를 갖거나, 계약·ADR·초안·법률·에이전트 예외가 감사 보고서에 명시된다.
- [x] 문서가 있는 모든 폴더에 README 색인이 있고, README는 내용을 복제하지 않는다.
- [x] 작업 목록은 이 로드맵 하나만 사용한다.
- [x] 깨진 상대 링크와 비표준 문서 참조가 자동 검사된다.
- [x] 사람이 읽는 유지 문서의 설명을 한글 정본으로 전환한다(코드·명령·식별자·외부 원문은
      원래 표기 유지). `audit-documentation.py --check`가 영문 전용 문서와 명백한 영문 문장을
      모두 차단한다.
- [x] 파일명 중복 후보를 범위별 정본으로 구분하고, 의도적 중복은 감사 보고서에 소유권을 기록한다.
      대체된 결정은 ADR supersession으로 연결한다.
- [ ] 문서 CI와 오프라인 링크 검사가 실제 보호 브랜치에서 통과한다.

### 2026-07-30 검증 기록

- 로컬 감사: `audit-documentation.py --check` 통과(예외를 제외한 영문 전용 유지 문서 0개,
  혼합 표기 유지 문서 85개(기술 표기 포함), 명백한 영문 서술 문장 0개, 메타데이터 누락 0개, 링크 위반 0개,
  비의도적 파일명 중복 0개).
- 자동 색인: `render-document-catalog.py --check` 통과.
- 감사 단위 테스트: 17개 통과.
- `git diff --check` 통과.
- Git Bash 경로에서 Docker 기반 `scripts/ci/lychee-docs.sh`가 통과했다
  (`857` 링크 입력, 오류 `0`). PowerShell의 `bash.exe` 래퍼로 직접 호출하면 Docker 엔진 연결
  시간 초과가 발생하므로 CI와 같은 Git Bash 실행 경로를 사용한다.
- 통합 `scripts/guard/monorepo-guard.sh`는 180초 실행에서 `container-runtime-policy`까지
  성공했지만 종료하지 않아 전체 통과로 표시하지 않았다. 개별 문서 감사·색인·lychee 증거와
  보호 브랜치 CI 결과를 분리해 기록한다.
