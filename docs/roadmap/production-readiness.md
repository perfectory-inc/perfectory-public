---
status: current
owner: repository-maintainers
doc_type: roadmap
last_reviewed: 2026-08-05
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

  > **2026-08-06 현재 상태:** 라이브 레인을 처음 실행하기 위해 **어드민 범위 카탈로그 토큰을
  > 임시로 쓰고 있다.** 위 원칙이 허용하는 "임시 대안"에 해당하며, 그 전 토큰은 개체 권한만 있어
  > 카탈로그가 `403 Forbidden`으로 거절했다 — 개체 API 권한과 Iceberg 카탈로그 권한이 별개라는
  > 이 문서의 서술이 실측으로 확인됐다. **좁히기 전에는 닫지 말 것.** 필요한 최소 권한은 이제
  > 관측으로 정할 수 있다: 스모크가 실제로 부른 것은 `GET /v1/config`, 네임스페이스 조회,
  > 테이블 로드뿐이고 쓰기는 없었다.
- [ ] 계정 수준 카탈로그 격리가 필요하면 출시 전에 운영 Cloudflare 계정 분리 여부를 결정하고,
  분리 시 객체·Iceberg 메타데이터 이전과 무중단 전환 계획을 승인
- [ ] Bronze 불변성, Postgres 백업/복구 리허설, RPO/RTO 증거 확보
- [ ] 수집 원본·ledger·outbox·quarantine의 보존 기간과 삭제 승인 절차 확정

## 우선순위 1 — 실제 파이프라인 완성

- [ ] 국가 수집 대상별 bulk/API 선택과 실제 공급자 자격증명·쿼터 검증
- [ ] Bronze → Silver → Gold를 실제 R2/Iceberg backend 자격증명으로 실행하고 결과를 검증

  > **2026-08-06 실측.** 라이브 레인 셋을 처음 돌렸고, 실버가 **어디까지 와 있는지가 관측으로
  > 확정됐다.**
  >
  > - **R2 객체 경로: 통과.** 쓰기·읽기·삭제 왕복과 인벤토리 조회가 실제 버킷에서 성공했다.
  > - **Iceberg 카탈로그: 통과.** 네임스페이스는 `silver`, `gongzzang_silver`,
  >   `tiles_slice_proof`이고 `silver`에는 `building_register_units`와
  >   `building_register_unit_areas`가 있다. 그 테이블의 현재 스냅샷을 읽는 데 성공했다.
  > - **`silver.industrial_complexes`는 존재하지 않는다.** 스모크의 기본 대상이 이것이어서 레인이
  >   실패했고, 원인은 파이프라인이 아니라 **없는 테이블을 가리킨 설정**이었다.
  > - **data.go.kr: 상대 서버 오류.** HTTP 502에 `returnReasonCode 04`(HTTP 에러)로, 인증 단계에
  >   도달하지도 못했다. 서비스 키 문제가 아니므로 재시도 대상이다.
  >
  > 남은 구간은 실버에서 Postgres canonical로 넘어오는 길이다. 건축물대장은 실버까지 와 있는데
  > `catalog.building`·`catalog.building_unit`에는 생산자가 없다
  > ([기반 목표](./foundation-goals.md) G1의 21표에 둘 다 포함된다). **읽을 데이터가 없어서가
  > 아니라 읽는 코드가 없어서다.**
- [ ] dbt Gold 모델 또는 Spark Gold projection 중 하나를 정식 Gold 계약으로 확정
- [ ] Trino/Spark/Iceberg catalog 연결, snapshot 승격·롤백·재처리 증명
- [ ] LLM 정규화 provider, 비용/쿼터, proposal 승인·적용 권한과 감사 로그 확정
- [ ] production orchestrator 선택, 소유자·스케줄·재시도·취소·롤백을 ADR로 결정

### 카탈로그 도메인 모델 교정

결정과 근거는 [ADR-0019](../adr/0019-membership-is-a-dated-fact-not-a-column.md)가 정본이며,
그 문서의 `남은 부채` 절이 아직 열린 항목을 소유한다. 여기서는 착수 순서만 고정한다.

- [ ] 1단계 — `catalog.parcel_complex_membership`을 추가하고 기존 `parcel.complex_id`에서
      백필한다. 기존 컬럼을 건드리지 않으므로 단독 배포 가능하다.
- [x] 2단계 — `/complexes/{id}/anchor-summary`의 읽기를 소속 표로 옮기고 `parcel.complex_id`에
      쓰기 금지 트리거를 걸었다. "현재"는 오늘이며 그 술어는 `catalog.parcel_current_complex`
      하나가 소유한다 ([ADR-0022](../adr/0022-current-means-today-and-one-view-says-so.md)).
      나머지 산단 스코프 읽기 셋은 옮기지 않고 삭제했으므로
      ([ADR-0021](../adr/0021-an-unread-surface-is-deleted-not-migrated.md)) 이전 대상은 넷이
      아니라 하나였다.
- [ ] 3단계 — 컬럼과 `ParcelResponse.complex_id`를 제거한다. OpenAPI 사본 두 개를 함께 고친다.
- [ ] 4단계 — 부속지번 실버가 실물이 된 뒤 `catalog.building_parcel`과
      `building_unit.building_id`를 착수한다. 지금은 소스가 `planned`라 착수하지 않는다.

> 이 순서는 위 G1 문제와 같은 뿌리다. 소속 표의 **운영** 생산자는
> `silver.complex_parcel_memberships`를 읽는 코드이며, 그 코드가 없는 이유는
> 건축물대장과 같다 — 읽을 데이터가 없어서가 아니라 읽는 코드가 없어서다.

### 공간 데이터 발행 경로

파이프라인의 서빙 끝단이다. 발행 상태 기계와 불변식은
[단일 출처 공간 데이터 공개 아키텍처](../architecture/single-source-spatial-publication.md)가 정본이며,
용어는 [전역 용어집](../glossary.md)의 공간 데이터 발행 용어를 따른다. 여기서는 남은 순서만 고정한다.

- [x] 발행 단위마다 완전한 활성 소스를 하나로 강제하고 포인터를 CAS로만 전진시킨다
- [x] 투영 적재를 정체성 있는 사실로 만들어 릴리스가 지명한 적재의 행만 서빙한다
      ([ADR-0016](../adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md))
- [x] 발행 리비전을 그것이 개정하는 단위에 스코프해 교차 단위 참조를 쓰기 시점에 막는다
      ([ADR-0017](../adr/0017-a-data-revision-belongs-to-the-unit-it-revises.md))
- [ ] `catalog.parcel`에 쓰는 프로덕션 경로의 소유자를 정한다. **이것이 parcels 적재기의
      선행조건이고 지금 주인이 없다.** 현재 그 표에 INSERT하는 것은 테스트와 Docker fixture뿐이고
      정본 실버는 PostgreSQL 밖에 있어 적재기가 읽을 Cargo 의존성이 없다. 선행조건 없이 적재기를
      먼저 만들면 fixture 규모가 천장이 된다.
- [x] 적재 경로와 승격 경로를 CI가 실행하는 검사로 덮는다 — 발행이 적재를 열고 닫는 것부터
      런타임 포인터가 그 적재를 서빙하도록 전진하는 것까지
      ([ADR-0016](../adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md) 남은 부채 4 해소)
- [x] 타일이 실제로 나오는지를 CI에서 확인한다 — `administrative-boundary-slice` 잡이
      `scripts/tiles/administrative-boundary-slice-proof.sh`를 돌리고 `required/foundation`이
      그것을 센다 ([기반 목표](./foundation-goals.md) G2).

      > **정정 (2026-08-06):** 이 항목은 그 스크립트가 "Docker Compose가 필요하다"고 적었으나
      > **사실이 아니었다.** compose 참조가 하나도 없고, 다이제스트 고정 이미지로 자기 Postgres와
      > Martin을 직접 띄우며 자격증명도 쓰지 않는다. 필요한 것은 Docker뿐이고 CI 러너에는 이미
      > 있었다. 막고 있던 것은 의존성이 아니라 **아무도 부르지 않는다는 사실**이었다 — 워크플로도,
      > xtask도, 가드도 이 스크립트를 참조하지 않았다. 잘못된 장애물 서술 때문에 한 줄이면 될 일이
      > 큰 일로 남아 있었다.
- [ ] PMTiles(정적) 경로는 여전히 CI 밖이다. 동적 경로만 위 잡이 덮으며, 정적 승격은 구현 자체가
      아직 포트 기본 구현(에러)이다 ([ADR-0014](../adr/0014-serving-generation-tracks-one-unit-source-selection.md) 남은 부채 1).
- [ ] 지오메트리 체크섬 계약이 기대는 인코딩 가정을 명시적으로 만든다. 생산자는 자신이 쓸 값을,
      소비자는 파일에서 **다시 파싱한** 값을 해시하므로 `print(parse(print(v))) == print(v)`가
      성립해야 하는데, `serde_json`은 자신이 출력한 17자리 값(`37.300000000000004`)을 1 ULP 다른
      f64(`37.3`)로 파싱한다. 오늘 터지지 않는 이유는 생산자의 좌표가 전부 파싱 결과이고 산술로
      만든 f64가 없기 때문이며, 이는 설계가 아니라 우연이다. 좌표를 변환하는 단계(재투영·단순화·
      반올림)가 들어오면 정상 데이터가 `geometry_sha256 mismatch`로 거부되고 그 메시지는 입력
      손상처럼 읽힌다.

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
- [ ] 일회용 하네스가 자기 저장소를 흘리지 않는지 기계적으로 검사한다. `scripts/verify/integration.sh`와
      `scripts/tiles/administrative-boundary-slice-proof.sh`는 `docker rm`에 `-v`가 빠져 실행마다
      PostgreSQL 익명 볼륨을 하나씩 남겼다. 107개(개당 약 115MB)가 쌓여 디스크가 차고 검증 실행이
      죽고 나서야 드러났다. 두 스크립트는 고쳤지만 세 번째가 같은 형태로 들어오는 것을 막는 것은 없다.
- [x] R2 환경변수 가드를 알려진 레거시 거부 목록에서 허용 목록으로 강화한다.
- [x] 문서 메타데이터 마이그레이션 후 문서 CI를 strict 모드로 전환한다.
- [ ] 현재 Cargo 검증을 유지하고, 두 번째 상시 엔지니어·CI 병목·원격 캐시 도입 때만
      Bazel/Buck2 재검토를 시작한다.

`AREAS`·`LiveLane`·`covers` 선언을 별도 구조화 입력으로 통합하는 안은 검토 후 기각했다.
세 선언은 이미 `tools/xtask/src/main.rs` 한 곳에 있고 셸 가드가 같은 파일을 읽는다. 중간
형식을 하나 더 두면 Rust 선언과 그 형식이 어긋날 수 있는 새 경로가 생기며, 이는 통합으로
제거하려던 문제와 같은 종류다.

## ADR이 기록한 남은 부채

각 ADR은 자신이 닫지 못한 것을 `남은 부채` 절에 기록한다. **그 항목의 본문은 ADR이 소유하고
이 표는 어디에 속하는지와 순서만 고정한다.** 항목을 여기에 옮겨 적지 않는다 — 복사본은 한쪽만
고쳐지는 순간 어긋나며, 그것이 아래에서 금지하는 중복과 같은 것이다.

`scripts/guard/roadmap-owns-recorded-debt.sh`가 `남은 부채`를 기록한 ADR이 이 표에서 빠지면
실패한다. 규칙이 산문뿐이던 동안 ADR 8개의 37개 항목 전부가 이 목록 밖에 있었다.

**기록된 항목 수는 남은 일의 수가 아니다.** `남은 부채` 절에는 세 가지가 섞여 있다 — 아직 열린
부채, ADR 자신에 대한 정정, 그리고 그 작업 중에 발견하고 **이미 고친 것**의 기록. 그리고 항목이
닫힐 때 이 절이 갱신된다는 보장이 없다. 가드는 목록이 **닿을 수 있는지**를 검사할 뿐 **최신인지**는
검사하지 않으며, 그것은 기계적으로 판정할 수 없다.

2026-08-05에 **여덟 ADR을 모두 실물과 대조했다.** 기록된 37항목 중 열린 것은 **17개**다. 나머지
20개는 이미 닫혔거나(10), 애초에 부채가 아니거나(정정·감사 결과·교훈 기록 3), 지켜야 할 코드가 아직
없어 도달할 수 없거나(4), 의도적 결정이다(3). 닫힌 10개 중 **셋은 아무도 ADR에 적지 않은 채로**
닫혀 있었고(그중 하나는 자기를 인용한 CI 주석까지 달려 있었다), 일곱은 이 대조를 하면서 닫았다.

`열림` 열이 실제 남은 일이다. `기록` 열은 그 절의 항목 수이며 위 세 가지가 섞여 있다.

| ADR | 기록 | 열림 | 열린 것이 무엇인가 | 우선순위 |
|---|---|---|---|---|
| [0010 라이브 자원 테스트 레인](../adr/0010-live-resource-test-lanes.md) | 11 | 2 | (자격증명 항목은 2026-08-06 해소 — 아래 참조) 정적 가드(부분 해소 — CI는 차단 밖), 임포트 게이트 부재 | 4 |
| [0011 테스트 실행 집합 완전성](../adr/0011-test-execution-set-completeness.md) | 4 | 3 | 정적 가드(부분 해소 — CI는 차단 밖), 자격증명 레인, 임포트 게이트 | 4 |
| [0012 검증 결과의 의미](../adr/0012-verification-results-must-mean-what-they-say.md) | 3 | 0 | — 전부 닫힘 | 4 |
| [0013 릴리스 유일성](../adr/0013-release-uniqueness-admits-both-source-kinds.md) | 2 | 0 | — 전부 닫힘 | 1 |
| [0014 제공 세대](../adr/0014-serving-generation-tracks-one-unit-source-selection.md) | 2 | 1 | 정적 승격이 아직 포트 기본 구현(에러)이라 이 규칙을 지난 적 없음 | 1 |
| [0015 멱등성 원장](../adr/0015-one-idempotency-ledger-for-keyed-catalog-mutations.md) | 5 | 3 | 키를 발급하는 클라이언트 부재, `validate()` 결합, 원장 우회 경로 둘 | 1 |
| [0016 투영 적재의 정체성](../adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md) | 6 | 4 | 적재 보존 기간, `failed` writer 부재, parcels 적재 경로, 지문 v1 의미 변화 | 1 |
| [0017 리비전의 소속](../adr/0017-a-data-revision-belongs-to-the-unit-it-revises.md) | 4 | 3 | `superseded` writer 부재, 계보를 CHECK로 강제할 수 없음, 사실 원장(`parcel_identifier`) 분리 미완 | 1 |
| [0018 두 언어의 어휘 대조](../adr/0018-a-vocabulary-written-in-two-languages-is-compared.md) | 3 | 2 | catalog 밖 도메인 어휘 미대조, 제약 읽기가 단일 목록 형태에 한정 | 2 |
| [0019 소속은 기간을 가진 사실](../adr/0019-membership-is-a-dated-fact-not-a-column.md) | 4 | 4 | 필지 전이표 부재, 건물↔필지·호실→건물 미교정, 소속 표의 운영 생산자 부재, 산단의 시군구 코드가 스칼라 | 1 |
| [0020 도형은 사실의 근거가 아니다](../adr/0020-geometry-is-not-evidence-for-a-fact.md) | 4 | 3 | `sandan_parcel` 수집기 부재, 한 필지 한 산단 미실측, 레이크하우스 계약의 도형 어휘 잔존, 덮어쓰기형 표 전환 | 1 |
| [0021 안 읽히는 표면은 지운다](../adr/0021-an-unread-surface-is-deleted-not-migrated.md) | 3 | 2 | (`anchor-summary` 읽기 이전은 0022가 해소) 산단 SSOT 제안 문서 전면 재검토, 등록부와 실물 라우트 대조 검사 부재 | 2 |
| [0022 "현재"는 오늘이다](../adr/0022-current-means-today-and-one-view-says-so.md) | 3 | 3 | `CURRENT_DATE` 시간대 미지정, `as_of` 파라미터 보류, `parcel.complex_id` INSERT는 여전히 자유 | 2 |

2026-08-06에 0018이 더해졌다(기록 3, 열림 2). 세 번째 항목은 0017 남은 부채 3과 **같은 건**이므로
열림으로 세지 않는다 — 같은 일을 두 번 세는 것이 이 표가 피하려는 것이다.

2026-08-07에 0019가 더해졌다(기록 4, 열림 4). 네 항목 모두 오늘 기록됐으므로 닫힌 것이 없다.
셋째 항목(운영 생산자 부재)은 0016 남은 부채 3과 **뿌리가 같지만 다른 표**다 — 저쪽은
`catalog.parcel`을 채우는 적재기이고 이쪽은 소속 표를 채우는 경로이며, 둘 다 Rust가 Iceberg를
읽을 수 있어야 열린다. 뿌리가 같다는 이유로 합쳐 세지 않는 대신 그 사실을 여기 적어 둔다.
표 합계는 기록 44, 열림 23이다.

2026-08-09에 0020이 더해졌다(기록 4, 열림 3). 첫째 항목(`sandan_parcel` 수집기 부재)은 0019 남은
부채 3(소속 표의 운영 생산자 부재)과 **같은 건**이므로 열림으로 세지 않는다 — 0020이 그 생산자가
읽어야 할 소스를 지목했을 뿐 새 일을 만들지 않았다. 넷째 항목(덮어쓰기형 표 전환)은 이 저장소의
사실 표 전체에 걸린 방향이며 한 증분이 아니라 프로그램이므로, 범위를 정하는 별도 ADR이 먼저다.
표 합계는 기록 48, 열림 26이다.

2026-08-09에 0021이 더해졌다(기록 3, 열림 3). 첫째 항목(`anchor-summary` 읽기 이전)은 0019
§이행 순서 2가 이미 담고 있던 일을 **좁힌** 것이지 새로 만든 것이 아니다 — 대상이 넷에서 하나로
줄었으므로 이 표에서는 0019 쪽이 아니라 여기에 적어 남은 범위를 정확히 보이게 한다.
표 합계는 기록 51, 열림 29다.

2026-08-10에 0022가 더해졌다(기록 3, 열림 3). 동시에 0021의 첫 항목(`anchor-summary` 읽기 이전)이
닫혔으므로 0021은 열림 3 → 2가 된다. 0022의 첫 항목(`CURRENT_DATE` 시간대)은 **이 결정이 만든 것이
아니라** `parcel_current_identifier`가 이미 갖고 있던 문제를 하나 늘린 것이며, 둘을 함께 고쳐야
하므로 한 항목으로 센다. 표 합계는 기록 54, 열림 31이다.

대조에서 나온 두 가지를 여기 적어 둔다. **2026-08-05 대조 시점의 열린 19개 중 상당수는 코드로 닫히지 않는다** — 자격증명
(5타깃), 정본 실버의 소유자(parcels 적재기), 클라이언트 구현(멱등성 키 발급)처럼 결정이 먼저인
것들이다. 그리고 **`남은 부채` 절은 닫힐 때 갱신되지 않는다.** 여섯 항목이 코드로는 닫힌 채 목록에
살아 있었고, 그중 하나는 자기를 인용한 CI 주석까지 달려 있었다. 이 표의 `열림` 수도 대조한 날짜의
사실일 뿐이며, 같은 방식의 재대조가 유일한 유지 수단이다.

## 문서 정리 상태

- [ ] `doc_type: documentation`인 28개 문서를 실제 종류로 좁힌다. 이 값은 아무것도 분류하지
      않으며(`intelligence-platform/docs/architecture.md`가 이 값을 달고 있다), 파일마다 판단이
      필요해 2026-08-06에 어휘를 강제할 때 **잠정 허용**으로 남겼다. 좁히고 나면
      `audit-documentation.py`의 `ALLOWED_DOC_TYPES`에서 뺀다.

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
