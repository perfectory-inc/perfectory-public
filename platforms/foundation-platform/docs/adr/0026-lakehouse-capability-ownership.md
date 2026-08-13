# ADR 0026: 레이크하우스 기능이 구체화·공개를 소유

- Status: Accepted
- Date: 2026-07-16
- Supersedes: Lakehouse ownership implied by the Catalog umbrella
- Related: Foundation ADR 0021, Gongzzang ADR 0048

## 배경

Foundation Platform은 이미 Catalog·Collection의 물리 소유자이며 Lakehouse,
Normalization, Spatial capability도 소유한다. Collection은 과거 Catalog umbrella에서
추출했지만 Lakehouse 동작은 여전히 `catalog-domain`, `catalog-application`,
`catalog-infrastructure`, `outbox-publisher`, 두 Foundation service에 분산되어 있다.

이 분배에는 명확한 결함이 있다.

- Registry asset·active-version·artifact write가 하나의 transaction이 아니다.
- Gold pointer read와 publication이 Catalog repository·unit-of-work port에 섞여 있다.
- Lakehouse error를 `CatalogError`로 나타낸다.
- domain object가 shared wire event를 직접 만든다.
- Silver materialization·lineage validation·quality policy의 단일 owner가 없다.
- file을 layer별로 옮기면 compile되지 않는 중간 commit이 남는다.

## 결정

### 기능 패키지

Lakehouse behavior moves to these packages:

```text
crates/lakehouse/lakehouse-domain
crates/lakehouse/lakehouse-application
crates/lakehouse/lakehouse-infrastructure
```

package 의존 방향은 다음과 같다.

```text
lakehouse-domain
        ^
        |
lakehouse-application ---> catalog-domain
        ^                   collection-domain
        |
lakehouse-infrastructure
        ^
        |
Foundation service composition roots
```

Catalog와 Collection은 Lakehouse package에 의존하지 않는다.

### 수직 슬라이스 전환

마이그레이션은 완전한 behavior slice를 옮긴다. slice에는 domain 계약, application port와
use case, infrastructure adapter, composition-root wiring, compatibility test가 포함된다.
모든 commit된 slice는 compile되고 focused test를 통과해야 한다.
커밋된 red architecture-test 단계나 호환성 re-export를 두지 않는다.

### 트랜잭션 소유권

`LakehouseRegistryUnitOfWork`는 namespace validation, asset upsert, active-version transition,
artifact insertion을 하나의 PostgreSQL transaction으로 소유한다.

`LakehousePublicationUnitOfWork`는 Gold pointer, source record, file asset, 해당 outbox event를
하나의 PostgreSQL transaction으로 소유한다. 기존
SQL·row lock·optimistic-version 동작과 이벤트 바이트는 유지한다.

별도 승인된 physical-schema migration 전까지 Lakehouse infrastructure는 canonical Catalog table을
읽을 수 있고 Lakehouse transaction에서는 다음 legacy-schema record만 쓸 수 있다.

- `catalog.source_record`
- `catalog.file_asset`
- `catalog.industrial_complex_gold_pointer`
- `catalog.lakehouse_*`
- `catalog.outbox_event`

이는 물리적 공존일 뿐 Catalog 기능 소유권을 뜻하지 않는다.

### 도메인 이벤트와 wire 계약

Lakehouse domain object는 Lakehouse 소유 domain event data를 만든다. shared protocol event
union을 import하지 않는다. infrastructure나 service adapter가 domain event를 기존
`foundation-shared-kernel::events::catalog_v1` wire DTO로 매핑한다. 정확한 JSON compatibility
test로 기존 consumer를 보존하면서 package dependency cycle을 피한다.

### 오류 경계

Lakehouse package는 `LakehouseError`를 사용한다. HTTP adapter는 이를 기존 public 400·409·
opaque 500 동작으로 매핑한다. outbound HTTP adapter는 transport failure를 `LakehouseError`로
매핑하며 Lakehouse code가 `CatalogError`를 만들지 않는다.

## 영향

### 긍정 효과

- Lakehouse ownership이 명시되고 Collection·Identity·Intelligence package와 같은
  capability/layer convention을 따른다.
- Registry와 Gold publication이 test 가능한 rollback 보장을 얻는다.
- API route·event name·object key·DB data를 바꾸지 않고 Catalog가 작아진다.
- 향후 Lakehouse worker와 Kafka adapter가 안정된 application boundary 하나를 사용한다.

### 비용

- service composition root가 잠시 Catalog와 Lakehouse adapter를 함께 주입한다.
- Lakehouse infrastructure가 legacy `catalog` PostgreSQL schema의 일부 table을 잠시 다룬다.
- public wire contract는 그대로지만 Rust ownership이 바뀌므로 compatibility test가 필요하다.

## 명시적 범위 밖

이 결정은 다음을 하지 않는다.

- PostgreSQL schema·table을 이동하거나 이름을 바꾼다.
- service deployable을 분리한다.
- Kafka·Kubernetes·Temporal·다른 orchestrator를 추가한다.
- Normalization·Spatial capability code를 추출한다.
- HTTP route·JSON field·event name·R2 key·CLI command·persisted value를 바꾼다.

이 변경은 별도 결정과 검증이 필요하다.

## 검증

완료에는 다음이 필요하다.

1. no Lakehouse/Iceberg/Silver/Gold implementation remains under `catalog-*`;
2. no Catalog or Collection package depends on `lakehouse-*`;
3. Registry rollback tests prove no active version exists without its artifact;
4. Gold failure-injection and concurrency tests prove atomic publication;
5. exact HTTP/OpenAPI/event/lineage compatibility tests remain green;
6. focused, workspace, clippy, formatting, and supply-chain gates pass.
