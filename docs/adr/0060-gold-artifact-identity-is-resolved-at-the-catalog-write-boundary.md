---
status: current
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-08-26
---

# ADR 0060: Gold 아티팩트 정체성은 Catalog 쓰기 경계에서 푼다

- Status: Accepted
- Date: 2026-08-26
- Related: ADR-0036, ADR-0043, ADR-0048, ADR-0051

## 배경

산업단지 하나에는 서로 계산되지 않는 식별자가 둘 있다. `catalog.industrial_complex.id`는
Catalog 행을 만들 때 발급한 UUIDv7이고, lakehouse `complex_id`는 Bronze→Silver에서 유도해
Gold 객체와 `complex` 타일 feature가 발행하는 UUIDv5다. 연결은
`catalog.industrial_complex.lakehouse_complex_id`가 소유하고
`CatalogRepository::find_complex_by_lakehouse_id`가 읽는다.

`publish-industrial-complex-gold-pointers`는 Gold 내보내기 요약의 `artifacts[].complex_id`를
`ComplexId`로 감싼 뒤 `catalog.industrial_complex.id`로 조회했다. 첫 실물 값
`001533c1-8504-5651-bd49-d9df4e87bc37`은 UUIDv5이므로 조회는
`industrial complex not found`로 끝났다. 단수 커맨드도 Gold 객체가 밝힌 같은 값을 환경변수에서
읽어 `ComplexId`로 감쌌으므로 같은 결함이었다.

2026-08-26 로컬 투영에서 다음을 실측했다.

- `catalog.industrial_complex`: 1,443행
- `lakehouse_complex_id IS NOT NULL`: 1,442행
- Gold 내보내기 요약: 1,442건
- 요약의 UUIDv5를 기존 lookup으로 해소하지 못한 건: 0건

근본 원인은 UUID가 틀린 것이 아니라, 문자열에서 강타입으로 들어가는 최초 경계가 정체성을 잘못
선택한 것이다. 두 newtype은 이미 달랐지만 잘못된 생성자를 고르면 컴파일러는 그 이후 흐름만
일관되게 지켰다.

## 결정

1. Gold 내보내기 요약의 `artifacts[].complex_id`와 단수 커맨드의
   `FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_COMPLEX_ID`는
   `LakehouseComplexId`로 읽는다. wire 이름은 Gold 계약이 소유하므로 바꾸지 않는다.
2. 객체 대조 증거인 `ClaimedGoldProfileArtifact`와 `VerifiedGoldProfileArtifact`도
   `LakehouseComplexId`를 보존한다. 저장된 프로필 문서가 밝힌 `complex_id`는 Catalog UUIDv7이
   아니라 Gold UUIDv5와 비교한다.
3. PostgreSQL 포인터를 쓰는 `foundation-outbox-publisher`가 기존
   `CatalogRepository::find_complex_by_lakehouse_id`로 UUIDv5를 UUIDv7에 연결한다. 레이크하우스
   내보내기는 재구성 가능한 투영의 UUIDv7을 알지 않는다.
4. 단수와 전량 커맨드는 같은 `resolve_catalog_complex_ids` 경계를 쓴다. 이 경계만
   `find_complex_by_lakehouse_id` 결과에서 `IndustrialComplex.id`를 꺼내며,
   `PublishIndustrialComplexGoldPointerInput`은 해소된 `ComplexId`를 명시적으로 받아야만 만들어진다.
5. 전량 커맨드는 1,442개 정체성을 **포인터 쓰기 전에 모두** 조회한다. 못 푼 값은 건너뛰지 않고
   전부 세어 ID 목록과 함께 실패한다. 따라서 이 결함 부류는 일부 포인터를 쓴 뒤 발견되지 않는다.
6. 별도 `--dry-run` 분기는 만들지 않는다. 정체성 사전 점검은 선택 가능한 예행연습이 아니라 모든
   발행이 반드시 통과하는 단계다. 선택 분기를 만들면 실제 발행자가 생략할 수 있고, 두 실행 모드가
   어긋날 새 경로가 된다.
7. 정체성 사전 점검 뒤의 객체 GET 또는 단지별 CAS 쓰기가 실패하면 실행은 즉시 멈춘다. 앞서 끝난
   단지의 성공을 롤백하지 않는다. 객체를 읽는 동안 1,442행 트랜잭션과 잠금을 유지하지 않으며,
   ADR-0051의 동일 버전 skip과 expected-version CAS가 재실행을 안전하게 만든다.

### 위협 모델

- **표현 불가능:** 요약과 객체 검증 단계에는 `ComplexId` 필드가 없으므로 Gold UUIDv5를 그 타입에
  담아 포인터 입력으로 바로 넘기는 기존 표현식은 컴파일되지 않는다.
- **탐지:** UUIDv5가 Catalog 투영에 없으면 전량 사전 점검이 누락 수와 값을 보고하고 쓰기 전에
  실패한다.
- **막는 사고:** Gold 객체 키를 든 호출자가 그 값을 `catalog.industrial_complex.id`로 조회해
  전량 발행이 첫 행에서 멈추거나, 누락값을 건너뛰어 일부 단지만 조용히 발행하는 일.
- **막지 않는 사고:** 다른 모듈이 raw `Uuid`에 잘못된 id newtype 생성자를 선택하는 일. 공용
  newtype의 `new`와 transparent serde가 UUID version을 검증하지 않는 문제는 저장소 전체의 별도
  변경이다.

## 기각한 대안

- **Gold 요약에 Catalog UUIDv7 추가:** 정본 레이크하우스가 재구성 가능한 PostgreSQL 투영의
  식별자를 알아야 하고, 투영을 다시 만들 때 아티팩트 계약까지 바뀐다.
- **포인터 표를 UUIDv5로 바꾸기:** 기존 기본키·외래키와 Catalog API가 쓰는 UUIDv7 정체성을
  뒤집는 별도 스키마 프로그램이며, 이미 채워진 연결 컬럼과 lookup을 버린다.
- **못 푼 항목 건너뛰기:** 실행은 성공처럼 보이지만 앱에는 일부 단지 프로필만 보이는 침묵 실패다.
- **모든 조회·객체 GET·쓰기를 한 트랜잭션에 묶기:** 외부 R2 GET 1,442번 동안 DB 연결과 잠금을
  유지한다. 독립 포인터의 재실행/CAS 보장을 버릴 이유가 없다.
- **선택형 dry-run 추가:** 반드시 지켜야 할 불변식을 운영자 선택에 맡기고 실제 경로와 예행 경로를
  둘로 만든다.

## 결과

스키마와 마이그레이션은 바뀌지 않는다. 전량 발행은 기존 객체 검증 1,442 GET 전에 Catalog lookup
1,442회를 추가하며 로컬 투영에서 전부 해소되는 것을 확인한다. 단수 커맨드도 같은 공통 경계를
지나므로 별도의 수동 우회가 없다. 다른 raw UUID→id 경계의 version 검증은 이 결정의 남은 범위가
아니며 전수 스캔 결과를 구현 보고에 남긴다.
