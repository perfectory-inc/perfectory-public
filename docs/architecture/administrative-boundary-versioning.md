---
status: current
owner: foundation
doc_type: architecture
last_reviewed: 2026-07-30
---

<!-- public-repository-safety: reviewed-public-contract -->

# 행정구역 경계와 필지 식별자 버전 관리

**상태:** 승인된 구현 계약

이 계약은 [ADR 0006](../adr/0006-object-storage-first-serving.md)과
[단일 출처 공간 데이터 공개 아키텍처](./single-source-spatial-publication.md)에 종속된다.
식별자와 버전만 다루며 타일 공개의 CAS 규칙을 대체하지 않는다.

Foundation은 공식 행정코드·이름·PNU를 토지의 정체성이 아니라 바뀔 수 있는 사실로 취급한다.
2026-07-01 시행된 **전남광주통합특별시** 설치는 그 사례다. 법적 근거는
[국가법령정보센터](https://www.law.go.kr/lsInfoP.do?efYd=20260701&joNo=029500&lsiSeq=284111&query=%EC%A0%84%EB%82%A8%EA%B4%91%EC%A3%BC%ED%86%B5%ED%95%A9%ED%8A%B9%EB%B3%84%EC%8B%9C+%EC%84%A4%EC%B9%98%EB%A5%BC+%EC%9C%84%ED%95%9C+%ED%8A%B9%EB%B3%84%EB%B2%95)와
[행정안전부 시행 공고](https://www.mois.go.kr/frt/bbs/type010/commonSelectBoardArticle.do?bbsId=BBSMSTR_000000000008&nttId=126841)다.

## 불변식

1. `catalog.parcel.id`와 `catalog.industrial_complex.id`는 변경되지 않는 내부 UUID다.
2. PNU·행정코드·행정명은 유효기간을 가진 외부 식별자다.
3. 표준 지적 PNU는 다른 필지에 재할당하지 않는다. 과거 식별자는 이력 조회 뷰에서 같은
   UUID로 해석하고, 현재 API 응답에는 현재 PNU를 반환한다.
4. 단순 명칭 변경은 같은 행정단위에 새 식별자·이름 사실을 추가하는 것이다. 통합·대체·분할은
   서로 다른 안정 단위 사이의 감사 가능한 전이이며 `renamed` 자기 간선은 만들지 않는다.
5. 승인된 사실은 `data_revision`, source snapshot, Catalog `source_record` UUID를 가진다.
   API용 사실은 추가 전용이며, 기간 종료와 기존 `catalog.parcel.pnu` 갱신은 publisher 함수만 수행한다.
6. 공개 타일은 정확히 하나의 `data_revision`에서 만든다. 동적 Martin과 정적 PMTiles는 그 리비전의
   파생 표현이지 별도 권위가 아니다.

`effective_period`는 PostgreSQL 반개구간 `[start, end)` 날짜 범위다. 법적 시행일은 한국 표준시
기준 하한이며 `infinity`는 현재 유효함을 뜻한다. 식별자 계약에서는 자정 UTC 변환을 하지 않는다.
현재 뷰는 DB 트랜잭션 날짜를 사용하고, 과거 별칭 조회는 PNU 소유권이 영구적이므로 날짜를 무시한다.

## 스키마와 호환성 다리

마이그레이션 `20260727000001_administrative_boundary_identity.sql`은 다음을 추가한다.

- `catalog.administrative_boundary_revision`: 정본 리비전 원장. 공개될 때 UUID를
  `vector_tile_release.data_revision`으로 재사용한다. 숫자 `canonical_iceberg_snapshot_id`와
  문자열 `source_snapshot_id`는 분리하고 복합 외래키로 묶는다.
- `catalog.administrative_unit` 및 유효기간별 코드·이름 행
  `catalog.administrative_unit_identifier`.
- `catalog.administrative_unit_transition`의 `replaced_by`, `merged_into`, `split_from` 전이와
  `catalog.administrative_unit_parent`의 유효기간별 계층.
- 안정적인 `catalog.parcel.id`에 연결되는 `catalog.parcel_identifier`와
  `catalog.parcel_administrative_unit` 소속 사실.
- 기간 중복·출처/리비전 참조·순환·단위 종류를 막는 제약과 추가 전용 트리거.
- 모든 과거 별칭 조회용 `catalog.parcel_identifier_lookup`, 현재 표시용
  `catalog.parcel_current_identifier`.
- 제한된 레거시 대체 경로를 감시하는 `catalog.parcels_missing_temporal_identifier` 뷰.
  표준 PNU가 여기에 남으면 마이그레이션 결함이지 정상 상태가 아니다.

기존 표준 PNU 필지는 결정론적인 `foundation.migration` source record와 기존 행의
`created_at`부터 시작하는 레거시 리비전으로 보정한다. 이는 출처를 꾸며내는 것이 아니라 호환성
다리다. 표준 PNU가 없는 블록·대장 필지는 기존 대장 키를 유지하며 PNU 별칭을 임의로 만들지 않는다.

새 수집은 실제 `catalog.source_record` 등록, 후보 리비전 생성, 전체 사실 삽입, 검증,
PostGIS 투영을 순서대로 수행한 뒤 기존 CAS 게이트로 타일 매니페스트를 공개한다.
`catalog.parcel.pnu` 갱신도 같은 publisher 트랜잭션에서 수행하므로 기존 호출자와 fixture는 그대로 동작한다.

ADR-0006의 점 조회 산출물은 별도 투영이다. JSON/KV PNU 산출물을 켜더라도 같은 리비전에서
기존 별칭과 현재 PNU(또는 resolver 레코드)를 함께 만들며 두 번째 식별자 권위를 만들지 않는다.

## 통합·대체·분할 처리

전남광주 변경과 같은 전이는 항상 `from_unit_id`(이전 단위)와 `to_unit_id`(다음 단위)를 사용한다.
`split_from`은 새 후속 단위마다 한 번만 기록한다.

1. 공식 원본 객체를 보존하고 `source_record`를 등록한다.
2. 후보 `administrative_boundary_revision`과 안정 단위를 만든다. 기존 단위·식별자는 보존하고
   기간 종료는 publisher만 수행한다.
3. 새 코드·이름 사실과 `merged_into`/`replaced_by` 간선을 추가한다. 단순 명칭 변경은 기존 단위의
   새 식별자 사실이다.
4. 영향받은 필지의 소속·PNU 사실을 조정한다. 필지 UUID·공간 계보·건물·매물은 명칭 변경만으로
   다시 만들지 않는다.
5. 건수·기하·계층·별칭 유일성·전이 범위·레벨별 현재 소속을 검증하고 하나의 PostGIS snapshot을
   만들며 같은 `data_revision`을 기록한다.
6. 즉시 공개가 필요하면 runtime-manifest CAS pointer로 동적 Martin을 승격한다.
7. 승인된 일정에 따라 같은 리비전의 불변 PMTiles를 만들고 검증·승격한다. 이전 릴리스는 롤백용으로
   보존하며 덮어쓰지 않는다.

PMTiles를 만드는 동안 정정이 도착하면 고정된 `data_revision`이 최신이 아니므로 해당 빌드를
`superseded`로 표시하고 새 리비전에서 다시 시작한다. 이 규칙으로 오래된 행정명·폴리곤의 재등장을 막는다.

## 타일·API 호환성

MVT의 `pnu` 속성은 기존 MapLibre 매니페스트·스타일·Foundation marker 계약 때문에 현재 PNU로
유지한다. 안정적인 필지 UUID는 내부 계보 식별자로 필요할 때 함께 싣는다. 클라이언트 DTO와
렌더러는 바꾸지 않는다. 정적·동적 소스는 [단일 출처 공개 계약](./single-source-spatial-publication.md)에
따라 하나의 완전한 소스로 함께 전환한다.

공식 경계 수집기는 각 Polygon/MultiPolygon과 결정론적 geometry hash를 source JSONL에 보존한다.
`write-administrative-spatial-scope-registry`가 증거를 검증하고,
`publish-administrative-boundary-postgis`가 추가 전용
`serving_postgis.administrative_unit_boundary_publication`에 투영한다. Martin의 `admin` 뷰는
완전한 runtime-manifest CAS 승격에 포함되기 전까지 비어 있다. publisher는 정부가 제공하지 않은
이름·상위 관계를 만들어내지 않으며, 수집·검증·투영·런타임 승격을 분리해 감사 가능하게 유지한다.
