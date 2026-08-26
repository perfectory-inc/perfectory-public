---
status: current
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-08-26
---

# ADR 0057: 레이크하우스 재고는 현재 Iceberg 메타데이터를 읽는다

- Status: Accepted
- Date: 2026-08-26
- 관련: [ADR-0040 아무도 채우지 않는 컬럼은 필수일 수 없다](./0040-a-column-no-producer-fills-cannot-be-required.md),
  [ADR-0012 검증 결과는 그 문면대로여야 한다](./0012-verification-results-must-mean-what-they-say.md)

## Context

`verify-lakehouse-registry`는 namespace 등록을 검증하지만 계약에 선언된 Iceberg 표가 실제로
존재하는지, 현재 snapshot이 무엇인지, 그 snapshot이 몇 행과 몇 파일을 가리키는지는 답하지
않는다. REST catalog를 손으로 호출하거나 Spark/Trino를 띄우는 방법은 warehouse 경로와 실행
환경에 따라 실패했고, 실패와 빈 표를 구분하는 재현 가능한 관측 경계도 남기지 못했다.

표 목록을 운영 커맨드에 다시 쓰는 것도 해결이 아니다. 정본은
`lakehouse-domain/src/lakehouse.rs`의 `industrial_complex_lakehouse_contracts()`이고, 별도 목록은
계약이 늘어날 때 조용히 빠지는 사본이 된다. 착수 시점 `origin/main`의 정본은 사전 설명의 11개와
달리 9개를 반환한다. 재고 커맨드는 어느 숫자도 고정하지 않고 그 slice 전체를 매 실행 순회한다.

Apache Iceberg 사양은 snapshot의 `timestamp-ms`를 표 검사에 쓰는 생성 시각으로 정의하고, manifest
`data_file`의 `record_count`와 `file_size_in_bytes`를 필수 필드로 정의한다. 따라서 행 수와 저장 크기를
알기 위해 Parquet 본문을 여는 것은 이미 있는 메타데이터를 버리고 더 느리고 더 넓은 읽기를 하는
것이다. 근거는 [Iceberg table format specification](https://iceberg.apache.org/spec/)의 Snapshots와
Data File Fields다.

Cloudflare R2 Data Catalog는 표 load 같은 읽기 작업에 read-only catalog/R2 권한을 지원하고, vended
credential도 호출 토큰의 R2 권한을 상속한다고 명시한다. 근거는
[Cloudflare R2 Data Catalog catalog management](https://developers.cloudflare.com/r2-data-catalog/manage-catalogs/)다.

## Decision

1. `foundation-outbox-publisher inventory-lakehouse`가
   `industrial_complex_lakehouse_contracts()`의 현재 반환값 전체를 순서대로 관측한다. 표 이름을
   커맨드에 복제하지 않는다.
2. 존재 여부와 현재 snapshot id는 기존 `IcebergRestCatalog`의 read-only load-table 경로에서 읽는다.
   snapshot `timestamp-ms`를 `updated_at_utc`로 직렬화한다.
3. 현재 snapshot의 manifest list와 manifest는 ADR-0040 결정 8의
   `lakehouse_snapshot_scan`/`LakehouseObjectReader`만 읽는다. 새 Iceberg 디코더를 만들지 않는다.
   live data file의 `record_count` 합이 `row_count`, 파일 수가 `data_file_count`,
   `file_size_in_bytes` 합이 `bytes`다. Parquet data file은 열지 않는다.
4. R2 object read에는 `FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID`와
   `FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY`만 사용한다. 이 커맨드에는 R2 list,
   put, delete 경로와 쓰기 확인 플래그가 없다.
5. 출력은 `foundation-platform.lakehouse_inventory.v1` pretty JSON 하나다. 표별 `state`는
   `present`, `absent`, `read_failed` 중 하나다. `absent`는 `exists=false`인 정상 관측이고 전체
   status를 실패로 바꾸지 않는다. 권한·catalog·manifest·timestamp 실패는 `read_failed`와
   고정된 `error_kind`로 기록하고, 모든 표를 끝까지 관측한 뒤 전체 status를 `degraded`로 만들고
   프로세스를 실패시킨다.
6. 오류 출력에는 transport URL, account id, access key, token과 원본 provider 오류를 싣지 않는다.
   `error_kind`는 실패 단계만 말한다.

## Rejected alternatives

- `COUNT(*)`나 Parquet row scan은 데이터 규모에 비례하고 manifest의 필수 `record_count`를 무시한다.
- Spark/Trino shell은 별도 compute 기동과 출력 해석이 필요하며 관측 커맨드의 최소 경계가 아니다.
- REST URL을 운영자가 직접 조립하면 catalog prefix와 warehouse 규칙을 기존 adapter 밖에서 다시
  구현한다.
- 없는 표를 0행으로 보고하면 “실물이 없음”과 “존재하는 빈 snapshot”이 같은 사실이 된다.
- 첫 오류에서 중단하면 뒤 표의 상태를 실행하지 않은 채 재고를 전수 확인했다고 오해하게 한다.

## Consequences

운영자는 한 명령으로 계약 표의 존재, 현재 snapshot, manifest 행/파일/byte 통계와 갱신 시각을
확인한다. 새 표 계약이 canonical slice에 추가되면 같은 명령이 자동으로 포함한다. 표가 존재하지만
현재 snapshot이 없으면 `present`, snapshot id `null`, 세 통계 `0`으로 보고하며, catalog 404인 표는
`absent`와 통계 `null`로 구분한다.

이 결정은 표를 생성하거나 채우지 않고 신선도 경보도 만들지 않는다. 현재 상태는 변하는 운영 사실이므로
문서에 정본으로 복제하지 않고 매 실행 JSON이 그 시점의 증거다.
