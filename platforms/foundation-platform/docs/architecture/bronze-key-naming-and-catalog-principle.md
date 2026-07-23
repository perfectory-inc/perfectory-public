# Bronze 객체 키 네이밍 & 사람-가독성 원칙 (with 업계 출처)

> 2026-06-23 정리. Bronze 객체 키를 "더 예쁘게/사람이 보기 편하게" 바꾸고 싶을 때 읽는 문서.
> **결론: 물리 키는 stable/machine-faithful로 두고, 사람용 이름은 카탈로그/Silver/Gold에 둔다.**
> 키를 가독성 때문에 rename 하는 것은 마이그레이션이며 claim-check 참조를 깨뜨린다.

## 1. 원칙

- **객체 키 = 집 주소(불변 주소).** 키는 원장(`bronze_object.object_key`),
  `collection.raw_written` 이벤트의 `bronze_object_key`(claim-check), Silver/Gold lineage가
  참조한다. 키를 바꾸면 그것을 적어둔 모든 곳을 고쳐야 한다(= 마이그레이션). 그래서
  **Bronze 키는 stable/immutable이 미덕**이다.
- **의미(사람 이름)는 경로에 박지 말고 카탈로그에 둔다.** 친절한 이름/설명/카테고리는
  `SourceCatalogEntry`(필드 `name`/`dataset_name`/`provider`) 같은 **별도 매핑 레이어**에 둔다.
  카탈로그 이름은 언제든 무손실로 바꿀 수 있다(데이터 안 건드림).
- **진짜 "사람이 보는 뷰"는 Silver/Gold에서** 깨끗한 테이블 이름/업무 파티션으로 만든다.
- **물리 키 정리가 꼭 필요하면** 한 번, 일찍, 계획된 마이그레이션으로 하고 포맷을 계약으로
  동결한다(이미 `ingest_date`를 키에서 제거하는 마이그레이션을 한 차례 수행한 선례 있음:
  `services/foundation-outbox-publisher/src/r2_bronze_key_migration.rs`).

## 2. 현재 레이아웃 (코드로 확정)

> 2026-07-20 좌표 갱신: capability extraction 으로 bronze/bulk 코드가 `crates/catalog/*` 에서
> `crates/collection/*` 로 이동했다. 또한 §2 의 슬러그 예시는 ADR 0014 개명 이전 표기이며,
> 현행 슬러그 SSOT 는 [`bronze-source-slug-rename.v1.md`](../catalog/bronze-source-slug-rename.v1.md)
> (`{providerid}__{dataset_slug}`) 다.

- 현재 키 포맷(`crates/collection/collection-domain/src/bronze.rs`):
  `bronze/source={slug}/run_id={uuid}/partition={...}/part-{NNNNNN}.{ext}`
- bulk 소스 partition suffix(`crates/collection/collection-application/src/public_data_bulk_plan.rs`):
  `operation={op}/provider_file_period={YYYY-MM}/provider_file_id={id}`
- **수집 날짜는 키에 없다.** "언제 수집했나"는 `run_id`(실행 UUID) + 메타데이터(DB
  `ingestion_run.started_at`, JSONL 원장 이벤트 타임스탬프)에 있다. 과거 키는
  `.../ingest_date=YYYY-MM-DD/...`였으나 의도적으로 제거(위 마이그레이션) - 같은 데이터를 다른
  날 재수집하면 폴더가 갈라지는 문제를 피하기 위함. = 좋은 모델링 결정.
- `provider_file_period`는 허브 페이지가 표기한 **파일 기간(년-월)**으로, 데이터가 다루는 달이지
  수집한 날이 아니다(`crates/collection/collection-infrastructure/src/building_hub_bulk.rs`에서 스크랩).
- 두 슬러그 계열이 공존:
  - 의미 있는 이름: `source=hub-building-building-electricity-usage` 등(건축 데이터셋들).
  - 자동 생성 코드: `source=hub-go-kr-public-bulk-task-{group}-{code}`
    (`services/foundation-outbox-publisher/src/building_hub_bulk_collection_plan.rs:319`) - hub.go.kr의 bulk
    다운로드 "작업" 하나당 폴더 하나라 수가 많다. -> 카탈로그에서 친절한 이름으로 매핑할 후보.
- `partition=operation=...`의 이중 `=`는 사소한 흠집(동작 무해). 물리 정리는 §1의 "계획된
  마이그레이션" 규율로만.

## 3. 업계 출처 (2026-06-23, 실제 fetch 후 verbatim 인용)

원칙("키는 기계용/stable, 사람 이름은 카탈로그/Silver/Gold")의 근거:

1. **Databricks - Medallion(Bronze=원본 / Gold=업무용) + Unity Catalog**
   - https://www.databricks.com/glossary/medallion-architecture
     > "The bronze layer contains raw, unvalidated data. … maintains the raw state of the data
     > source in its original formats … preserving the data's fidelity. … [Gold] layer is designed
     > for business users."
   - https://docs.databricks.com/aws/en/lakehouse-architecture/data-governance/best-practices
     > "the Unity Catalog is the central component for governing … provides a three-level namespace
     > … : Catalog, Schema, Table/view."
   - ⚠️ "사람 친화 이름 = Unity Catalog"는 두 페이지를 합친 해석(한 문장 공식 문구는 아님).

2. **Netflix -> Apache Iceberg (물리/논리 분리 + hidden partitioning)**
   - https://iceberg.apache.org/docs/latest/partitioning/ (공식 1차)
     > "Because Iceberg doesn't require user-maintained partition columns, it can hide partitioning.
     > … queries no longer depend on a table's physical layout. With a separation between physical
     > and logical, Iceberg tables can evolve partition schemes over time."
   - https://en.wikipedia.org/wiki/Apache_Iceberg
     > "Iceberg was originally developed at Netflix in 2017 … started at Netflix by Ryan Blue and Dan Weeks."
   - ⚠️ 넷플릭스 기원은 Wikipedia(2차) - 넷플릭스 테크블로그가 Medium 인증벽이라 1차 미확보.

3. **AWS - Glue Data Catalog(메타 레이어) + Athena Hive `key=value` 파티셔닝**
   - https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html
     > "The AWS Glue Data Catalog is a centralized repository that stores metadata … acts as an
     > index to the location, schema, and runtime metrics of your data sources."
   - https://docs.aws.amazon.com/athena/latest/ug/partitions.html
     > "Athena can use Apache Hive style partitions, whose data paths contain key value pairs
     > connected by equal signs (for example, `country=us/…` or `year=2021/month=01/day=26/…`) …
     > use the `PARTITIONED BY` clause."

4. **Uber -> Apache Hudi**
   - https://www.uber.com/us/en/blog/apache-hudi-at-uber/
     > "…led Uber engineers to build Hudi, a new class of storage engine purpose-built for the data
     > lake. … designed and built at Uber."
   - https://www.uber.com/us/en/blog/hoodie/
     > "…we built Hudi … Hudi maintains the metadata of all activity performed on the dataset as a timeline."
   - ⚠️ "경로 대신 카탈로그"는 정확히는 Hudi 자체 메타데이터(timeline/metadata table)로 파일 추적.

5. **Hive-style partitioning이 표준(`key=value/`)**
   - https://duckdb.org/docs/lts/data/partitioning/hive_partitioning
     > "Hive partitioning … search for a `'key' = 'value'` pattern. … Filters on the partition keys
     > are automatically pushed down … skips reading files that are not necessary to answer a query."
   - (Trino 공식 Hive connector 문서도 동일 레이아웃 확인.)

## 4. 권장 (안전하게 깔끔해지기)

1. Bronze 물리 키 **그대로 유지**(예쁘게 하려고 또 바꾸지 않기 - 마이그레이션 treadmill).
2. 사람용 이름은 **카탈로그로**: `SourceCatalogEntry`에 친절한 한국어 이름/설명/카테고리 채우기
   (특히 `hub-go-kr-public-bulk-task-*` 코드 슬러그 -> 의미 있는 이름 매핑). 무손실, 언제든 변경.
3. 물리 흠집(이중 `=` 등)은 **계획된 1회 마이그레이션 + 계약 동결**로만
   ([ADR 0015](../adr/0015-bronze-object-key-content-addressed-layout.md)와
   [ADR 0016](../adr/0016-bronze-commit-protocol.md)이 현행 포맷 계약).
4. 진짜 가독 뷰는 **Silver/Gold** + (장차) 카탈로그/대시보드.
