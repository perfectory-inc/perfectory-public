---
status: current
owner: foundation-platform
doc_type: architecture
last_reviewed: 2026-11-05
---

# Bronze 객체 키 이름과 사람이 읽기 쉬움의 원칙 (업계 출처와 함께)

> 2026-06-23 정리. Bronze 객체 키를 "더 예쁘게/사람이 보기 편하게" 바꾸고 싶을 때 읽는 문서.
> **결론: 물리 키는 안정되고 기계에 충실하게 두고, 사람용 이름은 카탈로그·Silver·Gold 에 둔다.**
> 읽기 쉽게 하려고 키 이름을 바꾸는 것은 마이그레이션이며 보관증(claim-check) 참조를 깨뜨린다.

## 1. 원칙

- **객체 키 = 집 주소(불변 주소).** 키는 원장(`bronze_object.object_key`),
  `collection.raw_written` 이벤트의 `bronze_object_key`(보관증), Silver·Gold 계보가
  참조한다. 키를 바꾸면 그것을 적어둔 모든 곳을 고쳐야 한다(= 마이그레이션). 그래서
  **Bronze 키는 안정·불변이 미덕**이다.
- **의미(사람 이름)는 경로에 박지 말고 카탈로그에 둔다.** 친절한 이름/설명/카테고리는
  `SourceCatalogEntry`(필드 `name`/`dataset_name`/`provider`) 같은 **별도 대응 계층**에 둔다.
  카탈로그 이름은 언제든 무손실로 바꿀 수 있다(데이터 안 건드림).
- **진짜 "사람이 보는 보기"는 Silver·Gold 에서** 깨끗한 표 이름·업무 파티션으로 만든다.
- **물리 키 정리가 꼭 필요하면** 한 번, 일찍, 계획된 마이그레이션으로 하고 포맷을 계약으로
  동결한다(이미 `ingest_date`를 키에서 제거하는 마이그레이션을 한 차례 수행한 선례 있음:
  `services/foundation-outbox-publisher/src/r2_bronze_key_migration.rs`).

## 2. 현재 레이아웃 (코드로 확정)

현행 키 계약의 정본은 [ADR 0019](../adr/0019-bronze-readable-object-lake-postgres-catalog-ssot.md)
(배치)·[ADR 0016](../adr/0016-bronze-commit-protocol.md)(커밋)·루트
[ADR-0152](../../../../docs/adr/0152-bronze-keys-of-reused-provider-file-numbers-name-their-bytes.md)(같은 파일 번호의
새 바이트)이고, 키를 만드는 코드는 `crates/collection/collection-domain/src/bronze.rs`의
`build_bronze_object_key`다. 슬러그 표기 정본은
[`bronze-source-slug-rename.v1.md`](../catalog/bronze-source-slug-rename.v1.md)(`{providerid}__{dataset_slug}`, ADR 0014)다.
여기서는 이 원칙과 맞닿는 성질만 적는다.

- 키 모양은 `bronze/source={slug}[/{hive key=value 분할}]/{leaf}.{ext}`이며, 제공기관이 같은 파일 번호에
  새 바이트를 올리는 경우 `{leaf}--sha256-{hex}.{ext}`로 바이트를 이름에 넣는다.
- **수집 날짜·실행 ID·스냅샷 기간은 키에 없다.** `run_id`, `ingest_date`, `provider_file_period`,
  `snapshot_*` 같은 분할은 키 생성기가 거부한다. "언제 수집했나"와 "어느 달의 파일인가"는 Postgres
  카탈로그 메타데이터에 있다. 과거 `.../ingest_date=YYYY-MM-DD/...` 키는 의도적으로 제거했다(§1의
  마이그레이션) — 같은 데이터를 다른 날 재수집하면 폴더가 갈라지는 문제를 피하기 위함이다.
- 대량 파일은 `provider_file_id`가 마지막 이름이 되고, `operation=` 분할은 슬러그가 이미 그 작업을
  담지 않을 때만 붙는다(`crates/collection/collection-application/src/public_data_bulk_plan.rs`).
- 의미 없는 자동 생성 슬러그(`hub-go-kr-public-bulk-task-*`)는 더 이상 만들지 않는다. 카탈로그에
  `dataset_slug`가 등록되지 않은 hub.go.kr 대량 작업은 수집을 거부한다
  (`crates/publisher/foundation-publisher-collection/src/building_hub_bulk_collection_plan.rs`).

## 3. 업계 출처 (2026-06-23, 실제로 받아 본 뒤 원문 그대로 인용)

원칙("키는 기계용으로 안정되게, 사람 이름은 카탈로그·Silver·Gold")의 근거(인용은 원문 그대로 둔다):

1. **Databricks - Medallion(Bronze=원본 / Gold=업무용) + Unity Catalog**
   - https://www.databricks.com/glossary/medallion-architecture
     > "The bronze layer contains raw, unvalidated data. … maintains the raw state of the data
     > source in its original formats … preserving the data's fidelity. … [Gold] layer is designed
     > for business users."
   - https://docs.databricks.com/aws/en/lakehouse-architecture/data-governance/best-practices
     > "the Unity Catalog is the central component for governing … provides a three-level namespace
     > … : Catalog, Schema, Table/view."
   - ⚠️ "사람 친화 이름 = Unity Catalog"는 두 페이지를 합친 해석(한 문장으로 된 공식 문구는 아님).

2. **Netflix -> Apache Iceberg (물리·논리 분리 + 숨은 파티셔닝)**
   - https://iceberg.apache.org/docs/latest/partitioning/ (공식 1차)
     > "Because Iceberg doesn't require user-maintained partition columns, it can hide partitioning.
     > … queries no longer depend on a table's physical layout. With a separation between physical
     > and logical, Iceberg tables can evolve partition schemes over time."
   - https://en.wikipedia.org/wiki/Apache_Iceberg
     > "Iceberg was originally developed at Netflix in 2017 … started at Netflix by Ryan Blue and Dan Weeks."
   - ⚠️ 넷플릭스 기원은 위키백과(2차) — 넷플릭스 기술 블로그가 Medium 인증벽이라 1차 출처를 못 얻었다.

3. **AWS - Glue Data Catalog(메타 계층) + Athena Hive `key=value` 파티셔닝**
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
   - ⚠️ "경로 대신 카탈로그"는 정확히는 Hudi 자체 메타데이터(타임라인·메타데이터 표)로 파일을 추적한다.

5. **Hive 식 파티셔닝이 표준(`key=value/`)**
   - https://duckdb.org/docs/lts/data/partitioning/hive_partitioning
     > "Hive partitioning … search for a `'key' = 'value'` pattern. … Filters on the partition keys
     > are automatically pushed down … skips reading files that are not necessary to answer a query."
   - (Trino 공식 Hive 커넥터 문서도 같은 배치를 확인했다.)

## 4. 권장 (안전하게 깔끔해지기)

1. Bronze 물리 키 **그대로 둔다**(예쁘게 하려고 또 바꾸지 않는다 — 마이그레이션 쳇바퀴).
2. 사람용 이름은 **카탈로그로**: `SourceCatalogEntry`(`name`/`dataset_name`/`provider`)에 친절한
   한국어 이름·설명·분류를 채운다. 손실 없이 언제든 바꾼다.
3. 물리 키 정리는 **계획된 한 번의 마이그레이션 + 계약 동결**로만 한다
   ([ADR 0019](../adr/0019-bronze-readable-object-lake-postgres-catalog-ssot.md)와
   [ADR 0016](../adr/0016-bronze-commit-protocol.md)이 현행 형식 계약).
4. 진짜 읽기 쉬운 보기는 **Silver·Gold** + (앞으로) 카탈로그·대시보드.
