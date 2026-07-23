# Lakehouse Industry Reference

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-14 |
| 범위 | `foundation-platform` R2 + Iceberg lakehouse 설계에 반영할 외부 레퍼런스 |
| 관련 ADR | [ADR 0006 - Lakehouse Table Format And Serving Architecture](../adr/0006-lakehouse-table-format-and-serving-architecture.md) |

## 결론

Netflix 사례는 우리가 선택한 방향을 지지한다. 핵심은 모든 데이터를 PostgreSQL/PostGIS에
넣는 것이 아니라, object storage 위에 Iceberg catalog, metadata, manifest, snapshot 계층을
두고 table처럼 안전하게 운영하는 것이다.

다만 Netflix의 내부 플랫폼을 그대로 복제하지 않는다. `foundation-platform`는 처음부터 거대한
Spark/Java 최적화 시스템을 만들지 않고, contract-first로 Iceberg table과 serving mirror를
정의한 뒤 데이터량과 쿼리 패턴이 증명될 때 maintenance worker를 확장한다.

## Netflix에서 배울 점

### 1. 파일을 그냥 나열하지 않는다

초기 Netflix Iceberg 저장소는 Iceberg가 개별 data file을 table metadata로 추적하고,
metadata 전환을 atomic operation으로 처리한다고 설명한다. snapshot은 특정 시점의 파일
집합이고, manifest가 data file, partition data, metrics를 가진다.

`foundation-platform` 적용:

- R2 object key 목록을 직접 훑는 방식을 query contract로 만들지 않는다.
- Lakehouse capability는 최신 Iceberg metadata pointer를 관리한다.
- API는 active snapshot과 lineage를 기준으로 읽는다.
- `*-index.json` 같은 수동 index 파일을 canonical index로 쓰지 않는다.

### 2. object storage의 약점을 table format으로 보완한다

Apache Iceberg Incubator 제안서는 Iceberg가 cloud blob store의 directory listing 지연,
rename 부재, 약한 namespace consistency 같은 조건에서도 잘 동작하도록 설계됐다고 설명한다.

`foundation-platform` 적용:

- R2를 단순 파일창고가 아니라 Iceberg table storage로 사용한다.
- Parquet/GeoParquet 파일 자체는 R2에 두되, table identity와 current snapshot은 catalog가
  관리한다.
- directory listing 기반 운영, 파일명 추측 기반 운영, 수동 pointer 운영을 금지한다.

### 3. 최적화는 별도 운영 계층이다

Netflix AutoOptimize 사례는 merge, sort, compaction, metadata optimization을 별도 시스템으로
다룬다. 중요한 원칙은 주기적으로 전체를 쓸어버리는 것이 아니라, 변경 이벤트와 snapshot
신호를 보고 필요한 만큼만 최적화하는 것이다.

`foundation-platform` 적용:

- 초기 PoC에서는 대형 maintenance system을 만들지 않는다.
- 대신 table contract에 partition, sort, quality gate, snapshot lineage를 먼저 고정한다.
- Rust domain에는 provider-neutral maintenance planner를 둔다.
- 데이터량이 커지면 `lakehouse-maintenance` worker를 추가한다.
- maintenance worker는 small file merge, sort rewrite, manifest rewrite, snapshot expiration,
  statistics refresh를 담당한다.

## 우리 구조로 번역

```text
Source API/File
  -> Bronze immutable object
  -> Silver Iceberg table
  -> Gold Iceberg projection or immutable artifact
  -> Derived serving layer
       - PostGIS mirror
       - search index
       - vector tile / PMTiles artifact
       - cache
```

`gongzzang`과 `dawneer`는 이 계층 중 Gold/API/manifest/event만 소비한다. 공통 데이터에 대한
Iceberg commit, R2 object write, PostGIS mirror write는 `foundation-platform`가 소유한다.

## Lakehouse Maintenance Backlog

초기에는 구현하지 않지만, 운영 데이터가 커지면 다음 순서로 추가한다.

1. Snapshot observer
   - Iceberg snapshot 변경을 감지한다.
   - 변경된 table, partition, file count, small file ratio를 기록한다.

2. Table health metrics
   - file count
   - average file size
   - manifest count
   - partition skew
   - row count drift
   - null ratio drift
   - geometry validity drift

3. Rewrite planner
   - 전체 overwrite가 아니라 필요한 file/partition만 고른다.
   - 비용 대비 효과가 낮은 rewrite는 보류한다.
   - hot serving table을 우선순위로 둔다.

4. Rewrite executor
   - small file merge
   - sort/order rewrite
   - manifest rewrite
   - expired snapshot cleanup

5. Promotion guard
   - rewrite 결과가 품질 gate를 통과해야 Gold/serving pointer를 갱신한다.
   - 실패하면 이전 snapshot으로 rollback 가능해야 한다.

초기 코드 계약:

- `lakehouse-domain::LakehouseTableHealth`
- `lakehouse-domain::LakehouseMaintenancePolicy`
- `lakehouse-domain::plan_lakehouse_maintenance`

이 계약은 Cloudflare, R2, Spark, SQL에 의존하지 않는다. Infra worker는 이 plan을 받아 실제
Iceberg rewrite job, manifest rewrite, snapshot expiration을 실행한다.

## 적용하지 않는 것

- Netflix 내부 Java/Spark 운영 구조를 그대로 복제하지 않는다.
- 초기 단계에서 multi-tenant priority queue, 대형 actor pool, 자동 튜닝 시스템을 만들지 않는다.
- Iceberg metadata를 무시하고 R2 object listing으로 직접 필터링하지 않는다.
- PostGIS를 canonical public-data store로 승격하지 않는다.

## 참고 자료

- [Netflix Iceberg GitHub repository](https://github.com/Netflix/iceberg)
- [Apache Iceberg Incubator Proposal](https://cwiki.apache.org/confluence/display/INCUBATOR/IcebergProposal?src=contextnavpagetreemode)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Apache Iceberg REST Catalog OpenAPI](https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml)
- [Netflix Technology Blog - Optimizing data warehouse storage](https://netflixtechblog.com/optimizing-data-warehouse-storage-7b94a48fdcbe)
- [Netflix Engineering Blog mirror - Optimizing data warehouse storage](https://www.engineering.fyi/article/optimizing-data-warehouse-storage)
- [Cloudflare R2 Data Catalog](https://developers.cloudflare.com/r2/data-catalog/)
