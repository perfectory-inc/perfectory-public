# ADR 0022 - 레이크하우스 전달과 저장 형식 경계

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-07-02 |
| Scope | foundation-platform lakehouse transport, Silver/Gold storage, AI normalization input |
| Related | ADR 0006, ADR 0007, ADR 0019, ADR 0021 |

> Package ownership update (2026-07-17): [ADR 0026](0026-lakehouse-capability-ownership.md)가
> 이 ADR의 package-owner reference만 대체한다. Lakehouse contract는 이제
> `lakehouse-domain`에 있으며 아래 format 경계 결정은 변하지 않는다.

## 결정

foundation-platform separates **transport handoff format** from **lakehouse physical storage format**.

```text
Bronze raw evidence
  -> source-native bytes in object storage + Postgres catalog metadata

Silver / Gold lakehouse tables
  -> Apache Iceberg tables
  -> Parquet for scalar tables
  -> GeoParquet for geometry tables

JSONL
  -> transient handoff / fixture / model-input transport only
  -> never the canonical physical storage format for Silver or Gold tables
```

## 형식별 역할 표

이 형식들은 서로 보완적이다. 각각 다른 경계에 속하므로 상호 교환 가능한 것으로 취급해서는
안 된다.

| Boundary | Default format | Role |
|---|---|---|
| Bronze raw evidence | Source-native bytes | 받은 provider byte를 ZIP·CSV·XML·JSON·SHP 등 그대로 보존한다. 저장을 정규화한다는 이유로 Avro·Parquet·JSONL로 다시 쓰지 않는다. |
| Kafka / event transport | 기본 Protobuf; topic 결정에 따라 Avro 허용 | id·trace id·object pointer·checksum·status·schema version을 담은 작은 schema-versioned event envelope다. raw file이나 전체 lakehouse row는 담지 않는다. |
| Rust / engine processing memory | Arrow | reader·normalizer·writer 사이 batch를 columnar in-memory data로 이동한다. Arrow는 processing/interchange format이지 canonical table store가 아니다. |
| Silver / Gold physical files | Parquet 또는 GeoParquet | analytics·compression·predicate pushdown·multi-engine read를 위한 canonical scalar·geometry table을 저장한다. |
| Silver / Gold table abstraction | Iceberg | Parquet/GeoParquet file 위에서 table snapshot·schema evolution·manifest·rollback·multi-engine commit을 관리한다. |
| AI context pack, test fixture, small handoff payload | JSONL | 제한된 일시적 model input·fixture·writer handoff에서 line-oriented text가 유용할 때만 사용한다. |

Kafka event format choice:

- foundation-platform service event의 기본은 **Protobuf**로 한다. Rust-first generated type가
  service 경계를 명시하기 때문이다.
- topic이 data-platform stream이고 Avro-first Schema Registry·Kafka Connect·Spark·Flink
  integration의 이점이 있으면 **Avro**를 사용한다.
- 하나의 topic family에서 Avro와 Protobuf를 섞지 않는다. event contract마다 하나를 선택하고
  version을 붙인다.
- Outbox JSON/JSONB는 local transactional event record이며 미래 Kafka wire format이 아니다.

큰 intermediate data의 기본값을 JSONL로 하지 않는다. payload가 scan·query·partition·반복
처리할 만큼 크면 in-memory batch에는 Arrow를, durable intermediate file에는
Parquet/GeoParquet를 사용한다.

`*SilverHandoff { jsonl: String }` 같은 기존 Rust struct는 lakehouse table storage가 아니다.
Spark/Iceberg writer·test·intelligence-platform proposal worker에 row를 전달하는 writer-neutral
transport payload다. 최종 table storage contract는 임시 handoff field 이름이 아니라
`lakehouse-domain`의 `LakehouseTableContract.physical_format`다.

## 이 형태를 선택한 이유

이는 일반적인 lakehouse 분리를 따른다.

1. Medallion layer가 data quality와 ownership을 정의한다. Bronze는 raw, Silver는 validated,
   Gold는 enriched/product-facing다.
2. Iceberg가 analytic table abstraction을 정의한다. snapshot·schema evolution·rollback·
   안전한 multi-engine access를 제공한다.
3. Parquet/GeoParquet가 Silver/Gold table 아래의 물리 data file이다.
4. JSONL은 line-oriented이고 stream·diff·model/writer 전달이 쉬워 edge에서 유용하다. table
   format이 아니며 Iceberg metadata·snapshot·manifest·partition evolution·Parquet statistic을
   대체하지 않는다.

So the correct boundary is:

```text
Rust foundation-platform control plane:
  contract, lineage, proposal input, review gate, promotion decision

Spark / Iceberg writer:
  승인된 handoff row를 Parquet/GeoParquet Iceberg table로 변환한다.

Trino / query layer:
  handoff JSONL이 아니라 Silver/Gold Iceberg table을 읽는다.

intelligence-platform:
  JSONL context pack을 proposal input으로 받을 수 있지만,
  proposal은 foundation-platform inbox로 돌아가며 Silver에 직접 쓰지 않는다.
```

## 규칙

1. app-layer handoff struct의 `jsonl` field는 transient transport로 문서화한다.
2. Silver/Gold contract가 `LakehousePhysicalFormat`으로 JSONL을 선언하지 않는다.
3. Silver canonical entity는 `LakehousePhysicalFormat::Parquet` 또는
   `LakehousePhysicalFormat::GeoParquet`를 사용한다.
4. AI normalization context pack은 canonical data가 아닌 proposal input이므로 JSONL을 쓸 수 있다.
5. 장기 storage·query·promotion·rollback은 handoff file path가 아니라 Iceberg snapshot/table
   contract를 참조한다.
6. Kafka topic은 임의 JSONL이 아니라 versioned Protobuf 또는 Avro event contract를 사용한다.
7. Kafka message는 raw payload blob이 아니라 R2/Iceberg/Postgres state의 claim-check pointer를
   담는다.
8. Arrow는 processing batch와 writer 경계에 사용할 수 있지만, 향후 ADR이 storage layer를
   명시적으로 바꾸지 않는 한 durable Silver/Gold table contract로 사용하지 않는다.

## 현재 적용

`silver.building_register_floors`는 canonical Silver table이며 Parquet로 유지한다.

```text
LakehouseTableContract {
  table_name: "silver.building_register_floors",
  layer: Silver,
  physical_format: Parquet,
  serving_role: Canonical,
}
```

새 `foundation-platform.floor_entity_context_pack.v1` payload는 intelligence-platform input
stream이다. 이것이 `silver.building_register_floors`를 JSONL로 저장한다는 뜻은 아니다.

## 참고 문서

- Databricks medallion architecture: Bronze raw, Silver validated, Gold enriched, with layered
  quality and governance:
  https://docs.databricks.com/aws/en/lakehouse/medallion
- Apache Iceberg: open table format for analytic datasets, shared safely by engines including
  Spark and Trino:
  https://iceberg.apache.org/
- Apache Iceberg specification: Parquet is one of the valid data file formats for Iceberg tables:
  https://iceberg.apache.org/spec/
- Trino Iceberg connector: Iceberg tables read/write Avro, ORC, and Parquet; default file format is
  Parquet:
  https://trino.io/docs/current/connector/iceberg.html
- Apache Arrow: language-independent columnar in-memory format for efficient analytic processing:
  https://arrow.apache.org/
- Apache Avro: data serialization system with binary encoding and schema evolution:
  https://avro.apache.org/
- Protocol Buffers: language-neutral, platform-neutral structured data serialization:
  https://protobuf.dev/
- Confluent Schema Registry: supports Avro, Protobuf, and JSON Schema for Kafka data contracts:
  https://docs.confluent.io/platform/current/schema-registry/
- Netflix Maestro + Apache Iceberg: Netflix-style data platform separates workflow orchestration
  and Iceberg table processing:
  https://netflixtechblog.com/incremental-processing-using-netflix-maestro-and-apache-iceberg-b8ba072ddeeb
