# ADR 0022 - Lakehouse Handoff Vs Storage Format Boundary

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-07-02 |
| Scope | foundation-platform lakehouse transport, Silver/Gold storage, AI normalization input |
| Related | ADR 0006, ADR 0007, ADR 0019, ADR 0021 |

> Package ownership update (2026-07-17): [ADR 0026](0026-lakehouse-capability-ownership.md)
> supersedes only the package-owner references in this ADR. Lakehouse contracts now live in
> `lakehouse-domain`; the format-boundary decision below is unchanged.

## Decision

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

## Format Role Matrix

These formats are complementary. They must not be treated as interchangeable
because each one belongs to a different boundary.

| Boundary | Default format | Role |
|---|---|---|
| Bronze raw evidence | Source-native bytes | Preserve provider bytes as received: ZIP, CSV, XML, JSON, SHP, or other source payloads. Do not rewrite raw evidence into Avro, Parquet, or JSONL just to normalize storage. |
| Kafka / event transport | Protobuf by default; Avro allowed by topic decision | Carry small, schema-versioned event envelopes: ids, trace ids, object pointers, checksums, status, and schema version. Never carry raw files or full lakehouse rows. |
| Rust / engine processing memory | Arrow | Move batches through readers, normalizers, and writers as columnar in-memory data. Arrow is a processing/interchange format, not the canonical table store. |
| Silver / Gold physical files | Parquet or GeoParquet | Store canonical scalar and geometry tables for analytics, compression, predicate pushdown, and multi-engine reads. |
| Silver / Gold table abstraction | Iceberg | Manage table snapshots, schema evolution, manifests, rollback, and multi-engine commits over Parquet/GeoParquet files. |
| AI context packs, test fixtures, small handoff payloads | JSONL | Use only for bounded, transient model input, fixtures, or writer handoff where line-oriented text is useful. |

Kafka event format choice:

- Use **Protobuf** as the default for foundation-platform service events because
  Rust-first generated types make service boundaries explicit.
- Use **Avro** when a topic is primarily a data-platform stream that benefits
  from Avro-first Schema Registry, Kafka Connect, Spark, or Flink integration.
- Do not mix Avro and Protobuf inside one topic family. Pick one per event
  contract and version it.
- Outbox JSON/JSONB is the local transactional event record. It is not the
  future Kafka wire format.

Large intermediate data must not default to JSONL. If the intermediate payload
is large enough to be scanned, queried, partitioned, or repeatedly processed,
use Arrow for in-memory batches or Parquet/GeoParquet for durable intermediate
files.

Existing Rust structs such as `*SilverHandoff { jsonl: String }` are not lakehouse table storage.
They are writer-neutral transport payloads used to hand rows to Spark/Iceberg writers, tests, or
intelligence-platform proposal workers. The final table storage contract is the
`LakehouseTableContract.physical_format` in `lakehouse-domain`, not the temporary handoff field name.

## Why This Is The Enterprise Shape

This follows the common lakehouse split:

1. Medallion layers define data quality and ownership: Bronze is raw, Silver is validated, Gold is
   enriched/product-facing.
2. Iceberg defines the analytic table abstraction: snapshots, schema evolution, rollback, and safe
   multi-engine access.
3. Parquet/GeoParquet are the physical data files used under Silver/Gold tables.
4. JSONL is useful at edges because it is line-oriented, easy to stream, easy to diff, and easy to
   feed to a model or writer. It is not a table format and does not replace Iceberg metadata,
   snapshots, manifests, partition evolution, or Parquet statistics.

So the correct boundary is:

```text
Rust foundation-platform control plane:
  contract, lineage, proposal input, review gate, promotion decision

Spark / Iceberg writer:
  converts approved handoff rows into Parquet/GeoParquet Iceberg tables

Trino / query layer:
  reads Silver/Gold Iceberg tables, not handoff JSONL

intelligence-platform:
  may receive JSONL context packs as proposal input,
  but proposals return to foundation-platform inbox and never write Silver directly
```

## Rules

1. `jsonl` fields in app-layer handoff structs must be documented as transient transport.
2. No Silver/Gold contract may declare JSONL as its `LakehousePhysicalFormat`.
3. Silver canonical entities use `LakehousePhysicalFormat::Parquet` or
   `LakehousePhysicalFormat::GeoParquet`.
4. AI normalization context packs may be JSONL because they are proposal input, not canonical data.
5. Long-term storage, query, promotion, and rollback must refer to Iceberg snapshot/table contracts,
   not handoff file paths.
6. Kafka topics must use a versioned Protobuf or Avro event contract, not ad-hoc JSONL.
7. Kafka messages must carry claim-check pointers to R2/Iceberg/Postgres state, not raw payload blobs.
8. Arrow may be used for processing batches and writer boundaries, but not as the durable
   Silver/Gold table contract unless a future ADR explicitly changes the storage layer.

## Current Application

`silver.building_register_floors` is a canonical Silver table and stays Parquet:

```text
LakehouseTableContract {
  table_name: "silver.building_register_floors",
  layer: Silver,
  physical_format: Parquet,
  serving_role: Canonical,
}
```

The new `foundation-platform.floor_entity_context_pack.v1` payload is JSONL because it is an
intelligence-platform input stream. It does not mean `silver.building_register_floors` is stored as
JSONL.

## References

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
