# ADR 0015 - Bronze 객체 키 배치(대체됨)

- Status: Superseded by [ADR 0016](./0016-bronze-commit-protocol.md) and
  [ADR 0019](./0019-bronze-readable-object-lake-postgres-catalog-ssot.md)
- Date: 2026-06-24
- Owner: foundation-platform

## 이 파일을 남기는 이유

이 ADR은 과거 링크를 보존하기 위해서만 남긴다. 기존 본문은 세 가지를 혼합했다.
ideas that are now intentionally separated:

1. R2 object keys as physical storage locations.
2. Content checksums for integrity and deduplication.
3. Postgres `bronze_object` rows as the catalog/control-plane source of truth.

That wording made it too easy to treat the object key itself as the truth. The
current contract rejects that model.

## 현재 결정

현재 수용된 Bronze 계약은 다음과 같다.

- R2 `object_key` is a readable physical location label.
- Postgres `bronze_object` is the SSOT for source identity, snapshot date/period,
  snapshot basis, checksum, lineage, and provider file metadata.
- Bronze writes must pass through `BronzeCommitter`.
- Immutable raw writes use create-only storage plus recoverable commit semantics.
- Production code must not infer catalog truth by parsing `object_key` path
  tokens.

See:

- [ADR 0016 - Bronze Commit Protocol](./0016-bronze-commit-protocol.md)
- [ADR 0019 - Bronze Readable Object Lake + Postgres Catalog SSOT](./0019-bronze-readable-object-lake-postgres-catalog-ssot.md)

## 전환 메모

Historical R2 migration/audit tools may still parse old `run_id=...` and
`partition=...` path shapes while cleaning pre-ADR-0019 data. That is legacy data
repair code, not the current write contract.
