---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-07-29
---

# Foundation lakehouse

Bronze 이후 Silver/Gold 데이터 처리의 도메인·애플리케이션·인프라 경계입니다. Iceberg,
dbt, Spark, Trino 실행 구성은 `infra/lakehouse`가 소유하고 이 crate는 계약과 조합을 담당합니다.

- 설계 문서: [`docs/adr/0006-lakehouse-table-format-and-serving-architecture.md`](../../docs/adr/0006-lakehouse-table-format-and-serving-architecture.md)
- 실행 문서: [`docs/runbooks/lakehouse-compute-engines.md`](../../docs/runbooks/lakehouse-compute-engines.md)
- 검증: `cargo test -p foundation-lakehouse-domain`
