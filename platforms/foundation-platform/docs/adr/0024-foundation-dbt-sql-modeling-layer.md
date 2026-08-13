# ADR 0024 - Foundation dbt SQL 모델 계층

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-07-09 |
| Scope | foundation-platform lakehouse SQL modeling |
| Related | ADR 0007, ADR 0022, Gongzzang ADR 0051 |

## 결정

foundation-platform adopts dbt Core with dbt-trino as the SQL modeling and SQL testing layer for Silver/Gold lakehouse models.

dbt owns:

- staging SQL models
- intermediate SQL models
- Silver/Gold SQL models
- SQL tests
- model and column documentation
- model-level lineage evidence

dbt does not own:

- Bronze collection
- source acquisition
- checksum truth
- AI calls
- human review workflows
- authorization
- publish authority
- rollback authority
- long-running acquisition workflows

## 근거

ADR 0007 already separates Rust control-plane, Spark batch compute, and Trino query. ADR 0022 separates transport handoff from final lakehouse storage. dbt fits as the SQL model layer on top of Trino without replacing Rust, Spark, Dagster, Temporal, or Foundation publish gates.

## 영향

- dbt project files live under `infra/lakehouse/dbt`.
- dbt target is Trino.
- dbt tests provide evidence consumed by Foundation quality gates.
- Foundation publish gates remain outside dbt.

## 참고 문서

- dbt docs: https://docs.getdbt.com/
- dbt Trino setup: https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup
