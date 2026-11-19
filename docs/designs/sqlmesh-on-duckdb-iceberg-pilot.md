---
status: current
owner: foundation-platform
doc_type: reference
last_reviewed: 2026-11-19
---

# SQLMesh 시범: DuckDB 로 R2 Iceberg 카탈로그에 쓰기 (2026-11-19, W81d)

결론: **지금은 쓰지 않는다.** SQLMesh 는 우리 엔진(DuckDB 2.0)에서 돌지 않고, 허용되는 마지막 DuckDB(1.5.6)에서도 R2 REST 카탈로그에는 모델 하나 짓기 전에 실패한다. 다시 시험하는 조건은 맨 아래에 있다.

Gold 의 바뀐 행 쓰기는 Dagster 파티션과 `code_version`, 그리고 ADR-0564 의 공용 부품(`_versions`, LatestPerKey)으로 한다. 같은 날 잰 DuckDB 2.0 의 DELETE·UPDATE·MERGE 결과도 이 문서에 적는다.

## 상류에서 읽은 것

| # | 사실 | 출처 |
|---|---|---|
| 1 | SQLMesh v0.236.4 는 `duckdb>=0.10.0,!=0.10.3,<2` 로 묶여 있다. main 도 같다. | `pyproject.toml` l.12, https://github.com/SQLMesh/sqlmesh/blob/v0.236.4/pyproject.toml |
| 2 | 묶은 까닭: SQLGlot 30.8 이 DuckDB 2.0 문법(한 화살표 람다, 0 으로 나눈 `//`·`%` 의 오류)을 옮기지 못하고, DuckDB 1.x 는 2.0 이 만든 파일을 열지 못한다. | PR https://github.com/SQLMesh/sqlmesh/pull/6158 (2026-10-21 병합) |
| 3 | SQLMesh 의 DuckDB 어댑터는 객체 목록을 읽기 전에 `USE "<catalog>"` 를 한다(`set_current_catalog`). 카탈로그 붙이기 설정은 `TYPE` 과 경로만 넘긴다(R2 의 `ENDPOINT`·secret 을 넘길 칸이 없다). | `sqlmesh/core/engine_adapter/duckdb.py` l.27·45-47, `sqlmesh/core/config/connection.py` `DuckDBAttachOptions` (v0.236.4) |
| 4 | `virtual_environment_mode: dev_only` 이면 prod 에서 뷰 없이 버전 없는 표 이름에 바로 쓴다. 환경 뷰는 다른 카탈로그로 보낼 수 있다(`environment_catalog_mapping`). | https://github.com/SQLMesh/sqlmesh/blob/v0.236.4/docs/guides/configuration.md (Virtual Data Environment modes, Environment view catalogs) |
| 5 | DuckDB iceberg 확장 `4c0c0874c4` 에서 표와 뷰의 `CREATE OR REPLACE` 는 예외를 던지고, `DROP … CASCADE` 도 예외를 던진다. `CREATE VIEW` 는 카탈로그가 `POST /v1/{prefix}/namespaces/{namespace}/views` 를 알릴 때만 된다. | `src/catalog/rest/catalog_entry/schema/iceberg_schema_entry.cpp` at https://github.com/duckdb/duckdb-iceberg/tree/4c0c0874c48ec82a3e29910554891d04bcc0e988 |
| 6 | 같은 확장의 시험이 분할된 표(`truncate`, `bucket`, 시간 변환)의 DELETE 와 MERGE INTO 를 다룬다(로컬 시험 카탈로그). | `test/sql/local/catalog_test_config_setup/catalog_agnostic/delete/partitions/`, `…/merge/` (같은 커밋) |
| 7 | Cloudflare 의 DuckDB 안내 쪽은 "DuckDB 는 분할된 표의 DELETE 를 아직 지원하지 않는다"고 쓴다. DuckDB 1.4 기준 글로 읽힌다. | https://developers.cloudflare.com/basin-catalog/config-examples/duckdb/ (2026-11-19 읽음) |

시범을 정하기 전에 읽은 SQLMesh 의 다른 사실은 아래 공식 문서에 있다.

| 사실 | 출처 |
|---|---|
| DuckDB 에서 `INCREMENTAL_BY_UNIQUE_KEY` 는 MERGE 가 아니라 맞는 행을 지우고 새 행을 넣는다 | https://sqlmesh.readthedocs.io/en/stable/concepts/models/model_kinds/ |
| 운영 상태 저장소로 권하는 것은 Postgres 다 | https://sqlmesh.readthedocs.io/en/stable/guides/configuration/ |
| SQLMesh 문서의 Dagster 연동은 Tobiko Cloud 전용이다 | https://sqlmesh.readthedocs.io/en/stable/cloud/features/scheduler/dagster/ |

## 잰 것 (ai-server, 운영 R2 카탈로그의 스크래치 namespace, 끝나고 지움)

### SQLMesh 0.236.4 + DuckDB 1.5.6 (SQLMesh 가 허용하는 마지막 판) + 그 판의 iceberg 확장

| 시험 | 결과 |
|---|---|
| DuckDB 직접: `CREATE SCHEMA`, `CREATE TABLE`, `INSERT`, `DELETE`, `ALTER TABLE … RENAME`, `information_schema` | 모두 됨 |
| DuckDB 직접: `CREATE VIEW`, `CREATE OR REPLACE VIEW` | `Not implemented Error: Create View` |
| DuckDB 직접: `CREATE OR REPLACE TABLE … AS` | `CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements` |
| `sqlmesh plan` (FULL 모델과 INCREMENTAL_BY_UNIQUE_KEY 모델 하나씩, `virtual_environment_mode` full 과 dev_only 둘 다; 카탈로그는 커서를 만들 때 붙여 둠) | 둘 다 모델 하나 짓기 전에 실패: `Catalog Error: SET schema: No catalog + schema named "r2" found.` (`duckdb.py:47 set_current_catalog` ← `base.py get_data_objects` ← `snapshot/evaluator.py _get_data_objects_in_schema`). `USE "r2"` 는 그 카탈로그의 `main` namespace 를 찾는다. |

운영 카탈로그에는 `main` namespace 를 만들지 않았다. 그래서 그것과 dev_only 를 함께 쓰는 우회로는 시험하지 않았다. 되더라도 우리 엔진이 아닌 DuckDB 1.5 에서 돌고, DuckDB 에서의 쓰기는 MERGE 가 아니라 지우고 넣기다.

### 우리 엔진: DuckDB 2.0.0 + 릴리스의 확장 파일(경로로 로드, 자동 설치 끔)

확장: iceberg `4c0c0874c4`, httpfs `77392a7207`, avro `345c3f4e82` (`duckdb-extensions.contract.json`).

표: `PARTITIONED BY (truncate(2, pnu)) SORTED BY (pnu)`. 합성 PNU 두 분할(`99`, `98`)에 10 행을 넣었다.

| 문장 | 결과 |
|---|---|
| 분할 값 하나 안의 `DELETE` | 됨, 1 행 |
| 두 분할에 걸친 `DELETE` (분할 칸이 아닌 조건) | 됨, 2 행 |
| `UPDATE` | 됨, 2 행 |
| `MERGE INTO` (`WHEN MATCHED AND … THEN DELETE`, `WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`) | 됨, 3 행 |
| 결과 | 7 행, `sum(v)=155`, 손으로 센 값과 같다. 새 연결로 읽어도 같다. |
| 쓴 파일 | 스냅숏 5개; 데이터 파일 6개(14 행), position delete 파일 7개(7 행). 데이터 파일 경로는 `truncate_2__pnu_1=99`, `…=98` 로 분할되어 있다. |

따라서 Cloudflare 쪽 문장(상류 7)은 DuckDB 2.0 과 이 확장에는 맞지 않는다.

재지 않은 것:
- 우리 Rust 스캐너(`iceberg_scan.rs`, position delete 만 적용)가 이 delete 파일을 바르게 읽는지
- R2 관리형 합치기가 delete 파일을 접는지
- 두 쓰기가 동시에 커밋할 때의 충돌 처리

## 다시 시험하는 조건

아래 둘이 **모두** 참일 때만 다시 본다.

1. SQLMesh 가 DuckDB 2.x 를 지원한다: `pyproject.toml` 의 `duckdb<2` 묶음이 풀린 릴리스가 나온다.
2. DuckDB iceberg 확장과 R2 Data Catalog(Basin Catalog)가 REST 카탈로그에서 뷰와 `CREATE OR REPLACE` 를 받는다. 또는 SQLMesh 가 `main` namespace 없이 Iceberg 카탈로그를 고른다.

다시 볼 때는 위 표의 두 시험(직접 DDL, `sqlmesh plan`)부터 같은 순서로 한다. 둘을 넘으면 그때 (b)~(g) 를 잰다: 바뀐 행만 쓰는지, 깨지지 않는 변경은 아래 모델을 다시 짓지 않는지, 운영 표와 같은지, 시간과 메모리, Postgres 상태 저장소, Dagster 에서 부르기.
