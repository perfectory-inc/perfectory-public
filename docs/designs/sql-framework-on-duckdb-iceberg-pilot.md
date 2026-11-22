---
status: current
owner: foundation-platform
doc_type: reference
last_reviewed: 2026-11-22
---

# SQL 프레임워크 시범: SQLMesh 와 dbt 를 DuckDB 로 R2 Iceberg 카탈로그에 (2026-11-19 W81d, 2026-11-22 W94)

결론: **"무엇이 바뀌었고 무엇을 다시 계산할지"를 정하는 도구로는 둘 다 지금 들이지 않는다.**

- **SQLMesh** 는 우리 엔진(DuckDB 2.0)에서 돌지 않는다. 허용되는 마지막 DuckDB(1.5.6)에서도 R2 REST 카탈로그에서는 모델 하나 짓기 전에 실패한다(1 절).
- **dbt**(dbt-core 1.12.7 + dbt-duckdb 1.11.1)는 우리 DuckDB 2.0.0 그대로 R2 카탈로그에 쓰고, 운영 표와 한 글자도 다르지 않은 결과를 냈다(2 절). 그러나 바뀐 것만 다시 계산하는 판단은 **전부 우리가 쓴 공용 매크로**가 했다. dbt 가 더한 것은 SQL 모델의 의존 그래프·글틀·시험·코드 변경 고르기(`state:modified`)뿐이다. 이것들은 Dagster(`code_version`, asset check)와 우리 계약 산출물이 이미 하거나 쉽게 더할 수 있다.
- 그래서 공용 변경 층은 발행기 안에 둔다. ADR-0564 의 `gold_versions_*` 에서 떼어 내고, 각 자산은 SQL 파일 하나와 선언한 범위(같은 키·같은 지역·전체)로 쓴다. 다시 시험하는 조건은 맨 아래에 있다.

이 문서는 두 도구의 시범을 한곳에 적는다. 같은 날 잰 DuckDB 2.0 의 DELETE·UPDATE·MERGE 결과도 1 절에 있다.

## 1. SQLMesh (2026-11-19, W81d)

### 상류에서 읽은 것

| # | 사실 | 출처 |
|---|---|---|
| 1 | SQLMesh v0.236.4 는 `duckdb>=0.10.0,!=0.10.3,<2` 로 묶여 있다. main 도 같다. | `pyproject.toml` l.12, https://github.com/SQLMesh/sqlmesh/blob/v0.236.4/pyproject.toml |
| 2 | 묶은 까닭: SQLGlot 30.8 이 DuckDB 2.0 문법(한 화살표 람다, 0 으로 나눈 `//`·`%` 의 오류)을 옮기지 못하고, DuckDB 1.x 는 2.0 이 만든 파일을 열지 못한다. | PR https://github.com/SQLMesh/sqlmesh/pull/6158 (2026-10-21 병합) |
| 3 | SQLMesh 의 DuckDB 어댑터는 객체 목록을 읽기 전에 `USE "<catalog>"` 를 한다(`set_current_catalog`). 카탈로그 붙이기 설정은 `TYPE` 과 경로만 넘긴다(R2 의 `ENDPOINT`·secret 을 넘길 칸이 없다). | `sqlmesh/core/engine_adapter/duckdb.py` l.27·45-47, `sqlmesh/core/config/connection.py` `DuckDBAttachOptions` (v0.236.4) |
| 4 | `virtual_environment_mode: dev_only` 이면 prod 에서 뷰 없이 버전 없는 표 이름에 바로 쓴다. 환경 뷰는 다른 카탈로그로 보낼 수 있다(`environment_catalog_mapping`). | https://github.com/SQLMesh/sqlmesh/blob/v0.236.4/docs/guides/configuration.md (Virtual Data Environment modes, Environment view catalogs) |
| 5 | DuckDB iceberg 확장 `4c0c0874c4` 에서 표와 뷰의 `CREATE OR REPLACE` 는 예외를 던지고, `DROP … CASCADE` 도 예외를 던진다. `CREATE VIEW` 는 카탈로그가 `POST /v1/{prefix}/namespaces/{namespace}/views` 를 알릴 때만 된다. | `src/catalog/rest/catalog_entry/schema/iceberg_schema_entry.cpp` at https://github.com/duckdb/duckdb-iceberg/tree/4c0c0874c48ec82a3e29910554891d04bcc0e988 |
| 6 | 같은 확장의 시험이 분할된 표(`truncate`, `bucket`, 시간 변환)의 DELETE 와 MERGE INTO 를 다룬다(로컬 시험 카탈로그). | `test/sql/local/catalog_test_config_setup/catalog_agnostic/delete/partitions/`, `…/merge/` (같은 커밋) |
| 7 | Cloudflare 의 DuckDB 안내 쪽은 "DuckDB 는 분할된 표의 DELETE 를 아직 지원하지 않는다"고 쓴다. DuckDB 1.4 기준 글로 읽힌다. | https://developers.cloudflare.com/basin-catalog/config-examples/duckdb/ (2026-11-19 읽음) |
| 8 | `SCD_TYPE_2` 는 실행마다 표 전체를 새로 쓴다(`_scd_type_2` 가 `replace_query` 로 끝난다). 부분 재적재는 안 되고, 다시 지으면 "자료를 잃을 수 있어" 기본으로 꺼져 있다. 덧붙이기만 하는 종류는 `INCREMENTAL_UNMANAGED` 하나이고, 바뀐 행을 가리지 않는다. | https://github.com/SQLMesh/sqlmesh/blob/main/sqlmesh/core/engine_adapter/base.py, https://github.com/SQLMesh/sqlmesh/blob/main/docs/concepts/models/model_kinds.md (2026-11-22 읽음) |

시범을 정하기 전에 읽은 SQLMesh 의 다른 사실은 아래 공식 문서에 있다.

| 사실 | 출처 |
|---|---|
| DuckDB 에서 `INCREMENTAL_BY_UNIQUE_KEY` 는 MERGE 가 아니라 맞는 행을 지우고 새 행을 넣는다 | https://sqlmesh.readthedocs.io/en/stable/concepts/models/model_kinds/ |
| 운영 상태 저장소로 권하는 것은 Postgres 다 | https://sqlmesh.readthedocs.io/en/stable/guides/configuration/ |
| SQLMesh 문서의 Dagster 연동은 Tobiko Cloud 전용이다. 공동체 패키지 `dagster-sqlmesh` 는 0.22.0(2025-11-11) 뒤로 릴리스가 없고 Python 3.13 미만만 받는다 | https://sqlmesh.readthedocs.io/en/stable/cloud/features/scheduler/dagster/, PyPI `dagster-sqlmesh` |

### 잰 것 (ai-server, 운영 R2 카탈로그의 스크래치 namespace, 끝나고 지움)

#### SQLMesh 0.236.4 + DuckDB 1.5.6 (SQLMesh 가 허용하는 마지막 판) + 그 판의 iceberg 확장

| 시험 | 결과 |
|---|---|
| DuckDB 직접: `CREATE SCHEMA`, `CREATE TABLE`, `INSERT`, `DELETE`, `ALTER TABLE … RENAME`, `information_schema` | 모두 됨 |
| DuckDB 직접: `CREATE VIEW`, `CREATE OR REPLACE VIEW` | `Not implemented Error: Create View` |
| DuckDB 직접: `CREATE OR REPLACE TABLE … AS` | `CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements` |
| `sqlmesh plan` (FULL 모델과 INCREMENTAL_BY_UNIQUE_KEY 모델 하나씩, `virtual_environment_mode` full 과 dev_only 둘 다; 카탈로그는 커서를 만들 때 붙여 둠) | 둘 다 모델 하나 짓기 전에 실패: `Catalog Error: SET schema: No catalog + schema named "r2" found.` (`duckdb.py:47 set_current_catalog` ← `base.py get_data_objects` ← `snapshot/evaluator.py _get_data_objects_in_schema`). `USE "r2"` 는 그 카탈로그의 `main` namespace 를 찾는다. |

운영 카탈로그에는 `main` namespace 를 만들지 않았다. 그래서 그것과 dev_only 를 함께 쓰는 우회로는 시험하지 않았다. 되더라도 우리 엔진이 아닌 DuckDB 1.5 에서 돌고, DuckDB 에서의 쓰기는 MERGE 가 아니라 지우고 넣기다.

2026-11-22 확인: 이 시범의 빈 namespace `sqlmesh__w81d_sqlmesh_probe` 하나가 카탈로그에 남아 있다(표 0 개).

#### 우리 엔진: DuckDB 2.0.0 + 릴리스의 확장 파일(경로로 로드, 자동 설치 끔)

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

## 2. dbt (2026-11-22, W94)

### 판과 사용 허가 (상류에서 읽은 것)

| 구성 | 판 | 사용 허가 | 덧붙임 |
|---|---|---|---|
| dbt-core (Python, 이제 "dbt OSS") | 1.12.7 (2026-11-12) | Apache-2.0 | v1 은 dbt-labs/dbt-core 의 `1.latest` 가지로 옮겨 갔다 |
| dbt v2 (Rust 로 다시 쓴 것, 옛 이름 Fusion, 2026-09-16 정식판) | 2.0.11 · PyPI `dbt` 2.0.12 | 소스는 Apache-2.0, **배포 실행 파일은 dbt 제품 사용 허가**(역설계 금지, 원격 측정 전송을 막지 말 것) | DuckDB 드라이버가 duckdb 1.5.2–1.5.4 까지만 있다(https://github.com/dbt-labs/dbt-core/blob/main/crates/dbt-adbc/drivers.toml). 우리 DuckDB 2.0 은 못 돈다 |
| dbt-duckdb | 1.11.1 (2026-10-26) | Apache-2.0 | `duckdb>=1.0.0`(위 제한 없음). DuckDB 2.0 은 야간 CI 로만 시험한다(dbt-duckdb PR #858) |
| dagster-dbt | 0.29.31 (dagster 1.13.31) | Apache-2.0 | Python 3.11–3.13. dbt v2 와 dbt-core 를 한 환경에 함께 깔면 안 된다(Dagster 문서) |

### 상류에서 읽은 것

| # | 사실 | 출처 |
|---|---|---|
| 1 | 증분 모델의 첫 빌드는 목표 표에 바로 CTAS 하고, 그다음부터는 로컬 TEMP 표를 지은 뒤 목표 표에 `append`(INSERT)·`merge`(MERGE INTO, DuckDB 1.4 이상)·`delete+insert` 를 한다. | https://github.com/duckdb/dbt-duckdb/blob/main/dbt/include/duckdb/macros/materializations/incremental.sql |
| 2 | `table` 물질화와 `--full-refresh` 는 CTAS 뒤 이름 바꾸기를 하는데, Iceberg REST 에서는 깨진다. 고치는 PR 둘이 열려 있다. | dbt-duckdb PR #747, #725 |
| 3 | 스냅숏(SCD2)은 옛 행의 `dbt_valid_to` 를 UPDATE 하고 새 판을 INSERT 한다. 덧붙이기만 하는 방식이 아니다. 지워진 행 처리는 `hard_deletes: ignore`(기본) · `invalidate` · `new_record` 이다. | https://github.com/duckdb/dbt-duckdb/blob/main/dbt/include/duckdb/macros/snapshot_helper.sql, https://docs.getdbt.com/reference/resource-configs/hard-deletes |
| 4 | dbt 가 아는 "바뀐 것"은 코드뿐이다: `state:modified`(본문·설정·매크로·계약)를 남겨 둔 `manifest.json` 과 견준다. 깨지는 변경과 안 깨지는 변경을 가르지는 않는다(계약 칸만 예외). 자료의 바뀐 행은 모른다. `microbatch` 는 시간 구간으로만 나눈다. | https://docs.getdbt.com/reference/node-selection/methods (state), dbt-duckdb README (microbatch) |
| 5 | dagster-dbt 의 자산마다 `code_version` 은 그 모델 SQL 의 해시다. dbt 시험은 asset check 가 된다. 파티션은 `--vars` 로 넘기고 거르는 SQL 은 우리가 쓴다. | https://docs.dagster.io/integrations/libraries/dbt/reference ("dbt models, code versions", "Building incremental models using partitions") |
| 6 | DuckDB 2.0 iceberg 확장은 R2 Data Catalog 에 붙어 INSERT·UPDATE·DELETE·MERGE INTO·CTAS·ALTER 를 한다. UPDATE·DELETE·MERGE 는 merge-on-read(position delete)만 된다. 가지·꼬리표 쓰기는 없다. | https://duckdb.org/docs/2.0/core_extensions/iceberg/writing_to_iceberg, duckdb-iceberg 이슈 #1252 |

### 잰 것 (노트북, 운영 R2 카탈로그의 스크래치 namespace `w94_dbt_probe`, 끝나고 지움, 남은 표 0)

대상: 실거래 지역 통계 Gold(`gold.real_transaction_region_stats`). W81c 가 판을 쌓는 표로 옮기는 두 Gold 가운데 하나다. SQL 은 W81c 의 `region_stats.sql` 을 글자 그대로 썼고, 자리표시 셋과 `deals` 이름만 dbt 글틀로 바꿨다. 법정동은 발행기의 `reference.sql`(쌍 → 현재 코드, 갈라진 코드)과 `dong_index`·`address_codes`(Rust)를 SQL 로 옮겨 정했다.

입력: 서빙 중인 표의 커밋이 적은 핀에서 읽었다(읽기 키). 2개 시도의 시군구 3개에 놓일 수 있는 거래의 모든 판 570,872 행, 법정동 코드 변경 3,519 행, 가장 새 법정동 목록 53,403 행. 이 사본을 스크래치 namespace 에 실어 바꿔 가며 시험했다.

틀: 로컬 DuckDB 파일에 준비 모델 넷, 지역 통계 SQL(ephemeral) 하나, Iceberg 의 `append` 증분 모델 하나, 시험 하나(현재 행의 키가 하나씩인지).

**공용 차분 매크로 하나** (`w94_versioned_append`, Jinja 144 줄):
- 목표 표의 가장 새 행에서 처리한 코드 판·입력 시각·코드 표 핀을 읽는다. 그 기록은 Iceberg 행 자체에 있고 dbt 는 따로 들고 있지 않다.
- 범위를 정한다: 첫 빌드·코드 변경·강제면 `all`, 아니면 바뀐 시군구(`region`) 또는 영향받는 키(`key`).
- 그 범위만 자산의 SQL 로 다시 계산하고, `row_digest` 로 현재 행(`version_seq` 로 rank)과 견준다.
- 바뀐 키는 새 `version_seq` 로, 범위에서 사라진 키는 `row_state = 'removed'` 로 덧붙인다(ADR-0564 모양).

자산이 따로 쓴 것은 키·내용 칸·지역 식, 그리고 범위 SQL 27 줄(바뀐 코드가 닿는 시군구, 바뀐 거래가 닿는 시군구, 영향받는 키)이다. **어느 도구도 이 범위를 SQL 에서 스스로 끌어내지 않는다.**

| 실행 | 바꾼 것 | 정해진 범위 | 덧붙은 행 | Iceberg 커밋 | 걸린 시간 | 최대 메모리 |
|---|---|---|---|---|---|---|
| 1 | 첫 빌드 | all | 85,029 | 있음, 6.58 MB | 56.7 s | 559 MiB |
| 판정 | — | — | **서빙 표와 같다**: 85,029 / 85,029, 한쪽에만 있는 행 0, 다른 행 0(업무 칸 전부와 `row_digest`), 순서 무관 검사합 같음 | — | — | — |
| 2 | 공용 매크로 편집(결과 같음) | all (코드 판) | 0 | **없음** | 48.9 s | 536 MiB |
| 3 | 없음 | all (2 에서 커밋이 없어 기록이 못 넘어감) | 0 | 없음 | 51.2 s | 533 MiB |
| 6 | 주석만 편집 + 법정동 하나 새 코드로 | all (코드 판) | 삭제 표시 154 + 새 행 154 | 있음, 30 KB | 52.0 s | 527 MiB |
| 9 | 법정동 하나 새 코드로 | **region: 그 시군구 하나** | 삭제 표시 229 + 새 행 229 | 있음, 42 KB | 65.4 s | 440 MiB |
| 10 | 위쪽 SQL 의 깨지는 편집 | all (코드 판) | 바뀐 840 | 있음, 78 KB | 52.7 s | 527 MiB |
| 11 | 거래 3건 | **key** | 5 | 있음, 4.6 KB | 64.2 s | 541 MiB |
| 12 | 없음, 전량 강제 | all | **0: 쌓아 온 결과가 전량 재계산과 같다** | 없음 | 55.5 s | 496 MiB |
| 13 | 없음, Dagster(dagster-dbt)로 | — | 0 | 없음 | 66.8 s | 재지 않음 |
| 기준선 | 같은 SQL 을 도구 없이 DuckDB 2.0 으로 | 전량 계산 | — | — | **7.7 s** | **403 MiB** |

빠진 실행 번호(4·5·7·8)는 같은 거래 변경을 다른 상태에서 되풀이한 것이다. 5 는 실패했다: 모델 파일 안에 정의한 매크로는 프로젝트 매크로에서 이름으로 부를 수 없다. 그래서 범위 SQL 을 `macros/` 로 옮겼다.

쓴 바이트: 커밋 7 번에 6.74 MB, 그 가운데 증분 커밋 6 번은 162 KB 였다. 같은 표의 운영 커밋(Spark, 11-18)은 표 전체를 다시 썼다: 3,204,170 행 지우고 3,776,648 행, 165 MB.

dbt 가 기준선보다 실행마다 약 45 초·130 MiB 를 더 쓴다. 그 가운데 약 25 초는 첫 모델 전의 연결 단계다. dbt-duckdb 가 붙인 운영 카탈로그 전체의 `information_schema` 로 관계 목록을 읽는 것과 맞는다(dbt-duckdb 이슈 #857). 원인까지 확인하지는 않았다.

### 알게 된 것

1. **dbt 와 DuckDB 2.0.0 은 우리 판 고정을 통과한다.** dbt-duckdb 1.11.1 은 DuckDB 판에 위 제한이 없고, 모든 실행이 2.0.0 으로 돌았다.
2. **바뀐 것을 가리는 일은 도구가 하지 않는다.** 자료가 바뀐 범위, 차분, 삭제 표시, 판 번호는 모두 우리 매크로가 했다. dbt 가 준 것은 모델 의존 그래프, 글틀, 시험, `state:modified` 다. 주석만 고쳐도 `state:modified` 가 그 모델을 고른다. 위쪽 SQL 을 고치면 `state:modified+` 가 그 모델과 Gold 모델을 고른다.
3. **아무 행도 덧붙지 않는 실행은 "어디까지 처리했나"를 적지 못한다.** DuckDB 의 SQL 커밋은 빈 커밋을 만들지 않고, 스냅숏 요약 속성도 쓰지 못한다. 그래서 결과가 같은 코드 변경 뒤에는 어떤 행이 바뀔 때까지 실행마다 `all` 로 다시 계산했다(실행 3·4·7·8). ADR-0564 는 iceberg-rust 의 빈 추가와 요약 속성(`foundation.version-seq` 등)으로 이것을 한다. DuckDB SQL 로는 같은 일을 못 한다.
4. **dagster-dbt 의 `code_version` 은 모델 자기 SQL 만 본다.** 위쪽 모델이나 매크로를 고쳐도 아래 자산의 판은 그대로다. 모델 안의 Jinja `graph` 에는 매크로 소스가 없다(잰 값 0). 그래서 모델 안에서 만든 해시도 위쪽 SQL 까지만 덮는다. 매크로 편집은 `state:modified.macros` 만 본다.
5. **Dagster 파티션은 필요 없었다.** 매크로가 한 실행 안에서 지역을 고른다. 시군구나 시도마다 파티션을 두면 `dbt build` 한 번이 커밋 한 번이 되고, 한 표에 커밋이 겹치며, 연결 단계(약 25 초)를 파티션마다 낸다.
6. **R2 는 표의 마지막 갱신 시각보다 이른 스냅숏 시각을 거부한다**(`InvalidTableUpdate: Invalid snapshot timestamp … before last updated timestamp`). 노트북 시계가 Cloudflare 보다 138 초 늦어서 CTAS 가 모두 실패했다. 그래서 표를 빈 채로 만들고, 한 표의 커밋 사이를 150 초 이상 띄웠다. 쓰는 호스트는 모두 시계를 맞춰야 한다(ai-server 는 0 초 차이).

### 스냅숏·SCD2 에서 남는 이력과 잃는 것 (사실만)

| 방식 | 지금 표에 남는 것 | 옛 스냅숏에만 남는 것(만료 전까지; 우리는 30 일 + 월말 꼬리표) | 만료 뒤 잃는 것 |
|---|---|---|---|
| dbt 스냅숏 (UPDATE + INSERT, position delete) | 판마다 한 행: 업무 칸, `dbt_valid_from`·`dbt_valid_to`·`dbt_updated_at` | 닫히기 전의 행 모습(`dbt_valid_to` 가 비어 있던 행) | 처음 발행된 행의 바이트(그 행은 다시 쓰였다), 닫음이 언제 쓰였는지(`dbt_updated_at` 말고는) |
| SQLMesh SCD_TYPE_2 (실행마다 표 전체 교체) | 판마다 한 행: `valid_from`·`valid_to` | 직전 실행까지의 표 전체 사본 | 옛 사본 전부. 다시 지으면 이력을 잃을 수 있어 기본으로 꺼져 있다 |
| ADR-0564 (`append` + 공용 매크로, 이번에 잰 것) | 모든 판과 모든 삭제 표시. 어떤 행도 다시 쓰지 않는다 | 없음 | 없음. 스냅숏 보존 기간에 기대지 않는다 |

### 들인다면 지워지는 것과 남는 것

- 지워지는 변경 코드는 없다. `gold_versions_change_set.rs`(541 줄)와 `gold_versions_commit.rs`(832 줄)의 차분 절반은 매크로로 다시 쓰일 뿐이다. 커밋 절반(`append_at_head`, `HeadMoved` 되찾기, 요약 속성, 꼬리표)은 dbt 에 맞는 것이 없다. `gold_incremental.py` 는 무엇을 고르든 Spark 와 함께 나간다. 지역 나누기도 남는다. dbt 는 시도마다 한 번 읽고 끝에 한 번 커밋하는 고리를 하지 않는다.
- 어느 쪽이든 남는 것: 지도 타일(tippecanoe), by-PNU 굽기, polylabel, H3(h3o), Rust 파일 파서, 코드로 도는 계보 걷기.

### 재지 않은 것

- 전국 규모에서 dbt 의 시간과 메모리, 그리고 연결 단계 25 초가 카탈로그 크기와 함께 느는지
- R2 에서 `merge`·`delete+insert` 전략과 `dbt snapshot` 실행(소스만 읽었다)
- dbt v2(Rust): DuckDB 2.x 드라이버가 없다
- DuckDB 의 `DROP TABLE` 이 스크래치 데이터 파일을 R2 에서 지웠는지
- 두 dbt 실행이 한 표에 동시에 커밋할 때

## 다시 시험하는 조건

**SQLMesh**: 아래 둘이 **모두** 참일 때만 다시 본다.

1. SQLMesh 가 DuckDB 2.x 를 지원한다: `pyproject.toml` 의 `duckdb<2` 묶음이 풀린 릴리스가 나온다.
2. DuckDB iceberg 확장과 R2 Data Catalog(Basin Catalog)가 REST 카탈로그에서 뷰와 `CREATE OR REPLACE` 를 받는다. 또는 SQLMesh 가 `main` namespace 없이 Iceberg 카탈로그를 고른다.

다시 볼 때는 1 절 표의 두 시험(직접 DDL, `sqlmesh plan`)부터 같은 순서로 한다. 둘을 넘으면 그때 (b)~(g) 를 잰다: 바뀐 행만 쓰는지, 깨지지 않는 변경은 아래 모델을 다시 짓지 않는지, 운영 표와 같은지, 시간과 메모리, Postgres 상태 저장소, Dagster 에서 부르기.

**dbt**: 아래 가운데 하나가 참일 때 다시 본다.

1. dbt v2(Rust)의 DuckDB 드라이버가 DuckDB 2.x 를 받는다. 그때는 2 절의 열세 번 실행을 같은 순서로 되풀이한다.
2. SQL 로 쓴 자산이 많아져, 모델 의존 그래프와 문서 화면이 Dagster 와 계약 산출물보다 필요해진다. 그때도 변경 판단은 발행기의 공용 층에 둔다. dbt 는 SQL 을 짓는 틀로만 들인다(ADR-0588 의 되돌아오는 조건을 함께 따른다).
