# ADR 0588: 아무도 돌리지 않는 dbt 프로젝트는 지우고, 변환은 Dagster 파티션과 code_version 위에 둔다 — SQL 프레임워크는 지금 들이지 않는다

- Status: Accepted
- Date: 2026-11-19
- Supersedes: FP-ADR-0024 (`platforms/foundation-platform/docs/adr/0024-foundation-dbt-sql-modeling-layer.md`, "dbt 가 Silver/Gold 모델을 가진다")
- Builds on: [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)(변환은 발행기 안의 DuckDB),
  [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)(현재 행 규칙은 계약 산출물이 한 번 렌더)

## Context

FP-ADR-0024 는 dbt Core + dbt-trino 를 Silver/Gold SQL 모델과 SQL 시험의 층으로 정했다. 지금의 모습(2026-11-19):

- `platforms/foundation-platform/infra/lakehouse/dbt`: 34 개 파일(모델 16, 매크로 5, 시험 5, seed·smoke·프로필 예시,
  `render_current_rows.py`).
- **아무것도 dbt 를 돌리지 않는다.** 정의·작업·워크플로·lefthook 어디에도 `dbt run|build|test|seed` 가 없고, dbt 판 고정도
  없다(저장소 검색). 운영 ai-server(읽기 전용, 2026-11-19)에는 dbt 실행 파일도 이미지도 그것을 부르는 유닛도 없고, 릴리스 트리에
  파일만 실려 간다. 도는 것은 xtask 가 돌리는 그 폴더의 Python 계약 시험뿐이었다(ADR-0011 이 편입).
- 변환은 이미 다른 곳에 있다: ADR-0559 로 발행기 안의 DuckDB 가 변환을 맡고, 현재 행 규칙은 ADR-0564 §4 로 계약 산출물이 한 번
  렌더해 Dagster 검사와 읽는 쪽이 같은 글자를 쓴다. dbt 매크로 렌더는 그 규칙의 세 번째 사본이었다.
- 사장 결정(2026-11-19): SQL 프레임워크는 지금 들이지 않는다. 후보였던 SQLMesh 는 `duckdb<2` 로 묶여 있어(사장이 근거로 든
  sqlmesh#6158) DuckDB 2.x 와 R2 카탈로그 위의 뷰를 받칠 때 다시 본다. 이 ADR 을 쓰며 그 이슈를 직접 읽지는 않았다.

## Decision

1. **지운다.** `infra/lakehouse/dbt` 전체(모델·매크로·시험·seed·smoke·`render_current_rows.py`), xtask 의 그 Python 시험 묶음,
   `.gitignore` 의 dbt 줄, API 교환 방향 계약의 `dbt-modeling-pull` 절과 그 시험 목록의 이름, 공개 고정물 가드의 dbt smoke 전용
   SQL 표시 규칙(그 파일 하나만 대상이었다)과 그 자기 시험.
2. **남기는 것: 단위 이름 정규화의 골든 벡터.** dbt 매크로 `foundation_normalized_unit_name` 을 돌려 채취한 정답
   (`unit_name_normalization_vectors.csv`)은 Rust 정규화(`foundation-normalization-domain`)의 시험 고정물로 크레이트 안
   `tests/fixtures/` 로 옮긴다. 매크로가 없어도 그 동작을 그대로 묶어 둔다(ADR-0107 §2).
3. **변환이 사는 곳.** 새 Silver/Gold 변환은 Dagster 파티션 자산 + `code_version` + ADR-0564 의 계약 산출물(현재 행 규칙)
   위에 두고, 계산은 ADR-0559 대로 DuckDB 다. 같은 일을 하는 SQL 프레임워크 층을 따로 두지 않는다.
4. **SQL 프레임워크가 돌아오는 조건.** 후보가 DuckDB 2.x 와 R2 Iceberg 카탈로그 위의 뷰를 받칠 때, 그 판을 고정하고 실제로
   돌리는 작업과 함께 들인다. 아무것도 돌리지 않는 모델 폴더는 다시 두지 않는다.

## Consequences

- 지운 줄: dbt 폴더 약 3,450 줄. 현재 행 규칙의 사본 하나(dbt 매크로 렌더)가 없어진다.
- FP-ADR-0024 는 이 ADR 이 대신한다. 그것을 따르던 영역 ADR 들(ADR-0106·0107 의 dbt 매크로 언급 등)은 기록으로 남고, 지금의
  정본은 Rust 정규화와 그 골든 벡터다.
- 잃는 것: dbt 의 모델 문서·계보 그림. 계보는 Dagster 자산 그래프와 DataHub 가 맡는다.
- 기각: 폴더를 두고 "나중에 돌린다"로 표시하기. 돌지 않는 모델은 계약 시험·문서·가드 비용만 내고, 규칙의 사본으로 어긋난다.
