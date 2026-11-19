# ADR 0588: 아무도 돌리지 않는 dbt 프로젝트는 지우고, 변환은 선언된 작업과 Dagster 자산 위에 둔다 — SQL 프레임워크는 지금 들이지 않는다

- Status: Accepted
- Date: 2026-11-19
- Supersedes: [FP-ADR-0024](../../platforms/foundation-platform/docs/adr/0024-foundation-dbt-sql-modeling-layer.md)(dbt Core + dbt-trino 를 Silver/Gold SQL 모델·시험 층으로)
- Supersedes in part: [ADR-0106](./0106-land-right-names-corroborate-unit-normalization.md) 의 dbt 조항(교차확증 후보를 dbt entity_resolution 모델로, 정규형 SSOT 를 dbt 매크로로),
  [ADR-0107](./0107-the-normalized-unit-designation-is-derived-not-approved.md) §2 의 dbt 조항(벡터를 dbt seed 로 두고 dbt 시험도 채점)
- Builds on: [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)(변환 엔진),
  [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md) §4(현재 행 규칙은 계약 산출물이 한 번 렌더)

## Context

FP-ADR-0024 는 dbt Core + dbt-trino 를 Silver/Gold SQL 모델과 SQL 시험의 층으로 정했다. 지우기 전의 모습(2026-11-19, main):

- `platforms/foundation-platform/infra/lakehouse/dbt`: 34 개 파일, 3,456 줄 — 모델 16(`models/**/*.sql`), 매크로 5, 시험 5(SQL 2·Python 3),
  seed 1(단위 이름 골든 벡터), smoke 1, `render_current_rows.py`, 프로젝트·프로필 예시·README.
- **아무것도 dbt 를 돌리지 않는다.** `git grep -iE "dbt (run|build|test|seed)|dbt-core|dbt-trino|dbt_project"` 를 `definitions/`·
  `orchestration/`·`.github/`·`lefthook.yml`·`tools/`·`scripts/` 에 걸어 0 건이고, dbt 버전 고정도 없다. 운영 서버(ai-server, 읽기 전용
  `command -v dbt`·`docker images`·유닛 파일 검색)에는 dbt 실행 파일도 이미지도 그것을 부르는 유닛도 없고, 배포된 릴리스 디렉터리에
  파일만 실려 간다. 도는 것은 xtask 가 돌리던 그 폴더의 Python 계약 시험뿐이었다(ADR-0011 이 편입) — dbt 모델은 한 번도 돌지 않았다.
- 현재 행 규칙은 ADR-0564 §4 로 계약 산출물이 한 번 렌더하고, Dagster 검사(`publish_checks.py`)와 dbt 매크로 렌더
  (`render_current_rows.py`)는 그 글자를 받아 쓰는 쪽이었다. 키 유일성도 Dagster `publish_checks.key_unique_sql` 이 본다.
- 사장 결정(2026-11-19): SQL 프레임워크는 지금 들이지 않는다. 사장이 든 근거는 후보 SQLMesh 가 `duckdb<2` 에 묶여 있다는 것
  (SQLMesh 이슈 #6158)이고, DuckDB 2.x 와 R2 Iceberg 카탈로그 위의 뷰를 받칠 때 다시 본다.

## Decision

1. **지운다.** `infra/lakehouse/dbt` 전체를 지우되 `seeds/unit_name_normalization_vectors.csv` 는
   `crates/normalization/foundation-normalization-domain/tests/fixtures/` 로 옮긴다. 같이 지우는 것:
   - `tools/xtask/src/main.rs` foundation `python_tests` 의 dbt 시험 묶음(과 `tests.rs` 의 계획 시험 순번);
   - `platforms/foundation-platform/docs/architecture/api-exchange-direction-contract.md` 의 `dbt-modeling-pull` 절·표 줄·금지 줄과
     그 시험(`test_api_exchange_direction_contract.py`)의 이름;
   - `scripts/guard/public-fixture-safety.py` 의 SQL 표시 규칙(dbt smoke 파일 하나만 대상이었다)과 그 자기 시험 사례;
   - `platforms/foundation-platform/.gitignore` 의 dbt 줄.
2. **골든 벡터는 남는다.** dbt 매크로 `foundation_normalized_unit_name` 을 돌려 채취한 정답을 이제 Rust 정규화 한쪽만 채점한다
   (ADR-0107 §2 의 두 구현 교차 채점 중 Rust 쪽). 정규형의 정본은 `foundation-normalization-domain` 이다.
3. **변환이 사는 곳.** 새 변환은 `definitions/<group>.definition.json` 에 선언하고 생성된 `orchestration/jobs.v1.json` 의 작업이
   돌린다. Dagster 자산은 선언에서 만들어지고, 그 `code_version` 은 작업의 코드 판이다(`observe_assets.py`·`job_code_version.py`, ADR-0499). 엔진은 ADR-0559·ADR-0564 그대로이고
   (아직 옮기지 않은 Spark 변환 포함), 그 위에 SQL 프레임워크 층을 따로 두지 않는다. 파티션은 Dagster 이전 작업(PR #1476, 진행 중)에서 정한다.
4. **데이터 시험은 Dagster asset check 다**(`publish_checks.py`: 현재 행·키 유일성 등). 지운 dbt 시험 5 개는 돌지 않았으므로 잃은
   보증은 없다.
5. **SQL 프레임워크가 돌아오는 조건.** 들이는 PR 이 함께 보인다:
   - 버전 고정: `tools/technology-versions.contract.json`(Python 도구) 에 그 판;
   - 돌리는 작업: `jobs.v1.json` 의 예약 작업이 그 프레임워크의 run/build 를 부르고, 그 출력 자산이 pipeline-graph 에 있다(CI·xtask
     시험만으로는 "돈다"가 아니다);
   - DuckDB 2.x 로 R2 Iceberg 카탈로그의 Silver 표 하나에 뷰를 만드는 스모크.
   이 조건은 가드가 아니라 리뷰가 확인한다.

## Consequences

- dbt 폴더 3,456 줄이 없어지고, 현재 행 규칙을 받아 쓰는 쪽 하나(dbt 매크로 렌더)가 없어진다.
- FP-ADR-0024 는 이 ADR 이 대신한다. ADR-0106·0107 의 dbt 조항은 여기서 대신하고 나머지 결정은 그대로다.
- ADR-0106 의 교차확증 후보 모델(`silver_entity_link_assertion_candidate` 와 `int_entity_resolution__*`)도 지워진다. 돈 적이 없어 잃은
  산출물은 없다. 다시 필요하면 결정 3 자리에 새로 짓는다.
- ADR-0286·ADR-0376 이 dbt 키 유일성 시험에 기댄 문장은 기록으로 남는다. 그 확인은 Dagster `publish_checks.key_unique_sql` 이 맡는다.
- 계보: 잃는 그림은 없다(돌지 않아 그림도 없었다). Dagster/DuckDB run 의 계보는 ADR-0565 §7(미구현)이 맡고, 그때까지 DataHub 에는
  선언 계보만 있다.
- ADR-0559·ADR-0564 의 dbt 독자 언급은 기록으로 남는다.
- 정비 목록의 dbt 항목(B-F18 등)은 이 PR 로 끝난다.
- 기각: 폴더를 두고 "나중에 돌린다"로 표시하기. 돌지 않는 모델은 계약 시험·문서·가드 비용만 내고, 규칙을 받아 쓰는 쪽이 하나 늘 뿐이다.
