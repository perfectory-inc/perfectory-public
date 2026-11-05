---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-11-05
---

# Foundation 레이크하우스 dbt

이 디렉터리는 Foundation Platform의 dbt 프로젝트다.

dbt는 Trino 위의 SQL 모델·SQL 테스트·모델 문서와 모델 단위 계보 증거를 담당한다.

dbt는 원천 수집, checksum 정본, AI 호출, 사람 검토, 인가, 공개 권한 또는 롤백 권한을 담당하지 않는다.

## 로컬 실행 형태

1. 선택형 레이크하우스 조회 프로필로 Trino를 시작한다.
2. `profiles.example.yml`을 dbt 프로필 디렉터리에 복사한다.
3. dbt Core와 dbt Trino를 준비한 뒤 이 디렉터리에서 dbt를 실행한다.

Example:

```powershell
docker compose -f compose.lakehouse.yml --profile lakehouse-query up -d trino
dbt parse --profiles-dir .
dbt test --profiles-dir . --exclude tag:full_quality
```

dbt 설치 방법과 버전은 저장소가 고정하지 않는다(버전 핀 정본 `tools/technology-versions.contract.json`에
dbt 항목이 없다).

## dbt 없이 하는 정적 검증

dbt를 설치하기 전에 다음을 실행한다.

```powershell
python infra/lakehouse/dbt/tests/test_no_dbt_forbidden_responsibilities.py
python infra/lakehouse/dbt/tests/test_model_contracts.py
python infra/lakehouse/dbt/tests/test_trino_catalog_template_contract.py
python infra/lakehouse/dbt/render_current_rows.py --check
```

이 검사는 dbt 패키지 없이 프로젝트 경계, 모델 계약, 로컬 Trino 카탈로그 템플릿을 검증한다.
`macros/foundation_current_rows.sql`과 `tests/foundation_current_keys_are_unique.sql`은 레이크하우스
계약에서 `render_current_rows.py`가 생성하므로 손으로 고치지 않는다.

## dbt를 준비한 뒤 런타임 검증

```powershell
cd infra/lakehouse/dbt
dbt parse --profiles-dir .
dbt test --profiles-dir .
```

dbt 런타임 실행에는 선언된 원천 테이블을 노출하는 실제 Trino 카탈로그가 필요하다. 로컬 Trino
카탈로그 이름은 `foundation_platform`이다. `dbt parse`는 실제 카탈로그 없이 프로젝트만 검사하지만,
`dbt compile`, `dbt run`, `dbt test`는 Trino/Iceberg 메타데이터 저장소가 초기화돼 있어야 한다.
로컬 JDBC 기반 Iceberg는 `infra/lakehouse/trino/templates/foundation-platform-jdbc-iceberg.properties.template`
에서 추적하지 않는 실제 카탈로그 속성 파일을 만들고 `foundation_platform.properties`로 저장한다.
dbt의 `database` 설정이 이 파일명을 카탈로그 이름으로 사용하기 때문이다. PostgreSQL 기반 Iceberg
JDBC 메타데이터 테이블은 `infra/lakehouse/trino/init/foundation-platform-jdbc-iceberg-catalog.sql`로
초기화한다.

격리된 로컬 smoke에서는 `smoke/source-fixtures.sql`을 적용하고 `smoke` target과
`FOUNDATION_DBT_SOURCE_SCHEMA=smoke_source`로 dbt를 실행한다. 이때 모델은 정본 계층 스키마가
아니라 `smoke_staging`, `smoke_intermediate`, `smoke_silver`에 기록된다.

법원 경매 원천 모델은 source snapshot과 lineage run 식별자가 명시되지 않으면 fail closed한다.
fixture smoke에서는 고정 fixture ID를 사용하고, Gongzzang이 공개한 실제 원천에서는 실제 공개
스냅샷과 계보 실행 ID를 지정한다.

```powershell
$env:FOUNDATION_DBT_COURT_AUCTION_SOURCE_SNAPSHOT_ID='smoke-court-auction-property'
$env:FOUNDATION_DBT_COURT_AUCTION_LINEAGE_RUN_ID='smoke-court-auction-lineage'
```

Gongzzang과 Foundation 원천이 서로 다른 스키마에 있으면 `FOUNDATION_DBT_SOURCE_SCHEMA`에
의존하지 말고 각각 지정한다.

```powershell
$env:FOUNDATION_DBT_GONGZZANG_SOURCE_SCHEMA='gongzzang_silver'
$env:FOUNDATION_DBT_FOUNDATION_SOURCE_SCHEMA='silver'
```

## Smoke 테스트와 전체 품질 테스트

Smoke 실행은 모델 SQL, 원천 연결, 후보 생성과 출력 계약이 동작함을 증명해야 한다. Foundation
Silver 건축물대장 전체를 읽어 유일성·완전성을 검사해서는 안 된다.

빠른 smoke에는 다음을 사용한다.

```powershell
dbt run --target smoke --exclude tag:full_quality --profiles-dir .
dbt test --target smoke --exclude tag:full_quality --profiles-dir .
```

전환·야간·릴리스 품질 검사는 다음을 사용한다.

```powershell
dbt run --target smoke --select tag:full_quality --profiles-dir .
dbt test --target smoke --profiles-dir . --select tag:full_quality
```
