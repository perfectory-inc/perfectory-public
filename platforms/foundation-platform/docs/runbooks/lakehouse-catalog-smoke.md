# Lakehouse Catalog Smoke

## 목적

`foundation-platform` 가 Iceberg REST Catalog 또는 Cloudflare R2 Data Catalog 에 read-only 로 접근해
현재 table snapshot 을 읽을 수 있는지 확인한다.

이 smoke 는 table 을 만들거나 commit 하지 않는다. `GET current snapshot` 성격의 read-only 확인만
수행한다.

## 필요한 환경변수

```text
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER=r2_data_catalog
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=
FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE=foundation-platform
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN=
FOUNDATION_PLATFORM_LAKEHOUSE_SMOKE_TABLE=silver.industrial_complexes
```

`FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER` 는 현재 다음 값을 허용한다.

- `r2_data_catalog`
- `iceberg_rest`

## 실행

live catalog 를 건드리려면 opt-in 값을 정확히 `1` 로 설정한다. 공백이 있는 `" 1 "` 이나
`true` 는 거부한다.

```bash
export FOUNDATION_PLATFORM_LAKEHOUSE_LIVE_SMOKE="1"
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER="r2_data_catalog"
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI="<catalog-uri>"
export FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE="foundation-platform"
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN="<token>"
export FOUNDATION_PLATFORM_LAKEHOUSE_SMOKE_TABLE="silver.industrial_complexes"

cargo test -p lakehouse-infrastructure --test lakehouse_live_smoke -- --ignored
```

성공 조건:

- Iceberg REST catalog endpoint 에 접근 성공
- smoke table 의 current snapshot 을 읽음
- snapshot id 와 metadata location 을 응답으로 파싱함

## 안전장치

기본 smoke table 은 `silver.industrial_complexes` 다.

`FOUNDATION_PLATFORM_LAKEHOUSE_SMOKE_TABLE` 를 바꿀 수 있지만 다음 값은 거부한다.

- 빈 문자열
- 앞뒤 공백이 있는 값
- `/` 로 시작하는 값
- `/`, `\`, `..` 를 포함하는 값
- namespace 와 table 을 구분하는 `.` 이 없는 값

이 smoke 는 `ensure_table` 의 create/commit 경로를 실행하지 않는다. table 이 없으면 실패해야 한다.
table 생성과 schema commit 은 별도 promotion/DDL workflow 에서 다룬다.
