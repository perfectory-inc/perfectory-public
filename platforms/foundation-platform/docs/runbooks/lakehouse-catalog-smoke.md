---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-05
---

# 레이크하우스 카탈로그 점검

## 목적

`foundation-platform` 가 Iceberg REST 카탈로그 또는 Cloudflare R2 Data Catalog 에 읽기 전용으로 붙어
표의 현재 스냅숏을 읽을 수 있는지 확인한다.

이 점검은 표를 만들거나 커밋하지 않는다. `GET current snapshot` 성격의 읽기 전용 확인만
한다.

## 필요한 환경 변수

```text
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER=r2_data_catalog
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=
FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE=foundation-platform
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN=
FOUNDATION_PLATFORM_LAKEHOUSE_SMOKE_TABLE=silver.industrial_complexes
```

`FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER` 는 현재 다음 값을 허용한다.

- `r2_data_catalog`
- `iceberg_rest`

## 실행

실제 카탈로그를 건드리려면 동의 값을 정확히 `1` 로 둔다. 공백이 있는 `" 1 "` 이나
`true` 는 거부한다.

```bash
export FOUNDATION_PLATFORM_LAKEHOUSE_LIVE_SMOKE="1"
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER="r2_data_catalog"
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI="<catalog-uri>"
export FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE="foundation-platform"
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN="<token>"
export FOUNDATION_PLATFORM_LAKEHOUSE_SMOKE_TABLE="silver.industrial_complexes"

cargo test -p lakehouse-infrastructure --test lakehouse_live_smoke -- --ignored
```

동의 값이 없으면 이 `#[ignore]` 시험은 건너뛰지 않고 실패한다. 레인으로 돌릴 때는
`cargo xtask integration foundation lakehouse`, R2 와 함께 돌릴 때는 `cargo xtask live r2-lakehouse`
([R2와 레이크하우스 실제 연결 확인](./r2-lakehouse-live-verification.md))가 동의 값을 넣는다.

성공 조건:

- Iceberg REST 카탈로그 호출 주소에 붙는다
- 점검 표의 현재 스냅숏을 읽는다
- 스냅숏 식별자와 메타데이터 위치를 응답에서 해석한다

## 안전장치

기본 점검 표는 `silver.industrial_complexes` 다.

`FOUNDATION_PLATFORM_LAKEHOUSE_SMOKE_TABLE` 를 바꿀 수 있지만 다음 값은 거부한다.

- 빈 문자열
- 앞뒤 공백이 있는 값
- `/` 로 시작하는 값
- `/`, `\`, `..` 를 포함하는 값
- 이름공간과 표를 구분하는 `.` 이 없는 값

이 점검은 `ensure_table` 의 생성·커밋 경로를 실행하지 않는다. 표가 없으면 실패해야 한다.
표 생성과 스키마 커밋은 따로 있는 승격·DDL 작업 흐름에서 다룬다.
