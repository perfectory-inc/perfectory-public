# ADR 0259: CI 는 운영 표의 모양에 대고 마이그레이션·읽기를 돌린다

- Status: Accepted
- Date: 2026-10-16
- Relates: ADR-0124(배포의 레이크하우스 마이그레이션), ADR-0194 §6(승인 관문), ADR-0203(CI 시간 예산)

## Context

코드와 **운영 표의 실제 모양**이 어긋난 것을 운영만 알아챈 일이 사흘에 세 번 있었다. 셋 다 CI 는 초록이었다.
CI 의 표는 매번 빈 스키마에서 새로 만든 것이라, 운영 표가 가진 것 — 스키마 진화 이력, 열이 더해지기 전에 쓰인
옛 데이터 파일, 이미 있는 행 — 이 없었다.

| 날짜 | PR | 운영 표가 가진 것 | 결과 |
|---|---|---|---|
| 10-13 | #469 | `gold.unit_price_panel` 에 **행이 있음** | 필수 열 `building_id` 에 백필 없음 → 배포 `lakehouse-migrate` 거부 → 모든 배포 실패·DAG 정지, #498 로 되돌림 |
| 10-14 | #465 | 운영 DB 에 승인 설정 **없음** | 승인 관문(`foundation.adr_0194_drop_approved`)이 마이그레이션 거부 → API 30분 다운·DAG 정지 |
| 10-15 | #522 | `reference.legal_dong_code_snapshot` 의 **옛 파일이 6열** (스키마 0, 지금 12열) | Rust 스냅숏 스캐너가 옛 파일을 거부 → 토지 Silver 6레인 매일 실패, #595 가 고침 |

이미 있던 것: 배포 마이그레이션의 판단 함수 `lakehouse_schema_migrate.plan_table`(순수 함수), 몇몇 시험에 손으로
베낀 "운영 열 목록"(`LIVE_SHAPE` 등 — 베낀 순간부터 낡는 거울), Spark 런타임 CI 레인, Postgres 일회용 DB 하네스.
없던 것: **운영 모양 그 자체**를 저장소가 아는 방법.

운영 모양은 카탈로그가 이미 안다. Iceberg 표 메타데이터는 `schemas` 에 지금까지의 모든 스키마를 필드 id 로 남기고,
현재 스냅숏 요약에 행 수가 있다. REST 카탈로그 `loadTable` 한 번이면 읽힌다. 값(행)은 하나도 오지 않는다.

## Decision

### 1. 운영 모양 원장 하나 — `lakehouse-production-shape.json`

`platforms/foundation-platform/infra/lakehouse/contracts/lakehouse-production-shape.json` 이 운영 레이크하우스의
모양이다. 계약이 쓰는 네임스페이스(gold·silver·reference)의 **모든 표**(계약 표만이 아님)에 대해:

- `has_rows` — 현재 스냅숏(`current-snapshot-id`, 목록의 마지막 아님)에 행이 있는가
- `fields` — 표가 가졌던 모든 필드 `[id, 이름, 논리 타입, nullable]`
- `columns` — 현재 스키마의 열(필드 id, 그 순서)
- `earlier` — 지금 살아 있는 데이터 파일이 가진, 현재와 다른 열 집합. Iceberg 매니페스트는 파일마다 쓰인 열의
  크기(`column_sizes`, 리프 필드 id)를 남기고, Trino 의 `"<표>$files"` 가 그것을 읽는다. 스키마 목록이 아니라
  파일이다: 백필이 모든 파일을 다시 쓴 표(`gold.parcel_panel`)에는 옛 스키마가 남아도 옛 모양 파일이 없고,
  `reference.legal_dong_code_snapshot` 에는 6열 파일 2개가 산다(2026-10-16 측정).

`scripts/ops/lakehouse-production-shape.py` 가 읽기 전용 호출(REST 카탈로그 + 서버 Trino)로 만든다(사람이 손으로
쓰지 않는다). 열 이름·타입·행 유무만 담으므로 공개 저장소에 둔다. 이름이 바뀌거나 타입이 바뀐 필드, 스키마에 없는
파일 열은 원장에 지어 넣지 않고 캡처가 멈춘다.

### 2. 레이크하우스 마이그레이션 관문 (#469 부류)

`lakehouse_schema_migrate.production_shape_refusals(contracts, shapes)` 는 배포가 돌리는 **바로 그 `plan_table`** 을
원장의 각 표(열·타입·nullable·행 유무)에 대고 돌려, 배포를 멈출 거부를 모두 돌려준다. 원장에 없는 표는 운영에 없는
표이고 배포도 그 표의 적재에 맡긴다(같은 규칙). `test_lakehouse_production_shape.py` 가 모든 PR 에서
"거부 0건" 을 요구한다. 판단 규칙의 복사본은 없다 — 배포와 CI 가 같은 함수를 부른다.

### 3. 읽기 관문 (#522 부류)

운영 표를 읽는 코드는 원장의 `columns` 와 `earlier` 의 **모든 모양**으로 합성 데이터 파일을 만들어 지금 계약으로 읽혀야
한다. Rust 스냅숏 스캐너(`lakehouse_snapshot_scan`)의 시험이 원장을 읽어 그 파일들을 만든다. 합성 값은 타입마다
고정된 가짜 값이고 실제 PNU·주소는 쓰지 않는다.

### 4. Postgres 승인 관문은 선언한다 (#465 부류)

마이그레이션 안의 최상위 `DO` 블록이 `current_setting('<이름>', true)` 를 읽으면 그것은 **운영 DB 에 미리 설정돼야
하는 승인 관문**이다. 그런 마이그레이션은 계약 파일에 그 설정과, 운영에 설정됐다는 선언(날짜·확인 방법)을 함께
적어야 하고, 없으면 CI 가 실패한다. 병합이 승인이 아니다 — PR 이 "운영에 설정됨" 을 말해야 병합할 수 있다.

### 5. 원장의 신선도

원장은 운영보다 늦을 수만 있다(운영은 계약 쪽으로만 움직인다). 늦음은 두 방향으로 드러난다.

- 배포의 `lakehouse-migrate` 가 찾은 표와 원장이 다르면 `lakehouse-migrate shape-ledger stale: [...]` 를 찍고 요약
  JSON 의 `shape_ledger_stale` 에 남긴다. 거부가 아니다(배포는 계속).
- 다시 찍는 명령은 하나다: `python3 scripts/ops/lakehouse-production-shape.py` (카탈로그 설정은
  `lakehouse-migrate` 가 쓰는 것과 같다). 그 diff 가 운영이 바뀐 내용이다.

### 6. 시간 예산

2·3·4 는 순수 계산이다(원장 45표 전부에 1초 미만). 영향받은 표만 고르는 선택기를 따로 두지 않는다 — 고르는 비용이 도는
비용보다 크다. 무거운 Spark 레인은 이미 `foundation_ci_scope.py` 가 경로로 고른다. ADR-0203 의 10분 안이다.

### 7. 재현 시험(replay)

배포를 멈췄던 실제 변경을 시험 안의 재현으로 남긴다(`test_lakehouse_production_shape.Replays`, 자체 시험의 #465 마이그레이션). 각 항목은 그 PR 이 낸 계약과
그때 운영이 가졌던 모양(카탈로그에서 읽은 것)이고, 관문은 그것을 배포가 거부한 그대로 거부해야 한다. 검사가 실패할 수
있음을 증명하는 자리다.

## 기각한 대안

- **빈 스키마에 마이그레이션 적용(지금 방식)**: 세 사고 모두 통과시켰다.
- **운영 표 복제본으로 CI**: 실제 데이터가 공개 CI 로 나간다. 비용도 크다.
- **git 이력의 계약 버전들로 옛 모양 추정**: 계약이 생기기 전에 만든 표, 배포 안 된 계약 버전을 구분 못 한다. 카탈로그가
  사실을 이미 갖고 있다.
- **시험마다 운영 열 목록을 손으로 적기**(`LIVE_SHAPE`): 같은 사실의 거울이 늘어난다. 원장으로 대체했다.
- **CI 가 카탈로그에 직접 묻기**: 공개 저장소 CI 에 카탈로그 토큰이 필요하다.

## Consequences

- 계약 변경 PR 이 배포에서 거부될 변경이면 PR 에서 실패한다. 첫 측정에서 열린 PR #570·#501(`gold.parcel_land_rights`
  에서 운영 표가 가진 `land_right_total` 제거)과 #489(두 표)가 걸린다.
- 원장은 운영이 바뀔 때 다시 찍어야 한다. 배포 로그의 `shape-ledger stale` 이 그 때를 알린다.
- 남은 일: Postgres 마이그레이션 체인을 운영 행이 있는 상태로 한 단계씩 적용하는 시험(운영 표별 행 유무 원장 +
  합성 행). 이 ADR 의 4 는 승인 관문만 다룬다.
