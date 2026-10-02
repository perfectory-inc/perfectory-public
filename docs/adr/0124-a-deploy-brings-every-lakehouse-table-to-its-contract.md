# ADR 0124: 배포는 모든 레이크하우스 표를 계약에 맞춘다

- Status: Accepted
- Date: 2026-10-02
- Builds on: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md)(실행),
  [ADR-0123](./0123-data-contracts-are-generated-odcs-from-the-two-sources-that-already-own-the-facts.md)(계약·품질 검사)

## Context

2026-10-01 첫 품질 검사가 `gold.parcel_panel` 에 계약의 `row_digest`(9/10)·`attached_via_json`(9/30)이 없는 것을
찾았다. 운영 표는 9/9 이후 다시 만들어지지 않았고, 표에 칸을 붙이는 코드(`evolve_iceberg_table_to_contract`)는
그 표를 다시 쓰는 작업 안에만 있었다. 작업이 돌지 않으면 칸도 붙지 않았다. 그 사이 매일 바뀐 필지만 굽기와
계보 속성 표시가 운영에서 동작할 수 없었고 3주 동안 아무도 몰랐다.

Postgres 는 같은 일이 생기지 않는다. 마이그레이션 57개가 배포의 `migrate` 단계에서 적용되고 빠지면 배포가 멈춘다.
대기업의 데이터 표도 같다: 칸 변경은 배포가 적용하고(Iceberg 는 칸 추가가 메타데이터 변경이라 몇 초다), 값이
필요한 새 칸은 등록된 백필이 채우고, 어긋남은 검사가 알린다.

## Decision

1. **배포의 `migrate` 가 레이크하우스도 맞춘다.** DB 마이그레이션 뒤에 systemd 서비스
   `foundation-lakehouse-migrate` 가 Spark 작업 `lakehouse_schema_migrate.py` 를 돌린다. 계약의 표 가운데
   레이크하우스에 있는 표마다 계약에 없는 칸이 붙고(계약 순서), 실패하면 배포가 멈춘다. 아직 없는 표는 그 표의
   적재가 만든다.
2. **값이 필요한 새 칸은 백필이 등록돼 있어야 한다.** 데이터가 있는 표에 필수(`required`) 칸이 새로 붙으면, 그
   칸을 채우는 함수가 `lakehouse_schema_migrate.BACKFILLS` 에 있어야 하고 같은 실행에서 채운다. 없으면 칸을 붙이기
   전에 멈춘다. 백필 함수는 그 표를 만드는 작업의 함수를 그대로 쓴다 — 다시 만든 값과 백필한 값이 같아야 한다.
3. **백필은 안전하게.** 시작 시점 스냅숏을 번호로 고정해 읽고, 같은 행 수와 빈 값 0 을 확인하고 덮어쓴 뒤 다시
   읽어 확인한다. 이전 스냅숏은 남는다.
4. **계약에 없는 칸이 운영 표에 있거나 칸 순서가 다르면** 배포를 멈춘다(위치로 쓰는 `INSERT` 가 값을 엉뚱한 칸에
   넣는 것을 막는 기존 규칙).
5. **알려진 예외는 이유를 적은 목록에만 있다.** `infra/lakehouse/contracts/lakehouse-known-drift.json` 에 표마다
   등록일·이유·풀리는 조건을 적으면, 그 표는 건드리지 않고 "알려진 예외"로 보고한다. 목록에 있는 표가 계약과
   맞게 되면 목록에서 지울 때까지 배포가 멈춘다 — 예외가 원인보다 오래 남지 않는다. 첫 항목은
   `silver.building_register_units`(2026-09-30 미병합 코드의 운영 쓰기, 칸 3개와 연결 277,356건 변경)다.
6. 매일 품질 검사(ADR-0117 §6)가 이 단계를 거친 뒤에도 남는 어긋남을 알린다.

## Consequences

- 칸을 더한 코드는 그 배포에서 운영 표에도 들어간다. 손으로 기억할 일이 없다.
- 배포마다 Spark 가 한 번 뜬다(바꿀 칸이 없으면 계약과 표를 대조만 하고 끝난다).
- 2026-10-01 의 필지 패널 전용 백필 작업은 이 단계의 등록 백필로 바뀐다.
