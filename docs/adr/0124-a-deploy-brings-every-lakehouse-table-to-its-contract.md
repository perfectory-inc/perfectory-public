# ADR 0124: 배포는 모든 레이크하우스 표를 계약에 맞춘다

- Status: Accepted
- Date: 2026-10-02
- Builds on: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md)(실행),
  [ADR-0123](./0123-data-contracts-are-generated-odcs-from-the-two-sources-that-already-own-the-facts.md)(계약·품질 검사)

## Context

표에 칸을 붙이는 코드(`evolve_iceberg_table_to_contract`)가 그 표를 다시 쓰는 작업 안에만 있으면,
작업이 돌지 않는 동안 새 계약이 기존 표에 도달하지 않는다. 배포 성공이 계약 적용을 보장하지 못하는 것이
근본 원인이다. 스키마 변경과 백필은 별도 커밋이므로, 칸을 붙인 뒤 값 채우기에 실패한 상태도 재시도해야 한다.

불변식은 기존 표의 계약 칼럼과 타입·순서가 배포의 계약과 일치하고, 등록된 필수 백필에 빈 값이 남지 않는 것이다.
계약 밖의 칸은 명시된 임시 호환 예외 외에는 거부한다. 예외가 있어도 나머지 검사를 생략하지 않는다.

## Decision

1. **배포의 `migrate` 가 레이크하우스도 맞춘다.** DB 마이그레이션 뒤에 systemd 서비스
   `foundation-lakehouse-migrate` 가 Spark 작업 `lakehouse_schema_migrate.py` 를 돌린다. 계약의 표 가운데
   레이크하우스에 있는 표마다 계약에 선언됐지만 표에 없는 칸이 붙고(계약 순서), 실패하면 배포가 멈춘다. 아직 없는 표는 그 표의
   적재가 만든다.
2. **값이 필요한 새 칸은 백필이 등록돼 있어야 한다.** 데이터가 있는 표에 필수(`required`) 칸이 새로 붙으면, 그
   칸을 채우는 함수가 `lakehouse_schema_migrate.BACKFILLS` 에 있어야 하고 같은 실행에서 채운다. 없으면 칸을 붙이기
   전에 멈춘다. 백필 함수는 그 표를 만드는 작업의 함수를 그대로 쓴다 — 다시 만든 값과 백필한 값이 같아야 한다.
3. **백필은 안전하게.** `refs`의 `main` 스냅숏을 번호로 고정한다. 시간상 최신 스냅숏은 롤백된 데이터일 수 있다.
   등록된 필수 칸이 이미 있어도 NULL 또는 빈 문자열이면 백필을 재계획한다. 행 수와 빈 값 0을 확인하고 덮어쓴 뒤
   다시 읽어 확인한다. Iceberg DataFrame overwrite의 `isolation-level=serializable`과
   `validate-from-snapshot-id`로 고정 시점 이후의 동시 쓰기를 검사하여, 충돌 시 커밋을 거부한다. 이전 스냅숏은 남는다.
4. **타입 불일치와 미등록 칸은 배포를 멈춘다.** 순서만 다르면 Iceberg 메타데이터로 계약 순서에 맞춘다.
   위치로 쓰는 기존 `INSERT`가 값을 엉뚱한 칸에 넣지 않도록 공용 스키마 진화 함수의 최종 순서 검사도 유지한다.
5. 매일 품질 검사(ADR-0117 §6)가 이 단계를 거친 뒤에도 남는 어긋남을 알린다.
6. **parent-link 계약 반영 전의 좁은 호환 예외.** `infra/lakehouse/contracts/lakehouse-known-drift.json`의
   `extra_columns`가 허용할 칸 이름·타입·nullable을 정의하는 유일한 목록이다. 각 항목은 `since`·`reason`·
   `closes_when`을 요구한다. `silver.building_register_units`의 알려진 증거 칼럼만 임시로 허용하고,
   실제로 존재하는 칸만 선택적 문자열로 보존한다. 새 칸을 만들거나 정본 계약·일반 적재 함수의 검사를 완화하지 않는다.
   칸 추가·순서 변경·백필에서도 그 값을 보존하고, 결과에 `temporary_extra_columns`를 명시한다.
   다른 칸·다른 표·잘못된 타입/nullable·미등록 필수 백필은 계속 실패한다. 표 전체를 건너뛰는 예외는 금지한다.
   후속 parent-link 계약에 이 칸들이 들어오거나 표에서 모두 사라지면 예외를 제거해야 한다. 계약 편입과 같은 변경에서
   JSON 항목을 제거하지 않으면 검사와 배포가 실패한다.

## 검토한 대안과 근거

- 표 전체를 건너뛰면 무관한 타입·백필 결함까지 숨기므로 기각한다.
- 정본 계약에 먼저 칸을 추가하면 parent-link의 적재·계보 의미를 승인하지 않은 채 계약만 넓히므로 기각한다.
- 별도 스키마 변경기나 잠금 시스템을 만들지 않고 기존 공용 진화 함수와
  [Iceberg의 스키마 진화](https://iceberg.apache.org/docs/latest/evolution/),
  [Spark overwrite 충돌 검증](https://iceberg.apache.org/docs/latest/spark-configuration/),
  [스냅숏 참조 조회](https://iceberg.apache.org/docs/latest/spark-queries/)를 재사용한다.
- 등록된 필수 백필의 빈 값은 배포에서 검사한다. 그 외 기존 값의 도메인 품질은 일일 품질 검사 책임을 유지한다.

## Consequences

- 칸을 더한 코드는 그 배포에서 운영 표에도 들어간다. 손으로 기억할 일이 없다.
- 배포마다 Spark 가 한 번 뜬다(바꿀 칸이 없으면 계약과 표를 대조만 하고 끝난다).
- 필지 패널 전용 백필 작업은 이 단계의 등록 백필로 바뀐다.
- 정적 계획 검사는 Spark 없이 실행한다. 실제 메타데이터 변경·백필·롤백·동시 쓰기는 자격증명 없는 로컬 Iceberg
  통합 테스트로 검증한다.
