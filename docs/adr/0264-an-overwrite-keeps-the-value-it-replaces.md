# ADR 0264: 덮어쓰는 쓰기는 바꾸기 전 값과 바꾼 주체를 append-only 기록에 남긴다

- Status: Accepted
- Date: 2026-10-16
- Builds on: [ADR-0023](./0023-an-edit-is-a-row-in-the-ledger-not-only-in-the-row.md) (`catalog.catalog_edit`), [ADR-0212](./0212-identity-service-capability-grants-are-an-append-only-ledger.md) (권한 원장), [ADR-0258](./0258-gongzzang-business-verification-lives-only-in-its-queue-with-an-append-only-history.md) (상태 이력)

## Context

사장 규칙은 "모든 데이터는 덮어쓰지 않고 쌓는다 — 지나간 사실을 잃지 않는다"이다. 2026-10-16
origin/main(7a628e36) 감사에서 그 규칙을 어기는 쓰기가 남아 있었다. 가장 직접적인 것은 산업단지
수정이다.

- `PATCH /catalog/v1/complexes/{id}`는 `catalog.industrial_complex`의 `name`·`area_m2`를 UPDATE한다.
  이전 값은 outbox 이벤트에만 남는데, outbox는 비우려고 만든 전달 통로이지 원장이 아니다
  (ADR-0023이 같은 이유로 거부했다).
- 라우트는 검증된 주체(`AuthorizedPrincipal`)를 **받고 버린다** — `Extension(_principal)`. 누가 고쳤는지가
  어디에도 없다.
- 그 기록을 위해 만든 표 `catalog.catalog_edit`는 이미 있다. before/after 스냅샷, 적용 주체, `rollback_of`,
  UPDATE·DELETE를 거부하는 트리거까지 갖췄다. 그러나 `target_kind`가 `parcel` 하나뿐이고, 필지 편집
  경로는 ADR-0194에서 사라져 이 표에 쓰는 곳이 없다. ADR-0023 남은 부채 5가 바로 이 상태를 적었다.

## Decision

### 1. 규칙

운영 데이터의 값을 **덮어쓰는** 쓰기는 같은 트랜잭션 안에서 바꾸기 전 값을 append-only 기록에 남긴다.
사람이 바꾼 것이면 바꾼 주체(검증된 principal)도 함께 남긴다. 기록 표는 UPDATE·DELETE를 트리거로
거부한다.

기록하는 방법은 표의 성격에 따라 이미 있는 선례 셋 중에서 고른다. 새 방식을 만들지 않는다.

| 성격 | 방법 | 선례 |
|---|---|---|
| 직원이 고치는 정본 값 | 쓰는 코드가 같은 트랜잭션에서 편집 원장에 before/after·주체 행을 넣는다 | ADR-0023 `catalog.catalog_edit` |
| 상태 전이 | 트리거가 이력 표에 전이마다 한 행을 넣는다 | ADR-0258 |
| 프로비저너가 맞추는 설정 값 | 값이 바뀔 때만 원장에 한 행을 넣는다 | ADR-0212 |

### 2. 산업단지 수정은 `catalog.catalog_edit`에 쓴다

- `catalog_edit_target_kind_check`에 `industrial_complex`를 더한다. 쓰는 코드와 같은 PR에서 더하므로
  writer 없는 어휘가 아니다(ADR-0023 남은 부채 5의 조건).
- `PgCatalogUnitOfWork::update_complex`는 행을 `FOR UPDATE`로 잠그고 버전을 확인한 뒤, 정규화 참가자와
  **같은** UPDATE 함수(`update_industrial_complex`)와 **같은** 스냅샷 모양(`industrial_complex_snapshot`)을
  쓴다. 같은 UPDATE 문이 두 벌이던 것이 한 벌이 된다.
- `command_type`은 `catalog.industrial_complex.update`, `applied_by_principal_id`는 라우트의 검증된 주체다.
  포트 `CatalogUnitOfWork::update_complex`가 주체를 필수 인자로 받으므로, 주체 없이 편집하는 호출은
  컴파일되지 않는다.
- 정규화 경로(`PgIndustrialComplexTransactionParticipant`)는 여기에 쓰지 않는다. 그 기록은
  `normalization_application`이 맡고, 이 표는 `*.normalization.*` 명령을 CHECK로 거부한다(ADR-0023 §2).

## Alternatives considered

- **산업단지 전용 이력 표를 새로 만든다.** `catalog_edit`가 모양·불변 트리거·되돌리기 칸을 모두 갖췄고
  쓰는 곳이 없다. 같은 기록을 두 표에 나누면 재구성기(ADR-0006)가 읽을 곳만 늘어난다. 기각.
- **`industrial_complex`에 UPDATE 트리거를 달아 이력 표를 채운다.** 트리거는 바꾼 주체를 모른다. 세션
  변수로 넘기면 주체 없는 UPDATE가 조용히 통과한다. 주체가 있는 편집은 포트 인자로 강제하는 편이
  기계적으로 막힌다. 기각.
- **outbox 이벤트를 기록으로 삼는다.** outbox는 비워지는 전달 통로다(ADR-0023). 기각.

## Consequences

- 산업단지 수정 한 건마다 `catalog.catalog_edit`에 한 행이 쌓인다. 직원 수동 편집 빈도라 크기는 무시할 만하다.
- 같은 감사의 나머지 덮어쓰기(Bronze 객체 등록, 서비스 주체, gongzzang 신고·사업자 인증 이력)는 위
  §1 표의 방법으로 각자 PR에서 고친다.
