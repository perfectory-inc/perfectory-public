# ADR 0268: 스튜어드 결정의 이유 코드와 메모는 선택이다 — 데이터가 정하는 항목은 기본 목록에서 빠진다

- Status: Accepted
- Date: 2026-10-17
- Changes: [ADR-0115](./0115-stewards-decide-lineage-items-through-one-api-and-decisions-fold-into-the-lakehouse.md) §2
  ("계보를 만드는 결정은 이유 코드가 있어야 한다"), [ADR-0263](./0263-legal-dong-pairs-are-decided-on-the-one-steward-path.md)
  ("법정동 결정은 메모가 필수")
- Keeps: [ADR-0144](./0144-region-code-changes-are-derived-from-downloaded-data-only.md) §4(분할·판단 대기는 사람이 승인할 수 없다),
  ADR-0115 §3(근거 지문)·§7(두 사람 승인)

## Context

- 사장이 법정동 짝을 승인하면서 "왜 메모를 쓰고 이유를 골라야 하나"라고 물었다(2026-10-17). 승인 한 번에
  입력 셋(후보·이유·메모)이 필요했고, 메모가 비면 API 가 거부했다.
- 결정 행은 이유 없이도 이미 누가(`decided_by`), 언제(`decided_at`), 무엇을(`outcome`·`predecessor_code`),
  어떤 근거 위에서(`evidence_etag` — 결정 시점의 상태·후보 지문, 바뀌면 결정이 무효) 정했는지 늘 남긴다.
  법정동 항목은 짝 맞추기가 기록한 근거(`evidence_json`: 폐지·생성일, 후보, 지번 비율, 30527 상태)를 함께 가진다.
- 분할(`split`)과 판단 대기(`awaiting_data`) 항목은 사람이 할 일이 없는데(ADR-0144 §4) 기본 목록에 섞여 있었다.

## Decision

1. 필지 계보와 법정동 짝 모두, `link`·`not_a_link` 결정의 이유 코드와 메모는 선택이다. 도메인
   (`stewardship_domain::check_decision`), DB(마이그레이션 `20261017021817_a_steward_decision_may_omit_its_reason.sql`
   이 `reason_code IS NOT NULL` CHECK 하나를 지운다), 짝 맞추기 런(`steward_rows` 가 빈 이유를 받는다) 세 곳이 같은 규칙이다.
2. 이유를 `other` 로 고르면 메모가 필요하다(무엇인지 말하는 자리). 고르지 않으면 아무것도 필요 없다.
3. 더니어 화면: 법정동 항목은 이유 선택을 보이지 않는다(짝 맞추기 근거가 이유다). 필지 항목은 이유가
   "선택 안 함"으로 시작한다. 메모는 둘 다 선택이다.
4. 목록 API 에 `actionable_only` 를 더한다: 데이터가 정하는 상태(`ReviewStatus::data_decides` — `split`,
   `awaiting_data`)를 뺀다. 더니어 기본 목록이 이것을 켜고, 거르개("분할·데이터 대기 항목도 보기")나 상태를
   직접 고르면 보인다. 항목은 지우지 않는다.
5. 바뀌지 않는 것: 후보가 아닌 코드의 승인, 분할·판단 대기 항목의 승인은 계속 거부된다(ADR-0144 §4).

## Consequences

- 승인은 후보 하나를 고르고 누르는 한 번이다.
- 이유 없는 법정동 결정의 변경표 행은 `detail` 이 비어(null) 있다. 근거는 결정 id(`derivation_run_id` 의
  `db:<결정 id>`)로 결정 행과 그 항목의 `evidence_json` 에 닿는다.
- 마이그레이션은 행·칸을 지우지 않고 CHECK 하나만 지운다. 운영 표의 CHECK 정의를 읽어 그 조건에 맞는 것이
  정확히 하나(`lineage_steward_decision_check1`)임을 확인했고, 하나가 아니면 마이그레이션이 멈춘다.
