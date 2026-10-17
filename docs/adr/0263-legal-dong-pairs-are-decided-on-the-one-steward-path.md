# ADR 0263: 법정동 코드 짝은 필지 계보와 같은 스튜어드 입구에서 결정한다 — 결정은 파일이 아니라 DB 행이다

- Status: Accepted
- Date: 2026-10-16
- Builds on: [ADR-0115](./0115-stewards-decide-lineage-items-through-one-api-and-decisions-fold-into-the-lakehouse.md)(스튜어드 API 하나),
  [ADR-0114](./0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md)(화면은 더니어),
  [ADR-0116](./0116-dawneer-keeps-staff-tokens-on-its-rust-server.md)(BFF),
  [ADR-0144](./0144-region-code-changes-are-derived-from-downloaded-data-only.md) §3·§4(남는 것만 사람, 분할·판단 대기는 데이터 몫)
- Changes: ADR-0144 §3.4 의 "사람 확인"이 받던 길(`legal-dong-code-collect.sh steward` → `steward/pending/` 파일)

## Context

- 사장 요청(2026-10-16): 인천 법정동 짝 두 건을 명령줄이 아니라 화면에서 승인하고, 같은 일은 하나로 합친다.
- 지금 사람의 결정을 받는 길이 둘이다.
  - **필지 계보**(ADR-0115): Foundation API `/catalog/v1/lineage-review/*`, Identity 권한(`foundation.lineage`), 맡기·근거 값(ETag)·멱등 키·append-only 결정 표,
    더니어 "필지 계보 검토" 화면, 결정은 레이크하우스로 접힌다.
  - **법정동 짝**(ADR-0144 §3.4): 짝 맞추기가 서버에 `steward-review.json` 을 쓰고, 스튜어드가 셸에서
    `legal-dong-code-collect.sh steward --approve OLD=NEW` 를 돌리면 `steward/pending/` 에 파일이 생긴다. 다음 짝 맞추기가 다시 검사해
    `reference.legal_dong_code_change` 에 `steward:<id>` 행을 쌓는다. 신원 확인·권한·화면·감사 기록이 없고, 누가 결정했는지는 스스로 적은 문자열이다.
- 두 길은 같은 일이다: 자동 판단이 남긴 항목 하나에, 후보 중 하나를 고르거나("짝") 모두 아니라고("아님") 사람이 이유와 함께 정하고,
  그 결정이 레이크하우스 행이 된다. 다른 것은 대상(필지 ↔ 폐지 동)과 접히는 표뿐이다.
- 결정 표들은 처음부터 `unit` 칸(`CHECK (unit IN ('parcel'))`)을 두어 두 번째 종류를 예정했다.

## Decision

1. **법정동 짝은 같은 스튜어드 API 의 두 번째 항목 종류(`unit = legal_dong`)다.** 경로·권한(`foundation.lineage` 의 `review`·`adjudicate`)·
   맡기·ETag·멱등 키·미리 검사·감사·OpenAPI 는 하나다. 새 엔드포인트 묶음을 만들지 않는다. 목록은 `kind` 로 거른다.
   - 항목의 대상(`subject_code`)은 폐지된 10자리 코드, 후보는 짝 맞추기가 제안한 새 코드(이름·생성일 포함)다.
   - 항목은 짝 맞추기가 기록한 근거를 함께 든다(`evidence_json`): 이름, 단위, 폐지일, 이름 규칙 결과, 지번 비교, 분할된 곳,
     기다리는 데이터, 필지고유번호변동연혁(30527)을 읽었는지, 그 공식 이력이 짝을 정하지 않고 옮긴 필지 수.
   - 결정의 `predecessor_code` 칸은 "링크가 고른 후보"다: 필지는 옛 PNU, 법정동은 새 코드(칸 주석에 적는다).
     API·화면의 이름은 `subject_code`·`candidate_code` 로 바꿨다(소비자는 더니어 하나이고 같은 PR 에서 함께 바뀐다).
2. **결정은 append-only DB 행이다.** 기존 `catalog.lineage_steward_decision`(수정·삭제를 트리거가 막는다)에 쌓인다: 누가(Identity 주체),
   언제, 이유 코드와 메모, 고른 새 코드. 승인은 `link`, 거절은 `not_a_link` 다. 법정동 결정은 메모(왜)가 필수다 — 변경표의 `detail` 이 된다.
3. **검사는 두 번, 같은 규칙으로 한다.**
   - 결정 때(API): 목록에 없는 코드는 항목이 없어 404, `split`·`awaiting_data` 항목은 승인·거절이 409(ADR-0144 §4: 데이터 몫),
     제안된 후보가 있으면 그중 하나만, 후보가 없으면 자기 자신이 아닌 10자리 코드만 받는다.
     여러 옛 동이 한 새 동으로 가는 것은 통합이라 막지 않는다(필지의 "옛 번호 하나 = 새 번호 하나" 규칙은 필지에만).
   - 반영 때(짝 맞추기): DB 가 내보낸 서 있는 승인을 그날의 목록과 전체 코드 표로 다시 검사한다(`steward_rows`). 통과한 것만 쌓고,
     쌓은 결정만 `catalog.lineage_steward_fold` 에 접힘으로 기록한다. 거부된 결정은 서 있는 채 남고 요약에 이유가 찍힌다.
4. **목록은 짝 맞추기가 DB 에 게시한다.** 짝 맞추기가 기존 넘김 형식(`lineage_review_handoff.v1`, unit `legal_dong`)으로 쓰고
   기존 `load-lineage-review-items` 가 그 unit 의 항목을 통째로 바꿔 넣는다. 항목 표는 투영이라 짝 맞추기를 다시 돌리면 언제든 다시 만든다.
   결정은 항목 id(`UUIDv5(lineage-review:legal_dong:<code>)`)에 묶여 목록이 바뀌어도 이력으로 남는다.
   짝 맞추기는 DB 가 목록을 받은 적이 없거나(`steward-list-published.json` 없음) 기록 안 된 승인이 있으면 그날 다시 돈다.
5. **정본은 레이크하우스다(ADR-0115 §9 와 같다).** 승인된 짝은 `reference.legal_dong_code_change` 의 `steward:<결정자>` 행이 된다.
   거절·모르겠음·넘김은 짝을 만들지 않으므로 DB 결정 행으로만 남는다.
6. **파일 길은 은퇴한다.** 화면이 운영에서 쓰이면 `legal-dong-code-collect.sh steward`, `stage-steward-decision`, `steward/pending` 읽기를
   다음 PR 에서 지운다. 그때 남은 파일은 런북의 일회성 절차로 한 번만 처리한다(AGENTS.md "한 번만 하는 일").

## 통합 검토 — 같은 모양인 것만

| 사람의 승인 | 모양 | 권고 |
|---|---|---|
| 필지 계보·법정동 짝 | 항목 하나에서 후보 고르기·아님, 이유, 레이크하우스로 접힘 | 이 ADR 로 하나 |
| 지도 폴리곤 편집(ADR-0112) | 도형을 쓰는 편집, 저장 시 검증, D1 저장소, 굽기로 접힘 | 합치지 않는다. 후보를 고르는 결정이 아니라 쓰기다. 더니어 화면의 "할 일" 목록에 링크로만 함께 둔다 |
| 정규화 제안 승인(`/catalog/v1/normalization`) | 제안 → 승인·거절 → 적용 → 되돌리기, 제안자 ≠ 승인자 | 가장 가깝다. 다음 단계에서 같은 받은편지함 화면에 올리는 것을 권한다. 저장 표는 적용·되돌리기가 있어 따로 둔다 |
| 공짱 사업자 확인(ADR-0258) | 공짱이 주인인 고객 대기열 | 주인이 달라 합치지 않는다 |

## 기각한 대안

- **법정동 전용 엔드포인트·표·화면**: 같은 일을 두 번 만든다. 권한·맡기·ETag·감사를 다시 구현하고 두 길이 어긋난다.
- **파일 길을 화면 뒤에 숨기기**(API 가 `steward/pending` 파일을 쓴다): 결정이 신원·트랜잭션·append-only 보장 없이 파일로 남고, API 가 서버 파일 시스템에 묶인다.
- **`predecessor_code` 칸 이름 바꾸기**: 운영 표의 칸 이름 변경은 계보 접기 경로 전체를 함께 바꿔야 한다. 칸 주석과 API 이름으로 뜻을 적는다.
- **짝 맞추기가 DB 에 직접 쓰기**: Spark 컨테이너에 DB 접속을 늘린다. 게시·접힘 기록은 이미 있는 publisher 명령(호스트)이 한다.

## Consequences

- 마이그레이션 `20261016120000_steward_review_items_have_a_kind.sql`: 두 표의 CHECK 를 unit 별로 바꾸고 `evidence_json` 칸을 더한다.
  행·칸을 지우지 않으므로 승인 관문(ADR-0259 §4)이 필요 없다. 기존 필지 행은 새 CHECK 를 그대로 통과한다.
- 새 publisher 명령 하나(`export-legal-dong-steward-decisions`). 기존 `load-lineage-review-items`·`record-lineage-steward-folds` 를 그대로 쓴다.
- 더니어 "검토" 화면이 두 종류를 한 목록에 보이고(종류 거르개), 법정동 항목 화면은 근거·후보·결정 이력을 보인다.
- DB 를 재구성하면 법정동 목록은 다음 짝 맞추기 때 다시 게시된다(그 사이엔 비어 있다). `steward-list-published.json` 을 지우면 바로 다시 게시한다.

- 2026-10-17 개정 주석: 결정의 이유 코드와 메모는 선택이 되었다 — [ADR-0268](./0268-a-steward-decisions-reason-and-note-are-optional.md).
