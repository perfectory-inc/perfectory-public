# ADR 0115: 스튜어드는 API 하나로 필지 계보를 결정하고, 결정은 레이크하우스 계보 행으로 접힌다

- Status: Accepted
- Date: 2026-09-30
- Builds on: [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) §4·§10(등급, 사람은
  needs_review·pending 만), [ADR-0103](./0103-place-identity-outlives-administrative-code-changes.md) §4(결정은
  데이터), [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md)(작은 저장소 →
  레이크하우스 접기), [ADR-0114](./0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md)(화면은 더니어,
  규칙은 플랫폼)

## Context

- `gold.lineage_review_queue`(ADR-0113 §10)가 사람이 볼 필지를 모은다. 인천 개편 기준 약 150건
  (needs_review 65, pending 89). 결정을 받는 길이 없다. 더니어(ADR-0114)는 아직 없다.
- 실제 사례(조사 2026-09-30, 출처는 PR 본문):
  - Reltio — "매칭 아님"은 쌍에 대한 저장된 사실이고 해제할 수 있다. 다음 자동 매칭이 먼저 읽는다.
  - Google AIP-154·OpenMetadata — 본 판이 바뀌었으면 결정을 거절한다(ETag).
  - Stripe — 같은 멱등 키의 재요청은 처음 결과를 돌려준다.
  - Semarchy·Informatica — 맡기(claim)·반납, 시간 제한.
  - Tamr — "모르겠음"은 정식 답이고 조정자가 가른다.
  - SAP MDG·DataHub — 큰 변경은 요청자 ≠ 승인자를 서버가 강제한다.
  - Esri Parcel Fabric — 기록(record)이 변경의 단위이고, 지운 것 없이 대체한다.
  - LINZ — 제출 전 같은 규칙으로 미리 검사한다.
  - OpenStreetMap — 이유는 정해진 코드와 자유 메모.
  - Amazon A2I — 자동 판정의 표본을 사람에게 보내 불일치율을 잰다.
- 우리 정규화 제안(`/catalog/v1/normalization/...`)은 이미 제출 → 승인·거절 → 적용 → 되돌리기 흐름과
  Identity 권한 검사를 가진다.

## Decision

1. **입구는 Foundation API 하나다.** 더니어·명령줄 도구·다른 어떤 화면도 레이크하우스나 DB 를 직접 쓰지
   않는다. 권한은 Identity Platform 이 판정한다: `foundation.lineage` 의 `review`(맡기·결정),
   `adjudicate`(승인·넘긴 건 결정). 직원(Staff) 토큰만 받는다.

2. **결정은 쌍에 대한 사실이다.** 결과는 넷이다:
   - `link` — 새 PNU 는 후보 옛 PNU 와 같은 땅이다(후보를 지정).
   - `not_a_link` — 목록의 어느 후보도 같은 땅이 아니다("원래 번호 없음", 예: 새로 등록된 임야). 한 후보를 버리고
     다른 후보를 고르는 것은 그 후보에 대한 `link` 다.
   - `unsure` — 이 사람은 가를 수 없다. 다른 스튜어드에게 간다.
   - `escalate` — 조정자(`adjudicate`)에게 넘긴다.
   `link`·`not_a_link` 만 계보 행을 만든다. 이유는 정해진 코드(`building_register`, `ownership_record`,
   `site_survey`, `official_document`, `cadastral_map`, `other`)와 자유 메모다.

3. **결정은 그때 본 근거에 묶인다.** 목록 항목의 `evidence_etag` = 항목의 `status` 와 `candidates_json` 의
   SHA-256 이다. 결정 요청은 이 값을 실어야 하고, 지금 값과 다르면 409 로 거절한다(본 뒤 자동 등급이 바뀌었거나
   다른 사람이 결정함). 결정 행도 이 값을 가진다 — 뒤에 근거가 바뀌면(새 원천이 등급을 바꾸면) 검토 목록이 그
   필지를 다시 올린다. 결정은 영구가 아니라 "그 근거에서" 유효하다.

4. **같은 요청은 한 번만 처리한다.** 모든 쓰기는 `Idempotency-Key` 를 요구한다. 키는 결정 행에 저장되고,
   같은 키·같은 본문은 처음 결과를, 같은 키·다른 본문은 409 를 돌려준다. 키는 레이크하우스로 접힐 때도
   `evidence_ref` 에 남아, 재구성한 DB 에서도 두 번 쌓이지 않는다.

5. **맡기는 시간 제한이 있는 임대다.** 한 사람이 맡으면 30분 동안 다른 사람의 결정은 409 다. 시간이 지나면
   자동 반납된다. 임대는 작업 상태라 DB 에만 있고 레이크하우스에 접히지 않는다.

6. **미리 검사는 결정과 같은 규칙을 돈다.** `dry_run` 은 저장하지 않고 같은 검사만 한다: 후보가 그 항목의
   후보인지, 그 옛 PNU 가 이미 다른 필지와 같은 땅으로 결정됐는지(한 옛 필지 → 두 새 필지 동일 주장 금지),
   순환이 생기는지.

7. **큰 결정은 두 사람이 한다.** 결정이 이미 효력 있는 연결(`evidence_weak` 이상)과 반대되면 — 표본 검사
   항목에서 자동 연결을 `not_a_link` 로 뒤집거나, 다른 후보를 `link` 하는 경우 — 결정은 `awaiting_approval`
   로 남고, 결정자가 아닌 `adjudicate` 권한자가 승인해야 효력이 생긴다. 요청자 = 승인자는 서버가 403 으로
   막는다. 일반 needs_review·pending 결정은 한 사람이다.

8. **결정은 지우지 않고 대체한다.** 결정 행은 append-only 다(DB 트리거가 수정·삭제를 막는다). 되돌리기는
   `supersedes_decision_id` 를 가진 새 결정이다. 접힌 계보 행도 같다 — 대체 결정은 새 steward 행을 쌓고, 효력은
   가장 최근 steward 행이다.

9. **결정은 레이크하우스로 접힌다(ADR-0112 방식).** DB 의 결정 표는 작은 입구일 뿐이다. 접기 작업이 효력 있는
   결정을 `silver.parcel_lineage` 에 `evidence_kind = steward` 행으로 쌓는다: `grade` 는 `link` 면
   `official`, `not_a_link` 면 `pending`(옛 PNU 없이 — 연결로 읽히지 않지만 항목을 닫는다), `evidence_ref` 는 결정 id·결정자·이유 코드·멱등 키·근거 값.
   접은 결정은 DB 에 접힘 기록이 남는다. 정본은 레이크하우스이고, DB 를 잃어도 접힌 결정은 계보에 있다.

10. **표본 검사 길을 둔다.** 검토 목록은 자동 연결(`evidence_strong`·`evidence_weak`, 일부 `code_derived`)의
    일정 비율을 `sample` 상태로 함께 올린다. 스튜어드가 동의·반대한 비율을 등급별로 재서 등급 사다리가 맞는지
    본다. 비율의 정본은 계약 파일이다.

11. **지표와 알림.** 대기 건수·가장 오래된 건의 나이(등급별 p50/p95), 하루 결정 수, 표본 불일치율을 매일
    슬랙에 보낸다. 처리 기한(SLA)은 건수가 늘 때 추가한다.

## Consequences

- 더니어가 생기면 이 API 에 화면만 붙인다. 그 전에는 같은 API 를 부르는 명령줄 도구로 결정한다.
- 후속 PR 순서: ① 계약 타입·DB 표(결정·승인·임대·접힘, append-only 트리거)·도메인 규칙
  ② API(목록·맡기·반납·미리 검사·결정·승인·내보내기) ③ 검토 목록의 Postgres 투영 적재(Gold → DB, 재구성 가능)
  ④ 접기 작업(DB 결정 → `silver.parcel_lineage`)과 검토 목록의 근거 값 비교 ⑤ 표본 검사 ⑥ 명령줄 도구·슬랙 요약.
- 비용: DB 표 넷, API 7개, 접기 작업 하나. 검토 목록이 DB 투영을 하나 더 가진다(레이크하우스에서 언제든 다시
  만든다).
- 한계: `link` 결정의 등급을 `official` 로 두는 것은 "사람이 확인한 사실"을 공식 자료와 같은 자리에 두는
  선택이다. 뒤에 실제 공식 자료(30527 등)가 다른 짝을 가리키면 ADR-0113 §4 의 불일치 경보가 울리고, 그 건은
  다시 검토 목록에 오른다.
