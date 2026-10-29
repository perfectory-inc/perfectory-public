# ADR 0329: 공식 근거는 기록된 추정 짝을 자동으로 대체한다

- Status: Accepted
- Date: 2026-10-29
- Amends: [ADR-0150](./0150-the-official-parcel-number-history-decides-before-the-jibun-sets.md) §2,
  [ADR-0156](./0156-only-decisive-official-evidence-of-the-same-change-contradicts-a-derived-pair.md) §4
- Related: [ADR-0103](./0103-place-identity-outlives-administrative-code-changes.md) (`superseded_by`),
  [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) (필지 계보),
  [ADR-0144](./0144-region-code-changes-are-derived-from-downloaded-data-only.md),
  [ADR-0145](./0145-one-source-of-truth-for-region-code-changes.md) (정본 하나),
  [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md),
  [ADR-0273](./0273-panel-gold-maps-code-zone-and-lineage-changes-to-pnus.md) (코드 변경 → PNU),
  [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md) (부분 처리)

## Context

법정동 코드 짝의 근거는 강한 것부터 넷이다.

1. 필지 번호 공식 이력(`silver.parcel_number_change_history`, VWorld 30527). 동 단위 행이 먼저다.
2. code.go.kr 날짜·이름 규칙(`derived:code-go-kr:date+name:*`).
3. 지번 겹침(`derived:parcel-jibun:*`).
4. 스튜어드(`steward:*`).

공식 이력은 몇 달 늦게 반영된다(ADR-0150). 그 사이 2·3 이 짝을 먼저 기록한다. 나중에 온 공식 근거가 같은 변경에서
그 짝과 다른 코드를 가리키면, 지금까지는 짝 맞추기가 `PairingConflict` 로 멈추고 아무것도 쓰지 않았다(ADR-0150 §2,
ADR-0156 §4). 사람이 원인을 찾아 스튜어드 짝을 기록해야 다시 돈다.

- 멈추는 동안 그 동만이 아니라 전국의 짝 맞추기·투영·스튜어드 목록이 함께 선다.
- 답은 이미 데이터에 있다. 더 강한 근거가 같은 변경을 말하고 있다.
- 사장 결정(2026-10-28): 낮은 확신의 추정은 권위 있는 근거가 오면 자동으로 대체하고, 감사 기록을 다 남기며, 영향받은
  데이터만 다시 계산한다.

대기업·오픈소스의 같은 문제:

- MDM 의 생존 규칙(Informatica MDM trust score, Reltio survivorship): 출처 등급이 높은 값이 낮은 값을 자동으로 이긴다.
  사람이 고정한 값(steward override)은 규칙이 덮지 않는다.
- 이력 보존(Kimball SCD2, dbt snapshot, Datomic retraction): 틀린 사실을 지우지 않는다. 철회 사실을 새 행으로 쌓고,
  읽는 쪽이 현재 유효한 것을 고른다. 언제부터 유효한지(valid time)와 언제 기록했는지(transaction time)를 따로 둔다.
- 증분 재계산(Iceberg/Delta 변경 키 전파): 바뀐 키에 닿는 결과만 다시 만든다.

## Decision

1. **등급과 대체는 정본 하나에 쌓는다.**
   - 짝의 근거 등급은 새 열이 아니라 이미 있는 `source` 문자열이다. `legal_dong_code_change_views.EVIDENCE_RANKS` 가
     한 곳에서 순서를 정한다: `steward:` > `official:` > 날짜·이름 > 지번 > `derived:children`.
   - 같은 변경의 결정적 공식 근거(ADR-0156 §1·§2 그대로: 변경 기간 안의 동 단위 행, 또는 계약 비율 이상의 필지 행)가
     기록된 파생 짝(`derived:parcel-jibun:*`, `derived:code-go-kr:date+name:*`)과 다른 코드를 가리키면, 짝 맞추기는
     멈추지 않고 한 번의 적재로 두 행을 쌓는다.
     - 공식 짝(`kind = pair`, `source = official:parcel-history`). 공식 행이 동을 나누거나(분할) 다시 폐지된 코드로
       보내면(사슬) 짝은 없고, 그 코드는 처음부터 짝이 없던 것처럼 판정된다(`split` 등).
     - 대체 기록(`kind = supersession`). `old_code`/`new_code` 는 물린 짝, `source` 는 물린 근거,
       `effective_date` 는 바로잡는 변경의 날(옛 코드의 폐지일, valid time), `recorded_at_utc` 는 바로잡은 때
       (transaction time). `detail`(JSON)은 감사 기록이다: `supersedes`(물린 행의 `change_key`), `superseded_by`(지금
       서 있는 짝의 키), `reason`, `evidence`(공식 행이 가리킨 코드, `dong_level`/`parcel_level`, 날짜).
   - 지우지 않는다. 물린 행은 원장에 그대로 남는다. ADR-0103 의 `superseded_by` 처럼 새 행이 옛 행을 가리킨다.
   - 읽는 쪽은 모두 `views.pair_rows` 를 거친다. 물린 짝은 거기서 빠지고, 남은 짝 중 가장 강한 근거가 선다. 정본 계약의
     품질 관문은 `kind is pair or supersession` 이다.
   - 같은 실행 안의 추정(날짜·이름, 지번)은 기록되기 전이라 물릴 것이 없다. 공식 답이 서고, 진 추정은 요약의
     `official_overruled_inferences` 에 남는다. 같은 변경의 결정적 공식 근거가 한 현존 코드를 가리키면 다른 날의 경계
     조정 행이 섞여 있어도 그것이 짝을 정한다(`official_answer`).
2. **스튜어드 짝은 자동으로 덮지 않는다.**
   - 공식 근거가 스튜어드 짝과 다르면 짝은 그대로 서고, 스튜어드 목록에 `kind`·`status` = `official_contradicts_steward`
     항목으로 두 답을 함께 올린다. `pair` 항목이 아니므로 승인 하나로 지워지지 않는다.
   - 요약의 `steward_contradicted_by_official` 가 세고, 법정동 단위(`legal-dong-code-load.sh`)가 실행마다 슬랙 #alerts 에
     한 번 알린다. 대체는 기록이 일하는 것이므로 알림이 아니라 저널 한 줄이다.
3. **영향받은 것만 다시 계산한다.** 대체 한 건이 닿는 곳은 옛 코드, 물린 코드, 지금 서 있는 코드 아래뿐이다.
   - 시군구 대응표: 실행마다 정본의 view 로 다시 만든다(작은 투영 하나, 지금 그대로). 물린 짝은 view 에 없다.
   - 행정경계 id: `views.corrected_successors` 가 {물린 코드: 지금 코드}를 준다. 다음 경계 적재에서
     `resolve_unit_ids` 가 물린 코드가 상속했던 id 를 지금 코드로 옮기고, 물린 코드는 상속이 없던 것처럼 정한다. 이미
     쌓인 스냅숏은 그대로다(그 적재는 스냅숏 하나를 통째로 덧붙이고, 다시 쓰지 않는다).
   - 필지 계보(ADR-0113): 계보의 단위는 시도 묶음이다(묶음 안의 필지끼리만 짝지어진다). `parcel_lineage_scheduled` 는
     옛 코드가 든 묶음의 계보가 대체보다 먼저 도출됐으면 그 묶음만 다시 도출한다(`superseded_since`). 계보도
     append-only 라 물린 짝이 만든 `same_lot_paired_dong` 링크가 남는다. 한 선대의 그런 링크 중 짝 근거 등급이 가장 높은
     것만 읽는다(`parcel_lineage.drop_outranked_code_changes`, 모든 소비자의 문인 `steward_resolved`; Spark 쪽은
     `parcel_lineage_section.drop_outranked_code_changes_frame`, 패널 계보 섹션과 매칭 관문).
   - Gold 패널과 by-PNU 굽기: ADR-0273 의 `code_change_pnus` 가 정본에서 달라진 행의 두 코드 아래 필지를 고른다
     (`views.changed_codes`). 대체 한 건은 공식 짝 행과 대체 행 두 줄이라 정확히 위 세 코드 아래 PNU 다. 그 PNU 만
     다시 만들어 합치고, 바뀐 PNU 기록(#902)·WAP(ADR-0321)이 그 집합을 굽기로 넘긴다.
4. **기간 밖의 공식 근거는 그대로 사람에게 간다**(ADR-0156 §2). 대체하지 않는다.
5. 심은 시험:
   - `test_code_go_kr_legal_dong.py::OfficialSupersedesInferenceTest`
     - `test_an_official_pair_arriving_later_supersedes_a_date_and_name_pair_and_reaches_only_its_pnus`
     - `test_official_evidence_against_a_steward_pair_goes_to_review_not_an_override`
     - `test_official_evidence_dated_outside_the_change_window_still_goes_to_a_person`
     - `test_rerunning_after_a_supersession_appends_nothing_and_decides_the_same`
   - 뷰·행정경계·계보·알림: `test_legal_dong_code_change_views.py::SupersessionTest`,
     `test_administrative_boundaries_handoff_to_silver.py`, `test_parcel_lineage_scheduled.py`,
     `test_parcel_lineage_section_spark.py`, `orchestration/tests/test_legal_dong_code_load.py::OfficialEvidenceDecided`.
   - 실패가 막는 사고: 공식 기록이 늦게 와서 추정 짝을 반박했을 때 전국의 짝 맞추기가 사람을 기다리며 멈추거나(지금까지),
     물린 짝이 대응표·행정경계 id·필지 계보·Gold 에 남는다.

## Consequences

- `PairingConflict` 는 없어진다. 짝 맞추기가 공식 근거 때문에 멈추는 일은 없다.
- 정본에 `kind = supersession` 행이 생긴다. `kind` 를 `pair` 로만 거르던 읽기(행정경계, 계보 입력, 장소 정체성
  resolver)는 둘 다 읽어 view 로 고른다.
- 대체가 생긴 날, 그 시도 묶음의 계보는 한 번 다시 도출된다. 같은 링크는 소비자가 값으로 합치므로 결과는 바뀐 링크만
  다르다. 묶음보다 잘게는 나누지 않는다: 합병·분할 근거가 묶음 안의 동을 넘나든다.
- 행정경계 id 는 다음 경계 적재 때 바로잡힌다. 그 사이 서빙 중인 스냅숏의 id 는 이전 그대로다.
- 시도·시군구의 묶기 짝(`derived:children`)은 대체 대상이 아니다(공식 근거가 동 단위다). 그 아래 동이 바뀌어도 기록된
  묶기 짝은 그대로 선다.
