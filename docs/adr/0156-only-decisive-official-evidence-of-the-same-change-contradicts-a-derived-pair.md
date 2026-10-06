# ADR 0156: 파생 짝을 반박하는 것은 같은 변경의 결정적인 공식 근거뿐이다

- Status: Accepted
- Date: 2026-10-06
- Amends: [ADR-0150](./0150-the-official-parcel-number-history-decides-before-the-jibun-sets.md) §2 (파생 짝을 공식 이력에 맞춰 보는 범위)
- Related: [ADR-0144](./0144-region-code-changes-are-derived-from-downloaded-data-only.md), [ADR-0145](./0145-one-source-of-truth-for-region-code-changes.md) (정본 하나), [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) (필지 계보)

## Context

ADR-0150 §2 는 공식 행이 옛 동을 옮기는 코드가 **하나라도** 있으면, 파생 짝이 그중 어느 것도 아닐 때 실행을 멈추게 했다.
날짜도, 몇 필지인지도 보지 않았다.

2026-10-06 운영에서 공식 이력을 처음 읽은 감독 실행(계보 단위, 릴리스 fd56f283)이 이 규칙으로 멈췄다. 아무것도 쓰지 않았다.

- 옛 동 하나(광주의 한 동)의 공식 행은 13개 필지 행이다. 토지이동일자 2026-04-15, 사유 52.
  - 이웃 동으로 13필지를 옮긴 경계 조정이다. 동은 그 뒤에도 그대로 있었다.
- code.go.kr 는 2026-07-01 그 동 전체를 새 코드로 바꿨다(전남·광주 통합). 날짜·이름 규칙의 짝은 맞다.
- 규칙은 석 달 앞선 13필지 이동을 동의 짝에 대한 반박으로 읽었다. 다른 사건이고, 동이 어디로 갔는지에 대해 아무 말도
  하지 않는 근거다.

공식 이력은 필지마다의 이동을 다 적는다. 경계 조정은 흔하다. 이 규칙이면 짝 맞추기 기간 안에 경계 조정을 한 번이라도
겪은 동은 모두 같은 이유로 멈춘다.

## Decision

1. **결정적인 공식 근거만 파생 짝을 반박한다.** 짝을 정할 때(ADR-0150 §1)와 같은 선이다.
   - 옛 동의 동 단위 행(대장구분 0). 하나를 가리키든, 여럿(분할)이든, 다시 폐지된 코드(사슬)든.
   - 또는 필지 행이 앞 판에서 옛 동이 가진 필지의 계약 비율(`pairing.jibun_overlap_min_share`) 이상을 한 코드로 옮길 때.
2. **같은 변경끼리만 비교한다.** 공식 행의 토지이동일자가 옛 동의 변경 기간 안이어야 한다.
   - 변경 기간은 code.go.kr 폐지일과 그 다음 날이다. 날짜·이름 규칙과 지번 단계가 쓰는 기간과 같다(`change_window`).
   - 다른 날의 이동은 다른 사건이다. 결정적이어도 이 짝을 반박하지 않는다.
   - 그래서 짝 맞추기가 읽는 공식 짝은 (옛 PNU, 새 PNU, 토지이동일자)다. 검증용 `--official-links` 파일도 날짜 칸을 가진다.
   - 그래도 조용히 버리지 않는다. 기간 밖 날짜의 결정적 근거가 짝과 다른 코드를 가리키면 사람에게 보인다. 제공자가 같은 개편의
     날짜를 하루보다 더 어긋나게 적었다면 짝이 틀렸을 수 있기 때문이다.
     - 요약의 `official_off_window_decisive`: 옛 코드 → `[{new, changed_on, kind, share}]`. `kind` 는 `dong_level`/`parcel_level`,
       `share` 는 필지 행이 옮긴 앞 판 필지의 비율이다(동 단위 행은 null).
     - 스튜어드 목록의 항목: `kind` = `official_off_window`, `status` = `official_disagrees_off_window`. 스튜어드 승인은 `pair`
       항목만 받으므로 규칙이나 승인 하나로 지워지지 않는다. 짝 자체는 날짜·이름·지번 근거로 기록될 수 있다.
3. **부분 이동은 동의 짝을 정하지도 반박하지도 않는다.**
   - 그 필지들의 계보 근거로 남는다. 저장은 그대로 원천 표 하나다(ADR-0150 §3, ADR-0145).
   - 요약의 `official_partial_moves`(옛 코드별 필지 수)가 센다.
4. **진짜 반박은 여전히 멈춘다.** 같은 변경의 결정적 공식 근거가 다른 코드를 가리키면, 쓰기 전에 `PairingConflict` 로 끝난다.
   - 메시지는 어느 행(`dong_level`/`parcel_level`)이 어느 날짜에 무엇을 말했는지 적는다.

스튜어드 짝을 다시 판정하지 않는 것, 공식 근거가 지번 단계보다 먼저 정하는 것(ADR-0150 §1)은 그대로다.

- 실패가 막는 사고: 같은 날 공식 기록이 동 전체를 다른 곳으로 옮겼는데, 추정 짝이 정본에 들어가 대응표·행정경계 id·필지 계보로
  퍼진다.
- 심은 시험이 지킨다(`test_code_go_kr_legal_dong.py`):
  - `test_a_boundary_adjustment_months_before_a_merger_does_not_contradict_it` — 운영에서 멈춘 모양. 옛 규칙에서는 같은
    `PairingConflict` 로 실패한다.
  - `test_a_partial_move_on_the_day_of_the_change_does_not_contradict_it` — 옛 규칙에서 실패한다.
  - `test_a_decisive_move_on_the_day_of_the_change_elsewhere_still_stops_the_run` — 동 단위, 다음 날, 필지 단위 셋 다 멈춘다.
  - `test_decisive_evidence_on_another_day_is_put_before_a_person_not_ignored` — 기간 밖 결정적 근거가 요약과 목록에 남고,
    승인으로 지워지지 않는다.

## Consequences

- 2026-10-06 멈춘 실행은 날짜·이름 짝을 기록하고 지나간다. 경계 조정은 `official_partial_moves` 에 숫자로 보인다.
- 공식 행의 날짜와 code.go.kr 폐지일이 하루보다 더 어긋나는 변경은 실행을 멈추지 않는다. 대신 `official_off_window_decisive` 와
  스튜어드 목록에 남는다. 그런 어긋남이 실제로 재어지면 기간을 계약으로 옮겨 넓힌다. 그 전에는 추측으로 넓히지 않는다.
- 공식 근거가 짝을 정하는 쪽(ADR-0150 §1)은 이번에 날짜를 보지 않는다. 날짜·이름 규칙이나 기록된 짝이 먼저 서는 동에는
  닿지 않는다.
