# ADR 0150: 필지 번호 공식 이력은 동 단위 행부터 읽고, 먼저 기록된 파생 짝도 그것과 맞아야 한다

- Status: Accepted
- Date: 2026-10-05
- Amends: [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md) §4 (공식 이력이 지번 단계보다 먼저 정한다는 결정의 입력과 범위)
- Related: [ADR-0144](./0144-region-code-changes-are-derived-from-downloaded-data-only.md) §4, [ADR-0145](./0145-one-source-of-truth-for-region-code-changes.md) (정본 하나), [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) (필지 계보)

## Context

ADR-0148 §4 는 필지고유번호변동연혁(VWorld MK/30527)의 공식 짝이 지번 단계보다 먼저 정하고, 같은 실행에서 둘이 다르면
멈추게 했다. 그때 이 원천은 수집되지 않았고, 짝의 모양도 재 보지 않았다.

2026-10-05 30527 을 처음 받아 열었다(인천·경기 시도 파일, 내려받기만, 적재 없음).

- 파일은 옛 PNU → 새 PNU 짝이다(19자리, 전 행). 토지이동사유와 토지이동일자가 함께 온다.
- 한꺼번에 다시 매긴 동은 필지마다가 아니라 **동마다 한 행**(대장구분 0, 본번·부번 0000)으로 온다.
  - 인천 2026-07-01: 44행 모두 동 단위(중구 28110 → 28125).
  - 화성 2026-02-01: 195행 모두 동 단위(41590 → 41591·41593·41595·41597).
  - 필지 행만 보는 짝 맞추기는 이 개편들을 하나도 못 정한다.
- 반영은 몇 달 늦다. 그 사이 지번 단계나 날짜·이름 규칙이 먼저 짝을 기록할 수 있다. ADR-0148 의 비교는 같은 실행
  안에서만 일어난다. 이미 기록된 짝은 비교 없이 그대로 서므로, 나중에 온 공식 기록이 그것을 반박해도 아무도 모른다
  (PR #339 리뷰 지적).

## Decision

1. **공식 이력은 동 단위 행부터 읽는다.**
   - 옛 동의 동 단위 행이 현존 코드 하나를 가리키면 그것이 짝이다(`detail` = `dong_level`).
   - 동 단위 행이 없으면 필지 행이 정한다(`parcel_level`). 조건은 둘이다: 연결된 필지가 모두 한 새 동으로 가고, 앞 판에서
     옛 동이 가진 필지의 계약 비율(`jibun_overlap_min_share`, 지번 단계와 같은 선) 이상을 잇는다. 그보다 적은 행(경계
     조정 몇 필지)은 그 필지들이 간 곳의 근거일 뿐 동의 짝이 아니다. 그 코드는 `awaiting_data`(`official partial`)로 남는다.
     앞 판이 없어 비율을 잴 수 없을 때도 마찬가지다.
   - 필지 행이 여러 새 동으로 나누면 짝이 아니라 분할(`split`)이다. `split_into` 가 어디로 몇 필지 갔는지 적는다.
2. **파생 짝도 공식 이력과 맞아야 한다.** 공식 행이 옛 동을 옮기는 코드들이 있는데, 다음 짝이 그 어느 것도 아닌 코드를
   말하면 실행은 `PairingConflict` 로 끝난다. 두 답을 모두 적고 변경표에 아무것도 쓰지 않는다. 공식 행이 하나를 정할 때,
   여러 동으로 나눌 때(분할), 다시 폐지된 코드로 갈 때(사슬) 모두 그렇다.
   - 변경표에 이미 기록된 파생 짝(`derived:parcel-jibun:*`, `derived:code-go-kr:date+name:*`)
   - 이번 실행의 날짜·이름 규칙
   - 이번 실행의 지번 단계(ADR-0148 §4 를 분할·사슬까지 넓힌다)

   스튜어드 짝은 사람이 이유를 적은 결정이라 다시 판정하지 않는다.
   - 실패가 막는 사고: 공식 기록보다 먼저 들어온 추정 짝이 틀렸는데 정본에 그대로 남아, 시군구 대응표·행정경계
     id·필지 계보로 퍼진다.
   - 심은 시험이 지킨다(`test_code_go_kr_legal_dong.py`):
     - `test_a_recorded_derived_pair_the_official_history_contradicts_stops_the_run`
     - `test_a_date_and_name_pair_the_official_history_contradicts_stops_the_run`
     - `test_official_history_decides_before_the_jibun_step_and_a_disagreement_stops_the_run`
     - `test_a_derived_pair_outside_an_official_split_or_chain_stops_the_run`
     - `test_a_few_parcel_rows_are_evidence_not_a_decision_for_the_dong`
3. **공식 짝의 집은 원천 표 하나다.**
   - `silver.parcel_number_change_history` 에 시도 파일을 받은 그대로 쌓는다. Bronze 객체 하나가 한 번의 적재다.
   - 짝 맞추기는 계약의 `floor_date` 이후 날짜가 붙은 행만 읽는다. 날짜를 읽을 수 없는 행, 수집 다음 날보다 뒤의 날짜,
     형식이 어긋난 PNU 는 격리되고 근거가 아니다.
   - `region-code-holders.json` 의 `source_tables` 가 이 표를 공식 짝의 집으로 적는다. 가드
     `region-code-pairs-have-one-home` 은 Bronze 객체 하나씩 적재하지 않는 원천 표를 거부한다.
   - 정한 코드 짝은 `reference.legal_dong_code_change` 에만 쌓는다(ADR-0145).
   - 필지 계보가 이 기록을 `official` 등급 근거로 쓸 때도 옮겨 담지 않고 이 표를 읽는다.
4. **변경표는 계보를 읽지 않는다.** 이건 그대로다(ADR-0145 §2).

## Consequences

- 지번까지 새로 매긴 동(인천 7월의 중구 → 28125, 화성 2월의 구 분리)은 변동연혁이 반영되는 대로 사람 없이 정해진다.
- 변동연혁은 code.go.kr 를 대신하지 않고 보탠다. 반영 전에는 판단 대기로 남는다.
- 공식 이력이 먼저 기록된 파생 짝을 반박하면 그날 짝 맞추기가 멈추고 투영은 바뀌지 않는다. 운영자가 원인을 고친다:
  판을 바로잡거나, 스튜어드 짝을 기록한다(런북 `legal-dong-code-changes.md` 4 절). 정본은 덮어쓰지 않으므로, 틀린
  파생 짝을 바로잡는 길은 새 결정 행이다.
- 수집은 `parcel_number_change_history` 작업이 한다(default_pool, Spark 없음). 제공자 갱신일이 바뀐 파일만 받는다.
  적재는 계보 단위가 짝 맞추기 앞에서 한다. 감독 실행 전까지 수집은 꺼져 있다.
