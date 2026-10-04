# ADR 0145: 지역 코드 변경의 정본은 하나다 — 코드 변경표와 필지 계보, 나머지는 거기서 파생한다

- Status: Accepted
- Date: 2026-10-04
- Supersedes:
  - [ADR-0103](./0103-place-identity-outlives-administrative-code-changes.md) ②③ (시간 사전 `reference.legal_dong_code` 와 seed 부트스트랩)
  - [ADR-0105](./0105-the-canonical-transition-table-is-produced-from-the-derived-crosswalk.md) (전이표의 생산 경로)
  - [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) §5·§9 (계보 안의 동 짝 계산, 선행 코드 지도 CLI 파일)
  - [ADR-0143](./0143-legal-dong-code-changes-come-from-code-go-kr.md) §5 (저장된 시군구 대응표와 seed 대조)
- Amends: [ADR-0142](./0142-hub-register-pnu-loss-is-refused-at-derivation-and-at-load.md) (seed 의 역할)

## Context

"지역 코드 X 가 Y 로 바뀌었다(언제, 왜)"라는 같은 사실이 2026-10-04 기준 13곳에 있다(전수 조사). 운영 카탈로그를 실측한 결과는
다음과 같다.

| 보관처 | 단위 | 쓰는 곳 | 운영 행 |
|---|---|---|---|
| `reference.legal_dong_code` (ADR-0103) | 코드 이력 | 수동 실행만 | 표 없음, 읽는 코드 없음 |
| `sigungu-canonical-crosswalk.contract.json` | 시군구 27쌍 (손으로) | — | 파일 |
| `reference.sigungu_canonical_crosswalk` | 시군구 짝 | 쓰는 곳 **둘** | 표 없음 |
| `catalog.administrative_unit_transition` (ADR-0105) | 단위 전이 | 없음 | 행 없음으로 추정 |
| `reference.legal_dong_code_change` (ADR-0143/0144) | 동·시군구·시도 짝 | 계보 관리 작업(꺼짐) | 표 없음 |
| `legal_dong_predecessor_map.py` 산출 파일 | 동 짝 | 수동 CLI | 파일 |
| `silver.parcel_lineage` (ADR-0113) | 필지 짝 (동 짝은 매번 다시 계산) | 수동 | 표 없음 |
| `reference.legal_dong_code_snapshot` | code.go.kr 원본 목록 | 수동·계보 관리 | **53,387행** |
| `gold.place_id_registry` 등 | 장소 ID | 수동 | **5,067행** |

조사에서 결함 넷이 나왔다.

1. 시군구 짝 표에 쓰는 곳이 둘이다. 멱등 키가 달라 같은 짝이 두 번 기록될 수 있다.
2. 동 짝을 만드는 알고리즘이 둘이다(`parcel_lineage.pair_legal_dongs`, `code_go_kr_legal_dong.pair_changes`). 둘의 결과가 어긋날 수 있다.
3. 변경표는 필지 계보를 증거로 읽고, 필지 계보는 동 짝을 따로 계산한다. 서로가 서로를 읽는다.
4. 방향이 섞여 있다. 시군구 짝 표는 새 → 옛, 전이표와 ADR-0103 은 옛 → 새다.

운영에 데이터가 거의 없는 지금이 정리 비용이 가장 낮은 때다.

## Decision

1. **코드 단위(시도·시군구·동)의 정본은 `reference.legal_dong_code_change` 하나다.**
   - 원본 증거는 `reference.legal_dong_code_snapshot`(code.go.kr 전체 코드 표)이다.
   - 짝을 만드는 알고리즘은 하나(`pair_changes`, ADR-0144 순서)다.
   - 방향은 옛 → 새로 통일한다.
2. **필지 단위의 정본은 `silver.parcel_lineage` 하나다.**
   - 동 짝은 직접 계산하지 않고 1의 정본에서 읽는다.
   - 변경표는 필지 계보를 증거로 읽지 않는다. 필지고유번호변동연혁 같은 원천에서 직접 읽는다. 순환을 끊기 위해서다.
3. **나머지는 정본에서 파생하거나 폐기한다.**

   | 보관처 | 처리 |
   |---|---|
   | 시군구 대응표 (적재 프로그램이 읽는 것) | 1의 정본에서 `level='sigungu'` 를 뽑은 **파생 투영**. 저장된 표 `reference.sigungu_canonical_crosswalk` 는 폐기 |
   | 동 선행 코드 지도 (행정경계 ID 발행이 읽는 것) | 1의 정본에서 `level='dong'` 을 뽑은 파생. CLI 파일 폐기 |
   | 장소 ID 등록부 | 그대로. 동 짝은 1에서 읽음 |
   | `reference.legal_dong_code` 와 그 작업 | 폐기 (읽는 곳 없음) |
   | `catalog.administrative_unit_transition` | 소비자가 생기기 전까지 생산하지 않음. 필요해지면 1의 파생 |
   | 손으로 만든 27쌍 목록 | 시험 자료로만 남김. 운영 대조는 첫 실운영에서 1이 27쌍을 재현한 뒤 뺀다 |
   | 허브 임시값 코드·부재 시도 상한 | 코드 변경 사실이 아니라 허브 원천의 특성이다. 허브 원천 계약으로 옮김 (ADR-0142 검사는 유지) |

4. **가드.** 같은 짝을 두 곳 이상에 저장하는 코드가 다시 생기지 않게, 저장소 검사가 위 폐기 대상의 쓰기를 거부한다.
   검사가 정의 파일을 가리키게 하고, 형태를 문장으로 옮겨 적지 않는다.

## Consequences

- 지역 코드 변경 하나에 대해 "어디가 맞나"를 묻는 곳이 두 군데(코드·필지)로 줄어든다.
- 리더 다섯 곳을 바꿔야 한다: 필지 계보, 변경표 짝 만들기, 행정경계 ID 발행, 허브 적재 대응표, 관련 시험.
- 운영에 옮길 데이터는 거의 없다(정본 후보 표가 아직 없음). 원본 스냅숏 53,387행과 장소 ID 5,067행은 그대로 둔다.
