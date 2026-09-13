# ADR 0105: 정본 전이표는 도출된 크로스워크에서 나온다

- Status: Accepted
- Date: 2026-09-14
- 관련: [ADR-0103 장소의 정체성은 행정코드 변경보다 오래 산다](./0103-place-identity-outlives-administrative-code-changes.md), [ADR-0104 권위 API가 현행뿐이라 크로스워크는 스냅숏 차이에서 나온다](./0104-the-authority-api-is-current-only-so-the-crosswalk-comes-from-snapshot-diffs.md)

## Context

ADR-0103·0104 로 지역코드 변경을 흡수하는 관을 세웠다: 권위 스냅숏 → `reference.legal_dong_code`
→ 도출 → `reference.sigungu_canonical_crosswalk`(현행 신코드 ↔ 폐지 구코드 쌍). 여기까지는
**코드쌍**에서 멈춘다.

그런데 ADR-0103 은 정체성을 코드가 아니라 **안정 unit ID** 위에 세우기로 했고(1·3항), 그
승계 관계를 담을 정본 표 `catalog.administrative_unit_transition`(마이그레이션
`20260727000001_administrative_boundary_identity.sql`)을 이미 정의해 두었다 — `from_unit_id ->
to_unit_id`(안정 ID FK), `transition_kind`, `effective_period`, append-only·비순환·GiST 배제
제약까지. 그러나 실측하면 이 표는 **생산자가 없는 canonical 표 17개 중 하나**다(2026-09-14
기준 `foundation-baseline.md`). 나아가 `transition_kind` 는 마이그레이션의 CHECK
(`replaced_by`/`merged_into`/`split_from`) 말고는 **의미를 정의한 문서도, 값을 읽는 코드도
없다** — 표가 제약만 있는 껍데기 상태다.

즉 코드쌍(크로스워크)은 있는데, 그것을 안정 ID 사이의 정본 승계 사실로 승격시키는 생산자가
비어 있다. `administrative_unit`·`administrative_unit_identifier`(코드↔unit 매핑)에는 생산자가
있으므로(발행자 `administrative_boundary_postgis_publish.rs`), 재료는 다 있고 조인만 없다.

## Decision

전이표는 크로스워크를 **시간 위의 정체성으로 다시 읽은 것**이다. 도출을 순수 커널로 두고(ADR-0104
와 같은 이유 — PySpark 없는 시험 레인이 그대로 검증), 발행자가 그 결과의 코드를 안정 ID로 해석해
적재한다.

1. **방향은 폐지 → 현행(predecessor → successor)으로 뒤집는다.** 크로스워크 행은 `source_code`
   =현행 신코드, `canonical_code`=폐지 구코드(지도가 아직 싣는 코드)다. 전이 엣지는 그 반대로
   `from_code`=폐지 구코드, `to_code`=현행 신코드다. ADR-0103 이 채택한 `superseded_by`
   방향(선례: Who's on First)과 같다. 시험: 시드의 `12240↔29140` 은 `29140 -> 12240` 엣지가 된다.

2. **`transition_kind` 는 변경의 카디널리티로 정한다.** 방향은 위 1항으로 **항상** 폐지→현행이고,
   종류만 다음으로 갈린다.
   - 1:1 → `replaced_by`(구코드가 신코드로 1:1 대체)
   - 다:1 → `merged_into`(여러 구코드가 한 신코드로 병합; 각 구코드마다 한 엣지)
   - 1:다 → `split_from`(한 구코드가 여러 신코드로 분리; 각 신코드마다 한 엣지)

   방향이 종류와 무관하게 고정이므로 순방향("무엇이 되었나": `from` 으로 조회)·역방향("무엇이었나":
   `to` 로 조회) 추적이 세 종류 모두에서 동일하게 동작한다. 크로스워크 두 도출기(ADR-0104
   ①·②)는 모호하지 않은 1:1 만 방출하므로 자동 도출분은 전부 `replaced_by` 이고, `merged_into`·
   `split_from` 은 스튜어드가 승인한 병합·분리 행에서만 나온다. 하나의 커널이 둘 다 처리한다.

3. **분류할 수 없는 것은 스튜어드에게 남긴다(ADR-0103 ④).** 한 쌍이 **공유 predecessor 와 공유
   successor 를 동시에** 가지는 다:다 재편은 추측 없이 종류를 정할 수 없으므로 방출하지 않는다.
   `from == to`(변경 아님)와 `valid_from` 없는 쌍(effective_period 를 열 수 없음)도 건너뛴다.
   시험: 2×2 이분 재편은 아무 엣지도 내지 않는다.

4. **provenance 는 결정적이라 재실행이 두 번 적재하지 않는다.** 각 엣지의 provenance 는
   `derived:transition:{kind}:{from}->{to}:{valid_from}` 로, append-only 크로스워크 전체를 다시
   읽어도 같은 엣지가 같은 provenance 로 나온다. 발행자는 이미 적재한 provenance 를 건너뛴다
   (`append_derived_crosswalk` 가 크로스워크에 쓰는 규율과 동일). 시험: 같은 입력 재실행은
   같은 목록을 내고 provenance 는 엣지마다 유일하다.

5. **커널과 발행의 경계.** 순수 커널 `derive_unit_transitions(crosswalk)` 는 코드 수준 엣지
   (`from_code`·`to_code`·`transition_kind`·`valid_from`·`provenance`)만 낸다. 코드→안정 unit ID
   해석과 `catalog.administrative_unit_transition` INSERT 는 그 표를 쓰는 유일한 곳인 발행자
   `administrative_boundary_postgis_publish.rs` 가 한다(`administrative_unit_identifier` 로
   `effective_period` 시점의 코드를 unit ID 로 해석). **생산자 없는 표 카운트(17→16)는 그 발행 INSERT
   가 붙을 때 닫힌다.** 본 ADR 은 설계를 고정하고 첫 증분으로 커널을 배선한다.

## Consequences

- **`transition_kind` 어휘가 처음으로 정의된다.** 지금까지 CHECK 값만 있고 뜻이 없던 표에 방향·
   종류·모호성 처리 규칙이 생겨, 이후 소비자(ADR-0103 ⑥ 양시간 조회)가 같은 뜻으로 읽는다.
- **발행 INSERT 의 선결 조건.** 발행자가 `from_unit_id` 를 채우려면 폐지 구코드(29/46)에도
   `administrative_unit`·`administrative_unit_identifier` 행이 있어야 한다(현재 발행자는 현행
   경계만 적재). 이 old-unit 정체성 적재가 후속 증분이며, 그 전까지 전이표는 비어 있는 채로 남되
   커널·설계는 검증돼 있다.
- **비용**: 커널 + 시험은 순수 파이썬 레인에서 0 인프라. 발행 배선은 발행자 한 파일과 그 시험만
   건드린다.
- **선례 준수**: 승계를 삭제 없는 방향 링크로 보존하는 것은 ADR-0103 이 인용한 Overture GERS·
   Who's on First·ONS CHD·행정표준코드가 이미 실물로 하는 방식이다.
