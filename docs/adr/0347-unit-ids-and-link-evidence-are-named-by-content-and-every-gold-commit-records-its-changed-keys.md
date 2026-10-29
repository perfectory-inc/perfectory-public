# ADR 0347: 호 id·연결 근거는 내용으로 짓고, 모든 Gold 커밋은 바꾼 키를 기록한다

- Status: Accepted
- Date: 2026-10-30
- Extends: [ADR-0331](./0331-building-floor-and-unit-area-rows-are-named-by-their-natural-key-not-their-file-line.md)
  (층·전유공용면적 → 호·연결 근거),
  [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md) §1
  (변경 기록 → 통째 덮어쓰기까지)
- Builds on: ADR-0320 (문서 판 확장/수축), ADR-0321 §5 (WAP 의 `row_digest` 차이), ADR-0314 (부분 처리가 기본),
  ADR-0346 (상한을 넘는 변경은 기록이 설명할 때 전량, PR #963)

## Context

**사고 (2026-10-29).** 건물 by-PNU 레인이 Gold 7009 → 6612 를 `not_a_delta` 로 거부했다. 5,945,309 문서 중
5,933,490 이 "바뀌었다". 실제 내용 변경은 0.17% 였고, 나머지는 원천 파일 이름 + 줄 번호로 만든 네 필드였다:
`floor_row_id`, `unit_row_id`, `building_link_source_record_id`, `building_link_input_sha256`. 매달 새 배치 파일이
오면 네 값이 모두 바뀐다. 층 id 는 #932(ADR-0331)가 이미 자연 키로 고쳤다. 남은 셋은 이렇다:

- `unit_row_id` = `building-register-unit:<bronze 파일>#line-N` (`row_identity`).
- `building_link_source_record_id` = 기본개요 파일 + 그 호의 줄 번호.
- `building_link_input_sha256` = 세 입력 파일(전유부·표제부·기본개요)의 키·크기·체크섬 digest. 행의 관계가
  아니라 실행 전체를 말하므로 모든 행이 같은 값이고, 파일이 바뀌면 모두 바뀐다.

**두 번째 구멍.** 통째 덮어쓰기 Gold 커밋(`Outcome.commit` 의 full 경로)은 바꾼 키를 기록하지 않았다.
병합(ADR-0312 §1)과 코드 판 공개(ADR-0321 §5)만 `gold.<t>__changes`·`foundation.keys-changed` 를 썼다.
그래서 정당한 대량 변경도 ADR-0346 에서 "설명 없음" 이 되고, 작은 변경도 굽기가 두 스냅숏 전체를 조인했다.

**측정 (2026-10-29/30, 운영 Trino 읽기 전용).**

- `silver.building_register_units` 10월 판: 19,860,453 행, `mgm_bldrgst_pk` 19,860,453 개(전부 고유).
- 운영 DB `catalog.normalization_proposal`·`normalization_application` 의 `building_register_unit` 건수 0.
  `unit_row_id` 를 대상으로 저장한 승인이 없으므로 옮길 것이 없다.

### 실사례

- **Kimball·dbt `generate_surrogate_key`**: 대리 키는 업무 키에서 만든다. 적재 위치에서 만들지 않는다(ADR-0331 과 같음).
- **Iceberg Write-Audit-Publish(Netflix), Delta Lake Change Data Feed, Hudi incremental query**: 쓰기마다
  바뀐 키를 커밋 옆에 남기고, 소비자는 두 스냅숏 전체를 다시 비교하지 않고 그 기록을 읽는다.
  전체 덮어쓰기도 예외가 아니다. Delta 는 `INSERT OVERWRITE` 에도 CDF 행을 쓴다.
- **Debezium·CDC**: 변경 이벤트의 정체성은 행 키다. 로그 파일·오프셋이 아니다.

## Decision

1. **호 id 는 호의 키로 짓는다.** `unit_row_id = natural_row_identity("building-register-unit", [mgm_bldrgst_pk], 순번)`
   이고, 모양은 `building-register-unit:nk1:<hex32>` 다. 순번은 ADR-0331 의 `NaturalKeyRank` 로 매긴다(키 지문 →
   나머지 원천 열의 지문 → 입력 순서). export 는 이미 있는 사전 검증 읽기(`ParentInputs::load`)에서 행마다
   20바이트 순번을 모은다. 그래서 zip 을 더 읽지 않는다. 메모리는 계산으로 19.86M × 20 B ≈ 400 MB 다.
   키가 고유하므로 순번은 늘 1 이다. 순번은 공급자가 중복을 보낼 때만 쓰인다.
2. **연결 근거는 근거의 내용으로 짓는다.**
   - `building_link_source_record_id` 는 관계를 진술한 기본개요 행의 자연 키 이름이다:
     `natural_row_identity("building-register-basis", [호 PK], 1)`. 기본개요 인덱스는 같은 PK 의 행을 한 항목으로
     접는다(충돌은 순서와 무관하게 남는다). 그래서 순번은 1 이다. 원천 행은 같은 배치의 기본개요에서 PK 로
     다시 찾는다.
   - `building_link_input_sha256` 은 그 호의 관계 판정이 읽은 사실의 SHA-256 이다
     (`BuildingRegisterBasisIndex::evidence_sha256`). 사실은 호 항목(부모 PK·종류·충돌)과 부모 항목(종류·충돌)이다.
     사실이 같으면 다음 배치에서도 같고, 바뀌면 바뀐다. 세 입력 파일의 digest 는 실행 요약
     (`source.building_link_input_sha256`)에만 남는다.
   - 계약 `relationship_evidence_policy` 의 필드·형식(`^[0-9a-f]{64}$`, 비어 있지 않은 원본 행)은 그대로다.
     Python·Rust 검증은 바뀌지 않는다.
3. **확장/수축으로 옮긴다(ADR-0320, ADR-0331 §4).** 열 이름·형·필수 여부와 by-PNU 문서 경로가 그대로이므로
   문서 판은 "같음" 이다. 첫 새 배치에서 호·근거를 싣는 문서가 한 번 바뀐다(PR #963 측정 5.95% = 353,978
   문서, 상한 아래 패치). 옛 Silver 판은 옛 값을 그대로 가진다(append-only).
4. **모든 Gold 커밋은 바꾼 키를 기록한다.** 통째 덮어쓰기(`Outcome.overwrite_recording_changes`)는 쓰기 전에
   현재 Gold 와 `row_key`·`row_digest` 로 차이를 낸다. 이 차이는 WAP 공개가 쓰는 `merge_source` 그대로다.
   바뀐 키는 로컬 디스크에 고정한다(`Stages`). 덮어쓰기 커밋은 `foundation.keys-changed` 를 싣고, 같은 단계로
   `gold.<t>__changes` 에 키를 붙인다(`record_changes`). append 쓰기는 붙인 키를 기록한다.
   - 기록하지 않는 경우는 둘이다. 하나는 첫 빌드다(비교할 이전 행이 없다). 다른 하나는 차이를 낼 수 없는
     경우다(현재 Gold 에 빌드가 더 이상 만들지 않는 열이 있을 때). 그때는 지금처럼 기록 없는 단계가 되고
     로그에 `gold-changes-unrecorded` 를 남긴다. 기록은 쓰기를 실패시키지 않는다.
   - 결과: 굽기의 변경 집합 잡은 늘 기록을 읽는다(`change_source recorded`). 큰 정당한 변경은 ADR-0346 의
     `recorded_change_set` 으로 설명되고, 작은 변경은 패치가 된다.
5. **shape-ledger 검사는 통째 덮어쓰기를 실패시키지 않고 이름만 단다.** 덮어쓰기가 이제 `keys-changed` 를
   말하므로, 검사는 Iceberg `operation = overwrite` 인 마지막 커밋을 증폭 실패가 아니라 노트로 남긴다
   ("full rebuild that changed x% of its keys"). 통째 재구축은 ADR-0314 의 선언된 예외다. 증폭 실패는
   병합(부분 쓰기)이 키 몫보다 많이 쓸 때의 것이다(2026-10-26 사고).

## Consequences

| 항목 | 전 | 뒤 | 근거 |
|---|---:|---:|---|
| 월 배치 하나에서 바뀐 건물 by-PNU 문서 | 5,946,006 (바뀜 5,933,490 · 새 6,029 · 삭제 6,487) / 5,945,309 | **19,882** (바뀜 7,366 · 새 6,029 · 삭제 6,487), 0.33% | 계산: 운영 Gold 7009 ↔ 6612 전국을 `pnu` 로 맞대고, 네 위치 필드 값을 지운 `buildings_json`·`unlinked_units_json` 해시 비교(Trino, 2026-10-30) |
| 그 배치의 굽기 판정 | `not_a_delta` 거부 | 패치(상한 0.5 의 0.7%) | 계산 |
| 통째 덮어쓰기의 변경 기록 | 없음, 굽기가 두 스냅숏 조인 | 키·수 기록, 굽기는 기록만 읽음 | 시험 `RecordedChangesIcebergTest` |
| 덮어쓰기 비용 추가분 | — | `row_key`·`row_digest` 조인 한 번과 키 목록 쓰기 | WAP `diff` 단계와 같은 일 |

- "뒤" 수치의 한계: 층 순번 뒤바뀜(ADR-0331 측정 361 행)과 근거 digest 가 관계 사실 변경으로 바뀌는 호는
  지운 필드라 세지 않았다. 사실이 바뀌어 판정 결과(부모·방법·사유)도 바뀐 호는 다른 필드로 이미 세어졌다.
  남는 것은 결과는 같은데 사실만 바뀐 호다(예: 부모 종류 충돌이 생겼다 사라짐). 그래서 7,366 은 하한에 가깝다.
- 한 번의 전환 비용: 새 id 세대가 처음 들어오는 Gold 재구축은 호·근거를 싣는 문서 모두가 한 번 바뀐다.
  이 커밋도 바꾼 키를 기록하므로(결정 4) 설명된 변경이 된다. 상한 아래면 패치다.
- data.go.kr JSON 경로는 export 의 전체 순위가 없으면 배치 안에서 순번을 센다(ADR-0331 Consequences 와 같다).
  키가 고유하므로 결과는 같다.
- 시험(사고를 이름으로): `building_register_unit_silver_plan::a_unit_keeps_its_id_when_the_next_release_moves_its_line`,
  `building_register_basis::evidence_follows_the_relation_facts_not_the_file_or_its_lines`,
  `parent_key_tests::the_next_release_with_moved_lines_keeps_unit_ids_and_link_evidence`,
  `test_gold_fingerprint_iceberg.RecordedChangesIcebergTest`(덮어쓰기가 기록한 키로 굽기 delta 가 조인과 같은 수),
  `test_shape_ledger_check::a_full_rebuild_that_says_what_it_changed_is_named_not_failed`.
- Gold 고정물(`building_panel_gold_row.json`)과 by-PNU 골든 팩은 합성 불투명 값(`synthetic-row:UNIT-1`)을 쓴다.
  Rust plan 이 만든 값이 아니어서 다시 만들 것이 없다.

## Rejected

- **네 필드를 문서·비교에서 빼기**: 문서가 필수로 싣는 필드다(판 기록). 빼면 깨는 변경이다(ADR-0320 §3).
  ADR-0331 Rejected 와 같다.
- **입력 파일 digest 를 행마다 그대로 두기**: 실행을 말하는 값이지 관계를 말하는 값이 아니다. 모든 행이 매달
  함께 바뀌는 열은 변경 감지를 무력하게 만든다. 계보는 실행 요약에 있다.
- **근거 원본 행 id 에 내용 해시 쓰기**: 행 이름과 행 내용이 한 열에 섞인다. 이름은 키로 짓고(결정 2),
  내용은 digest 열이 말한다. ADR-0331 의 "식별 열과 내용 열을 나눈다" 와 같다.
- **덮어쓰기를 모두 WAP 공개(`--publish-changed-keys`)로 돌리기**: 부분 쓰기 자체가 바뀌는 더 큰 결정이고
  그림자 브랜치가 하나 더 쓰인다. 여기서 필요한 것은 기록이다. 공개 방식은 ADR-0321 §5 그대로 둔다.
- **굽기 쪽에서 두 스냅숏을 조인해 키를 만들기**: 지금의 대체 경로이고, 사고가 그 경로였다. 생산자가 이미
  가진 차이를 커밋에 남기는 편이 싸고(Delta CDF·Hudi 와 같은 방식), 설명의 근거도 된다(ADR-0346).
