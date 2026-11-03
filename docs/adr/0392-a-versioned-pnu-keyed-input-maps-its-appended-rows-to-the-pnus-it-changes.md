# ADR 0392: PNU 를 키에 담은 버전 입력은 append 된 행의 PNU 가 곧 바뀐 PNU 다

- Status: Accepted
- Date: 2026-11-04
- Amends: [ADR-0189](./0189-an-appended-silver-input-is-its-own-change-set.md) 결정 2(필지 패널은 `append_inputs` 를 선언하지 않는다)
- Builds on: [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md),
  [ADR-0267](./0267-a-versioned-table-declares-its-current-row-and-every-reader-takes-it-from-the-contract.md)·[ADR-0275](./0275-silver-release-lanes-append-the-changed-keys-instead-of-overwriting.md)(버전 표의 현재 행 = 키마다 최신, Silver 는 바뀐 키만 append),
  [ADR-0344](./0344-gold-merges-write-only-the-changed-rows-as-merge-on-read-deletes-that-compaction-folds.md)(#951)

## Context

`gold_panel_rebuild`(유닛 160분 = 9,600초)이 10-16 ~ 11-03 사이 열 번 시간 초과로 죽었다. 11-03 두 번
(08:07Z, 10:53Z)은 모두 같은 원인이다. `silver.land_transfer_history` 가 append 됐고, 필지 패널의 증분 재생성이 돌았다.

필지 패널 실행 로그의 `gold-incremental-phase` 줄을 운영 호스트에서 읽기 전용으로 재 보았다. 증분 실행 9번이다.

| 실행 | affected | restrict | build | parity | merge | 결과 |
|---|---:|---:|---:|---:|---:|---|
| 10-16 08:57 | 3,933 | — | — | — | — | 시간 초과 |
| 10-21 09:46 | 5,800 | 2,439 | 12 | 515 | — | 시간 초과 |
| 10-21 12:37 | 5,421 | 1,937 | 7 | 70 | — | 시간 초과 |
| 10-22 06:52 | 6,827 | 866 | 6 | 67 | — | 시간 초과 |
| 10-22 09:34 | 6,709 | 1,209 | 5 | 67 | — | 시간 초과 |
| 10-22 12:15 | 7,831 | 1,385 | 5 | 66 | — | 시간 초과 |
| 10-23 06:56 | 4,800 | 848 | 5 | 66 | 1,858 | 커밋 |
| 10-26 03:52 | **118** | 994 | 5 | 75 | 1,597 | 커밋 |
| 11-03 10:53 | 7,661 | — | — | — | — | 시간 초과 |

(초, 실측)

시간의 대부분은 **affected**(어느 PNU 가 바뀌었나)였다. MERGE 쓰기 쪽이 아니다. #951(ADR-0344)이 고친 것은
copy-on-write MERGE 가 파일을 통째로 다시 쓰는 일이다. 1·2단계(#959·#960)는 병합됐다. 하지만 affected 를 줄이지는
않으므로 이 시간 초과를 고치지 못한다.

affected 가 비싼 이유: 필지 패널은 바뀐 입력마다 옛 판과 새 판을 **둘 다 통째로** 읽어 다중집합 차이(`changed_rows`)를
구한다. ADR-0189 는 append 만 쌓인 입력의 차이를 건너뛰게 했지만, 필지 패널은 그 길을 선언하지 않았다(ADR-0189 결정 2).

11-03 의 `silver.land_transfer_history`(읽기 전용 Trino `$snapshots`, 실측):
- 핀 `5614432088871267699` 에서 새 판 `7782311539463799564` 까지의 부모 사슬은 append 16개다.
- 그 16개가 더한 것은 약 182,700 행이다. 표 전체는 1억 3,185만 행이다.
- 그런데도 affected 는 1억 3,185만 행짜리 두 판을 통째로 비교했고, 7,661 초가 걸렸다.

ADR-0189 가 필지 패널을 뺀 까닭은 두 가지였다. "읽기가 행 단위 필터가 아니다"(버전 표의 키마다 최신 행 선택, 계보 후보).
그러나 필지 패널은 변경 **행**이 아니라 변경 **PNU** 만 쓴다. 필지 속성 입력 여섯 개는 모두 버전 표이고, 키가 PNU 를
담는다(`current_row_latest.key` = `pnu` 또는 `(pnu, 순번)`). append 는 그 키의 새 버전(또는 삭제 표시)을 더할 뿐이다.
그러니 어떤 키의 현재 행이 바뀌려면 그 키의 행이 append 안에 있어야 한다. append 된 행의 PNU 는 바뀐 PNU 의 상위집합이다.
계보 후보는 그 뒤 `affected_pnus` 가 PNU 집합에서 그대로 따라간다(직접 PNU → 후속 PNU).

## Decision

1. **PNU 를 키에 담은 버전 입력은 append 를 PNU 로 매핑한다.** 필지 패널의 속성 입력 중
   `current_row_latest` 가 있고 그 키가 `pnu` 를 담는 입력(`APPEND_KEYED_SOURCES`, 규칙은 공용 `gold_incremental.appends_map_to_keys`, 추가분 키는 `appended_keys`)은
   `Lane.append_inputs` 에 선언한다. 핀부터 새 판까지 append 뿐이면(`appended_span`, ADR-0189 결정 1), 그 입력의
   변경 PNU 는 증분 append 읽기(`read_appended`)로 읽은 추가분의 `pnu` 다.
   - 원래 코드의 PNU 와, 빌드가 쓰는 서빙 코드로 바꾼 PNU(`renumbered`)를 둘 다 넣는다.
   - 지역 실행의 계보 원천 규칙(`with_lineage_sources`)을 그대로 따른다.
   - 두 판의 차이는 계산하지 않는다. append 가 아닌 스냅숏이 하나라도 끼면 예전처럼 차이를 구한다.
2. **상위집합은 안전하다.** 같은 내용의 새 버전이 닿은 PNU 도 다시 계산된다. 하지만 `row_digest` 가 같으므로
   합쳐지지 않는다. 늘어난 수가 `max_changed_key_fraction` 을 넘으면 기존대로 전량 재생성이다.
3. **가드.** `test_every_parcel_attribute_maps_its_appends_to_pnus` 가 두 가지를 확인한다.
   - 필지 속성 입력 여섯 개가 모두 이 길을 탄다. 하나가 빠지면 11-03 의 2시간짜리 차이로 조용히 돌아가기 때문이다.
   - PNU 없는 키나 버전 없는 표를 이 규칙이 거절한다.

   `test_parcel_incremental_merge_equals_the_full_rebuild` 의 append 단계(실제 Iceberg)는 세 가지를 확인한다.
   - 내용이 바뀐 새 버전, 내용이 같은 새 버전, 삭제 표시를 append 한다.
   - 그 결과 `appended_inputs` 에 그 입력이 적히고, 닿은 PNU 가 정확히 그 셋이다.
   - 병합 결과가 전량 재생성과 같다.

## Consequences

- 필지 패널의 affected 단계가 두 판 전부 읽기(실측 3,933 ~ 7,831 초)에서 append 된 행 읽기로 준다. 10-26 실행이
  차이 없이 118 초였다(실측). 11-03 와 같은 변경이라면 약 18만 행을 읽는다.
- 한 실행의 예상(계산): 10-16 ~ 11-03 의 실측 최댓값을 더한다.
  - 기동 60 초 + affected 약 120 초 + restrict 2,439 초 + build 12 초 + parity 515 초 + merge 1,858 초 = 약 5,000 초.
  - merge 는 copy-on-write 시절 값이다. merge-on-read(#960) 이후는 더 작을 것이다.
  - 9,600 초 유닛의 52% 다. ADR-0386(#990)의 90% 여유선 안이다.
- 남는 위험: 같은 유닛이 필지 패널 뒤에 건물 패널을 굽는다. 건물 패널은 [ADR-0326](./0326-a-gold-rebuild-that-costs-more-to-find-the-change-than-to-rebuild-runs-whole.md) 에 따라 전량이다(10-28 실측
  6,667 초). 그래서 둘 다 바뀐 날에는 합이 유닛을 넘을 수 있다. 그것은 #951 의 나머지(건물 패널 증분화)와 #919 몫이다.
- restrict(최대 2,439 초)도 여전히 입력 표 전체에 키마다 최신을 매긴 뒤 키로 거른다. 키가 PNU 를 담으므로 키 필터를
  최신 선택 앞으로 옮기면 더 줄일 수 있다. 이번 결정에는 넣지 않는다.
- 같은 `affected_pnus` 를 쓰는 종류 Gold(`gold_kind_rebuild`, `by_pnu_kind_silver_to_gold`)도 같은 길을 탈 수 있다. 다음 조각이다.
- 다른 회사 사례:
  - Iceberg 증분 append 읽기(`start-snapshot-id`)와 Delta Lake Change Data Feed 는 같은 원리다. 변경 집합을 다시 계산하지 않고,
    커밋이 이미 아는 추가분을 읽는다.
  - Hudi 의 증분 쿼리도 마찬가지다. 커밋 시각 이후의 레코드 키만 읽는다.
