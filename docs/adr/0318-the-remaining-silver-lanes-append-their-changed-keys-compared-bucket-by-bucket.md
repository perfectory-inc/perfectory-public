# ADR 0318: 남은 Silver 레인도 바뀐 키만 쌓고, 비교는 키 묶음마다 따로 커밋해 다음 실행이 잇는다

- Status: Accepted
- Date: 2026-10-28
- Extends: [ADR-0311](./0311-silver-lanes-overwrite-only-with-a-declared-reason-and-the-compare-commit-is-the-changelog.md)
  (`full_reason` 으로 남겨 둔 레인 다섯을 비교로 옮긴다), [ADR-0275](./0275-silver-release-lanes-append-the-changed-keys-instead-of-overwriting.md) §4.3
- Builds on: [ADR-0183](./0183-a-release-loaded-in-batches-is-staged-on-a-branch-and-published-at-once.md),
  [ADR-0267](./0267-a-versioned-table-declares-its-current-row-and-every-reader-takes-it-from-the-contract.md),
  [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md), [ADR-0259](./0259-ci-migrates-the-production-table-shape.md)

## Context

ADR-0311 은 토지 레인 다섯을 비교 후 추가로 옮기고, 아직 판 전체를 쓰는 레인 다섯에 `full_reason` 과 잰 값을 적게
했다. 그 다섯이 남은 일이다. 값은 운영 Trino(읽기 전용, 2026-10-20·10-28)로 잰 것이다.

| 레인 | 한 번 적재가 쓰는 행 | 왜 아직 통째였나 |
|---|---:|---|
| `building_register_titles` | 8,064,354 · 668 MB | 허브 레인은 load branch 없이 main 에 쓰고, 건물 패널은 입력마다 `source_snapshot_id` 하나만 받았다 |
| `building_register_units` | 19,860,453 · 1.39 GB | 같음 |
| `building_register_unit_areas` | 114,461,830 · 8.38 GB | 같음 |
| `land_use_plan` | 247,584,668 · 2.68 GB | 비교 한 번이 150분 단위 상한을 넘는다(아래) |
| `land_use_zone_code` | 1,270 · 37 KB | `source_snapshot_id` 가 날짜가 아니라 최신 순서가 못 된다 |

`land_use_plan` 의 문제는 비교 작업의 모양이다. 2026-10-21 토지 이동 이력 1억 3,166만 행의 비교가 한 덩어리로 87분
걸렸다(첫 시도는 `TimeoutStartSec=150m` 에 걸려 실패). 2억 4,758만 행이면 선형으로 약 165분이다. 한 번에 다 하거나
아무것도 못 하는 작업은 상한을 올려도 다음 성장에서 다시 걸린다. 시도별 분포(2026-10-28)는 41(경기) 46,842,275행이 가장
크고(18.9%), 47·46·48 이 2,600만~3,100만 행이다. 시도 하나는 같은 비율로 약 31분이다.

허브 대장 셋의 `mgm_bldrgst_pk` 는 지역 코드가 아닌 일련번호다(앞 두 자리 10·11·12 가 각각 약 256만~287만 행).

이 밖에 append 레인 둘(`building_register_apartment_price`, `building_register_exclusive_unit`)은 판 전체를 다시
붙이고, `parcel_number_change_history` 는 제공자가 파일을 다시 올리면 그 파일의 1,370,351행을 똑같이 한 번 더 붙인다.
앞의 둘은 V1(ADR-0317, 공시가격 판을 바뀐 행만 담는 모델)이 표 모델째 다시 정하므로 여기서 건드리지 않는다.

## Decision

1. **비교는 키 묶음(bucket)마다 따로 한다.** 레인 계약 `compare` 블록에 `bucket_prefix_length` 를 적는다(필수, 0 이면
   한 덩어리). 키 첫 열의 앞 N 글자가 같은 키들이 한 묶음이다. 토지 시도 레인은 2(PNU 의 시도), 허브 대장은 2(PK 앞
   두 자리, 약 3묶음), 코드표는 0. N 보다 짧은 값은 그 값 자체가 한 묶음이고(정확히 같음으로 고른다), null 은 따로
   한 묶음이라 어느 키도 두 묶음에 들지 않는다. 한 묶음은 두 쪽에서 자기 행만 읽는다(`startsWith`, Iceberg 가 파일과
   행 그룹 경계로 거른다). 키의 행은 모두 같은 첫 열 값을 가지므로 묶음의 현재 행은 표 전체의 그 키 현재 행과 같다.

2. **바뀐 행은 changes branch 에 묶음마다 커밋하고, 다 되면 main 을 한 번에 앞으로 민다.** 비교 작업
   (`silver_release_compare.py`)은 main 이 아니라 판의 changes branch(`changes_<load 와 같은 digest>`, main 머리에서
   만든다)에 묶음 하나를 한 커밋으로 붙인다. 커밋 요약에 판(`foundation.compare-release`)과 묶음
   (`foundation.compare-bucket`, JSON 으로 적어 null 과 빈 문자열이 갈린다)을 적는다. 묶음이 다 들어가면 빈 커밋 하나가
   판의 Bronze 객체를 `foundation.ingest-batch-objects` 로 기록한다(봉인). 레인은 그 브랜치를 `publish_branch` 로 main 에
   fast-forward 한다(ADR-0183 과 같은 원자적 발행). 읽는 쪽은 판의 반쪽을 보지 않고, "이 판은 실렸나" 는 전과 같이
   main 의 기록으로 답한다. 모든 커밋이 `foundation.change-record` 를 단 append 라 `changelog.changes_between`
   (ADR-0311)은 바뀐 행만 그대로 읽는다.

3. **시간이 모자라면 다음 실행이 잇는다.** 레인은 실행 시작 + `LAST_BUCKET_START_MINUTES`(110분)를 마감으로 넘긴다.
   마감이 지나면 새 묶음을 시작하지 않고 `deadline` 을 보고한다. main 은 그대로이고, 실행은
   `outcome=unchanged reason=compare_resumes_next_run` 으로 끝난다(실패 경보가 아니다). 다음 실행은 export 없이
   (허브: 브랜치가 판을 기록하면 내보내기를 건너뛴다, 토지: 이미 있는 handoff 와 브랜치 묶음은 다시 읽지 않는다) changes
   branch 가 기록하지 않은 묶음만 비교한다. 110분은 가장 큰 묶음(약 31분)과 발행이 150분 안에 끝나는 값이고, 시험이
   단위 파일의 `TimeoutStartSec` 를 읽어 상수와 맞춘다. main 이 그사이 움직이면(압축 등) changes branch 는 낡았으므로
   지우고 처음부터 비교한다(`remove_stale_branch`): 낡은 main 에 대해 비교한 묶음을 새 main 위에 발행하지 않는다.

4. **허브 대장 셋도 비교한다.** 한 실행이 한 판인 레인(표제부·전유부·전유공용면적)은 판을 load branch 에 통째로
   올리고(`overwrite`, ADR-0183) 비교 후 추가한다. 키는 `mgm_bldrgst_pk`, 최신 순서는 `source_snapshot_id`(실행 이름
   `silver-refresh-<lane>-YYYYMM-<digest>` 라 제공자 월 순이다). `content_columns` 는 판마다 모든 행에서 바뀌는 계보 열
   (행 id 가 Bronze 객체와 줄 번호로 만들어진다, `row_identity`; 행 체크섬; `valid_from_utc`; 대장 연결 입력의 출처와
   해시; 적재 열)을 넣지 않는다. 바뀌지 않은 대장은 옛 행(옛 행 id)을 그대로 둔다. part 레인(가격·전유부 다리)은
   append 라 비교를 받지 않는다(`lane.rs` 가 거부).

5. **모든 독자는 현재 행이나 "판 X 시점의 행"을 계약에서 받는다.** 새 함수 `platform_contracts.rows_as_of(frame,
   contract, release)` 는 계약의 `newest_first` 한 열이 `release` 이하인 행의 현재 행이다(시점 복원: 키가 마지막으로
   바뀐 판의 행). 버전이 없는 표는 그 판의 행을 그대로 읽는다. 옮긴 독자:
   - `building_panel_silver_to_gold`: 판 id 는 현재 행의 최댓값(토지 패널과 같은 규칙), 버전 있는 입력은
     `read_appended` 가 아니라 현재 행의 차이(ADR-0180)로 바뀐 PNU 를 찾는다.
   - `address_pnu`: 표제부 현재 행. `parcel_lineage_to_silver.read_buildings`: `rows_as_of`.
   - `pnu_null_share_guard.table_baseline`: 기준선이 표 전체가 아니라 독자가 보는 현재 행.
   - dbt `foundation_current_rows` (생성): 전유부·전유공용면적 규칙. `parcel_panel` 은 이미 계약의 현재 행을 읽는다.

6. **코드표도 비교한다.** `land_use_zone_code` 의 최신 순서는 `ingested_at_utc`(한 적재는 한 시각)다. 1,270행이라
   한 덩어리(0)다. 통째로 쓰는 것이 더 싸다는 이유는 "부분이 기본, 전량은 잰 예외"(ADR-0314) 앞에서 남길 만큼 크지 않다.

7. **필지고유번호변동연혁은 표가 갖지 않은 행만 붙인다.** `vworld_parcel_number_change_history load` 는 객체마다
   표가 이미 가진 행(`EVENT_COLUMNS`: 옛·새 PNU, 동 단위, 사유, 날짜와 원문, 격리 사유, 시군구, 시도; null 끼리도 같게)을
   빼고 붙이며, 붙일 행이 없어도 객체를 같은 커밋에 기록한다(`append_source_object`). 표의 뜻은 그대로다: 제공자가
   낸 적 있는 연결은 모두 남고(append-only), 뒤 파일이 빠뜨린 연결을 지우지 않는다.

8. 이제 저장소의 덮어쓰는 레인 10개는 모두 `compare` 를 적고 `full_reason` 을 적은 레인은 없다. ADR-0311 의 규칙
   (둘 중 정확히 하나)은 그대로이고, 새 예외는 잰 값과 함께 다시 적을 수 있다.

## 실제 사례

- **Write-Audit-Publish(Netflix, Apache Iceberg 브랜치)**: 쓰기는 브랜치에, 검사 뒤 `fast_forward` 로 main 에
  원자적으로 발행한다. 2번이 같은 모양이고, 묶음 커밋은 같은 브랜치 위의 여러 감사 단계다.
- **키 범위로 나눈 비교(Datafold `data-diff`)**: 큰 두 표를 키 범위 조각으로 나눠 조각마다 체크섬으로 비교하고
  다른 조각만 내려간다. 1번의 묶음이 그 조각이다(여기서는 다른 묶음을 내려가지 않고 묶음 안에서 키마다 비교한다).
- **재개 가능한 분할 백필(Airflow/Dagster partitioned backfill)**: 분할마다 완료를 기록하고 실패·시간 초과 뒤에는
  남은 분할만 돈다. 3번의 마감과 커밋 요약의 묶음 기록이 그 완료 기록이다.
- **SCD Type 2 / 시점 조회(Kimball, Delta·Iceberg time travel)**: 바뀐 행만 새 판으로 쌓고, 어떤 시점의 상태는 그 시점
  이하의 최신 버전으로 복원한다. 5번의 `rows_as_of` 가 그 조회다.

## Consequences

- 한 판이 main 에 쓰는 행은 바뀐 키의 행과 삭제 표시뿐이다(추정: 대장은 행 수 차이 +12,849·+93,447 이 하한,
  토지 용도는 시도마다 바뀐 PNU 의 행). 판 전체는 load branch 에만 올라가고 발행 뒤 지운다(ADR-0275 가 받아들인 값).
- 각 레인의 첫 비교는 판 하나를 가진 main 과 비교하므로 표 크기만큼 읽는다(대장 1억 1,446만 행은 묶음 3개).
  추정이고 아직 재지 않았다. 마감 덕에 상한을 넘으면 실패가 아니라 다음 날 이어진다.
- 현재 행을 고르는 창 함수(`rank` over key)가 건물 패널·dbt 독자의 읽기에 붙는다(전유공용면적 1억 1,446만 행).
- 판이 이틀에 걸치면 main 은 그동안 앞 판이다. 하루 늦어질 수 있지만 반쪽 판은 보이지 않는다.
- 남은 일: 가격·전유부 다리 append 레인은 ADR-0317(V1). 묶음 비교의 운영 실측(묶음별 시간·읽은 바이트)은 첫 판이
  들어올 때 잰다. 압축이 changes branch 를 낡게 만드는 날이 잦으면 압축 창을 비교 창과 떼는 일을 따로 정한다.
