# ADR 0180: 패널 Gold 는 입력이 바뀐 PNU 만 다시 만들어 합친다

- Status: Accepted
- Date: 2026-10-11
- Builds on: [ADR-0139](./0139-panel-gold-is-rebuilt-when-a-silver-input-changes.md) (Silver 가 바뀌면 다시 만든다, 판 기록),
  [ADR-0164](./0164-panel-gold-is-written-in-pnu-order-and-a-shard-reads-only-its-files.md) (PNU 순서 쓰기),
  [ADR-0130](./0130-panel-input-snapshots-are-complete-and-bound-before-spark.md) (입력 판 고정),
  [ADR-0099](./0099-daily-serving-updates-bake-only-changed-parcels.md) (`row_digest` 와 굽기의 변경 집합)

## Context

2026-10-10 운영 측정: hub.go.kr 의 월간 표제부 릴리스 하나(`silver.building_register_titles` 806만 행, 새
`source_snapshot_id` 로 덮어쓰기)가 `gold.building_panel` 전체 재생성을 불렀다. 약 595만 PNU, 건물 Silver
다섯 표를 전부 읽고 20g Spark 상한에서 약 39–40분 걸렸다. ADR-0139 의 계획기는 "아무것도 안 바뀜" 은 가려내지만,
바뀌면 표 전체를 다시 만든다. 하류는 이미 증분이다: Gold 는 PNU 범위 순서로 쓰이고(ADR-0164), 굽기는
`row_digest` 변경 집합(`by_pnu_panel_delta.py`)으로 바뀐 문서만 굽는다. 가운데 Gold 만 전량이다.

Silver 쪽 사실:

- 표제부는 릴리스마다 **덮어쓴다**. Iceberg 증분 읽기(append 스냅숏 사이의 추가분)로는 "전부 바뀜" 이 된다.
- 입력 표에 내용 해시 열은 일정하지 않다(`row_checksum_sha256` 은 건물 넷에만 있고 무엇을 덮는지 이 결정의 대상이
  아니다). 행 id(`title_row_id`, `source_record_id`) 와 `source_snapshot_id`·`ingested_at_utc` 는 릴리스마다 바뀐다.
- Silver 스냅숏은 7일 뒤 만료된다(`lakehouse-maintenance.contract.json`). 월간 입력의 지난 판은 다음 릴리스
  때쯤 이미 없다.
- 굽기의 스냅숏 스캔(`lakehouse_snapshot_scan`)은 delete 파일을 거부한다. merge-on-read 는 쓸 수 없다.

## Decision

1. **바뀐 행 = 빌드가 읽는 열이 다른 행.** 생산자마다 `BUILD_COLUMNS`(입력별로 빌드가 읽는 열)를 선언하고,
   전량이든 증분이든 빌드는 그 열로 줄인 프레임만 받는다(`gold_incremental.project_build_inputs`). 선언에 없는 열을
   빌드가 읽으면 열 없음으로 실패하므로, 선언이 실제 의존보다 좁아질 수 없다. 증분 실행은 현재 Gold 가 만들어진 판과
   새 판을 생산자의 읽기 경로 그대로 두 번 읽어, 그 투영의 다중집합 차이(양방향 `exceptAll`)를 바뀐 행으로 본다.
   그래서 덮어쓰기로 계보 열만 바뀐 행은 바뀐 것이 아니다. 판이 같은 입력은 읽지 않는다.
   - 필지 생산자의 대지권 중복 제거는 `source_snapshot_id` 를 마지막 순서 키로 썼다. 생산자는 입력마다 배치가 하나일 때만
     돌므로 그 키는 상수였다. 순서 키에서 뺐다(결과 불변).
2. **바뀐 행 → PNU, PNU → 읽을 행.** 두 사상은 빌드 옆, 생산자에 둔다(`affected_pnus`, `restrict_inputs`).
   - 건물: 행은 등록 키로 PNU 에 닿는다. 표제부는 자기 PNU, 층·면적·가격은 이름 붙은 표제부·호의 PNU, 호는 자기
     PNU 와 부모 표제부의 PNU, 표제부는 거기 매달린 호(사라지면 미연결로 호 자신의 PNU)까지. 옛 행과 새 행을 둘 다
     보므로 행이 떠난 PNU 도 다시 만든다.
   - 필지: 모든 섹션이 자기 PNU 다. 계보 이어붙이기로 후계 PNU 는 선대의 섹션을 읽으므로, 바뀐 선대는 후계에 닿고,
     바뀐 계보는 후보가 달라진 후계에 닿는다. 용도지역 코드표(`silver.land_use_zone_code`)는 앵커 하나가 그 아래
     모든 필지에 닿으므로 PNU 로 사상하지 않는다(`WHOLE_TABLE_INPUTS`): 바뀌면 전량이다.
   - 바뀐 PNU 의 행은 **같은 빌드 함수**(`build_gold_panel_frame`, `build_panel`)로 다시 만든다. 사업 논리의 두 번째
     사본은 없다. 입력을 그 PNU 가 읽는 행으로 좁힐 뿐이다.
3. **합치기는 copy-on-write `MERGE INTO` 하나.** 다시 만든 행 중 `row_digest` 가 현재 Gold 와 다른 것만, 사라진
   PNU 는 삭제로, 스냅숏 하나에 합친다. 그 스냅숏 요약에 새 판 기록(ADR-0139 `foundation.source-iceberg-snapshots`)을
   SQL 경로로 남긴다(Iceberg `CommitMetadata`, py4j 콜백). 다른 PNU 의 행은 바이트·`source_snapshot_id`·
   `published_at_utc` 가 그대로이고, 합칠 PNU 가 없는 데이터 파일은 다시 쓰지 않는다. 표의 `write.merge.mode` 가
   copy-on-write 가 아니면 거부한다(굽기가 delete 파일을 거부하므로). 바뀐 PNU 가 하나도 없으면 빈 append 로 판만
   기록하고, 작업 출력은 `unchanged` 다(굽기를 부르지 않는다).
4. **전량으로 돌아가는 조건(이유를 기록).** 계획기(`gold_rebuild.py`)가 `mode` 를 정하고, 생산자가 한 번 더 판단한다.
   - 계획: 계약에 `incremental` 이 없음, `--unconditional`, Gold 없음, 현재 Gold 머리가 판 기록을 가진 생산자 커밋이
     아님(사이에 압축 `replace` 만 허용 — 백필이나 손 쓰기는 행을 바꿨을 수 있다), 입력 목록이 바뀜, 바뀐 입력이
     `WHOLE_TABLE_INPUTS` 임, 비교할 옛 판이 Silver 에 더는 없음.
   - 생산자: 바뀐 PNU 가 Gold 의 `max_changed_key_fraction`(계약, 두 표 0.3)을 넘음, 패리티 표본 불일치(5항).
     생산자는 같은 실행 안에서 전량 빌드로 넘어가고, 실행 요약 `rebuild.full_reason` 에 이유를 남긴다.
   - `--unconditional` 은 지금처럼 전량이다. 행 하한(ADR-0139 5항)·품질 검사는 두 경로 모두 쓰기 전이다(증분은 다시
     만든 행의 검사와, 합친 뒤 남을 행 수의 하한).
5. **정합성 관문 = 패리티 표본.** 입력이 바뀌지 않은 PNU 중 결정적 표본(새 판으로 시드한 해시, 계약
   `parity_sample_keys` 2만)을 같은 좁힌 읽기에서 함께 다시 만들어, 현재 Gold 의 내용 열·`row_digest` 와 같아야
   한다. 다르면 현재 Gold 는 "판 기록이 말하는 입력의 빌드 결과" 가 아니다(생산자 코드가 바뀌었거나 차이 계산이
   변경을 놓쳤다) — 전량으로 간다. 합친 뒤에는 표 전체를 다시 읽어 품질 검사와 행 수(현재 − 삭제 + 삽입)를 확인한다.
   시험(`test_gold_incremental_iceberg.py`)은 모든 PNU 에서 증분 결과 = 전량 결과를 확인한다.
6. **비교할 옛 판을 남긴다.** 생산자는 커밋 뒤 입력마다 고정한 Silver 스냅숏에 태그
   `foundation-gold-input-gold-<표>` 를 옮겨 단다(`CREATE OR REPLACE TAG`). 만료는 태그가 가리키는 스냅숏을 지우지
   않으므로 다음 증분 실행이 비교할 판이 남는다. 입력·Gold 표마다 태그 하나라서 붙잡는 옛 판도 하나다. 이 작업은
   `spark` 풀을 혼자 쓰므로(ADR-0139 7항) Silver 쓰기와 겹치지 않는다.
7. **바뀌지 않는 것.** 작업 출력 `foundation-job-outcome changed|unchanged`, 잠금, 시간·메모리 상한, 굽기의
   `row_digest` 의미, 덮어쓰지 않고 쌓기(합치기도 새 스냅숏 하나, 이전 스냅숏은 역사에 남는다).

## 예상 시간 (추정, 측정 아님)

표제부 월간 릴리스 하나, 운영 규모, 같은 20g·`local[8,8]`:

| 단계 | 근거 | 추정 |
|---|---|---:|
| 표제부 옛 판·새 판 읽기 + 8열 투영 차이 | 806만 × 2 행, 좁은 열, 셔플 한 번 | 2–4분 |
| 바뀐 등록 키 → PNU | 호 1,940만 행의 키 3열 semi-join 두 번 | 1–2분 |
| 좁힌 입력 읽기 | 다섯 표를 PNU·등록 키로 semi-join(가격 2억 행이 대부분), 투영된 열만 | 4–8분 |
| 바뀐 PNU + 표본 2만 다시 만들기 | 월간 변동을 표제부의 1–5% 로 보면 PNU 6–30만, 전량 집계의 1/20–1/100 | 1–3분 |
| MERGE (copy-on-write) | 바뀐 PNU 가 전국에 흩어지면 Gold 파일 대부분을 복사(595만 행 JSON) — 계산 없는 복사·정렬 | 4–8분 |
| 합친 표 검사 | 595만 행 스캔 한 번 | 1–2분 |
| 합계 | | **약 13–27분** (전량 39–40분) |

- 지배 항은 계산이 아니라 I/O(좁힌 읽기, 합치기의 파일 복사)다. 바뀐 PNU 가 한 지역에 모이면 합치기는 그 지역 파일만
  쓴다. 전국 흩어진 변경에서 파일 복사를 없애려면 merge-on-read + 굽기의 delete 파일 읽기가 필요하다(후속, 아래).
- 실제 값은 다음 표제부 릴리스의 감독 실행에서 잰다: 실행 요약 `rebuild.counts` 와 단계 시간.

## 검토한 대안

- **Iceberg 증분 읽기/changelog 뷰**: 덮어쓰기 입력에서 "전부 바뀜" 이고, 계보 열만 바뀐 행을 걸러내지 못한다.
- **merge-on-read**: 쓰기는 가장 작지만 굽기(Rust 스캔)가 delete 파일을 거부한다. 굽기에 position delete 읽기를
  더하는 것은 별도 결정이다.
- **전량 계산 + 바뀐 행만 쓰기**: 쓰기만 줄고 40분의 대부분인 계산이 그대로다.
- **Silver 에 행 해시 열 추가**: 모든 적재기를 바꿔야 하고, 해시가 덮는 열과 빌드가 읽는 열이 어긋나면 변경을 놓친다.
  빌드가 읽는 열을 그 자리에서 비교하는 쪽이 정확하고 적재기를 건드리지 않는다.
- **입력별 증분 로직을 따로 쓰기**: 사업 논리의 두 번째 사본이 된다. 같은 빌드 함수에 좁힌 입력을 넣는다.
- **Spark DataFrame `mergeInto`**: Spark 4 부터다. 3.5 에서는 SQL `MERGE` + `CommitMetadata` 로 같은 커밋을 만든다.

## Consequences

- Gold 행의 `source_snapshot_id`·`published_at_utc` 는 "그 행을 마지막으로 바꾼 빌드" 가 된다. 분할 열
  `source_snapshot_id` 에 값이 여럿 생긴다. 서빙 문서는 이 열을 읽지 않는다(내용 열과 스냅숏 출처만).
- 입력마다 옛 판 하나가 태그로 남아 그만큼의 데이터 파일이 다음 Gold 커밋까지 R2 에 머문다(표제부라면 한 판 크기).
- 생산자 코드만 바뀐 경우는 여전히 계획기가 다시 만들지 않는다(ADR-0139). 다음 증분 실행의 패리티 표본이 이를
  보고 전량으로 간다 — 표본에 걸리는 만큼만이다. 코드 변경은 지금처럼 `--unconditional` 로 다시 만든다.
- 지역 실행(`--region-prefix`·`--pnu-prefix`)과 `--price-source-snapshot-id` 는 증분과 함께 쓸 수 없다(거부).
- 후속: (1) 다음 표제부 릴리스에서 단계별 시간 측정. (2) 흩어진 변경의 파일 복사를 줄이려면 굽기의 delete 파일 읽기
  후 merge-on-read 검토. (3) 필지 계보 이어붙이기는 아직 측정 전 입력이다(ADR-0139 11항) — 증분에서도 같다.
