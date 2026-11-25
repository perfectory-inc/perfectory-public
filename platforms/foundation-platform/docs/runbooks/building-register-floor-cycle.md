---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-22
---

# 건축물대장 층별 정보(FLOOR) 주기

`foundation-building-register-floor.service`(작업 `building_register_floor`, 매일 08:20)가
`scripts/ops/building-register-floor-cycle.sh` 로 발행기의 `run-building-register-floor-cycle` 을 한 번 돌린다.
Spark 는 쓰지 않는다(root ADR-0599). 예전의 수동 원격 실행기 `run-remote-lakehouse-job` 과 그 smoke 작업들은
없어졌다: 그 작업들이 싣던 `silver_scalar_handoff_to_lakehouse.py` 가 이 주기와 함께 사라졌다.

## 한 번의 주기

| 단계 | 어디서 | 무엇을 |
|---|---|---|
| 입력 고르기 | 발행기 프로세스 | Bronze 원장에서 가장 새 완전한 FLOOR·표제부 짝을 고른다(`select_inputs`). |
| 입력 준비·검사 | 제어 컨테이너(`lakehouse-control`, 고정 이미지) | `stage-building-register-floor-inputs` 로 그 짝만 받아 두고, 내보내기의 원천 검사로 영수증 `source.json` 을 쓴다. |
| 이력 검사 | 발행기 프로세스 | `history_gate_run`(옛 `building_register_floor_history.py`): 증인 스냅숏과 머리까지의 append 사슬, 기록된 판의 행 계보를 DuckDB 로 읽어 확인한다. 이미 실린 판이면 `already_retained` 로 여기서 끝난다(종료 0). |
| 내보내기 | 제어 컨테이너 | `export-building-register-floor-silver-handoff` 가 판을 Parquet handoff 로 쓴다. |
| 적재 | 발행기 프로세스 | `floor_load`: handoff 를 DuckDB 가 그 자리에서 읽어 판 검사(필수값·체크섬·정규화 상태·한 판·행 수)를 하고, `bucket(16, mgm_bldrgst_pk)` 버킷마다 파일로 준비해 한 커밋으로 판 전체를 덧붙인다(판 기록 포함). 판 단위 전체 적재는 이 레인의 선언된 예외다(정의의 `full_reason`). |

결과 요약은 Spark 적재가 내던 `foundation-platform.spark_run_summary.v1` 그대로라 검증과 배치 감사는 바뀌지 않았다.
마지막 줄은 `remote-lakehouse-job-ok … persisted_row_count=N` 이고, 스크립트가 `foundation-job-outcome` 으로 바꾼다.

## 자리와 자격

- 상태(입력 ZIP, handoff): `FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT`(`/data/foundation-platform/building-register-floor`).
- DuckDB 작업 폴더(준비 파일·임시 파일): `FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_DUCKDB_ROOT`
  (`/var/lib/foundation-platform/building-register-floor/duckdb`, 시스템 디스크). 임시 파일의 상한은
  DuckDB 의 기본값(그 디스크 남은 자리의 90%, `duckdb_parquet_stage::engine_config`)이다.
  두 자리는 `foundation-release.sh` 의 FLOOR 상태 준비가 만들고 유닛 drop-in 의 `ReadWritePaths` 에 있다.
- 읽기: 표의 데이터 파일은 읽기 전용 키 쌍(`lakehouse-reader.env`)으로 읽는다. 쓰기: 커밋은 기록 토큰으로,
  입력 준비만 기록 키 쌍을 이름으로 받는다. 증인 태그 `foundation-floor-history-witness` 는 이미 증인을 가리키면
  다시 쓰지 않는다.
- 비공개 증인 파일 `FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH` 는 공개 소스 밖에 있고 읽지도 옮기지도 않는다.

## 손으로 돌리기

같은 코드를 명령 하나씩 돌릴 수 있다(판정·조사용). 둘 다 쓰지 않으면 아무것도 쓰지 않는다.

```bash
# 이력 검사: 종료 0 = 이미 실림, 10 = 내보내기 필요
FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_CHECK_RECEIPT_PATH=… \
FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_CHECK_OUTCOME_PATH=… \
FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_CHECK_WORK_DIR=… \
  foundation-outbox-publisher check-building-register-floor-history

# 적재: LIVE_WRITE=1 이 없으면 검사·준비만 하고 준비 파일을 WORK_DIR/verdict-staged 에 남긴다
FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_LOAD_EXPORT_SUMMARY_PATH=… \
FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_LOAD_HANDOFF_PATH=… \
FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_LOAD_WORK_DIR=… \
FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_LOAD_OUTPUT_PATH=… \
  foundation-outbox-publisher load-building-register-floor-silver
```

환경에 `FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN` 이 있으면 두 명령은 카탈로그도 읽기 토큰으로 읽는다
(`read_catalog_config`). 판정은 읽기 자격만으로 돈다.

## 되돌리기

이 주기는 덧붙이기만 한다. 코드가 틀렸으면 그 PR 을 되돌린다(revert). 잘못 실린 판은 지우지 않는다(append-only):
다음 판의 기록과 `row_versions`(editions)가 읽는 판을 정한다.
