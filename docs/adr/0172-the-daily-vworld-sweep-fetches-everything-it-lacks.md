# ADR 0172: 매일 훑기의 VWorld 레인은 가지지 않은 것을 전부 받는다 — 바이트 예산 대신 안내 한 줄

- Status: Accepted
- Date: 2026-10-10
- Amends: [ADR-0168](./0168-the-daily-sweep-collects-the-vworld-land-datasets-within-a-byte-budget.md) §4(하루 새 바이트
  예산)·§5(밀린 것은 운영자가 받는다)
- Related: [ADR-0170](./0170-large-vworld-files-come-through-the-raon-agent-on-the-data-host.md)(RAON 대용량 레인의 상한은
  그대로), [ADR-0077](./0077-the-pipe-looks-at-its-sources-every-day.md)

## Context

ADR-0168 §4 는 VWorld 레인에 하루 새 바이트 예산(16GiB)을 두고, 가지지 않은 파일의 목록 크기 합이 넘으면 **하나도 받지 않고
실패**하게 했다. 밀린 것은 운영자가 예산을 올려 따로 받는다(§5).

운영의 첫 훑기들은 가지지 않은 파일 881개, 39.8GB 를 찾았고 매일 거부했다(종료 1). 배포는 그 작업의 DAG 를 멈춘 채로
두었다. 밀린 것은 저절로는 절대 줄지 않았다 — 예산이 막는 것은 바로 밀린 것을 받는 일이었다.

예산이 지키려던 것은 비용과 놀람이다. 비용은 사소하다(R2 저장 약 $0.015/GB-월, 40GB 면 한 달 0.6달러). 위험한 부분 — 중간에
죽은 실행 — 은 이미 막혀 있다: 파일마다 따로 커밋하고(단일 커미터, ADR-0016), 가진 파일은 건너뛰므로 죽은 실행은 다음
실행이 이어 받는다. 소유자 결정(2026-10-10): 가지지 않은 것은 할 수 있는 한 빨리 전부 받는다.

## Decision

1. **VWorld 레인에는 바이트 예산이 없다.** 카탈로그의 `daily_collections.source_sweep.new_bytes_budget`, 스크립트의
   예산 전달, `ingest-vworld-dataset-files` 의 예산 거부(`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_NEW_BYTES_BUDGET`,
   `blocked_new_bytes_budget`, `deferred_new_bytes_budget`)를 없앤다. 운영자 run `source-sweep-vworld-backlog` 도 없앤다 —
   밀린 것을 받는 것은 매일 실행 그 자체다. 원장이 가진 것인지 묻는 한 가지 확인(`partition_held_files`)은 RAON 선택 묶음을
   가르는 데 그대로 쓴다.
2. **예산 대신 안내.** journal 한 줄은 이번 실행이 받아야 했던 것과 받은 것을 센다: `pending=`(가지지 않았던 파일 수)
   `pending_bytes=`(그 목록 크기) `landed_bytes=`(커밋한 바이트). 한 실행이 카탈로그의
   `daily_collections.source_sweep.landed_bytes_notice`(100GiB)보다 많이 받으면 슬랙에 ℹ️ 한 줄을 보낸다 — 제공자가 과거
   판을 대량으로 다시 올린 날을 사람이 알게. 거부도 실패도 아니다. 기준값은 계획이 카탈로그에서 읽어 보고서
   (`landed_bytes_notice`)로 넘기고, 선언되지 않은 수집은 여전히 계획이 거부한다.
3. **실행 시간의 상한은 남기되 예산이 아니다.** 단위 `TimeoutStartSec` 4h → 8h, Airflow `timeout_minutes` 250 → 490.
   상한에 걸려 멈춘 실행은 실패로 알려지지만 커밋한 파일은 남고, 다음 실행(또는 `airflow-runtime.sh trigger
   source_sweep`)이 나머지를 받는다. 저장소는 운영 전송 속도를 모른다 — 운영 증거를 읽지 않았다. 8h 는 40GB 를
   1.4MB/s 로도 한 실행에 받는 크기이고, 매일 03:40 일정과 겹치지 않으며 기본 풀 대기 상한 검사(`pool_starvation`)를
   통과한다. 첫 완주 실행의 `landed_bytes` 와 실행 시간으로 다시 본다.
4. **RAON 대용량 레인은 그대로다.** `selection_archive_new_bytes_budget`(0 = 꺼짐, ADR-0170)은 비용이 아니라 아직 감독
   실행을 거치지 않은 브라우저 경로를 막는 장치이므로 이 결정 밖이다.

## Consequences

- 배포 뒤 첫 실행이 39.8GB 를 받기 시작한다. 8h 안에 끝나지 않으면 그 실행은 시간 초과로 실패하고 다음 실행이 나머지를
  받는다 — 실패 알림이 그 사이 한두 번 올 수 있다. 스풀(ADR-0168 §9)은 동시에 `MAX_IN_FLIGHT` 개 파일만 데이터 디스크에 둔다.
- 시험(`orchestration/tests/test_daily_source_sweep_command.py`): 큰 날은 받고 ℹ️ 한 줄만, 🔴 없음; 평범한 날은 추가 알림
  없음; 카탈로그·스크립트·계약에 옛 예산이 없음; 수집기에 VWorld 예산 변수가 넘어가지 않음; journal 의 pending·landed.
  큰 날을 실패로 되돌리면 시험이 빨개짐을 확인했다. Rust 시험은 예산 시험을 지우고 계획 시험을 안내 기준으로 바꿨다.

- 길어질 수 있는 실행이 되었으므로 매일 훑기는 `started_once_after_deploy` 에서 빠진다. 배포가 몇 시간짜리 수집을 기다리며 다음 배포를 막지 않는다. 매일 예약이 같은 일을 한다(루트 ADR-0159 의 "배포 안에서 돌 만큼 짧은 작업만" 규칙).
