# ADR 0606: 매일 훑기의 VWorld 레인은 한 실행에 새 바이트 상한까지만 받고, 나머지는 다음 날 받는다

- Status: Accepted
- Date: 2026-11-23
- Amends: [ADR-0172](./0172-the-daily-vworld-sweep-fetches-everything-it-lacks.md) §1(가지지 않은 것을 한 실행에 전부)·§3(8 시간
  상한에 걸린 실행은 실패로 알리고 다음 실행이 잇는다)
- Related: [ADR-0170](./0170-large-vworld-files-come-through-the-raon-agent-on-the-data-host.md)(RAON 레인의 한 실행 상한),
  [ADR-0514](./0514-the-raon-lane-takes-a-capped-count-of-the-newest-files-every-day-and-a-listed-file-not-in-bronze-pages.md)

## Context

ADR-0172 는 VWorld 레인이 가지지 않은 파일을 한 실행에 전부 받게 했다. 8 시간 상한에 걸린 실행은 실패로 알려지고, 다음 실행이
나머지를 잇는다. 2026-11-21 #1587 이 NA/23 연속지적도형정보의 역사(이 레인 몫 3,811 파일, 384 GB)를 매일 훑기에 넣었다.
2026-11-22 의 운영 기록(journal, `catalog.bronze_object` 의 커밋 시각)은 이렇다.

- 03:40Z 실행은 `TimeoutStartSec=8h` 에 걸려 11:40Z 에 죽었다. 커밋은 시간마다 198–446 파일, 29–35 GB 였다. 같은 unit 에서
  VWorld 레인 뒤에 도는 RAON 레인은 돌지 못했다.
- 11:45Z 재실행은 6 시간 20 분 돌았다. VWorld 레인이 3 시간 05 분 동안 1,213 파일, 124.4 GB 를 받았고(40.3 GB/h), RAON 이
  3 시간 12 분 걸렸다(이미지 다시 짓기 85 분, 파일 7 개).
- 긴 실행은 시작할 때의 릴리스를 붙든다. 그동안의 두 배포에서 prune 이 그 릴리스를 거부했다(exit 75).

시간당 파일 수는 두 배 넘게 흔들리지만 바이트는 고르다. 그래서 한 실행의 몫은 파일 수가 아니라 바이트로 잰다. 수집기의
기존 `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES` 는 이 일을 못 한다. 그 값은 원장 확인 전에 목록 전체(2026-11-22 에
14,862 파일, 대부분 이미 가진 것)를 자르므로, 가장 새 파일만 계속 고르고 밀린 역사에는 닿지 않는다.

## Decision

1. **상한은 카탈로그에 둔다.** `daily_collections.source_sweep.vworld_max_new_bytes_per_run` 이 상한이고 지금 50 GiB 다.
   다른 레인 상한(`selection_archive_*`)과 같은 자리다. `scripts/ops/vworld-sweep-lane.sh` 가 그 값을
   `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_NEW_BYTES` 로 넘긴다. 매일 훑기와 스테이징 스모크(ADR-0177)가 같은 값을
   받는다. 호스트 손 설정은 없다.
2. **상한은 새 파일에만 센다.** 쓰기 실행의 `ingest-vworld-dataset-files` 는 먼저 원장에서 가진 파일을 가른다
   (`partition_held_files`, 다운로드 길과 같은 확인). 그 뒤 Bronze 에 없는 파일을 제공자 갱신일 내림차순으로, 목록 크기
   (`size_kib` × 1024) 합이 상한 안인 동안 앞에서부터 받는다(`within_new_bytes_cap`). 늘 앞에서부터라 더 오래된 파일이 더 새
   파일보다 먼저 가지 않는다. 첫 파일은 상한보다 커도 받는다. 영원히 기다리는 파일은 없다.
3. **나머지는 실패가 아니라 기다림이다.** 증거에 `deferred_run_cap` 으로 남고, journal 줄에 `deferred_run_cap=N` 이 붙는다.
   다음 실행은 이번에 받은 것을 건너뛰고 이어 받는다. ADR-0172 §1 의 "거부하는 예산은 없다"는 그대로다. 모두 받되 하루 몫씩이다.
4. **크기.** 50 GiB 는 잰 속도로 1.3–1.9 시간이다. 잰 RAON 3 시간 12 분을 더해도 한 실행은 약 5.2 시간에 끝난다. 8 시간
   상한은 안전망으로 남는다. 한 실행의 `landed_bytes` 와 실행 시간이 바뀌면 이 수를 다시 잰다.

## Consequences

- 큰 역사 몰아받기는 하루 50 GiB 씩 여러 날에 나뉜다(384 GB 면 약 8 일). 대신 매 실행이 끝까지 돌아 성공한다. 실패 알림이
  없고, 8 시간 동안 옛 릴리스를 붙들지도 않는다. RAON 레인도 같은 날 돈다.
- 비용: 다운로드 전에 목록의 파일마다 원장을 한 번 묻는다(전엔 다운로드 중에 파일마다 물었다). 상한을 넘긴 파일은 열지 않는다.
- 시험
  - Rust: 상한 안의 앞부분만 가고 뒤는 기다린다. 상한보다 큰 첫 파일도 간다.
  - `test_daily_source_sweep_command.py`: 수집기가 카탈로그의 상한을 받는다. 카탈로그 값은 잰 가장 느린 시간의 2 시간 몫
    이하다.
