# ADR 0566: 경매 수집 실행은 수집만 한다 — 사진 사다리는 매시 백로그가 만들고, 백로그의 첫 실행은 자기 예산이 끝낸다

- Status: Accepted
- Date: 2026-11-14
- Amends: [ADR-0384](./0384-court-auction-photos-are-read-from-the-item-detail-within-the-night-and-stored-once-by-content.md)
  (물건상세는 실행 예산에서 사다리 예산을 뺀 때까지 묻는다 → 실행 예산 전부),
  [ADR-0429](./0429-a-photo-whose-collector-run-left-its-ladder-gets-it-from-the-silver-photo-index-hourly.md)
  (수집 실행이 그 밤 사진의 사다리를 먼저 만들고 남은 것만 백로그가 → 백로그가 전부)
- Builds on: [ADR-0427](./0427-a-photo-is-shown-from-a-rendition-ladder-made-once-from-its-stored-original.md)(사다리),
  [ADR-0457](./0457-a-job-never-run-takes-its-first-run-unattended-bounded-by-its-unit-and-the-run-is-the-measurement.md)(스스로 멈추는 작업의 첫 실행),
  [ADR-0540](./0540-court-auction-loads-every-document-of-a-case-and-keeps-personal-names-in-bronze.md) §2(진행 물건 전부)

## Context

2026-11-13 16:05 UTC 경매 수집(진행 물건 전부를 묻는 둘째 밤, #1374 배포 뒤)은 작업 한도 5 시간에서 시간 초과로 끝났다(journal:
`Result=timeout` 21:05:05, memory peak 1.9G, swap 685M, `MemoryHigh=1843M`). 단계 커밋 덕에 목록과 물건상세 part0001(1 만 건)은
Bronze 에 남았지만, 두 번째 part 와 사다리 단계는 남지 않았다. 20:32 에 잰 메모리는 2.08 GB 로 상한 바로 밑이었고, 그 시각은
물건상세 예산(14,200 초)이 끝나고 사다리 단계(사진 원천을 AV1 로 동시 4 개 인코딩)가 돌던 때다.

한 실행이 두 일을 한다: 사이트에 묻는 일(네트워크, 수백 MB)과 사진마다 표시 사다리를 만드는 일(디코드·인코딩, 원천 하나에
수백 MB). 둘이 한 단위의 메모리 상한과 시간 한도를 나눠 쓰니, 뒤의 일이 밀리면 앞의 일이 남긴 것까지 위험해진다. 사다리를
따로 만드는 작업(`media_ladder_backlog`, ADR-0429)은 이미 있다. 다만 "운영자가 한 번 돌려 재야 켠다"(ADR-0386 §3, ADR-0457 이전의 규칙)에 걸려 꺼져
있었다. 사장 규칙(2026-11-07): 사람 단계와 고정 시간창은 두지 않는다.

## Decision

1. **경매 수집 실행은 사다리를 만들지 않는다.** 물건상세는 실행 예산(`collection.session.run_time_budget_seconds`) 전부를 쓴다.
   계약의 `item_detail.photos.renditions` 와 실행 안의 `ensure_ladders` 호출을 없앤다.
2. **사다리는 `media_ladder_backlog` 가 매시 만든다.** Silver 사진 색인(`media.ladder_backlog.index_tables`)에서 사다리가 없는
   원천을 가져와, 자기 단위의 메모리 상한(3G)과 동시 수·예산(`media.ladder_backlog`)으로 만든다. 사다리는 하나씩 한 번 쓴다
   (`as_it_goes`). 한 실행이 남긴 것은 다음 시각이 가져간다.
3. **백로그의 첫 실행은 사람 없이, 자기 예산이 끝낸다.** ADR-0457 의 길(`per_run_cap.first_run`: 추정 초와 근거, `as_it_goes`,
   재시도 0)로 켠다. 추정은 계산이다: 색인 읽기 + 예산 1,500 초 + 그때 돌던 넷(약 4.5 초씩) ≈ 1,650 초, 40 분 한도의 90 %
   (2,160 초) 안. 첫 실행이 끝나면 journal 의 잰 값(`measured_run_seconds`)이 추정을 대신한다.

## Consequences

- 수집 실행의 메모리는 사이트 응답과 그 밤의 줄만 쥔다(사다리 디코드·인코딩이 빠진다). 물건상세는 1,800 초를 더 얻는다(초당 약 1 건이면
  한 밤 약 1,800 건 더).
- 사진은 사다리가 생기기 전까지 원본(저장소의 `sha256/…`)으로 보인다(화면은 사다리가 없으면 원본을 쓴다, ADR-0427). 첫 바퀴는
  ADR-0429 의 계산대로 며칠 걸린다.
- 다음 수집 실행에서 잰다: 메모리 최고·스왑·걸린 시간·물건상세 수, 그리고 frontier 가 part0001 의 1 만 건을 다시 묻지 않는지.

## Rejected

- **수집 실행 안 사다리의 동시 수를 줄이기:** 한 단위에 두 일이 남는다. 사다리가 밀리면 여전히 수집의 시간·메모리를 먹는다.
- **수집 단위의 메모리 상한을 더 올리기:** 원인(한 단위 두 일)을 두고 숫자만 키운다.
- **백로그를 운영자가 한 번 돌린 뒤 켜기:** 사람 단계다(사장 2026-11-07). 스스로 멈추는 작업은 ADR-0457 로 첫 실행을 혼자 한다.
