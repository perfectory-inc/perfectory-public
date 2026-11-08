# ADR 0446: 거절당한 수집기는 기다렸다 다시 묻고, 사람이 지워야 하는 파일은 어떤 실행도 막지 않는다

- Status: Accepted
- Date: 2026-11-08
- Supersedes: [ADR-0348](./0348-real-transactions-come-from-the-rt-molit-bulk-export-and-recent-months-are-pulled-again.md) §3 의
  `refused.json` 정지(거절 뒤 사람이 파일을 지울 때까지 모든 실행이 요청 없이 실패)
- Builds on: [ADR-0289](./0289-court-auction-is-collected-as-a-browser-at-an-adaptive-pace-and-never-gives-up.md)(막힘은 거절이
  아니다, 세션을 새로 열고 다시 묻는다), [ADR-0433](./0433-a-by-pnu-cut-over-and-a-full-bake-are-approved-in-the-console-and-the-host-runs-them-in-the-operator-window.md)(사람
  몫은 콘솔 한 번 누름), [ADR-0445](./0445-a-failed-host-unit-names-its-owner-and-runbook-and-a-unit-that-stays-failed-calls-its-owner.md)(실패한
  유닛은 주인을 부른다)

## Context

사장 규칙(2026-11-07): 정상 경로에 사람이 없다. 그날 실측한 사건:

| 원천 | 무엇이 막았나 | 결과 |
|---|---|---|
| 법원경매 (`court_auction_collect`) | 11-06 실행이 1,528 요청째 `ipcheck=false` 를 거절로 읽고 `collector/refused.json` 을 남김 | 11-07 16:05Z 실행이 요청 없이 실패("no run starts until a person ... removes refused.json"). #1155(ADR-0289 2단계 d, 11-08 00:26Z 배포)가 파일을 읽는 코드를 없앴다 |
| 실거래가 (`rt_molit_real_transaction_collect`) | 같은 설계(ADR-0348 §3): HTTP 403·CSV 자리의 HTML·재시도를 넘긴 바쁨이면 `collector/refused.json` | 아직 거절은 없었으나 한 번이면 매시간 실행이 사람이 지울 때까지 전부 실패. 11-07 22:25Z~11-08 05:25Z 매시간 `stopped_by=provider_daily_limit`(하루 한도는 이미 자동으로 다음 날 기다림) |

나무 전체를 찾았다(`git grep`: refused/blocked/halt/stop/ack 파일, "until a person/operator removes|clears", "사람이 지워", `exists()` 로 실행을 거부하는 곳).
남은 다른 후보는 사람 몫이 아니었다.

| 후보 | 판정 |
|---|---|
| `gold_incremental.py` `refused.txt` | 같은 계획(핀+코드) 디렉터리 안의 표지. 다시 돌면 곧바로 전량 재생성으로 넘어간다(ADR-0438). 자동 |
| `land_use_zone_area_tile_handoff.py` `<summary>.refused.json` | 거절 보고서. 다음 실행이 읽지 않는다 |
| `switches/<job>.stop` (`job_switch_board.rs`) | 더니어 작업 스위치(ADR-0282·0433): 콘솔 한 번 누름으로 켜고 끈다. 정상 경로가 아니라 운영자 선택 |
| `catalog.collection_job` `dead_lettered` | DB 상태다. 되살리는 CLI 는 호출자 0 으로 지워졌다(#1053). 파일 관문이 아니어서 이 결정 밖, 후속으로 남긴다 |

검토한 사례:

- 회로 차단기(Netflix Hystrix, resilience4j `CircuitBreaker`): 실패가 이어지면 열고, 정해진 대기 뒤 반열림(half-open)으로 한 번 시험해
  성공하면 닫는다. 사람이 닫지 않는다.
- AWS SDK·Google API 클라이언트의 지수 백오프와 상한: 거절·스로틀은 늘어나는 간격으로 다시 묻는다.
- Kubernetes `CrashLoopBackOff`: 실패한 컨테이너를 10 초에서 5 분까지 늘어나는 간격으로 다시 띄운다. 지울 표지 파일이 없다.
- Google SRE Book *Eliminating Toil*: 사람이 손으로 반복해 상태를 되돌리는 일은 자동화 대상이다.

## Decision

1. **거절은 기다림이다.** 실거래가 수집기는 거절(HTTP 403, JSON·CSV 자리의 페이지, 재시도를 넘긴 바쁨)을 받으면 그 실행을 실패로
   끝내고(ADR-0445 단위 실패 알림이 곧 호출), 상태(`collector/state.json` 의 `refusal`)에 연달아 몇 번째인지와 다시 물을 때를 적는다.
   n 번째 거절의 대기는 계약 `rt-molit-real-transaction.contract.json` `manners.refusal_waits_seconds[n]`(지금 1 시간·6 시간·하루, 마지막
   반복)이다. 대기 안의 실행은 요청 없이 `stopped_by=refusal_wait` 로 끝나고, 대기가 지난 첫 실행이 다시 묻는다(반열림). 답 하나를 받으면
   횟수는 0 으로 돌아간다. 하루 한 번 묻는 것은 제공자 한도(하루 100 건)의 1% 다.
2. **거절은 쌓인다.** 거절마다 `collector/refusals.jsonl` 에 한 줄(때·이유·연달아 몇 번째·다시 물을 때·그 실행의 보고)을 덧붙인다(append-only).
3. **옛 정지 파일은 유산이다.** 옛 수집기가 남긴 `collector/refused.json` 은 처음 만난 실행이 그 내용을 장부에 `legacy_file` 줄로 한 번
   옮겨 적고, 파일은 그대로 두며, 다시 읽지 않는다. 법원경매의 같은 파일은 #1155 뒤 아무것도 읽지 않는다(런북 court-auction.md).
4. **사람이 지워야 실행이 풀리는 파일은 새로 만들 수 없다.** Semgrep 규칙 `no-hand-cleared-gate-file`(`tools/semgrep/`)이
   "until a person removes|clears|deletes", "사람이 … 지워야" 같은 문구로 실행을 막는 코드·스크립트를 CI 에서 거부한다(후속 PR).
   정말 위험해 사람이 판단해야 하면 그 답은 콘솔 한 번 누름(ADR-0433)이지 손으로 지우는 파일이 아니다.

## Consequences

- 실거래가: 거절 한 번이 사람을 기다리던 정지에서 1 시간 뒤 자동 재시도로 바뀐다. 계속 거절하면 6 시간, 그 뒤 하루 한 번. 알림은 거절한
  실행마다 한 번이다(대기 안의 실행은 성공으로 끝나 알림 없음; 원천이 낡는 것은 ADR-0443 의 공급 주기 경보가 잡는다).
- ADR-0348 의 "우회는 하지 않는다" 는 그대로다. 같은 세션·같은 User-Agent 로 늘어나는 간격을 두고 다시 묻는 것은 우회가 아니다. 경로를
  바꾸는 결정(다른 채널·중단)은 여전히 사장 몫이고, 그 판단 재료가 거절 장부다.
- 대기 중에도 하루 한도 계산·계획은 그대로다. 대기 안의 실행 비용은 상태 파일 읽기뿐이다.
- 후속: `catalog.collection_job` 의 `dead_lettered` 되살리기는 콘솔 동작 또는 자동 재시도로 옮긴다(이 ADR 밖).
