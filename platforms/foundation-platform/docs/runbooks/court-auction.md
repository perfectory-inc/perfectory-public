---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-08
---

# 법원경매정보 (courtauction.go.kr) — 수집·적재 런북

루트 ADR-0280(소유)과 ADR-0289(브라우저로 묻고, 스스로 속도를 조절하고, 막혀도 끝까지 모은다)의 운영 절차다.
값(엔드포인트·헤더·세션 풀·막힘 신호·대기 단계·속도 조절·시간 예산)은 `infra/lakehouse/contracts/courtauction-go-kr.contract.json`
하나에 있다. 이 문서는 값을 다시 적지 않는다.

## 흐름

| 작업 | 단위 | 하는 일 |
|---|---|---|
| `court_auction_collect` (작업 등록부 일정, default_pool) | `foundation-court-auction-collect.service` → `scripts/ops/court-auction-collect.sh` | `collect-courtauction-go-kr`: 물건 검색 네 조합 → 매각결과 검색 두 조합. 조합마다 첫 쪽으로 쪽 수를 알고, 나머지 쪽은 계약 `collection.pace` 의 폭만큼 동시에. 검색 하나에 gzip JSON Lines Bronze 객체 하나 |
| `court_auction_silver` (spark 3칸) | `foundation-court-auction-silver-load.service` → `scripts/ops/court-auction-silver-load.sh` | `plan-silver-load-objects` 가 R2 만 보고 검색마다 표의 커밋 요약이 아직 기록하지 않은 객체를 받고(계약 `silver_load`), `courtauction_go_kr.py load`: 주소로 PNU, `silver.court_auction_item`·`silver.court_auction_sale_result` 에 바뀐 행만 |

상태: `/var/lib/foundation-platform/court-auction/`(`journal.log`, 실행마다 `runs/<실행>/run.log`). 다음 실행을 막는 상태 파일은 없다.

## 막혔을 때 (사람 할 일 없음)

사이트는 막힘을 HTTP 200 + `data.ipcheck=false`, 리다이렉트, JSON 자리의 HTML 로 답한다(계약 `collection.block_signals`).
막힘의 단위는 쿠키다. 수집기는 막힘을 **거절로 읽지 않는다**: 계약 `collection.session.block_waits_seconds` 만큼 기다린 뒤
막힌 세션을 버리고 첫 화면으로 새 세션(새 쿠키)을 열어 같은 쪽을 다시 묻는다. 횟수 상한은 없다. 막힐 때마다 그 묶음의 동시
요청 폭이 줄어든다(AIMD). 프록시·IP 돌리기·CAPTCHA 풀기는 하지 않는다(ADR-0289 §6).

- 실행의 끝은 계약의 시간 예산(`run_time_budget_seconds`)뿐이다. 다 못 읽은 검색은 불완전 전수로 남고(삭제 표지 없음), 다음 실행이 처음부터 다시 읽는다.
- 막힘이 아닌 실패(네트워크·5xx·JSON 깨짐)는 계약 횟수만큼 다시 묻고, 그래도 안 되면 그 쪽을 Bronze 행의 `request_params.night.failed_pages` 에 남긴다(append-only 실패 장부, 루트 ADR-0352 §2). 조용히 버리지 않는다.
- **진행 없음 알림:** 한 실행이 답을 하나도 못 받으면(시간 예산 내내 막힘) 그 실행만 실패로 끝난다. 그 실패가 단위 실패 알림과 준비도 보고(ADR-0442)에 뜬다. 알림은 알리기만 한다. 다음 실행은 일정대로 다시 묻는다.
- 실행마다 무엇이 있었는지는 `manifest.json` 과 Bronze 행의 `request_params.night` 에 있다: `asking`(요청·답·막힘·세션 갱신·재시도·실패·미완 수)과 `sessions`(세션마다 연 시각·요청 수·막힌 시각).
- 운영자가 시간대를 좁히고 싶으면 더니어의 작업 창 스위치(루트 ADR-0282, `switches/court_auction_collect.window`)를 쓴다. 기본은 하루 어느 때나다.

옛 방식(ADR-0281: 거절 시 정지, `refused.json`, `job clear-refusal`, 새벽 창과 `job start-outside-quiet-hours`)은 없앴다.
2026-11-06 실행이 1,528 요청째 `ipcheck=false` 로 멈추고 11-07 실행이 그 파일 때문에 시작조차 못 한 것이 마지막이다.
배포 뒤 서버에 남은 `collector/refused.json` 은 아무것도 읽지 않는 파일이다.

## 실행 한 번의 양 (참고)

물건 검색 ±365일 창 약 108,306행(2026-10-21) ÷ 50 = 약 2,167쪽, 매각결과 약 251쪽 = 약 2,420 요청. 시제품(2026-10-21)은 목록
전체를 약 8분에 받았다. Bronze 는 요청당 약 9.9KB 라 한 실행 약 24MB, 한 해 약 8.8GB(append-only), R2 저장 월 약 $0.13.
사이트 호출 비용은 없다.

## 필지 패널에 처음 띄우기 (ADR-0353, ADR-0382)

명령은 사장 터미널(PowerShell·cmd)에 그대로 붙인다. `$` 도 작은따옴표 바깥 묶음도 없다: 바깥은 큰따옴표, 시각은 서버의
`date +%s` 가 `xargs -o` 로 넘긴다(`-o` 는 `sudo` 에 터미널을 되돌려 준다).

1. #969 병합·배포 뒤 첫 밤 전, 거절 파일을 지운다(위 "거절당했을 때" 3번과 같은 길, 지울 것이 없으면 66):

   ```
   ssh -tt ai-server "date +%s | xargs -o -I{} sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh job clear-refusal court_auction_collect --requested-at {} --reason 'the 2026-10-20 refusal was a daytime attended run; the 2026-10-19 night passed; owner approved collection 2026-10-30'"
   ```

2. 다음 날 06:30 KST 적재 뒤 요약 줄(`court-auction-summary-json`)의 `with_pnu` 를 본다. 코드 길(ADR-0382)은
   `pnu_basis` 가 `legal_dong_code`·`legal_dong_code_superseded` 인 행이다.
3. 07:05 KST(22:05 UTC) `gold_court_auction_rebuild` 첫 커밋 뒤, 창(04:30–21:50 UTC) 밖에서 레인을 처음 전환한다.
   먼저 `--dry-run`, 그다음 진짜:

   ```
   ssh -tt ai-server "date +%s | xargs -o -I{} sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh parcel-court-auction cutover 1 --dry-run --requested-at {} --reason 'first cut-over of the court auction lane, dry run (ADR-0353)'"
   ssh -tt ai-server "date +%s | xargs -o -I{} sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh parcel-court-auction cutover 1 --requested-at {} --reason 'first cut-over of the court auction lane (ADR-0353)'"
   ```

   그 전까지 `by_pnu_court_auction_bake` 는 `nothing to do` 로 끝난다. 그 뒤로는 Gold 커밋마다 이 레인만 굽는다.

## 출처 표시

원천의 출처 표시 의무(대한민국법원 법원경매정보, 저작권 보호정책)는 ODCS 목적 칸과 Silver 행의 원천·Bronze 키에
남는다(ADR-0281 §6). 공짱 화면은 출처를 보이지 않는다(루트 ADR-0428).

## 법정동 코드 변경 따라가기

적재가 끝나면 같은 실행이 `reference.legal_dong_code_change` 를 본다(루트 ADR-0329 §3, ADR-0416). 표의 커밋 기록
`follow:reference.legal_dong_code_change@<스냅숏>@<UTC>` 이후 변경 표가 움직였으면, `views.changed_codes` 가 이름 대는 코드
아래의 현재 행과 PNU 가 없는 행만 다시 잇고, 연결이 바뀐 행만 덧붙인다. 따라가기 기록이 아직 없는 표는 처음 한 번 현재
행 전부를 다시 잇는다. 요약 줄의 `follow`/`follows` 가 `checked`·`appended` 를 센다.
