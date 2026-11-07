---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-06
---

# 법원경매정보 (courtauction.go.kr) — 정직한 수집·적재 런북

루트 ADR-0280(소유)과 ADR-0281(신원을 밝히고 천천히 모으며, 거절당하면 멈춘다)의 운영 절차다. 예의의 값(간격·시간대·
요청 상한·거절 신호)은 `infra/lakehouse/contracts/courtauction-go-kr.contract.json` 하나에 있다.

## 흐름

| 작업 | 단위 | 하는 일 |
|---|---|---|
| `court_auction_collect` (01:05 KST, default_pool) | `foundation-court-auction-collect.service` → `scripts/ops/court-auction-collect.sh` | `collect-courtauction-go-kr`: robots.txt → 물건 검색 네 조합 → 매각결과 검색 두 조합, 쪽마다 한 요청. 06:00 KST 이나 요청 상한에 닿으면 그 자리에서 끝난다(불완전 전수, 거절 아님). 검색 하나에 gzip JSON Lines Bronze 객체 하나 |
| `court_auction_silver` (06:30 KST, spark 3칸) | `foundation-court-auction-silver-load.service` → `scripts/ops/court-auction-silver-load.sh` | `plan-silver-load-objects` 가 R2 만 보고 검색마다 표의 커밋 요약이 아직 기록하지 않은 객체를 받고(계약 `silver_load`: 최근 7일, 표마다 7개), `courtauction_go_kr.py load`: 주소로 PNU(지번 → 도로명, `address_pnu.py`), `silver.court_auction_item`·`silver.court_auction_sale_result` 에 바뀐 행만 |

상태: `/var/lib/foundation-platform/court-auction/`(`collector/refused.json`, `journal.log`, 실행마다 `runs/<실행>/run.log`).
대기열은 없다: 거절당한 밤이 받은 것도 Bronze 에 있으니 다음 적재의 계획이 찾는다.

## 거절당했을 때 (멈춤)

수집기는 403·429·503, `Retry-After`, JSON 자리의 HTML(로그인·안내·차단·CAPTCHA 화면), `ipcheck=false` 중 하나를 보면
즉시 멈추고, 그때까지 받은 것을 Bronze 에 남기고, `collector/refused.json` 에 이유를 쓰고, 실패로 끝난다(Airflow 실패
알림). **그 파일이 있는 동안 다음 실행은 시작하지 않는다.** 우회하지 않는다(프록시·IP/세션 돌리기·위장·CAPTCHA 풀기 없음).

1. `journal.log` 와 `refused.json` 의 이유를 사장에게 보고한다.
2. 원인을 확인한다. 우리 요청이 문제였으면(간격·시간대) 계약 값을 느리게 바꾸는 PR. 사이트가 자동 수집을 막기로 했으면
   사장이 결정한다(법원행정처 사용자지원센터 문의 등).
3. 사람이 결정한 뒤에만 `refused.json` 을 지운다. 운영자 경로로 지우고, 지운 이유와 지운 사람은 저널에 남는다:
   `ssh -tt ai-server 'sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh job clear-refusal court_auction_collect --requested-at "$(date +%s)" --reason "<확인한 원인과 결정>"'`.
   수집이 돌고 있으면 거부하고, 지울 거절이 없으면 66 으로 끝난다. 기록은 `journalctl -t foundation-operator-start` 에서 읽는다.

## 처음 켜기

1. 새벽(01:00–06:00 KST)에 한 번 시작해 지켜본다: 더니어 데이터 흐름 → 예약 작업 → 이 작업 줄의 운영 → "지금 한 번 돌리기",
   이유를 적고 누른다(루트 ADR-0282; 꺼진 작업도 된다). 그때(2026-10-19)는 운영자 스크립트의 `job start` 로 했고, 그 명령은 은퇴했다.
   요청 수·소요 시간·거절 없음을 확인한다. 2026-10-20 01:02 KST(2026-10-19 16:02 UTC) 첫 밤: 1,377요청(물건 1,125 · 매각결과 251 · robots 1),
   1시간 18분, 거절 없음, Bronze 13.6MB. 적재: 물건 43,821행 중 PNU 86.5%, 매각결과 10,120행 중 97.0%.
   User-Agent 는 `perfectory-foundation-court-auction/<버전>` 이다. 연락처는 선택이다(ADR-0283):
   `/etc/foundation-platform/court-auction.env` 에 `FOUNDATION_PLATFORM_COURT_AUCTION_CONTACT` 가 있으면 `(+연락처)` 로 붙는다.
2. 적재 단위를 한 번 시작하고 요약 줄(`court-auction-summary-json`)의 `with_pnu` 를 본다.
3. 첫 밤의 Bronze 크기를 perfectory-ops 비용 추정에 덧붙인다.
4. 두 작업의 `enabled` 를 켜는 PR(2026-10-30, 사장 승인 "모든 데이터 수집을 승인").

## 켠 뒤 첫 밤 전에 (2026-10-30)

- 2026-10-20 09:42 KST 의 낮 실행(감독 면제)이 174 요청째 `ipcheck=false` 로 거절당해 `collector/refused.json` 이 남아 있다.
  이 파일이 있는 동안 켜진 작업도 매 밤 시작하지 않는다. 낮 실행이 원인이었고 밤 실행(10-19)은 거절 없이 끝났으므로,
  위 "거절당했을 때" 3번의 운영자 경로로 지운다. 지우는 것은 사람이다(수집기는 스스로 지우지 않는다).
- 한 밤의 양(계산): 물건 검색 ±365일 창 약 108,306행(2026-10-21) ÷ 50 = 약 2,167쪽, 매각결과 약 251쪽, robots 1 =
  약 2,420 요청(계약 상한 3,500), 요청당 3.4초(실측)로 약 2시간 17분. Bronze 는 요청당 약 9.9KB(13.6MB ÷ 1,377)라 한 밤 약
  24MB, 한 달 약 0.7GB, 한 해 약 8.8GB(append-only). R2 저장 $0.015/GB-월로 한 해 쌓인 뒤에도 월 약 $0.13(약 190원).
  수집은 사이트 무료 공개 목록이라 호출 비용이 없다.

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
