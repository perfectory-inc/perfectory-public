---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-19
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

1. 새벽(01:00–06:00 KST)에 단위를 직접 한 번 시작해 지켜본다:
   `sudo systemctl start foundation-court-auction-collect.service`. 요청 수·소요 시간·거절 없음을 확인한다.
   User-Agent 는 `perfectory-foundation-court-auction/<버전>` 이다. 연락처는 선택이다(ADR-0283):
   `/etc/foundation-platform/court-auction.env` 에 `FOUNDATION_PLATFORM_COURT_AUCTION_CONTACT` 가 있으면 `(+연락처)` 로 붙는다.
2. 적재 단위를 한 번 시작하고 요약 줄(`court-auction-summary-json`)의 `with_pnu` 를 본다.
3. 첫 밤의 Bronze 크기를 perfectory-ops 비용 추정에 덧붙인다.
4. 두 작업의 `enabled` 를 켜는 PR.

## 출처 표시

이 데이터를 보여 주는 소비자는 출처(대한민국법원 법원경매정보)를 표시한다(저작권 보호정책; ADR-0281 §6, ODCS 목적 칸).
