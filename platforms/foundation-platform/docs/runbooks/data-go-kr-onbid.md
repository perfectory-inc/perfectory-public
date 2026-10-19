---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-19
---

# 온비드 공매 (data.go.kr) — 수집·적재 런북

루트 ADR-0280(공매·법원경매·등록공장은 Foundation 이 PNU 로 묶는 공공 원천) 의 운영 절차다.
엔드포인트·호출 상한·주기는 `infra/lakehouse/contracts/data-go-kr-onbid.contract.json` 하나에 있다. 이 문서는 값을 다시 적지 않는다.

## 흐름

| 작업 | 단위 | 하는 일 |
|---|---|---|
| `onbid_public_sale_collect` (매시 7분, default_pool) | `foundation-onbid-collect.service` → `scripts/ops/onbid-collect.sh` | `collect-data-go-kr-onbid`: 공고·공고 입찰결과(매시간), 물건 전수(계약의 간격마다), 목록에서 빠진 회차의 입찰결과. API 하나에 gzip JSON Lines Bronze 객체 하나. 적재로 넘기는 것은 없다 |
| `onbid_public_sale_silver` (매시 50분, spark 1칸) | `foundation-onbid-silver-load.service` → `scripts/ops/onbid-silver-load.sh` | `plan-silver-load-objects` 가 R2 만 보고 표마다 커밋 요약이 아직 기록하지 않은 Bronze 객체를 받고(계약 `silver_load`: 최근 7일, 표마다 24개), `data_go_kr_onbid.py load` 가 객체마다 커밋 하나로 `silver.onbid_real_estate_item`·`silver.onbid_item_bid_result` 에 쌓는다. 쌓을 것이 없으면 Spark 없이 끝난다. 받은 사본은 끝나면 지운다 |

상태는 `/var/lib/foundation-platform/onbid/` 에 있다: `collector/state.json`(KST 날짜별 API 호출 수, 마지막 완전 전수 시각, 그 전수의 열린 회차, 결과를 기다리는 회차), `journal.log`. 대기열은 없다: 적재가 실패한 객체는 표에 기록이 없으니 다음 계획이 다시 받는다.

## 처음 켜기 (사장 단계 하나)

1. 키 파일을 놓는다. 값은 data.go.kr 마이페이지의 일반 인증키다. 2026-10-19 에 이 키로 네 API 가 모두 응답하는 것을 확인했다.
   값은 명령줄에 쓰지 않는다. 한 줄 파일(`DATA_GO_KR_SERVICE_KEY=...`)을 편집기로 만들어 넘기고 지운다.

   ```bash
   sudoedit /root/data-go-kr.lines
   sudo python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py set data-go-kr /root/data-go-kr.lines
   sudo shred -u /root/data-go-kr.lines
   ```

2. 감독 아래 한 번 돌린다. 꺼진 작업은 `start-scheduled-job.sh` 가 거부하므로 단위를 직접 시작한다:
   `sudo systemctl start foundation-onbid-collect.service` 뒤 `journalctl -u foundation-onbid-collect.service`.
   첫 전수는 약 95호출, 수 분이다. 물건 전수가 3시간마다인 것은 호출 한도가 아니라 Bronze 저장 비용 때문이다(계약 `item_list` 설명).
3. `foundation-onbid-silver-load.service` 를 한 번 시작하고 요약 줄(`data-go-kr-onbid-summary-json`)의 `item_rows`·`items_with_pnu` 를 본다.
4. 첫 적재의 Bronze 바이트를 재서 perfectory-ops 의 비용 추정(`runs/2026-10-19-parcel-sources-cost.md`)에 덧붙인다. 추정의 2배를 넘으면 켜지 않는다.
5. 두 작업의 `enabled` 를 켜는 PR(ADR-0122 §4).

## 읽는 법

- **API 가 멈췄다** (`report.stopped_by_budget`): 그 API 의 하루 상한(계약 `daily_call_limit_per_api`, 게이트웨이 한도 100,000 의 1/5)을 다 썼거나 게이트웨이가 429(쿼터)로 답했다. 실패가 아니다. 다음 KST 날에 이어진다. 같은 키를 쓰는 다른 곳이 있으므로 상한을 올릴 때는 응답 헤더 `X-RateLimit-Remaining` 을 먼저 본다.
- **실행이 실패했다**: 00·03 이 아닌 답(키 미등록, 필수 파라미터, 서버 오류). 메시지에 resultCode 가 있다. 키는 로그·Bronze 어디에도 쓰지 않는다.
- **결과를 못 받은 회차** (`state.results_given_up`): 목록에서 빠졌는데 계약의 횟수만큼 물어도 결과가 없던 회차. 목록이 쪽을 넘기는 사이 밀려 잠깐 안 보였던 회차일 수 있다. 다음 전수에 다시 보이면 Silver 에 새 행으로 다시 들어온다.
