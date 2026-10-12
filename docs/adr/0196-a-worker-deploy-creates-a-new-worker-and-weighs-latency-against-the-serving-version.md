# ADR 0196: Worker 자동 배포는 없는 Worker 를 만들고, 지연은 서빙 중인 버전과 견준다

- Status: Accepted
- Date: 2026-10-12
- Amends: [ADR-0175](./0175-workers-deploy-themselves-after-the-host-deploys-main.md) 결정의 배포 단계(지금 100% 인 버전
  하나를 읽는다)와 스모크(레인의 서빙 감시를 그 호스트로 돌린다)
- Builds on: [ADR-0154](./0154-building-pack-read-path-is-placed-beside-the-bucket.md)(차가운 비교의 차이는 대부분 R2
  온도), [ADR-0190](./0190-unit-prices-are-their-own-gold-panel-and-by-pnu-lane.md)(단가 레인)

## Context

2026-10-12 릴리스(ADR-0190 의 단가 Worker 를 더한 커밋)에서 자동 배포가 Worker 셋을 세 번씩 시도하고 포기했다.

1. **단가 Worker**: 계정에 아직 없는 Worker 였다. 배포기는 지금 서빙 중인 버전을 `deployments status` 로 먼저 읽는데,
   Cloudflare 는 `This Worker does not exist on your account. [code: 10007]` 로 답했다. `versions upload` 는 있는 스크립트에
   버전을 더할 뿐 스크립트와 그 커스텀 도메인을 만들지 못한다. 두 호스트명은 521 이었다.
2. **필지·건물 Worker**: 미리보기 단계의 서빙 감시가 p95 한도(3,000ms)를 넘었다(필지 9,559·3,324·5,013ms, 건물 3,358·3,580·
   6,255ms). 실패 0, 문서 불일치 0. 같은 시각 **바뀌지 않은 운영 호스트명**의 시간별 감시도 넘고 있었다(08 UTC 부터 필지
   p95 3,680–5,225ms, 건물 3,330–9,860ms; 전날은 p50 약 270ms·p95 300–2,000ms). 원인은 새 코드가 아니라 길이었다: 이 존
   (Free 플랜)의 한국 방문은 그 시각 Cloudflare SEA 로 들어갔고(`/cdn-cgi/trace` 의 `colo=SEA`, 같은 회선의 cloudflare.com 은
   ICN), 손대지 않은 타일 호스트명까지 같은 만큼 느렸다. 절대 한도 하나로는 새 버전이 만든 느림과 길이 만든 느림을 가를 수
   없어서, 배포가 무관한 일로 막혔다.

## Decision

1. **계약에 있는 Worker 가 계정에 없으면 만든다.** `deployments status` 가 code 10007 로 답하면 그 대상은 "없음"이다. 그
   대상 차례에(미리보기 먼저, 운영은 미리보기가 스모크를 통과한 뒤에만) Wrangler 설정 그대로 `wrangler deploy` 를 한 번
   돌려 스크립트·routes·커스텀 도메인을 만들고, 그렇게 서빙된 버전을 "옛 버전"으로 삼아 평소 흐름(upload → 변수·비밀 비교
   → 100% 이동 → triggers → 스모크, 실패하면 그 버전으로 되돌림)을 그대로 탄다. 다른 오류는 전처럼 실패다. 만드는 이름은
   ADR-0175 의 규칙(설정의 이름 = 게이트웨이 블록의 이름) 안에서만 나온다. 첫 버전에는 비밀이 없으니 비밀이 필요한
   Worker 는 이 길로 처음 올리지 않는다.
2. **지연만 넘으면 서빙 중인 버전과 같은 표본·같은 온도로 견준다.** 감시 보고서(`MONITOR_REPORT_PATH`)를 읽어:
   - 실패·무응답·불일치가 하나라도 있으면 그대로 막는다(비교하지 않는다).
   - p95 만 넘었으면 같은 표본을 비교 호스트명에서 읽고, 이어서 이 호스트명에서 다시 읽는다(둘 다 따뜻). 비교 호스트명은
     미리보기 단계에서는 운영(아직 옛 버전), 운영 단계에서는 미리보기(새 버전이 이미 옛 버전과 견줘 통과). 다시 읽은
     p95 가 `max(한도, 계약의 monitor_latency.ratio_to_compared_hostname(1.5) × 비교 p95)` 를 넘으면 막는다 — 진짜
     퇴행은 여전히 막힌다. 아니면 통과시키고 세 수치를 저널에 남긴다.
   - 보고서를 못 쓰고 끝난 감시(입력·manifest 를 못 읽음)는 실패다.
3. 시간별 감시(`foundation-by-pnu-serving-monitor@`)의 절대 한도와 Slack 경보는 그대로다. 사용자가 겪는 느림은 거기서
   계속 보인다. 이 결정은 배포 관문만 바꾼다.

시험: `platforms/foundation-platform/orchestration/tests/test_worker_autodeploy.py` — 없는 Worker 를 미리보기 통과 뒤에만
운영에 만듦, 미리보기가 실패한 새 Worker 는 운영에 생기지 않음, 길이 느린 호스트명은 막지 않음, 서빙 버전보다 느린 버전은
막음, 보고서 없는 감시는 실패. 각 시험은 해당 코드를 깨뜨려 실패하는 것을 확인했다.

## Consequences

- 새 by-PNU 레인(ADR-0190 처럼)은 손 배포 없이 병합만으로 Worker 와 호스트명이 생긴다.
- 지연이 넘을 때 배포 하나가 감시를 두 번 더 돈다(표본 20 PNU, 수십 초).
- 비교는 20개 표본의 p95 라 잡음이 크다. 길이 매우 나쁠 때는 비교도 넘을 수 있고, 그때는 ADR-0175 의 재시도(3번)와 다음
  병합이 남는다.
- 한국 방문이 SEA 로 가는 것은 이 결정이 고치지 않는다. Free 플랜 존의 입구 colo 는 Cloudflare 가 정하고, 바꾸려면 유료
  플랜 결정이 필요하다(별도 판단).
