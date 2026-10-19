# ADR 0283: 법원경매 수집기의 User-Agent 는 연락처 없이 우리 이름만 밝힌다

- Status: Accepted
- Date: 2026-10-19
- Amends: [ADR-0281](./0281-court-auction-is-collected-openly-politely-and-stops-when-refused.md) §1 (연락처를 필수에서 선택으로)

## Context

ADR-0281 §1 은 모든 요청의 `User-Agent` 를 `perfectory-foundation-court-auction/<버전> (+<연락처>)` 로 정하고, 연락처(`FOUNDATION_PLATFORM_COURT_AUCTION_CONTACT`)가 비어 있으면 수집기가 시작하지 않게 했다.

사장 결정(2026-10-19): 수집기에 연락처 주소를 싣지 않는다.

신원을 밝히는 데 필요한 것은 우리 수집기라는 사실이다. 그것은 제품 토큰이 말한다. 연락 수단은 그 위에 얹는 선택이다. ADR-0281 의 나머지 규칙은 연락처와 무관하다.

## Decision

1. 모든 요청의 `User-Agent` 는 `perfectory-foundation-court-auction/<버전>` 이다. 우리 수집기임을 그대로 밝히고, 브라우저를 흉내 내지 않는다(가짜 `User-Agent`, `sec-ch-ua`, `Sec-Fetch-*`, `Origin`, `Referer` 를 보내지 않는다).
2. `FOUNDATION_PLATFORM_COURT_AUCTION_CONTACT` 는 선택이다. 값이 있으면 `(+<연락처>)` 로 덧붙인다. 값이 없거나 비어 있어도 실행을 막지 않는다. 값에 괄호나 줄바꿈이 있으면 거부한다(헤더 주입 방지).
3. ADR-0281 의 나머지는 그대로다. 연결 하나, 요청 사이 최소 3초, 새벽(01:00–06:00 KST)에만, 실행당 요청 상한, robots.txt 를 먼저 읽기, 거절 신호를 보면 즉시 멈추고 사람이 확인할 때까지 다시 시작하지 않기, 프록시·IP/세션 돌리기·위장·CAPTCHA 풀기 금지.

## Consequences

- 법원경매 수집 작업을 켜는 데 사장의 연락처 설정 단계가 필요 없다. 남은 단계는 새벽의 첫 감독 실행이다.
- 연락처 그룹(`/etc/foundation-platform/court-auction.env`)은 선택으로 남는다. 놓으면 User-Agent 에 붙는다.
