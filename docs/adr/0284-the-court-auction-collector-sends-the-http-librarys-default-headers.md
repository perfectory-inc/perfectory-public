# ADR 0284: 법원경매 수집기는 HTTP 라이브러리의 기본 머리글만 보낸다

- Status: Accepted
- Date: 2026-10-20
- Supersedes: [ADR-0283](./0283-the-court-auction-user-agent-names-us-without-a-contact-address.md)
- Amends: [ADR-0281](./0281-court-auction-is-collected-openly-politely-and-stops-when-refused.md) §1 (신원 표시)

## Context

ADR-0281 §1 은 모든 요청의 `User-Agent` 에 우리 제품 이름을 싣게 했다. ADR-0283 은 연락처를 선택으로 바꿨다.

사장 결정(2026-10-20): 요청에 우리를 밝히지 않는다. 수집기 `User-Agent` 에서 perfectory 이름을 빼고, HTTP 라이브러리의 기본값을 보낸다. 연락처 설정은 없앤다. 이것은 다른 무엇으로 행세해도 된다는 허락이 아니다.

수집기의 HTTP 라이브러리(`reqwest`)는 기본으로 `User-Agent` 머리글을 보내지 않는다. 그래서 요청에는 이름 없는 프로그램의 요청이 그대로 드러난다.

## Decision

1. 수집기는 `User-Agent` 를 정하지 않는다. 요청 머리글은 라이브러리 기본값 그대로이고, 제품 이름도 연락처도 싣지 않는다. 연락처 설정(`FOUNDATION_PLATFORM_COURT_AUCTION_CONTACT`)과 그 환경 파일 그룹을 없앤다.
2. 행세하지 않는다. 브라우저 `User-Agent`, `sec-ch-ua`, `Sec-Fetch-*`, `Origin`, `Referer`, 쿠키를 보내지 않는다. `User-Agent` 를 돌리거나 고르는 코드도 없다. 검사기가 수집기 코드에서 `.user_agent(`, `USER_AGENT`, `Mozilla/`, `fake_user_agent`, `rand::` 같은 이름을 거부하고, 시험이 실제 요청 머리글에 브라우저 이름이나 우리 이름이 없음을 확인한다.
3. robots.txt 는 `*` 그룹의 규칙을 따른다. 우리 이름을 보내지 않으므로 우리를 지목하는 그룹은 없다.
4. ADR-0281 의 나머지는 그대로다. 연결 하나, 요청 사이 최소 3초, 새벽(01:00–06:00 KST)에만, 실행당 요청 상한, robots.txt 먼저, 거절 신호를 보면 즉시 멈추고 사람이 확인할 때까지 다시 시작하지 않기, 프록시·IP/세션 돌리기·위장·CAPTCHA 풀기 금지.

## Consequences

- 사이트 운영자는 요청이 우리 것임을 머리글로 알 수 없다. 대신 이름 없는 프로그램의 요청임은 그대로 보인다. 거절당하면 멈추는 규칙이 그 대가를 진다.
- `court-auction` 환경 파일 그룹과 수집 단위의 `EnvironmentFile` 줄이 없어진다.
- ADR-0283 은 이 ADR 로 대체된다.
