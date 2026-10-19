# ADR 0281: 법원경매는 신원을 밝히고 천천히 모으며, 거절당하면 멈춘다

- Status: Accepted
- Date: 2026-10-19
- Relates to: [ADR-0280](./0280-public-sale-court-auction-and-factory-registration-are-foundation-parcel-sources.md) (소유), [GZ-ADR-0022](../../products/gongzzang/docs/adr/0022-bronze-scraping-isolated-python-service.md) (옛 설계, stealth 수집 — 이 ADR 이 그 방식을 금지한다)

## Context

사장 결정(2026-10-19): 법원경매정보(courtauction.go.kr)는 우리가 직접 모은다. 법적 위험은 사장이 받아들였다. 조건은 "정직하고 예의 바른 수집만"이다.

2026-10-19 사이트에서 직접 확인한 것:

| 항목 | 확인한 내용 |
|---|---|
| robots.txt | 없다(`/robots.txt` 가 404, 안내 화면). 막는 경로 선언이 없다 |
| 이용약관(`/pgj/pgj111/selectUtilClse.on`) | 회원(‘을’) 의무 제15조: 1호 "부정하게 접속하는 방법 등으로 서비스를 방해하거나 설비에 손상을 주는 행위" 금지, 2호 "부정한 목적으로 또는 ‘갑’의 동의 없이 서비스 내용을 가공하거나 입찰참가 외의 영리목적으로 이용" 금지. 자동 수집을 직접 언급하는 조항은 없다 |
| 저작권 보호정책 | 법원이 저작재산권 전부를 가진 저작물은 저작권법 제24조의2(공공저작물 자유이용)에 따라 출처를 밝히면 자유이용. 협의가 필요하면 사용자지원센터에 문의 |
| 화면 구조 | WebSquare. 화면은 `/pgj/ui/**.xml`, 데이터는 `/pgj/**.on` 에 JSON 을 POST 해서 받는다. 비회원 조회는 로그인 없이 된다 |

약관 2호의 "동의 없이 가공·영리 이용" 은 사장이 받아들인 법적 위험이다. 1호(서비스 방해·설비 손상)는 우리 수집 방식이 절대 넘지 않아야 할 선이다. 옛 설계(GZ-ADR-0022)는 Scrapling stealth 와 "anti-bot 강함" 을 전제로 했고, 실제 수집기는 이 저장소에 들어온 적이 없다.

## Decision

1. **신원을 밝힌다.** 모든 요청의 `User-Agent` 는 `perfectory-foundation-court-auction/<버전> (+<연락처>)` 하나다. 연락처는 실행 설정 `FOUNDATION_PLATFORM_COURT_AUCTION_CONTACT` 에서 읽고, 비어 있으면 수집기는 시작하지 않는다. 브라우저를 흉내 내는 헤더(가짜 `User-Agent`, `sec-ch-ua` 등)를 보내지 않는다.
2. **천천히, 한 줄로.** 연결 하나, 요청 사이 최소 3초(계약 값), 실행 하나에 요청 상한(계약 값). 새벽(01:00–06:00 KST)에만 돈다. 재시도는 네트워크 오류에만, 같은 요청에 최대 2번, 지수 대기.
3. **거절당하면 멈춘다.** HTTP 403·429·503, `Retry-After`, 로그인·CAPTCHA·차단 안내 화면, JSON 이 와야 할 자리에 HTML 이 오는 것 중 하나라도 보면 그 실행을 즉시 끝내고 `refused` 로 기록한다. 같은 날 다시 시도하지 않는다. 사람이 원인을 확인하고 상태 파일을 지울 때까지 다음 실행도 시작하지 않는다.
4. **피하지 않는다.** 프록시, IP·세션·`User-Agent` 돌리기, stealth·헤드리스 지문 위장, CAPTCHA 풀기, 차단 우회를 쓰지 않는다. Scrapling 은 HTML/JSON 파싱과 평범한 요청에만 쓰고 stealth fetcher(`StealthyFetcher`, `DynamicSession` 의 위장 옵션)는 쓰지 않는다. 검사기가 수집기 코드에서 이 이름들을 거부한다.
5. **robots.txt 를 매 실행 처음에 읽는다.** 지금은 없지만 생기면 그 규칙을 따른다(막힌 경로는 수집하지 않고, `Crawl-delay` 가 3초보다 길면 그 값을 쓴다).
6. **출처를 밝힌다.** Silver 행은 원천(`courtauctiongokr__court_auction`)과 Bronze 객체 키를 가진다. 화면에 내보낼 때 출처 표시는 소비자(Gongzzang)의 몫이지만, 데이터 계약(ODCS)에 저작권 보호정책의 출처 표시 의무를 적는다.
7. 승인 제공자 목록(`APPROVED_PROVIDER_DOMAINS`)에 `courtauction.go.kr` 을 더한다(사장 승인 2026-10-19).

## Consequences

- 받는 것은 목록 두 개뿐이다: 물건 검색(`/pgj/pgjsearch/searchControllerMain.on`)과 매각결과(`/pgj/pgjsearch/selectDspslSchdRsltSrch.on`). 사건·법원·물건번호·감정가·최저매각가격·매각기일·상태·매각가·대표지번이 이 목록에 있다. 하룻밤 약 1,200–2,000 요청이다. 옛 수집기는 사건마다 상세 6종을 불러 하루 약 185만 요청을 64개 동시 연결과 프록시로 보냈다. 이 규칙 아래에서는 불가능하고, 필요도 없다.
- 물건 상세(`/pgj/pgj15B/selectAuctnCsSrchRslt.on`)는 사진을 base64 로 담아 건당 10–20MB 다. 받지 않는다. 필요해지면 받을 칸과 저장 비용을 따로 결정한다.
- 사이트가 막으면 경매 탭 값은 마지막으로 받은 날짜에 멈춘다. 그때는 보고하고 사람이 결정한다(사용자지원센터 문의 등). 우회 코드는 쓰지 않는다.
- 옛 Kotlin 수집기(저장소 밖)에서 엔드포인트·요청 모양·파싱·표 모양만 옮겨 온다. 프록시·세션 돌리기·브라우저 위장 부분은 옮기지 않는다.

---

개정 각주(2026-10-19): §1 의 연락처 필수 규칙은 [ADR-0283](./0283-the-court-auction-user-agent-names-us-without-a-contact-address.md) 이 고쳤다. User-Agent 는 제품 이름만 필수이고, 연락처는 선택이다.
