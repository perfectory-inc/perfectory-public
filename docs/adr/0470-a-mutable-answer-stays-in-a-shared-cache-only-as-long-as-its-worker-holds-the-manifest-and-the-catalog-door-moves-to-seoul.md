# ADR 0470: 바뀌는 답은 공유 캐시에 Worker 가 매니페스트를 쥐는 만큼만 머문다 — 그다음 카탈로그 문을 서울 CloudFront 로

- Status: Accepted
- Date: 2026-11-10
- Builds on: [ADR-0461](./0461-korean-visitors-enter-through-a-cloudfront-front-door-in-seoul-in-a-separate-aws-account-applied-by-the-host.md)(서울 앞문, 캐시는 원점의 `Cache-Control` 이 정한다),
  [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md)(by-PNU 문서 `public, max-age=3600`),
  [ADR-0323](./0323-parcel-tiles-patch-only-the-tiles-changed-parcels-touch.md)(패치 타일 `public, max-age=3600`),
  [ADR-0111](./0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md)(타일 Worker 의 응답 헤더)

## Context

ADR-0461 의 앞문은 `tiles`·`web_preview` 가 2026-11-09 CloudFront ICN 으로 전환됐고 `catalog` 는 보류됐다. 보류 이유: **같은 주소의 답이
바뀌는** URL 이 `public, max-age=3600` 이다(필지·건물·단가 by-PNU 문서, 패치 대상 unit 의 타일). Cloudflare 의 엣지는 Worker 의 답을 저장하지
않으므로 그 한 시간은 지금까지 브라우저의 몫이었다. Worker 자신은 새 발행을 매니페스트로 안다 — by-PNU 는 `manifest_edge_cache_seconds`
(60 s) 마다, 타일은 `patches.manifest_cache_seconds`(60 s) 마다 다시 읽고, 답의 캐시 키에 서빙 상태의 지문(패치 세대)을 넣는다. 그래서
Cloudflare 경로의 낡음 상한은 60 s 다. CloudFront 는 공유 캐시라 `max-age=3600` 을 자기 것으로 읽는다: 문 뒤에서는 발행 뒤 한 시간 동안
옛 문서를 낸다. 매시 서빙 모니터·컷오버 verify·방문자 모두 그 한 시간을 본다.

같은 날 조정자가 잰 "CloudFront 경유 타일에 압축이 없다"(zoning z10, 539,454 B, `Content-Encoding` 없음, ICN57)도 다시 쟀다(2026-11-09
18:4xZ, 노트북 KT):

| `zoning-01a1bb36…/10/873/398` 요청의 `Accept-Encoding` | 크기 | `Content-Encoding` | `X-Cache` |
|---|---|---|---|
| 없음 | **539,454 B** | 없음 | Hit |
| `br, gzip` (두 번) | 237,381 B | br | Miss → Hit |
| `gzip` (두 번, 873/396) | 486,893 B | gzip | Miss → Hit |
| `gzip, deflate, br, zstd` (브라우저, 873/396) | 472,004 B | br | Hit |

539,454 B 는 **`Accept-Encoding` 없이 보낸 요청**의 답이다(기본 `curl`). 압축을 받지 않겠다는 요청에 identity 로 답한 것이라 옳다. 캐시
정책은 이미 br·gzip 을 키에 넣고(`enable_accept_encoding_brotli`·`_gzip`), 배포는 `compress = true`, 원점은 `application/x-protobuf`
(Cloudflare 의 압축 목록, ADR-0111 W42)로 gzip 아카이브를 그대로 내고, CloudFront 는 원점의 `Content-Encoding` 을 그대로 넘긴다. 바꿀 것이
없다. 측정 스크립트 `korea-edge-measure.py` 는 처음부터 `Accept-Encoding: br, gzip` 을 보낸다.

## 비교

| | A. `s-maxage` = Worker 의 매니페스트 초 | B. 발행이 CloudFront 무효화 | C. 바뀌는 답은 `no-cache` |
|---|---|---|---|
| 낡음 상한(문 뒤) | 60 s(CloudFront) + 60 s(Worker) | 무효화 전파(보통 수십 초~2분) + 60 s | 60 s(Worker) |
| 적중 | 60 s 마다 조건부 재검증(304, 본문 없음) | 무효화 전까지 적중 | 매 요청 원점 왕복(0.6 s 대 경로를 다시 탄다) |
| 새 비밀·새 경로 | 없음(헤더 문자열) | 발행기가 AWS 키를 갖는다, 발행마다 API 호출 | 없음 |
| 부분 처리 | 바뀐 것만 재검증으로 드러난다 | 바뀐 동만 와일드카드 경로 — 경로 1,000/월 무료, 넘으면 $0.005/경로 | — |
| 브라우저 | `max-age=3600` 그대로 | 그대로 | 매번 묻는다(행동 변화) |

**A 를 고른다.** RFC 9111 §5.2.2.10 의 `s-maxage` 는 공유 캐시에만 적용되고 CloudFront 는 그것을 `max-age` 보다 먼저 읽는다(min/max TTL
안에서). 브라우저 쪽 결정(ADR-0193·0323)은 그대로다. AWS 는 바뀌는 콘텐츠를 무효화보다 **버전 붙은 이름**으로 내라고 권하고 — 우리 불변
URL(타일 릴리스·팩 세대·미디어·프로필, `immutable`)이 이미 그렇다 — 버전을 붙일 수 없는 주소에는 짧은 TTL 을 권한다. Fastly·Varnish·Vercel
도 같은 모양(`s-maxage` 로 CDN 만 짧게, 브라우저는 따로)이다. B 는 이득(수십 초)보다 비밀 하나와 실패 경로 하나가 크고, 무효화는 결국
일관성이 늦게 퍼진다. C 는 문의 이득(ICN 적중 0.04–0.08 s)을 버린다.

## Decision

1. **바뀌는 답의 `Cache-Control` 은 공유 캐시 수명을 적는다.** 같은 URL 의 답이 바뀌고 캐시할 수 있는(`max-age` > 0, `immutable` 아님)
   모든 `*cache_control` 은 `s-maxage=<같은 블록의 manifest_edge_cache_seconds 또는 manifest_cache_seconds>` 를 갖는다. 지금 넷:
   `parcel_by_pnu_gateway`·`building_by_pnu_gateway`·`by_pnu_lanes.unit-price` 의 `cache_control`, `vector_tile_gateway.patches.cache_control`
   — 모두 `public, max-age=3600, s-maxage=60`. 정본은 정의(`definitions/platform.definition.json`·`unit-price.definition.json`)이고 R2 연결
   계약은 거기서 생성된다. 문 뒤에 있지 않은 Worker 도 같은 규칙이다(문이 나중에 서도 고칠 것이 없다).
2. **가드.** `orchestration/tests/test_cdn_shared_cache_lifetime.py` 가 생성된 계약 전체를 훑어 1 을 지키지 않는 값을 거부한다. 막는 사고:
   2026-11-09 카탈로그 문이 이 한 시간 때문에 보류됐다(이미 전환된 `tiles` 의 패치 타일도 같은 한 시간을 CloudFront 에 남기고 있었다).
3. **CloudFront 무효화는 쓰지 않는다.** 버전이 바뀌는 판정 읽기는 이미 원점 이름으로 한다(#1219). 사용자 경로의 읽기(15분 엣지 탐침·매시
   서빙 모니터·컷오버 verify)는 공개 이름 그대로이고, verify 는 60 s 간격 5회라 상한 120 s 안에 새 답을 본다.
4. **그다음 카탈로그 문을 전환한다.** 1 이 배포된 Worker(원점에서 `s-maxage=60` 확인) 뒤에, 별도 PR 이
   `doors.catalog.answered_by` 를 `cloudfront` 로 바꾼다. 하나로 묶지 않는 이유: 같은 배포 틱에서 Worker 배포가 실패하고 엣지 적용만
   서면 CloudFront 가 옛 `max-age=3600` 을 한 시간 쥔다.

## Consequences

- 전환 전 실측(2026-11-09 18:5xZ, 노트북 KT AS4766, `korea-edge-measure.py --cloudfront catalog=… --reads 5 --globalping 6`, 필지 문서
  3개 + `_capabilities`):

  | 읽기 | 경로 | 입장 | TTFB 중앙값(최소–최대) |
  |---|---|---|---|
  | by-PNU 문서 878–964 B gzip | Cloudflare 존 주소(지금의 공개 경로) | LAX | 0.617 s (0.569–0.739), 15회 |
  | 〃 | CloudFront, Miss | ICN53-P1 | 0.189 s (0.115–0.266) |
  | 〃 | CloudFront, Hit | ICN53-P1 | 0.068 s (0.042–0.462), 12회 |
  | `_capabilities` 72 B (`no-store`) | Cloudflare | LAX | 0.593 s |
  | 〃 | CloudFront(저장 안 함, 매번 원점) | ICN53-P1 | 0.160 s (0.125–0.360) |

  Globalping 한국 6탐침: Cloudflare 경로는 KT 가 LAX 294 ms, 데이터센터는 ICN 50–65 ms; CloudFront 경로는 6/6 ICN53-P1, Hit TTFB 4–16 ms.
- 1 뒤 문 뒤의 by-PNU 문서는 60 s 마다 CloudFront 가 원점에 조건부로 묻는다(Worker 는 ETag 가 같으면 304). 그 재검증은 Miss 와 같은
  경로(0.1–0.3 s)이지만 본문이 없다. 브라우저 행동은 바뀌지 않는다.
- **비용(계산).** 재검증은 PoP·주소마다 60 s 에 최대 한 번이라 출시 전 트래픽에서는 요청 수가 무료 1천만/월 안이다($0). 무효화를 쓰지
  않으므로 무효화 경로 비용 $0. Worker 요청은 문 뒤에서 오히려 준다(적중분).
- 위험: CloudFront 는 4xx/5xx 를 기본 10 s 저장한다(오류 캐시 최소 TTL). 없는 PNU(404 `no-store`)가 발행으로 생겨도 최대 10 s 늦다 —
  감수한다.
- 출처: [RFC 9111 §5.2.2.10 s-maxage](https://www.rfc-editor.org/rfc/rfc9111#section-5.2.2.10),
  [CloudFront: 콘텐츠가 캐시에 머무는 시간](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/Expiration.html),
  [CloudFront: 무효화 대신 버전 붙은 이름](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/Invalidation.html),
  [CloudFront 압축](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/ServingCompressedFiles.html),
  [CloudFront 요금(무효화 경로)](https://aws.amazon.com/cloudfront/pricing/).
