# ADR 0049: 사람이 훑는 컬렉션은 쪽나눔·필터·전체건수를 함께 발행한다

- Status: Accepted
- Date: 2026-08-23
- 관련: [ADR-0048 발행된 feature id 에는 그 id 로 여는 조회구가 필요하다](./0048-a-published-feature-id-needs-a-read-keyed-on-it.md), [ADR-0040 아무도 채우지 않는 컬럼은 필수일 수 없다](./0040-a-column-no-producer-fills-cannot-be-required.md), [ADR-0043 정본 id 는 다시 계산하지 않고 읽는다](./0043-a-canonical-id-is-read-not-recomputed.md)

## Context

`GET /catalog/v1/complexes` 는 2026-08-23 기준 `catalog.industrial_complex` 의 **비아카이브 전체
행을 배열 하나로** 돌려준다. 오늘 그 수는 1,448 이고, 행마다 Gold 포인터를 한 번씩 더 읽는다.

이 모양으로는 사람이 쓰는 화면을 만들 수 없다. 구체적으로 세 가지가 없다.

1. **검색이 없다.** 1,442곳 중 하나를 이름으로 찾으려면 클라이언트가 1,448행을 받아 스스로 훑어야
   한다. ADR-0048 이 지도 클릭에 조회구를 붙였지만, **지도를 누르지 않고** 단지에 닿는 길은
   그 뒤로도 없었다.
2. **쪽나눔이 없다.** 상한이 없는 컬렉션 조회구는 인증된 세션 하나가 표 전체를 한 요청에
   끌어갈 수 있다는 뜻이다.
3. **전체 건수가 없다.** 배열의 길이는 "조건에 맞는 것이 몇 개인가"가 아니라 "이번에 몇 개
   받았는가"다. 둘이 항상 같았던 이유는 쪽나눔이 없었기 때문이고, 쪽나눔을 붙이는 순간
   갈라진다. 화면은 "1,442곳 중 20곳"을 말해야 하는데 그 앞의 수를 아무도 주지 않는다.

이 저장소에는 **같은 문제를 이미 푼 본**이 있다 —
`products/gongzzang/services/gongzzang-api/src/routes/listings/search.rs` 의 매물 검색.
`page`(0부터) · `size`(기본 20, 최대 100) · `sort`, comma-separated 다중 필터, `total`/`has_next`
를 담은 응답 봉투. 두 번째 컬렉션이 다른 이름을 쓰면 한 화면에서 둘을 넘기는 호출자가 두 벌을
외워야 한다.

## Decision

1. **사람이 훑는 컬렉션 조회구는 쪽나눔·필터·전체건수를 함께 발행한다.** 셋 중 하나라도 없으면
   그 발행은 화면을 만들 수 없고, 없는 것을 클라이언트가 메우면 그 클라이언트가 표 전체를
   내려받는다.

2. `GET /catalog/v1/complexes` 는 `q`(이름·단지코드 부분일치) · `sido_code`(2자리) ·
   `status`(comma-separated) · `page` · `size` · `sort` 를 받고, **배열이 아니라 봉투**
   `IndustrialComplexListResponse { complexes, total, page, size, has_next }` 를 돌려준다.
   파라미터 이름·기본값·상한은 위 매물 검색을 그대로 따른다. 새 모양을 만들지 않는다.

3. **상한은 검사가 아니라 타입이 강제한다.** `catalog_application::complex_search` 의
   `ComplexSearchPaging` 은 `1..=100` 밖의 크기를 가진 채 존재할 수 없고, `ComplexSearchText` 는
   호출자가 친 `%`·`_` 를 이스케이프한 패턴만 내놓는다. 핸들러가 검증을 빠뜨리면 상한을 건너뛴
   쿼리가 만들어지는 것이 아니라 **쿼리가 만들어지지 않는다.**
   이 방어가 막는 실제 사고: `size=100000` 한 요청이 정본 1,448행 + 행당 Gold 포인터 조회를
   끌어가는 것. 무력화 실험으로 확인했다 — 상한 분기를 지우면
   `a_page_size_above_the_maximum_is_refused` 와 `list_complexes_refuses_a_page_size_past_the_bound`
   가 빨개진다.

4. **`q` 는 오늘 인덱스 없이 전수 스캔이다.** `name ILIKE '%…%'` 는 인덱스를 쓸 수 없고,
   매 검색이 1,448행을 훑는다. 이 크기에서는 괜찮다 — 표는 배치 작업이 한 해에 몇 번 쓰고
   화면 하나가 읽는다. 괜찮지 않게 되는 시점은 행이 10만을 넘거나 이 조회구가 여러 세션에서
   타건마다 불릴 때이고, 그때의 답은 `name` 에 대한 `pg_trgm` GIN 인덱스다.
   **그 인덱스는 지금 없다.** `sqlx_repository.rs::search_complexes` 주석이 이 문단을 그대로
   진다 — 없는 인덱스를 있다고 적지 않는다.

5. **전체 건수는 쪽을 고르는 문장이 함께 센다.** `COUNT(*) OVER ()` 는 빈 쪽에서 행을 하나도
   내지 않으므로, 끝을 넘어선 쪽 요청이 "전체 0건"으로 답한다. `total` 을 별도 CTE 로 두고
   `LEFT JOIN … ON true` 로 붙여, 쪽이 비어도 전체 건수는 살아 있다.

6. **모든 정렬은 `official_complex_code, id` 로 전순서를 만든다.** 이름이 같은 단지가 실제로
   있고, 동률에서 순서가 흔들리면 한 행이 두 쪽에 나오거나 어느 쪽에도 안 나온다.

7. **Gongzzang 은 이 컬렉션을 자기 라우트 계약으로 중계한다.** `GET /api/complexes` 는 단건
   중계(`GET /api/complexes/:lakehouse_complex_id`)와 같은 reader 포트를 쓰고, 목록 행에는
   **패널을 여는 열쇠인 `lakehouse_complex_id` 만** 싣는다 — 정본 `id` 는 싣지 않는다.
   ADR-0048 이 만든 구분이 목록에서 다시 섞이지 않게 하는 자리다. 웹의
   `lib/complexes/panel-target.ts` 가 `UUIDv5` 형식을 다시 확인하고, 형식이 아닌 값은 **누를 수
   없는 줄**로 그린다. 무력화 실험으로 확인했다 — 그 확인을 지우면 정본 `id` 를 물린 줄이
   눌리게 되고 `refuses the catalog id for the same complex` 가 빨개진다.

8. **`lakehouse_complex_id` 가 없는 단지는 목록에 남되 열리지 않는다.** 2026-08-23 정본 1,448행
   중 6행이 그렇다(쓰기 API 로 등록). 목록에서 빼면 전체 건수가 거짓말을 하고, 누를 수 있게
   하면 404 가 난다.

## Consequences

- `GET /catalog/v1/complexes` 의 응답 모양이 **배열에서 봉투로 바뀐다.** 이 저장소 안에 이
  조회구의 프로그램 소비자는 없었고(레이크하우스 materialization 은 HTTP 가 아니라 리포지토리
  포트를 직접 쓴다), 제품은 아직 런칭 전이라 바깥 소비자도 없다. 기본 응답 크기도 1,448행에서
  20행으로 바뀐다.
- `docs/openapi/catalog.v1.json` 은 `export-catalog-openapi` 재생성으로 갱신되고, gongzzang 의
  소비자 pin(`foundation-platform-catalog-api-contract.v1.pin.json`)이 새 sha256 과
  `listComplexes` 항목을 함께 싣는다.
- `CatalogRepository::list_complexes` 는 **그대로 둔다.** 정본 표 전체가 필요한 배치 소비자가
  있고, 그쪽에 쪽을 물리면 조용히 앞 20행만 처리한다. 상한은 요청이 닿는
  `search_complexes` 쪽에 있다.
- 조성 단계 필터는 화면에서 `operating`/`developing`/`planned` 셋만 노출한다. 나머지 셋
  (`changed`/`abolished`/`unknown`)은 정본에 한 행도 없어, 항상 0건인 필터 칩은 화면이 고장 난
  것처럼 읽힌다. 계약은 여전히 여섯을 받는다.
- 목록 한 줄에 **준공일을 넣지 않는다.** 정본의 26%가 준공일이 없어, 네 줄 중 세 줄에만 날짜가
  붙으면 나머지 한 줄이 "빠진 값"이 아니라 "고장"으로 읽힌다. 그 값은 줄을 눌러 열리는 패널이
  이미 보여 준다.
