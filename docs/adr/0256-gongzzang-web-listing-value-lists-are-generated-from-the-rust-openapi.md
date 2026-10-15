# ADR 0256: 공짱 웹의 매물 값 목록은 Rust OpenAPI에서 생성한다

- Status: Accepted
- Date: 2026-10-16
- Builds on: [ADR-0097](./0097-dependencies-track-latest-and-version-pins-live-in-one-contract.md) (버전 핀은 계약 하나)

## Context

매물 유형·거래 유형·연락처 공개 범위·카드 정렬 값은 Rust `shared-kernel`·`listing-domain` 열거형이 정본이다
(각 열거형의 `ALL`). 웹은 같은 목록을 손으로 다시 적었다. 2026-10-16 기준 TS에 14벌이었다.

| 파일 | 손으로 적은 목록 |
|---|---|
| `apps/web/lib/listings/schema.ts` | `LISTING_TYPES`, `TRANSACTION_TYPES`, `CONTACT_VISIBILITIES` |
| `apps/web/lib/listings/filters.ts` | `ListingType`·`TransactionType`·`SortKey` 유니온, `VALID_TYPES`·`VALID_TXNS`·`VALID_SORTS` |
| `apps/web/lib/listings/api.ts` | `listing_type`·`transaction_type` `z.enum` |
| `apps/web/components/listings/filter-bar.tsx` | `TXNS`, `SORTS` |
| `packages/ui/tokens.ts` | `LISTING_TYPE_COLORS` 키 |

Rust 쪽 `GET /listings`의 `sort` 파서도 값을 문자열로 한 번 더 적었다. Rust 값이 바뀌면 어느 검사도 웹을 붙잡지 않았다.
`packages/api-types`는 `services/gongzzang-api/openapi.json`에서 TS를 만드는 생성기를 이미 갖췄지만 그 파일이 없었다.

## Decision

1. **Foundation·Identity와 같은 방식으로 utoipa를 쓴다.** 공짱 워크스페이스 의존성은 두 플랫폼과 같은 선언
   (`version = "5"`, `axum_extras`·`chrono`·`uuid`)이다. 다섯 열거형에 `ToSchema`를 붙인다.
2. **내보내기 바이너리와 커밋된 문서.** `gongzzang-api`의 `export-gongzzang-openapi`가 `openapi.json`을 쓴다
   (Foundation `export-*-openapi`와 같은 꼴). 같은 파일의 단위 테스트가 커밋본과 생성본의 일치, 그리고 스키마 `enum`과
   `ALL`·`as_str`의 일치를 확인한다.
3. **TS는 생성본만 읽는다.** 생성기는 `enumValues`로 런타임 값 배열도 낸다. 웹의 zod 스키마·필터 파서·정렬 목록은
   그 배열을 쓰고, `LISTING_TYPE_COLORS`는 `satisfies Record<ListingType, string>`로 묶인다.
   `generate.tests.sh`는 생성 성공과 커밋본 일치를 요구한다.
4. **요구 버전은 계약 한 곳.** `tools/technology-versions.contract.json`의 `cargo_version_requirements.utoipa`가
   유일한 값이다. technology-version 가드는 저장소의 모든 `Cargo.toml`에서 그 크레이트의 리터럴 요구 버전이 이 값과
   같은지 확인한다(`workspace = true` 상속은 통과).

## Consequences

- 손으로 적은 값 목록은 TS 14벌, Rust 1벌에서 0이 된다. 남은 것은 의도적 부분집합(필터 바의 노출 유형 3종)과
  라벨·색 맵인데, 둘 다 생성 타입으로 묶여 값이 늘거나 줄면 타입 검사가 실패한다.
- 열거형 값을 바꾸면 `export-gongzzang-openapi`와 `pnpm --filter @gongzzang/api-types generate`를 다시 돌려
  두 생성본을 함께 커밋한다. 빠뜨리면 Rust 테스트 또는 `api-types` 테스트가 실패한다.
- 이 문서는 아직 경로가 없다. 라우트를 OpenAPI로 옮기는 것은 별도 결정이다.
- 기각: 열거형 값 목록 JSON을 따로 두고 양쪽이 읽는 방식. 이미 있는 생성기 경로(`openapi.json` → `api-types`)를
  두고 두 번째 형식을 만드는 것이다.
