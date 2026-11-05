---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# 네이밍 + ID 컨벤션

## 1. 케이스 규칙

| 영역 | 규칙 | 예시 |
|------|------|------|
| Rust 타입 | `PascalCase` | `Listing`, `Pnu` |
| Rust 함수/필드/변수 | `snake_case` | `find_by_id`, `area_m2` |
| Rust 상수 | `SCREAMING_SNAKE_CASE` | `MAX_LISTINGS` |
| Cargo 패키지/디렉터리 | `kebab-case` | `crates/listing-domain` |
| Rust 모듈 | `snake_case` | `listing_review` |
| TS 타입/컴포넌트 | `PascalCase` | `Listing`, `MapView` |
| TS 함수/변수 | `camelCase` | `findById`, `areaM2` |
| TS 파일 | `kebab-case` | `listing-detail.tsx` |
| TS 폴더 | `kebab-case` | `industrial-complex/` |
| DB 테이블 | `snake_case` 단수 | `listing`, `audit_log` |
| DB 컬럼 | `snake_case` | `created_at`, `owner_id` |
| API URL | `kebab-case` 복수 | `/listings`, `/map/v1/marker-tiles/listing/...` |
| API 응답 필드 | 현재 Rust DTO 는 serde 기본 `snake_case` | `owner_id`, `price_krw` (`services/gongzzang-api/src/routes/listings/detail.rs`) |
| 에러 코드 | `SCREAMING_SNAKE_CASE` | `LISTING_NOT_FOUND` |
| 환경 변수 | `SCREAMING_SNAKE_CASE` | `VWORLD_API_KEY` |
| Git 브랜치 | `kebab-case` + 타입 prefix | `feat/listing-search`, `fix/auth-token-leak` |

## 2. ID 전략 — ULID + 도메인 prefix

Gongzzang 소유 aggregate ID = `<3자 prefix>_<26자 ULID>` (총 30자, Crockford Base32, 시간순 정렬 가능, URL 안전).
prefix 목록의 정본은 `crates/shared-kernel/src/id.rs`의 `IdPrefix` 구현이다(예: `usr_`, `lst_`, `aud_`, `evt_`).
새 aggregate 는 그 파일에 marker 를 추가하고, 이 문서에 목록을 복사하지 않는다.

필지·건물·산업단지 같은 Catalog 사실의 식별자는 Foundation Platform 이 소유하며 이 prefix 체계 밖이다.

PNU (필지 번호)는 한국 정부 표준 (19자리) — ID prefix 사용 안 함. 그러나 도메인 코드에서는 `Pnu` 값 객체로 캡슐화.

## 3. 도메인 용어

[../glossary.md](../glossary.md) SSOT. 글로서리 준수는 리뷰로 확인한다(자동 검사 없음).

## 4. 좌표/면적/통화

| 필드 | 단위 | 예시 |
|------|------|------|
| 면적 | `_m2` (㎡) suffix | `area_m2`, `floor_area_m2` |
| 좌표 | EPSG SRID 명시 | `geometry(Polygon, 4326)` |
| 통화 | `_krw` suffix (KRW 고정) | `price_krw`, `monthly_rent_krw` |
| 시간 | UTC 저장 | `created_at TIMESTAMPTZ` |

UI 표시 시:
- 면적: ㎡ + 평 동시 (1평 = 3.305785㎡)
- 가격: 억/만원 (`850,000,000` → "8억 5,000만원")
- 시간: KST 변환

## 5. 시간 컨벤션

- DB 저장: `TIMESTAMPTZ` (UTC)
- API 응답: ISO 8601 with timezone (`2026-04-22T10:30:00+09:00`)
- UI 표시: KST 변환 + 한국식 (`2026년 4월 22일 오후 10시 30분`)

## 6. 자동 강제

- Rust: clippy 기본 `wrong_self_convention`, pedantic `module_name_repetitions` (warn, `Cargo.toml [workspace.lints]`)
- ID prefix: `Id<P>` 파싱이 prefix 불일치를 거부 (`crates/shared-kernel/src/id.rs`, `IdError::WrongPrefix`)
- TS 네이밍·DB 네이밍·글로서리: 자동 검사 없음 (리뷰)
