---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# 테스트 컨벤션

## 1. 분류

| 종류 | 도구 | 위치 | 실행 |
|------|------|------|------|
| Rust 단위 | `cargo test` | 같은 크레이트의 형제 파일 (`entity.rs` → `#[path = "entity_tests.rs"] mod entity_tests;`) | `cargo xtask verify gongzzang --affected` (`gongzzang-ci.yml`) |
| Rust 통합 | `cargo test` + 실제 Postgres (`DATABASE_URL`) | `crates/<crate>/tests/` (`#![cfg(feature = "integration")]`) | `cargo xtask integration gongzzang postgres --affected` (`gongzzang-db-migrations.yml`) |
| 웹 단위 | Vitest `unit` 프로젝트 | `apps/web/tests/unit/`, `apps/web/lib/**/*.test.ts` | `pnpm turbo test` (`gongzzang-frontend.yml`) |
| 웹 통합 | Vitest `integration` 프로젝트 (실제 Redis) | `apps/web/tests/integration/` | `pnpm --filter=@gongzzang/web run test:integration` |
| E2E·접근성 | Playwright + `@axe-core/playwright` | `apps/web/tests/e2e/` | `gongzzang-frontend.yml` |
| 부하 | k6 | `tests/load/` | [부하 테스트](../testing/load.md) |

Vitest 레인 구분은 디렉터리 하나로만 한다 (`apps/web/vitest.config.ts` 의 `test.projects`).
어느 레인에도 안 걸리는 테스트 파일은 `scripts/ci/vitest-lane-completeness.sh` 가 막는다.

## 2. 커버리지

CI 커버리지 게이트는 지금 없다. `gongzzang-ci.yml` 머리말이 정본이다 — tarpaulin 90% 게이트는
런칭 후 필요하면 다시 넣는다. `tarpaulin.toml` 은 그때를 위한 설정으로만 남아 있다.

Catalog 소스 클라이언트, raw capture, 공용 공간 데이터 리더는 Foundation Platform 소유다.
공짱 테스트를 그 crate를 다시 만들거나 관리 대상으로 해석하면 안 된다.

## 3. 네이밍 규칙

`<주체>_<can/cannot/returns/throws>_<조건>` — 한 줄이 검증 사실.

```rust
// Rust
#[test]
fn listing_can_be_published_when_owner_has_business_number() { ... }

#[test]
fn listing_cannot_be_published_when_status_is_draft() { ... }

#[test]
fn listing_publish_returns_error_when_already_sold() { ... }

#[test]
fn pnu_try_new_throws_when_input_is_not_19_digits() { ... }
```

```ts
// TS (Vitest)
test("listing can be published when owner has business number", () => { ... });
test("listing cannot be published when status is draft", () => { ... });
```

## 4. AAA 패턴

```rust
#[test]
fn ... () {
    // Arrange
    let owner = User::with_business_number("123-45-67890");
    let listing = Listing::draft(&owner);

    // Act
    let result = listing.submit_for_review();

    // Assert
    assert!(result.is_ok());
    assert_eq!(listing.status(), ListingStatus::Review);
}
```

## 5. mock 정책

| 종류 | 정책 |
|------|------|
| DB | 실제 Postgres. 통합 테스트는 `DATABASE_URL` 로 붙고, 매 테스트 시작에 `common::truncate_all` 로 비운다 |
| 웹 외부 호출 | Vitest 단위 레인은 mock; 실제 Redis 가 필요한 것은 통합 레인으로 |

→ "거의 항상 실제 DB"는 SSOT 원칙과 일치 (mock이 사본을 만들지 않게).

## 6. Rust 단위 테스트 예시

```rust
// crates/listing-domain/src/entity.rs
#[cfg(test)]
#[path = "entity_tests.rs"]
mod entity_tests;
```

```rust
// crates/listing-domain/src/entity_tests.rs — 파일 자체가 테스트 모듈
#[test]
fn listing_can_be_published_when_owner_has_business_number() {
    // Arrange / Act / Assert
}
```

## 7. 통합 테스트 (실제 Postgres)

```rust
// crates/gongzzang-persistence/tests/bookmark_integration.rs
#![cfg(feature = "integration")]

mod common;
use common::{setup_test_pool, truncate_all};

#[tokio::test]
async fn round_trip_listing_bookmark_with_audit() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    // seed → repo 호출 → audit/outbox 확인
}
```

`integration` feature 없이는 파일이 비어 컴파일된다. 어떤 테스트 대상이 어떤 레인에서 도는지는
`tools/xtask` 의 레인 선언이 정본이다 (루트 ADR-0011).

## 8. E2E (Playwright)

```ts
// apps/web/tests/e2e/listings.spec.ts
test("/(authenticated)/listings 미인증 → /login redirect", async ({ page }) => {
  await page.goto("/listings");
  await page.waitForURL(/\/login/, { timeout: 10000 });
  expect(page.url()).toContain("returnTo=%2Flistings");
});
```

요소 선택은 역할(`getByRole`)·라벨 기준 (CSS 클래스로 선택 X — 변경 깨짐 방지).
실행 환경은 [Playwright 런타임](../testing/playwright-runtime.md).

## 9. 금지 패턴

- ❌ Sleep 기반 동기화 (`tokio::time::sleep` 단순 대기) — 이벤트 기반으로
- ❌ 환경 의존 (테스트 머신 시간, 로케일 등) — 명시적 fixture로
- ❌ 테스트 간 공유 state (각 테스트 격리)
- ❌ "skip" 또는 "ignore" 누적 — 깨진 테스트는 즉시 수정 또는 삭제
- ❌ 실제 외부 API 호출 (Zitadel, V-World, Naver Maps 등) — 기본 레인에서는 항상 mock
- ❌ 통과를 꾸미는 테스트 — 루트 Conftest 정책 `tools/conftest/lefthook`·`tools/conftest/workflow` 가 막는다

## 10. 자동 강제

정본은 CI 다. 로컬 훅은 빠른 사전 거름이고 테스트를 돌리지 않는다 (루트 `lefthook.yml`).

- Rust: `gongzzang-ci.yml` (`cargo xtask verify gongzzang --affected` — fmt·clippy·test)
- Rust 통합: `gongzzang-db-migrations.yml` (`cargo xtask integration gongzzang postgres --affected`)
- 웹: `gongzzang-frontend.yml` (biome · typecheck · unit · integration · e2e · a11y · bundle)
