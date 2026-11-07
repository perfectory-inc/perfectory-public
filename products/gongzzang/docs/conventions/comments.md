---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# 주석 컨벤션

## 1. 원칙: Why over What

- ❌ **What** (이 코드가 *뭘* 하는지) — 좋은 네이밍이 대신함
- ✅ **Why** (왜 이렇게 했는지) — 좋은 코드는 *왜*를 못 말함

```rust
// ❌ What
// listing의 status를 Active로 변경
listing.status = ListingStatus::Active;

// ✅ Why
// 소유자 검증 이벤트 수신 후 자동 Active 전환 (운영자 검수 미요구 — ADR-0010)
listing.status = ListingStatus::Active;

// 또는 더 좋게: 이름이 말하게
listing.activate_after_external_verification();
```

## 2. 남은 일은 코드 밖에 적는다

코드 안에 `TODO` 주석을 남기지 않는다. 남은 일은 루트
[운영 준비 작업 목록](../../../../docs/roadmap/production-readiness.md)이나 이슈에 등록하고, 코드에는 필요하면
현재 제약(Why)만 적는다.

## 3. 금지 키워드

`.github`·`apps`·`services`·`crates`·`packages` 아래에서 `TODO`·`HACK`·`XXX`·`TEMP`·
`ALLOWED_FOR_FRONTEND_TEMP`·`임시` 마커는 금지다. 정본 목록과 검사는
루트 Semgrep 규칙 `tools/semgrep/rules/gongzzang-implementation-markers.yml`(CI `semgrep-rules`)이다.

| ❌ | ✅ 대신 |
|---|--------|
| `TODO` / `HACK` / `XXX` / `TEMP` | 작업 목록·이슈 등록 + 코드엔 제약(Why)만 |
| `FIXME` | 즉시 수정 |

## 4. 도메인 결정 근거

```rust
// ADR-0007: moka L1 + Valkey L2 — Redis SSPL 라이선스 회피
let cache = MokaCache::new(...);
```

```ts
// see: docs/adr/0010-scope-information-platform-option-a.md
// 옵션 A — LLM 생성 텍스트 사용자 노출 금지
const description = listing.descriptionFromUser; // NOT generated
```

## 5. 외부 참조

```rust
// Provider schema reference: https://example.com/provider/schema/v1
const PROVIDER_SCHEMA_VERSION: &str = "2026-05-28";
```

링크는 *영구 식별자* (commit SHA, archive.org) 또는 공식 문서 표준 URL.

## 6. unsafe 또는 비표준 사용 시

`unsafe` 자체는 `forbid` (`workspace.lints`). 만약 매크로 등으로 우회 필요 시:

```rust
// SAFETY: <왜 안전한지>
// (이 코드는 [ADR-XXXX]에서 승인됨)
unsafe { ... }
```

→ Rust 표준 SAFETY 코멘트 컨벤션 따름. ADR 링크 의무.

## 7. 라이센스/저작권 헤더

- 본 프로젝트: 소스 열람 가능한 독점 저장소이므로 *모든 파일에* 개별 저작권 헤더는 필요 X (루트 라이선스/REUSE가 SSOT)
- 외부 OSS 코드 차용 시: 그 라이선스 고지 의무 (`reference/`에서 차용 후 직접 작성하면 우리 코드)

## 8. 모듈 독스트링 (Rust)

```rust
//! crates/listing-domain - 매물 도메인
//!
//! 매물 등록·검수·상태 머신·공개/만료 규칙.
//! → docs/glossary.md 의 `Listing` 정의 참조.

pub struct Listing { ... }
```

`//!` = 모듈 도큐멘트. `///` = 항목 도큐멘트.

## 9. TS 컴포넌트 docstring

```tsx
/**
 * 매물 상세 카드. 매물 클릭 시 모달로 띄우거나 페이지 전환.
 *
 * @see docs/frontend/listings-search.md — 매물 검색 화면과 연동
 */
export function ListingCard({ listing }: Props) { ... }
```

## 10. 자동 강제

정본은 `Cargo.toml [workspace.lints]`와 위 금지 마커 스크립트다.

- Rust: `missing_docs` (warn)
- Rust: clippy lint `clippy::todo`, `clippy::unimplemented` deny
- 금지 마커: 루트 Semgrep 규칙 `tools/semgrep/rules/gongzzang-implementation-markers.yml` (CI `semgrep-rules`)

## 11. 좋은 주석 vs 나쁜 주석 비교

```rust
// ❌ 나쁨
let i = 0; // 카운터

// ❌ 나쁨 (코드 반복)
// price를 2배로 곱한다
let doubled = price * 2;

// ✅ 좋음 (Why)
// 부가세 10% 포함 가격 — NTS API와 비교 시 사용 (sub-project 결제 단계)
let price_with_vat = price.with_vat();

// ✅ 좋음 (제약)
// PostGIS 5179는 미터 단위, 4326은 degree — st_dwithin 거리는 *연산 좌표계* 기준
let geom_5179 = parcel.geom.transform_to(5179);
```
