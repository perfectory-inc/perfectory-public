---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# crates/product-identity-infrastructure

공짱 인증 핵심 게이트 — Zitadel access_token JWT 검증 + 첫 로그인 시 `User` 자동 생성.

## 책임

- JWT 검증 (Zitadel 발급, JWKS `kid` 캐시 1h TTL) — 서명·exp·iss·aud.
- Axum middleware — `Bearer` 추출 → 검증 → `User` 조회(`find_by_zitadel_sub`) 또는 자동 생성 →
  `Extension<AuthenticatedUser>` 주입.
- RBAC — `user-domain`의 `UserRole`(7종) 기준 `require_role` / `require_one_of_roles`.
- JTI denylist (`JtiDenylist`, Redis 프로토콜/`deadpool-redis`) — logout·refresh rotation·role change 시 토큰 즉시 무효화.
- 인증 이벤트 6종(`Login`, `Logout`, `RefreshSucceeded`, `RefreshFailed`, `RoleGuardDenied`, `RoleChanged`)을
  `audit_log`에 기록.

## Modules

- [errors](src/errors.rs) — `AuthError` + `IntoResponse`
- [claims](src/claims.rs) — `Claims` struct
- [jwks_cache](src/jwks_cache.rs) — JWKS 1h TTL 캐시
- [verifier](src/verifier.rs) — `JwtVerifier`
- [middleware](src/middleware.rs) — Axum tower layer (`auth_layer`)
- [extractor](src/extractor.rs) — `AuthenticatedUser`
- [role_guard](src/role_guard.rs) — `require_role`
- [jti_denylist](src/jti_denylist.rs) — JTI 무효화 목록
- [audit](src/audit.rs) — `AuthEvent` → `audit_log` writer

## 의존

- `shared-kernel`, `user-domain`
- `axum`, `tower`, `jsonwebtoken`, `reqwest`(JWKS fetch — crate 단위 `disallowed_types` 허용),
  `deadpool-redis`, `sqlx`(audit writer), `tracing`

## 정책

- JWT 검증 실패 = 401 (재시도 X)
- RBAC 위반 = 403 (`RoleGuardDenied` 감사 이벤트)
- `User` 자동 생성 실패 = 500

Current decision and operating guidance: [ADR-0005](../../docs/adr/0005-auth-zitadel.md) and
[docs/auth/README.md](../../docs/auth/README.md).
