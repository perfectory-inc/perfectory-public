---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-11
---

# 공짱 인증 인프라 (`product-identity-infrastructure`)

공짱 인증의 핵심 관문이다. Zitadel 접근 토큰(JWT)을 검증하고, 첫 로그인 때 `User` 를 자동으로 만든다.

## 책임

- JWT 검증(Zitadel 발급, JWKS `kid` 캐시 수명 1시간): 서명·만료·발급자·대상.
- Axum 미들웨어: `Bearer` 추출 → 검증 → `User` 조회(`find_by_zitadel_sub`) 또는 자동 생성 →
  `Extension<AuthenticatedUser>` 주입.
- 역할 기반 접근 제어: `user-domain` 의 `UserRole`(7종) 기준 `require_role` / `require_one_of_roles`.
- 토큰 거부 목록(`JtiDenylist`, Redis 프로토콜/`deadpool-redis`): 로그아웃·갱신 토큰 교체·역할 변경 때 토큰을 바로 무효화한다.
- 인증 이벤트 6종(`Login`, `Logout`, `RefreshSucceeded`, `RefreshFailed`, `RoleGuardDenied`, `RoleChanged`)을
  `audit_log`에 기록.

## 모듈

- [`errors`](src/errors.rs): `AuthError` + `IntoResponse`
- [`claims`](src/claims.rs): `Claims` 구조체
- [`jwks_cache`](src/jwks_cache.rs): JWKS 캐시(수명 1시간)
- [`verifier`](src/verifier.rs): `JwtVerifier`
- [`middleware`](src/middleware.rs): Axum·tower 계층(`auth_layer`)
- [`extractor`](src/extractor.rs): `AuthenticatedUser`
- [`role_guard`](src/role_guard.rs): `require_role`
- [`jti_denylist`](src/jti_denylist.rs): 토큰 무효화 목록
- [`audit`](src/audit.rs): `AuthEvent` 를 `audit_log` 에 기록

## 의존

- `shared-kernel`, `user-domain`
- `axum`, `tower`, `jsonwebtoken`, `reqwest`(JWKS 가져오기, 크레이트 단위로 `disallowed_types` 허용),
  `deadpool-redis`, `sqlx`(감사 기록), `tracing`

## 정책

- JWT 검증 실패 = 401(재시도 없음)
- 역할 위반 = 403(`RoleGuardDenied` 감사 이벤트)
- `User` 자동 생성 실패 = 500

현재 결정과 운영 안내: [ADR-0005](../../docs/adr/0005-auth-zitadel.md),
[docs/auth/README.md](../../docs/auth/README.md).
