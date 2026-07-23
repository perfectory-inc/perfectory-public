# Frontend Auth Integration — 운영 SSOT

> SP6-i 의 운영 가이드. 디버깅·장애 대응·로컬 개발 절차의 단일 출처.

## 1. 로컬 개발 환경

```bash
# 1. 커밋되지 않는 로컬 자격증명 생성 (매번 새 개발 스택에 새 값 사용)
umask 077
{
  printf 'ZITADEL_DB_PASSWORD=%s\n' "$(openssl rand -hex 24)"
  printf 'ZITADEL_MASTERKEY=%s\n' "$(openssl rand -hex 16)"
  printf 'ZITADEL_ADMIN_PASSWORD=%s\n' "$(openssl rand -base64 24)"
  printf '%s\n' \
    'ZITADEL_ADMIN_USERNAME=admin' \
    'ZITADEL_ADMIN_EMAIL=admin@zitadel.localhost' \
    'ZITADEL_DB_HOST_PORT=15433' \
    'ZITADEL_HOST_PORT=18443' \
    'ZITADEL_REDIS_HOST_PORT=6379'
} > infra/zitadel/.env

# 2. Zitadel + Redis dev container 시작
docker compose --env-file infra/zitadel/.env \
  -f infra/zitadel/docker-compose.yml up -d

# 3. Zitadel 첫 부팅 후 (~30초 대기)
sleep 30

# 4. OIDC app 등록 (idempotent)
bash infra/zitadel/init-zitadel.sh > /tmp/zitadel.out
cat /tmp/zitadel.out  # CLIENT_ID 확인

# 5. apps/web/.env.local 작성 (CLIENT_ID 반영)
cp apps/web/.env.local.example apps/web/.env.local
# CLIENT_ID 수정

# 6. 백엔드 실행 (별도 터미널) — 앱 Postgres 는 infrastructure/docker/docker-compose.yml (호스트 포트 기본 15432)
DATABASE_URL=postgres://gongzzang:gongzzang@localhost:15432/gongzzang \
ZITADEL_ISSUER=http://localhost:18443 \
ZITADEL_AUDIENCE=$CLIENT_ID \
REDIS_URL=redis://localhost:6379 \
cargo run -p gongzzang-api

# 7. 프론트엔드 실행
pnpm --filter=@gongzzang/web dev
```

브라우저로 `http://localhost:3000/login`을 열고, 커밋되지 않은
`infra/zitadel/.env`의 `ZITADEL_ADMIN_EMAIL`과 `ZITADEL_ADMIN_PASSWORD`로 로그인한다.
실제 Zitadel E2E를 실행할 때도 해당 값을 별도
`ZITADEL_E2E_USERNAME` / `ZITADEL_E2E_PASSWORD` 환경 변수로 명시적으로 주입한다.

```bash
set -a
source infra/zitadel/.env
set +a
ZITADEL_E2E_REAL=true \
ZITADEL_E2E_USERNAME="$ZITADEL_ADMIN_EMAIL" \
ZITADEL_E2E_PASSWORD="$ZITADEL_ADMIN_PASSWORD" \
pnpm --filter=@gongzzang/web test:e2e
```

**포트 주의**: Windows Hyper-V 제외 범위 회피로 dev 의 Zitadel 가 18443, Zitadel 전용 Postgres(zitadel-db)가 15433.
앱 Postgres 는 별도로 `infrastructure/docker/docker-compose.yml` 이 기본 15432 로 노출한다.
Linux/macOS 는 `docker-compose.yml` 의 ports 를 8443/5433 으로 변경 가능
(단 `.env.local` 의 `ZITADEL_ISSUER` 와 `init-zitadel.sh` 의 `ZITADEL_HOST` 도 일관 변경).

## 2. 인증 흐름

```
사용자 → /login → POST /api/auth/login (PKCE start, HMAC-signed tmp cookie 발급)
       → 302 → Zitadel /oauth/v2/authorize
       → 사용자 인증 → 302 → /api/auth/callback?code=&state=
       → state CSRF 검증 (timingSafeEqual) → token exchange → Redis session 발급 (sid)
       → Set-Cookie __Host-sid → 302 → returnTo (sanitizeReturnTo, default /profile)
```

**보안 layer 4종**:

1. `__Host-` cookie prefix + Secure + HttpOnly + SameSite=Strict + Partitioned
2. PKCE code\_verifier (43+ chars, S256 challenge)
3. State CSRF 검증 (timing-safe HMAC tmp cookie)
4. Backend Verifier (Zitadel JWT signature + JTI denylist)

## 3. 디버깅

| 증상 | 원인 후보 | 확인 방법 |
| --- | --- | --- |
| `/login` 누르면 401 state mismatch | tmp cookie 만료 (10분) 또는 HMAC tampered | `__Host-auth-tmp` 쿠키 존재 + verifyTempPayload 결과 확인 |
| `/profile` 가 무한 redirect | Redis 연결 실패 → session null | `redis-cli ping`, proxy fail-closed |
| 401 token revoked | logout 후 재사용, 또는 role 변경 직후 | `redis-cli GET jti:deny:<jti>` 확인 |
| 403 forbidden | role 이 admin/broker/operator 아님 | profile 화면에서 role 확인 |
| 429 rate limit | login 5/min/IP, callback 10/min/IP, refresh 30/min/sid 초과 | `redis-cli ZRANGE rate:login:<ip> 0 -1 WITHSCORES` |

## 4. 장애 대응

### Zitadel 다운

- 기존 session 은 access\_token TTL (5분) 까지 동작
- 만료 후 refresh 시도 → fail → frontend 가 /login redirect → Zitadel 다운 시 503 ProblemDetails (idp-unavailable)
- 영향: 신규 로그인 + token refresh 차단. 기존 세션 처리는 가용

### Redis 다운

- frontend `getSession` fail → proxy 가 /login redirect (closed-fail, session lookup 차원)
- backend JTI denylist check → tracing::warn! + JWT 검증 통과 (fail-open, 가용성 우선)
- audit\_log emit fail → tracing::warn 로깅, 사용자 영향 없음

### Postgres 다운

- frontend 인증은 동작 (Zitadel + Redis 만 의존)
- backend `/me` 등 user 조회 실패 → 502 → frontend RFC 7807 응답

## 5. JTI denylist 운영

```bash
# 특정 jti 무효화 (관리자 수동 — role 변경 시 backend 가 자동 처리)
redis-cli SET jti:deny:<jti> 1 EX 300

# 활성 deny 목록
redis-cli KEYS "jti:deny:*"

# 사용자의 모든 활성 jti (role 변경 직전 조회)
psql -c "SELECT after_state->>'jti' FROM audit_log
         WHERE resource_id = '<zitadel_sub>'
           AND action IN ('auth.login', 'auth.refresh.succeeded')
           AND created_at > now() - interval '30 days';"
```

## 6. 모니터링 (SP7-i 통합 후)

| 메트릭 | 임계 | 의미 |
| --- | --- | --- |
| `auth.login.failure_rate` | > 5% | Zitadel 또는 frontend 버그 |
| `auth.refresh.failure_rate` | > 1% | Zitadel down 또는 refresh\_token 만료 비율 비정상 |
| `auth.role_guard.denied` | spike | 권한 설정 오류 또는 공격 |
| `redis.session.miss_rate` | > 0.1% | Redis 데이터 손실 또는 TTL 설정 오류 |

## 7. 미래 sub-project 의 자리

- **SP6-CI** (KISA 본인확인): `users.ci` 채움. NICE/Toss SDK 통합 + CI state machine.
- **SP6-Social**: 카카오/네이버/Google federation. `external_account` 가 매 provider 채워짐. 동일인 매칭 = `users.ci` UNIQUE.
- **SP6-org**: organization 분리, JWT `org_id` claim, org switcher UI.
- **SP6-iam-infra**: Zitadel self-host 의 Pulumi 코드화, JWKS rotation, DB backup, alert.

## 8. 의존성

| 외부 시스템 | 역할 | SSOT |
| --- | --- | --- |
| Zitadel | Identity (사용자 인증) | dev: docker-compose, prod: SP6-iam-infra Pulumi |
| Redis | Active session + JTI denylist + ratelimit | dev: docker-compose, prod: SP6-iam-infra |
| Postgres | users.role authorization, audit\_log | 기존 SP1-3 |

## 9. 현행 계약 참조

- ADR-0005: [docs/adr/0005-auth-zitadel.md](../adr/0005-auth-zitadel.md)
- Auth index: [docs/auth/README.md](./README.md)
