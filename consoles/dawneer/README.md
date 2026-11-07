---
status: current
owner: dawneer
doc_type: README
last_reviewed: 2026-09-30
---

# 더니어 (Dawneer) — 직원 통합 콘솔

직원이 파이프라인·인사·데이터·승인을 다루는 화면이다([ADR-0114](../../docs/adr/0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md)).
더니어는 틀만 가진다: 데이터와 규칙은 각 플랫폼의 API 에 있고, 더니어는 그 API 를 부른다.

## 구조

```
consoles/dawneer/
├── server/   Rust 서버(BFF): Zitadel 로그인, 세션, 허용된 플랫폼 경로 중계
└── web/      화면: React·TypeScript. API 타입은 Foundation OpenAPI 문서에서 생성(`pnpm api:generate`), 어긋나면 `api:check` 실패
```

서버가 하는 일([ADR-0116](../../docs/adr/0116-dawneer-keeps-staff-tokens-on-its-rust-server.md)):

| 경로 | 하는 일 |
|---|---|
| `GET /auth/login` → `GET /auth/callback` | Zitadel 로그인(인가 코드 + PKCE, 기밀 클라이언트). 토큰은 서버에만, 브라우저엔 `HttpOnly` 세션 쿠키 |
| `POST /auth/logout` | 세션을 끝내고 Zitadel 로그아웃 주소를 돌려준다 |
| `GET /api/session` | 누가 로그인했는지 + CSRF 값(토큰은 주지 않는다) |
| `/oauth/v2/authorize`, `/ui/login/…`, `/oidc/v1/end_session` | Zitadel 의 로그인 화면을 이 주소에서 중계한다([ADR-0434](../../docs/adr/0434-dawneer-is-served-from-the-data-host-behind-cloudflare-access-and-a-tunnel-and-relays-zitadels-sign-in-pages.md) §2). 브라우저는 issuer 에 직접 가지 않고, 토큰 교환은 서버가 issuer 루프백에 한다 |
| `/api/foundation/<경로>` | 허용 목록(`server/src/relay.rs`)에 있는 Foundation 경로만 세션의 토큰을 실어 넘긴다. 쓰기는 `x-dawneer-csrf` 필수 |
| 그 밖 | 화면(`web/dist`) |

데이터 흐름 화면의 「서비스 점검」 탭은 서버가 매시 잰 한국→서비스 주소 경로(colo·p50·p95)와 주별 PR CI 시간을
보인다([ADR-0209](../../docs/adr/0209-the-data-flow-map-shows-the-edge-path-from-korea-and-the-ci-time-trend.md)).

## 어디서 도나

데이터 호스트(ai-server)에서 돈다([ADR-0434](../../docs/adr/0434-dawneer-is-served-from-the-data-host-behind-cloudflare-access-and-a-tunnel-and-relays-zitadels-sign-in-pages.md)).
주소는 `https://console.perfectory.io` 하나이고 Cloudflare Access(메일 PIN) → Zitadel(비밀번호 + 2차 인증) 순서로 들어온다.

| 무엇 | 어디 |
|---|---|
| 이미지 | `Dockerfile`. CI 가 서명한 릴리스에 든다(ADR-0276), 호스트는 빌드하지 않는다 |
| 컨테이너 | `platforms/foundation-platform/compose.console.yml` 의 `dawneer`·`console-tunnel`(프로필 `console`). foundation-api 의 네트워크 이름공간을 빌려 issuer·Foundation 을 루프백으로 부른다 |
| 켜지는 조건 | 배포의 `migrate` 가 `/etc/identity-platform/secrets/dawneer-oidc-client.env` 와 `/etc/foundation-platform/console-tunnel.env` 가 둘 다 있을 때만 띄운다. 없으면 `console: off` 한 줄 |
| 설정 | `DAWNEER_PUBLIC_BASE_URL`, `DAWNEER_ZITADEL_ISSUER_URL`, `DAWNEER_ZITADEL_PROJECT_ID`, `DAWNEER_FOUNDATION_API_BASE_URL`, `DAWNEER_BIND`, `DAWNEER_WEB_DIR`, 그리고 `configure-zitadel.sh` 가 쓴 `OIDC_CLIENT_ID`·`OIDC_CLIENT_SECRET` |

처음 한 번의 설정(Zero Trust·Access·터널 토큰·Zitadel)은 ADR-0434 「소유자가 한 번 하는 일」에 있다. 노트북 실행(ADR-0116 §5)은
은퇴했다 — 노트북에 client secret 을 복사하지 않는다. 화면을 고칠 때는 `cd web && pnpm build` 뒤 `bff.rs` 의 가짜 issuer 로 시험한다.

## 검증

`cargo xtask verify dawneer` — fmt, clippy, 시험, 그리고 화면의 typecheck·test·build·api:check. 로그인 시험(`server/tests/bff.rs`)은 가짜 발급자와 가짜 Foundation 을
스스로 띄운다.
