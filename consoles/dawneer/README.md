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
└── web/      화면(다음 단계)
```

서버가 하는 일([ADR-0116](../../docs/adr/0116-dawneer-keeps-staff-tokens-on-its-rust-server.md)):

| 경로 | 하는 일 |
|---|---|
| `GET /auth/login` → `GET /auth/callback` | Zitadel 로그인(인가 코드 + PKCE, 기밀 클라이언트). 토큰은 서버에만, 브라우저엔 `HttpOnly` 세션 쿠키 |
| `POST /auth/logout` | 세션을 끝내고 Zitadel 로그아웃 주소를 돌려준다 |
| `GET /api/session` | 누가 로그인했는지 + CSRF 값(토큰은 주지 않는다) |
| `/api/foundation/<경로>` | 허용 목록(`server/src/relay.rs`)에 있는 Foundation 경로만 세션의 토큰을 실어 넘긴다. 쓰기는 `x-dawneer-csrf` 필수 |
| 그 밖 | 화면(`web/dist`) |

## 노트북에서 실행

1. 터널과 계정: [직원 콘솔 로그인 준비](../../platforms/identity-platform/docs/runbooks/staff-console-sign-in.md)
2. 서버의 `dawneer-oidc-client.env` 를 노트북의 저장소 밖 경로로 복사한다.
3. 실행:

```bash
cd consoles/dawneer
DAWNEER_OIDC_CLIENT_FILE=<복사한 파일> \
DAWNEER_ZITADEL_ISSUER_URL=http://127.0.0.1:18453 \
DAWNEER_ZITADEL_PROJECT_ID=<perfectory 프로젝트 id> \
cargo run -p dawneer-server
```

`http://127.0.0.1:3120` 을 연다. 기본값: 듣는 주소 `127.0.0.1:3120`, Foundation `http://127.0.0.1:18080`, 화면 `web/dist`.

## 검증

`cargo xtask verify dawneer` — fmt, clippy, 시험. 로그인 시험(`server/tests/bff.rs`)은 가짜 발급자와 가짜 Foundation 을
스스로 띄운다.
