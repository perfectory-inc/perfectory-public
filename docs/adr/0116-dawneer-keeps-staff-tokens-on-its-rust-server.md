# ADR 0116: 더니어는 직원의 토큰을 자기 Rust 서버에만 두고, 브라우저에는 세션 쿠키만 준다

- Status: Accepted
- Date: 2026-09-30
- Builds on: [ADR-0114](./0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md)(더니어는 틀만, 서버는 Rust),
  [ADR-0115](./0115-stewards-decide-lineage-items-through-one-api-and-decisions-fold-into-the-lakehouse.md)(첫 기능: 필지 계보 검토),
  [ADR-0080](./0080-the-issuer-answers-on-every-loopback.md)·[ADR-0081](./0081-identity-endpoints-derive-from-one-contract.md)(신원 운영)

## Context

- 직원 로그인은 Zitadel 이 한다(ADR-0080). Foundation API 는 직원의 JWT 접근 토큰을 스스로 검증하고, 권한은 신원
  플랫폼의 정책 결정 API 에 묻는다. 토큰에는 `principal_kind = staff` 청구가 있어야 한다(Zitadel 액션).
- 브라우저 앱이 토큰을 직접 들면, 페이지에 끼어든 스크립트(XSS)가 토큰을 읽어 갈 수 있다. IETF 의 "OAuth 2.0 for
  Browser-Based Applications" 는 가장 안전한 형태로 BFF(Backend for Frontend: 토큰은 서버가 들고 브라우저는
  세션 쿠키만 가진다)를 권한다. 사내 선례도 같다: 공짱 웹(Next.js 경로 핸들러)과 새날 임시 어드민
  (`interim-admin`, 그 ADR-0003) 이 이 방식이다.
- 사용자 결정(2026-09-30): 더니어의 BFF 는 Rust 로 짓는다(ADR-0114 §4 유지). 우선 직원 노트북에서만 쓴다.
- 운영 신원(ai-server)의 issuer 는 루프백 `http://127.0.0.1:18453` 이고 공개 https issuer 는 아직 없다(ADR-0080 §6).
  직원(사람) 토큰 경로는 운영에서 검증된 적이 없다(ADR-0081 남은 부채).

## Decision

1. **더니어 서버(Rust)가 BFF 다.** 인가 코드 + PKCE 로 Zitadel 에 로그인하는 기밀 클라이언트(client secret)이고,
   접근·갱신 토큰은 서버의 세션 저장소에만 있다. 브라우저에는 무작위 세션 id 를 담은 `HttpOnly`·`SameSite=Lax`
   쿠키만 간다(https 에서는 `Secure`). JavaScript 는 토큰을 볼 수 없다.

2. **더니어는 정해진 길만 중계한다.** 브라우저의 `/api/...` 요청을 Foundation API 의 허용 목록 경로로만 넘기고,
   그때 세션의 접근 토큰을 싣는다. 목록에 없는 경로는 404 다. 판단·검증·권한은 전부 Foundation 과 신원 플랫폼이
   한다(ADR-0114 §3) — 더니어 서버에는 업무 규칙이 없다.

3. **상태를 바꾸는 요청은 CSRF 를 막는다.** 로그인 때 세션마다 무작위 CSRF 값을 만들고, 화면은 그 값을 머리글로
   실어야 한다. `SameSite=Lax` 와 같은 출처 검사를 함께 쓴다. `Idempotency-Key` 같은 업무 머리글은 그대로 넘긴다.

4. **Zitadel 앱은 코드로 만든다.** 직원 콘솔 앱 목록의 정본은 `platforms/identity-platform/config/
   staff-console-applications.v1.json` 이고, `configure-zitadel.sh` 가 멱등으로 만들고 맞춘다(웹 앱, 코드 + 갱신
   토큰, JWT 접근 토큰, basic 인증). client secret 은 한 번만 보이므로 곧바로 0600 파일로 가고 화면에 찍지 않는다.
   토큰 요청에는 프로젝트 audience 범위(`urn:zitadel:iam:org:project:id:<id>:aud`)를 넣는다 — 없으면 Foundation 이
   `aud` 로 거절한다.

5. **첫 판은 노트북에서 돈다.** 더니어 서버는 `http://127.0.0.1:3120` 에서 뜨고, 운영 서버의 Zitadel(18453)과
   Foundation API(18080)에 SSH 로컬 포워딩으로 붙는다. 포트가 같으므로 토큰의 `iss` 가 운영의 기대값과 그대로 맞는다.
   로그인 서버·API 를 노트북에 따로 띄우지 않으니 노트북 부담이 없고, 운영의 직원 토큰 경로를 실물로 검증한다.
   Zitadel 앱은 이 때문에 개발 모드(http 루프백 리디렉트 허용)다 — 공개 https 주소로 옮길 때 끈다.

6. **사람 계정과 역할은 사람이 만든다.** 직원 본인이 Zitadel 에서 자기 계정을 만들고(비밀번호는 본인만 안다), 그
   `sub` 로 신원 플랫폼의 직원 행을 만든다. 첫 `MASTER_ADMIN` 은 신원 API 의 부트스트랩 환경 변수로, 그 뒤의 역할은
   `POST /identity/v1/staff/{id}/roles` 로 준다. 절차는 런북에 둔다.

7. **세션 저장소는 첫 판에서 메모리다.** 노트북 한 대, 프로세스 하나라서다. 더니어가 서버로 옮겨 여러 벌로 뜨면
   공유 저장소(Postgres)로 바꾼다 — 저장소는 트레이트 하나 뒤에 둔다.

## Consequences

- 더니어는 `consoles/dawneer` 에 Rust 서버(`dawneer-server`)와 화면(React·TypeScript)을 둔다. 화면의 API 타입은
  Foundation 의 OpenAPI 문서에서 생성하고, 생성물이 계약과 다르면 검사가 실패한다.
- 운영 신원의 "직원 경로 미검증" 부채가 첫 로그인으로 닫힌다.
- 공개 접속(Cloudflare Access 등)과 https issuer 는 이 ADR 밖이다. 옮길 때 §5 의 개발 모드와 §7 의 메모리 세션을 바꾼다.
- 기각: Next.js BFF(사내 선례는 있으나 사용자 결정으로 Rust), 브라우저가 토큰을 직접 드는 SPA(XSS 에 토큰 노출),
  매 요청 토큰 조회(introspection, 가용성이 Zitadel 에 묶임 — 새날 ADR-0003 과 같은 이유).
