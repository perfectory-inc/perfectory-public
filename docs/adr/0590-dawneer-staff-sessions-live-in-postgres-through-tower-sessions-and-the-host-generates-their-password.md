# ADR 0590: 더니어 직원 세션은 tower-sessions 로 Postgres 에 두고, 그 역할의 비밀번호는 호스트가 만든다

- Status: Accepted (2026-11-19, 코디네이터 승인 — brief 「Reinvention and tool-underuse inventory」 항목 29 의 셋째)
- Date: 2026-11-19
- Supersedes (일부): [ADR-0116](./0116-dawneer-keeps-staff-tokens-on-its-rust-server.md) §7(첫 판의 메모리 세션). 나머지는 그대로다.

## Context

ADR-0116 §7 은 세션 저장소를 "첫 판에서 메모리"로 두고, 더니어가 서버로 옮기면 바꾼다고 적었다(같은 ADR 「Consequences」). 더니어는 ADR-0434 로
데이터 호스트에서 돈다. 그래서 컨테이너가 다시 뜰 때마다(릴리스마다) 모든 직원이 로그아웃된다. 재발명 목록 항목 29 는 이것을 nonce·ID 토큰 검증과
함께 보안 결함으로 묶었고(#1479 가 앞의 둘을 고쳤다), 정한 도구는 `tower-sessions` 와 Postgres 저장소다.

상류에서 읽은 것(2026-11-19):

- `tower-sessions-sqlx-store` 0.16.0 `postgres_store.rs`: 표는 `id text`·`data bytea`·`expiry_date timestamptz`, 기본 스키마 `tower_sessions`·표 `session`.
  `migrate()` 는 `CREATE SCHEMA IF NOT EXISTS` 와 `CREATE TABLE IF NOT EXISTS` 를 한 트랜잭션에서 돈다. 만료된 줄은 읽을 때 거르고
  `ExpiredDeletion::continuously_delete_expired` 가 지운다.
- Postgres 17 에서 데이터베이스 CREATE 권한이 없는 역할의 `CREATE SCHEMA IF NOT EXISTS` 는 스키마가 이미 있어도 `permission denied for database` 로
  거부된다(일회용 `postgres:17-alpine` 에서 측정).

## Decision

1. **세션과 진행 중인 로그인은 `tower-sessions` 저장소의 레코드다.** 호스트에서는 `PostgresStore`(표 `tower_sessions.session`), 시험과 로컬 http 실행에서는
   `MemoryStore`. 레코드의 열쇠는 브라우저가 가진 무작위 값(로그인의 `state`, 세션 쿠키)의 SHA-256 앞 128비트다: 표에는 누군가를 로그인시킬 값이 없다.
   만료는 레코드의 `expiry_date`(세션 12시간, 로그인 10분)이고, 만료된 줄은 한 시간마다 지운다. 쿠키는 #1479 그대로 axum-extra `CookieJar` 가 만든다.
2. **https 로 뜨는 더니어는 `DAWNEER_DATABASE_URL` 없이 시작하지 않는다.** 메모리 세션은 http(로컬·시험)에서만이다.
3. **역할 `dawneer_session` 은 스키마 하나와 표 하나만 가진다.** compose 부트스트랩(`bootstrap-foundation.sql`)이 역할을 만들고, 그 역할이 주인인
   `tower_sessions` 스키마와 저장소 모양의 `session` 표를 만든다. 데이터베이스에는 CONNECT 뿐이고 다른 어떤 스키마에도 권한이 없다. 더니어는
   `migrate()` 를 부르지 않는다(위 측정: 그 역할로는 실패한다). 비밀번호가 없는 곳(staging smoke)에서는 역할이 로그인할 수 없다.
4. **비밀번호는 사람이 아니라 호스트가 만든다.** 런타임 비밀 계약(`config/runtime-secrets.contract.json`)의 `recovery` 그룹이
   `DAWNEER_SESSION_DATABASE_PASSWORD` 를 갖고 `generated` 로 적는다. 릴리스는 환경 검사 전에 `runtime_secrets.py generate` 를 돌려, 파일에 그 이름이
   없으면 무작위 64자 hex 를 쓴다(있으면 그대로, 값은 출력하지 않는다). 부트스트랩과 더니어 컨테이너만 그 값을 받는다. 바깥 누구도 알 필요가 없는
   값이라 소유자 단계가 없다.

## Consequences

- 릴리스·재시작이 직원을 로그아웃시키지 않는다. 더니어를 여러 벌 띄울 수도 있다.
- `generated` 는 같은 종류(호스트의 두 서비스만 나누는 비밀번호)에 다시 쓸 수 있다. 바깥에서 받는 값(API 키·토큰)은 여전히 `env-set` 이다.
- 저장소 판이 표 모양을 바꾸면 부트스트랩의 표도 같이 바꿔야 한다(`migrate()` 를 부르지 않으므로). 버전은 `consoles/dawneer/Cargo.toml` 에 고정돼 있다.
- 더니어 컨테이너 메모리 상한을 32m 에서 48m 으로 올린다(연결 풀 최대 넷).
- 배포 뒤 확인: 로그인 → 컨테이너 재시작 → `GET /api/session` 이 여전히 200.

## 출처

- ADR-0116 §7·「Consequences」, ADR-0434, ADR-0153(런타임 비밀 계약), ADR-0247(`env-set`).
- `tower-sessions` 0.15.0, `tower-sessions-core` 0.15.0, `tower-sessions-memory-store` 0.15.0, `tower-sessions-sqlx-store` 0.16.0(`postgres_store.rs`, `Cargo.toml`).
- PostgreSQL 17 「CREATE SCHEMA」(데이터베이스 CREATE 권한), 측정 2026-11-19.
