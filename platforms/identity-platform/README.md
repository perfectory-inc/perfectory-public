---
status: current
owner: identity-platform
doc_type: README
last_reviewed: 2026-11-05
---

# Identity Platform

직원(Staff)·서비스 신원의 인증과 policy decision API 를 담당하는 수평 플랫폼.
Staff 로그인/세션, workload(service) identity 발급·검증, 그리고 다른 영역이
소비하는 인가 정책 결정(policy decision)을 이 영역이 소유한다. 제품(예:
Gongzzang)의 B2C 사용자 인증은 여기 소관이 아니다 — 각 제품이 소유한다.

구성: `services/identity-api` (세션·정책 결정 API), `services/identity-policy-worker`
(Identity outbox 이벤트 발행 워커), `crates/{staff-identity, service-identity, authorization, ...}`,
`tools/{identity-database-migrator, identity-service-provisioner}`. Cargo workspace 멤버의 정본은
[`Cargo.toml`](./Cargo.toml)이다.

## 책임 트리

```text
identity-platform/
├── crates/staff-identity/       직원 세션·ZITADEL subject 연결
├── crates/service-identity/     workload 자격 검증·서비스 호출 인가
├── crates/authorization/        role·정책 결정
├── services/identity-api        HTTP API
├── services/identity-policy-worker  outbox 이벤트 발행 워커
└── tools/                       DB 마이그레이터·workload 주체 프로비저너
```

문서 지도: [Identity docs](./docs/README.md) ·
[전체 문서 색인 생성 안내](../../docs/README.md)

## Quick start

```bash
cp .env.example .env   # placeholder 값을 로컬 시크릿으로 교체
docker compose up
```

컨테이너 스모크 검증:

```bash
bash scripts/compose-smoke.sh -- start-all
```

## API

- OpenAPI 스펙: [docs/openapi/identity.v1.json](./docs/openapi/identity.v1.json)
- 모든 경로는 `/identity/v1/...` 네임스페이스를 사용한다.
- 헬스체크: `/healthz` (liveness) · `/readyz` (readiness)

## 운영

- 운영 런북 목록: [docs/runbooks/](./docs/runbooks/README.md) — workload identity 발급·회전,
  LAN 호스트 기동, 직원 콘솔 로그인 준비

## 규칙

- 모노레포 공통 규칙: 루트 [AGENTS.md](../../AGENTS.md) →
  [ADR-0001](../../docs/adr/0001-monorepo-governance-and-conventions.md)
- 영역 결정 이력: [docs/adr/](./docs/adr/README.md)
