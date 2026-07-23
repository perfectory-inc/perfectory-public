# Identity Platform

직원(Staff)·서비스 신원의 인증과 policy decision API 를 담당하는 수평 플랫폼.
Staff 로그인/세션, workload(service) identity 발급·검증, 그리고 다른 영역이
소비하는 인가 정책 결정(policy decision)을 이 영역이 소유한다. 제품(예:
Gongzzang)의 B2C 사용자 인증은 여기 소관이 아니다 — 각 제품이 소유한다.

구성: `services/identity-api` (인증·정책 결정 API), `services/identity-policy-worker`
(정책 반영 워커), `crates/{staff-identity, service-identity, authorization, ...}`.

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

- Workload identity 발급/회전 절차:
  [docs/runbooks/workload-identity-provisioning.md](./docs/runbooks/workload-identity-provisioning.md)

## 규칙

- 모노레포 공통 규칙: 루트 [AGENTS.md](../../AGENTS.md) →
  [ADR-0001](../../docs/adr/0001-monorepo-governance-and-conventions.md)
- 영역 결정 이력: [docs/adr/](./docs/adr/README.md)
