# AGENTS.md — identity-platform

Identity Platform에서 작업하는 AI 에이전트 공용 진입점. 모노레포 공통 규칙은
[루트 AGENTS.md](../../AGENTS.md) →
[루트 ADR-0001](../../docs/adr/0001-monorepo-governance-and-conventions.md)이 SSOT이며,
이 파일은 그 위에 영역 규칙을 추가한다.

## 영역 정체

- 직원(Staff)·서비스(workload) 신원의 **인증**과 타 영역이 소비하는
  **정책 결정(policy decision) API**를 소유한다.
- IdP(Zitadel)는 이 영역이 래핑한다: staff는 `zitadel_subject`로 연결되며, 타 영역은
  Zitadel을 직접 참조하지 말고 이 영역의 published 계약(`/identity/v1/*`)을 소비하는
  방향으로 수렴한다 (전환기 예외는 루트 AGENTS.md 참조).
- 제품(예: Gongzzang)의 B2C 사용자 인증은 소관이 아니다 — 각 제품이 소유한다.

## BC 지도

- `crates/staff-identity/` — staff-identity-{domain, application, infrastructure}
- `crates/service-identity/` — service-identity-{domain, application, infrastructure}
- `crates/authorization/` — authorization-{domain, application, infrastructure}
- `crates/identity-contracts` · `crates/identity-shared-kernel`
- `services/identity-api`(인증·정책 결정 API) · `services/identity-policy-worker`(정책 반영 워커)
- `tools/identity-database-migrator` · `tools/identity-service-provisioner`

## 절대 규칙

- **최소권한 DB role 7종 구조 유지**: `identity_admin`(compose 부트스트랩 전용) +
  `identity_migrator` / `identity_api` / `identity_policy_worker` / `identity_provisioner` /
  `identity_recovery` / `identity_operations_admin`
  (`infra/compose/bootstrap-identity.sql`). 런타임 서비스가 admin 자격을 쓰게 만들지 말 것.
- 워크스페이스 린트 `unwrap_used`·`expect_used`·`panic`·`todo`·`unimplemented` 등 = **deny**
  (`Cargo.toml` — dbg/print 계열 포함). 우회(`#[allow]`) 금지.
- HTTP 경로는 `/identity/v1/*` 네임스페이스 고정. 헬스는 `/healthz`(liveness)·`/readyz`(readiness).
- 시크릿 하드코딩 금지 — compose는 `IDENTITY_*_PASSWORD` env 5종을 fail-fast로 요구한다
  (`.env.example` 참조).
- 타 영역 DB 직접 접근 금지. 영역 간 결합은 published HTTP 계약/이벤트만 (루트 ADR-0001).

## 검증 명령

```bash
# 모노레포 루트에서 — CI와 동일한 fmt+clippy+test (Docker 필요)
bash scripts/verify/cargo-verify.sh platforms/identity-platform

# 이 디렉토리에서 — 컨테이너 스모크 (부트스트랩→마이그레이션→grants→API+worker)
bash scripts/compose-smoke.sh -- start-all
```

## 문서 라우팅

- [README.md](./README.md) — quick start + API 개요
- [docs/openapi/identity.v1.json](./docs/openapi/identity.v1.json) — published 계약
  (staff sessions verify/revoke, staff roles, policy decisions + 헬스 2종)
- [docs/runbooks/workload-identity-provisioning.md](./docs/runbooks/workload-identity-provisioning.md)
  — workload identity 발급/회전 절차
- [docs/adr/](./docs/adr/README.md) — 영역 결정 기록 (0001 = 루트 컨벤션 상속)
