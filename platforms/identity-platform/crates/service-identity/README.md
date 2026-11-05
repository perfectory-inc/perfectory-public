---
status: current
owner: identity-platform
doc_type: README
last_reviewed: 2026-11-05
---

# service-identity

서비스 workload 자격(ZITADEL 서비스 토큰) 검증과 서비스 호출 인가를 담당합니다. 최소권한 role과
workload subject 경계는 Identity Platform이 소유하며, 주체 발급·회전은 아래 운영 절차가 다룹니다.

- 운영 절차: [`docs/runbooks/workload-identity-provisioning.md`](../../docs/runbooks/workload-identity-provisioning.md)
- 검증: `cargo test -p service-identity-domain`
