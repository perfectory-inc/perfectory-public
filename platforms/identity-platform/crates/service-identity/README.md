# service-identity

서비스 workload identity의 발급·검증·회전을 담당합니다. 최소권한 role과 workload
subject 경계는 Identity Platform이 소유합니다.

- 운영 절차: [`docs/runbooks/workload-identity-provisioning.md`](../../docs/runbooks/workload-identity-provisioning.md)
- 검증: `cargo test -p service-identity-domain`
