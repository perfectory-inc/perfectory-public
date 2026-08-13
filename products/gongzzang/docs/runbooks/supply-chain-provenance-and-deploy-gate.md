---
status: current
owner: gongzzang-제품
doc_type: runbook
last_reviewed: 2026-07-29
---

# 공급망 원천 게이트

## 현재 계약

공개 저장소는 원천·의존성 무결성을 검증하지만 운영 배포 admission 경로를 포함하지 않는다.

- `.github/workflows/gongzzang-ci.yml`은 `cargo-deny` 의존성 정책 gate와 Gongzzang 검증/guardrail job을 실행한다.
- `.github/workflows/secret-scan.yml`은 루트 `.gitleaks.toml` 설정으로 worktree와 Git history에 gitleaks를 실행한다.
- Third-party Action은 불변 commit SHA로 고정하고 의존성 갱신 pull request로 검토한다.
- `cargo xtask verify gongzzang`은 로컬·CI 검증 진입점이다.

기계 판독 정책은
[`supply-chain-policy.v1.json`](../architecture/platform-integration/supply-chain-policy.v1.json).

## 운영 승격

릴리스 provenance·SBOM 증명·서명·운영 배포 승인은 출시 전에
[ADR 0044](../adr/0044-bazel-transition-reconciliation.md)에 따라 별도 운영 gate로 설계한다. 향후
production promotion gate는 실제 배포 target과 threat model에서 출발해야 한다. 새 ADR, protected
environment, 최소권한 identity, artifact identity 계약, rollback 절차, 검증 증거가 필요하다.

과거 workflow·script 이름은 실행 가능한 런북이 아니므로 저장소에 다시 복사하지 않는다.
