# ADR 0033 - 핵심 아키텍처 강제

| Field | Value |
|---|---|
| Date | 2026-05-11; amended 2026-07-15 |
| Status | Accepted with product-first scope |
| Architecture | [ADR 0048](./0048-horizontal-platform-redefinition.md) |

## 배경

원래 결정은 광범위한 아키텍처 가드 7개를 제안했다. 경험상
showed that broad registries and self-validating evidence systems added ceremony
without protecting product behavior. The product-first rules in `AGENTS.md`
supersede that approach.

## 결정

입증된 실패 mode에 연결된 집중 enforcement만 남긴다.

| Enforcement | Real failure prevented |
|---|---|
| Cargo/package dependency direction | Product or domain code importing another platform's internals |
| Foundation ownership boundary check | Catalog clients, ETL, or canonical tables returning to Gongzzang |
| Fresh migration smoke | Deleted legacy tables or missing final tables entering a new deployment |
| API/event contract tests | Producer and consumer silently disagreeing on a published wire contract |
| gitleaks and dependency audits | Secrets or known-vulnerable dependencies entering source control |
| formatter, clippy, typecheck, and focused tests | Build and behavior regressions in changed code |
| file-size limit | Unreviewable source files growing beyond the repository rule |

다른 guard·registry·checklist를 검증하기 위한 목적으로만 guard를 만들지 않는다.
evidence bundle. A new guard requires one sentence naming the real incident it
prevents.

## 현재 강제 출처

- `AGENTS.md` defines repository-wide product-first and boundary rules.
- `docs/architecture/foundation-platform-boundary.v1.json` defines Foundation
  ownership for the focused boundary check.
- Cargo manifests define compile-time package dependencies.
- SQL migrations define the database schema.
- API/event schemas and consumer tests define published contracts.

## 영향

Architecture remains machine-enforced where a bypass can damage product data,
security, or runtime behavior, while obsolete governance ceremony is deleted
instead of renamed.
