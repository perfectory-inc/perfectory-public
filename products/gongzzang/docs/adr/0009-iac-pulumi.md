# ADR 0009 - Gongzzang 인프라에 Pulumi 사용

| Field | Value |
|---|---|
| Date | 2026-05-01 |
| Status | Accepted |
| Decision owner | Gongzzang |

## 배경

Product infrastructure needs reviewable desired state, environment separation,
and drift detection. Console-only changes cannot provide those guarantees.

## 결정

Gongzzang-owned cloud infrastructure is declared with Pulumi and TypeScript in
`infrastructure/`. `infrastructure/Pulumi.yaml`, `infrastructure/index.ts`, and
the package lock are the source-controlled definition. Runtime secrets are
supplied through an approved secret store or encrypted Pulumi configuration and
must never be committed.

Development, staging, and production use separate stacks. A change is reviewed
with `pulumi preview` before an authorized operator applies it. The public
repository contains no workflow that is authorized to mutate production.

## 대안

- OpenTofu/Terraform remains a viable migration target if portability or state
  ownership outweighs the TypeScript reuse benefit.
- AWS CDK and CloudFormation increase AWS coupling.
- Crossplane is deferred because the project does not require Kubernetes as an
  infrastructure control plane.

## 영향

- Desired infrastructure changes remain code-reviewed and reproducible.
- Pulumi state and credentials are operational assets outside the public source
  tree.
- A future deployment workflow requires a separate security decision, explicit
  environment protection, and least-privilege credentials; its filename is not
  part of this ADR.

## 참고 문서

- `infrastructure/README.md`
- [Pulumi documentation](https://www.pulumi.com/docs/)
