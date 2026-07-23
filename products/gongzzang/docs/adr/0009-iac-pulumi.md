# ADR 0009 - Pulumi For Gongzzang Infrastructure

| Field | Value |
|---|---|
| Date | 2026-05-01 |
| Status | Accepted |
| Decision owner | Gongzzang |

## Context

Product infrastructure needs reviewable desired state, environment separation,
and drift detection. Console-only changes cannot provide those guarantees.

## Decision

Gongzzang-owned cloud infrastructure is declared with Pulumi and TypeScript in
`infrastructure/`. `infrastructure/Pulumi.yaml`, `infrastructure/index.ts`, and
the package lock are the source-controlled definition. Runtime secrets are
supplied through an approved secret store or encrypted Pulumi configuration and
must never be committed.

Development, staging, and production use separate stacks. A change is reviewed
with `pulumi preview` before an authorized operator applies it. The public
repository contains no workflow that is authorized to mutate production.

## Alternatives

- OpenTofu/Terraform remains a viable migration target if portability or state
  ownership outweighs the TypeScript reuse benefit.
- AWS CDK and CloudFormation increase AWS coupling.
- Crossplane is deferred because the project does not require Kubernetes as an
  infrastructure control plane.

## Consequences

- Desired infrastructure changes remain code-reviewed and reproducible.
- Pulumi state and credentials are operational assets outside the public source
  tree.
- A future deployment workflow requires a separate security decision, explicit
  environment protection, and least-privilege credentials; its filename is not
  part of this ADR.

## References

- `infrastructure/README.md`
- [Pulumi documentation](https://www.pulumi.com/docs/)
