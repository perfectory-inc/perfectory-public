# Workload Identity Provisioning

## Purpose

Identity Platform is the only owner of workload principal IDs, capability grants, and the mapping
from an environment's signed ZITADEL subject to a principal. Consumer services never write the
Identity database and never repeat capability policy in deployment configuration.

## Sources Of Truth

| Fact | Source of truth | Deployment form |
|---|---|---|
| Principal ID, display name, exact capabilities | `config/workload-principal-policy.v1.json` | Compiled into the reviewed provisioner binary |
| ZITADEL subject for one environment | Secret-managed binding document | Read-only file mounted into the one-shot job |
| Provisioned state | `identity.service_principal` and `identity.service_capability_grant` | Written in one PostgreSQL transaction |
| Contract parser and reconciliation behavior | `tools/identity-service-provisioner` | Versioned Rust code and tests |

The example binding document contains invalid placeholders by design. It cannot provision an
environment. Never commit a real ZITADEL subject or duplicate the capabilities in a binding file.

## Required Deployment Inputs

- `IDENTITY_PROVISIONER_PASSWORD`: secret for the dedicated `identity_provisioner` database role.
- `IDENTITY_WORKLOAD_PRINCIPAL_BINDINGS_FILE`: host path to a strict
  `identity.workload-principal-bindings.v1` document delivered by the environment's secret manager.
- The reviewed Identity runtime image. The image contains the policy artifact and provisioner.

The provisioner role has only these database capabilities:

- `SELECT`, `INSERT`, and `UPDATE` on `identity.service_principal`;
- `SELECT`, `INSERT`, and `DELETE` on `identity.service_capability_grant`;
- no staff, session, outbox, schema-create, database-create, or role-management access.

## Deployment Order

The deployment controller executes these jobs in order and stops on the first failure:

1. `identity-bootstrap` creates hardened roles and database connectivity.
2. `identity-database-migrator` applies the reviewed schema.
3. `identity-runtime-grants` grants exact runtime and provisioner privileges.
4. `identity-workload-provisioner` validates and resolves policy plus environment bindings, then
   reconciles all listed principals in one transaction.
5. `identity-finalize` revokes temporary database creation rights and verifies role hardening.
6. Identity API and policy worker may start only after the chain succeeds.

For the local Compose contract, configure the five required password secrets from `.env.example`
(`IDENTITY_{ADMIN,MIGRATOR,API,POLICY_WORKER,PROVISIONER}_PASSWORD`), point
`IDENTITY_WORKLOAD_PRINCIPAL_BINDINGS_FILE` at a real local binding file, then run:

```bash
scripts/compose-smoke.sh -- start-all
```

The command returns a secret-safe JSON report containing only status and row counts. Binding paths,
database URLs, ZITADEL subjects, and tokens are never emitted.

## Failure And Retry Semantics

- Unknown fields, unsupported versions, placeholders, duplicate subjects, missing services, and
  capability drift fail before a database connection is used.
- All principal and capability changes share one PostgreSQL transaction. A subject collision or any
  SQL failure rolls back every principal in that run.
- Re-running an identical policy and binding is idempotent.
- Capabilities for a listed principal are synchronized exactly; removed capabilities are revoked.
- A principal removed from policy is not silently deleted. Principal retirement is an explicit,
  separately reviewed revocation operation.

## Change And Rollback Procedure

1. Change the policy artifact in Identity Platform only and review the capability delta.
2. Run unit, strict Clippy, disposable PostgreSQL, and Compose contract tests.
3. Build one immutable runtime image and record its digest.
4. Version the environment binding in the secret manager without committing it.
5. Run the one-shot deployment chain before starting consumers.
6. Verify principal/grant counts and the consuming service's signed-token authorization smoke.

Rollback uses the previously recorded image digest and previous secret-manager binding version, then
reruns the same idempotent provisioner. Never repair grants with ad hoc SQL or consumer-service DB
access.

## Verification Evidence

- `manifest_contract.rs` proves strict versioning, exact binding coverage, placeholder rejection, and
  the committed least-privilege policy.
- `live_provisioning.rs` proves idempotency, exact grant removal, empty-grant revocation, and complete
  rollback on a unique-subject collision against disposable PostgreSQL.
- `scripts/compose-smoke.sh` proves the deployment order, dedicated-role ACL, principal/grant rows,
  helper non-root UID, rerun behavior, and runtime credential isolation.
