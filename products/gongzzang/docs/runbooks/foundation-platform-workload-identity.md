# Foundation Platform Workload Identity Runbook

## Scope

Gongzzang services call `foundation-api` with a short-lived Zitadel workload
bearer loaded from the file named by
`FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE`. Static service-token
fallback is forbidden.

The active callers are:

- `gongzzang-api` for published Catalog reads.
- `gongzzang-outbox-publisher` for the Lakehouse Registry contract.

## Runtime Contract

- Identity Platform issues or refreshes the workload credential.
- The deployment mounts the credential as a file readable only by the caller.
- Gongzzang reads the file before each Foundation Platform request, so rotating
  the file does not require changing process configuration.
- Foundation Platform validates the bearer and applies its default-deny
  authorization policy for the caller and requested resource.
- Gongzzang never sends authorization scope or policy decisions in custom
  headers; the identity and authorization owners derive them from the bearer.

## Rotation

1. Issue a replacement workload credential for the same service identity.
2. Atomically replace the mounted credential file.
3. Confirm one allowed Foundation Platform request succeeds from the caller.
4. Confirm one request outside the caller's allowed contract is denied.
5. Revoke the previous credential after both checks pass.

## Failure Handling

- Missing, empty, or unreadable token file: fail startup or the request before
  contacting Foundation Platform.
- Rejected bearer: stop retrying authentication failures and rotate or repair
  the workload credential.
- Foundation Platform unavailable: use the caller's timeout and circuit breaker;
  do not switch to a static token.

## Evidence

Record the deployment revision, caller service identity, credential rotation
time, allowed-call result, denied-call result, and correlation ID. Never record
the bearer value or token-file contents.
