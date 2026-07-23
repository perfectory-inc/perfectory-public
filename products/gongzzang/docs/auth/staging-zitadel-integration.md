# Staging Identity Integration Contract

## Status

Real-provider staging verification is a deployment-owned launch gate. Normal
pull-request verification uses deterministic local identity fixtures and does
not require repository secrets or a live identity tenant.

## Required Staging Proof

Before production promotion, an authorized operator must prove all of the
following against a dedicated non-production tenant:

1. obtain a standards-compliant OIDC access token through an approved machine
   or test-user flow;
2. verify issuer, audience, signature, expiry, and required claims in the
   deployed API;
3. exercise an allowed request and a denied request end to end;
4. prove key rotation and provider unavailability fail closed;
5. record redacted evidence outside the public source repository.

The staging workflow, tenant identifiers, client identifiers, and credentials
belong to the private deployment boundary. If automation is added later, it
must use a protected environment, short-lived credentials, idempotent setup,
and explicit cleanup. Its path is not part of this public contract.

## Separation From Pull Requests

Untrusted pull-request code must never receive staging credentials. Repository
CI may validate token-verifier behavior with fixtures, but passing fixture tests
does not count as the real-provider launch proof.
