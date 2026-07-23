# ADR-0049: Identity-Platform Contract Design

| Field | Value |
|---|---|
| Date | 2026-07-02 |
| Status | Accepted |
| Decision owner | perfectoryinc |
| Related | ADR-0030, ADR-0031, ADR-0048, foundation implementation ADR-0021, foundation implementation ADR-0023 |

> This ADR is the authoritative Identity Platform contract implementing ADR-0048. It defines the
> *contract*: ownership boundary, v1 API/event
> surfaces, service-identity staging, and the authorization model. It does
> **not** authorize DB migration, repo split, or new infrastructure — those
> remain separately approval-gated.

## Context

ADR-0048 redefined the cross-repo architecture as horizontal platforms and
assigned shared identity to `identity-platform`:

- staff identity
- service identity and service tokens
- session verification
- role/permission/policy model
- cross-service authorization contracts
- audit principal resolution
- identity-related outbox/events

Today every one of those responsibilities is implemented inside the legacy
core repository (the transitional physical home of `foundation-platform`).
ADR-0048's migration strategy requires identity responsibilities to move
behind identity-platform *contracts* before any physical repository split.
This ADR is that contract.

Two constraints shape the design:

1. **Product-first / YAGNI** (AGENTS.md top rule). The platform has zero
   users. The v1 contract is therefore the surface that *exists today*,
   renamed and versioned — not a new engine. Heavier machinery (ReBAC
   authorization engine, SPIFFE workload identity infrastructure) is
   designed here only as named, trigger-gated future stages.
2. **Infrastructure quality is non-negotiable** (owner directive,
   2026-07-02). The contract must be aligned with published enterprise
   practice, so the staging targets below cite what Google, Airbnb, Uber,
   and Netflix actually run (see References).

### Current state (code-verified 2026-07-02)

**Staff identity** — implemented in the legacy repo, `crates/workforce/*`:

- Aggregates: `Staff` (id `StaffId`/UUID, unique `zitadel_subject`, email,
  display_name, primary_role_code, version), `StaffRole` (staff_id,
  role_code matching `[A-Z0-9_]+`, granted_at, granted_by), `StaffSession`
  (session_id, staff_id, unique `jti`, issued_at, expires_at).
- IdP: Zitadel (OIDC). `HttpZitadelClient` verifies ID tokens against
  cached JWKS (RS256/384/512, ES256/384) and extracts
  sub/email/name/jti/iat/exp/roles. JTI revocation uses the
  `workforce.revoked_jti` table.
- Use cases: `VerifyStaffSession` (token → staff + session + roles),
  `AssignStaffRole` (only `MASTER_ADMIN` may grant, via
  `can_grant_roles()`), `BootstrapPlatformAdmin` (idempotent first admin;
  runs as an identity-platform-internal startup routine — no HTTP route,
  not a cross-service surface).
- HTTP: `POST /workforce/v1/sessions/verify` (id_token → staff_id,
  session_id, email, display_name, roles[], expires_at) and
  `POST /workforce/v1/staff/{id}/roles` (Bearer actor token;
  400/401/403/404/409). OpenAPI `docs/openapi/workforce.v1.yaml`,
  operationIds `verifySession` / `assignRole`.
- Events (shared-kernel `workforce_v1.rs`, compatibility corpus):
  `workforce.staff.invited.v1`, `workforce.staff.role_assigned.v1`,
  `workforce.staff.session_revoked.v1` (reason:
  logout|admin_revoke|role_changed|security), published via
  `workforce.outbox_event`.
- DB schema `workforce.*`: staff, staff_role, staff_session, revoked_jti,
  outbox_event.
- Error model: StaffNotFound, DuplicateZitadelSubject, DuplicateRole,
  RoleNotFound, SessionExpired, JtiRevoked, InvalidClaims,
  PermissionDenied, Infrastructure → 400/401/403/404/409/500.

**Service identity** — `services/gongzzang-api/src/routes/service_identity.rs`:

- Consumers today: gongzzang (`gongzzang-api` catalog:read,
  `gongzzang-worker` lakehouse:write), dawneer (`dawneer-api`
  catalog:read), intelligence-platform (normalization:propose).
- Mechanism: static bearer token or workload-identity token *file* re-read
  per request (preferred over static). Token comparison is constant-time.
  Metadata headers per family: policy-id, source, target, allowed-call-id
  always required; scope required on the dawneer and intelligence lanes,
  optional-but-validated-when-present on gongzzang lanes. Routes are
  deny-by-default via the `SERVICE_IDENTITY_ROUTES` table (parcel/building
  catalog reads, lakehouse artifact writes, normalization proposal
  submission).
- Header families: `x-gongzzang-*`, legacy `x-foundation-platform-*`, preferred
  `x-foundation-platform-*` (added 2026-07-02). Policy-id/target *values*
  intentionally still carry pinned legacy contract IDs until a
  versioned-contract slice.
- Gongzzang client side (`crates/auth/src/foundation_platform_service.rs`)
  already enforces token metadata discipline: token minimum length (16
  chars, client-enforced), scope, issued_at, expires_at (RFC 3339, TTL
  capped at 90 days), rotation_owner; env aliases prefer
  `FOUNDATION_PLATFORM_*`. No server-side length check exists in v1.
- Policy registries (JSON): `foundation_platform.traffic_auth_policy_registry.v1`
  (4 consumer policies, deny default) and
  `gongzzang.traffic_auth_policy_registry.v1` (exposure classes
  public_derived/authenticated_user/privileged/service_to_service).

**Foundation-platform's dependence on identity** — Catalog normalization
commands carry `reviewer_staff_id` / `applied_by_staff_id` /
`rolled_back_by_staff_id` as audit principals, and Catalog uses an ACL
adapter (`ActorDto`, `workforce_acl.rs`) so it never imports workforce
domain types. This existing pattern is blessed below as the standard
boundary shape.

## Decision

### 1. Ownership boundary

`identity-platform` owns:

- Staff/admin identity lifecycle (invite, role grant, session, revocation)
- Staff session verification and JTI revocation state
- Service identity: service principals, token/verification rules, and the
  shared cross-service traffic-auth policy registry
- The role/permission model (role codes today; richer models later)
- Cross-service authorization contracts (who may call what, deny default)
- Audit principal resolution (opaque principal id → human-renderable
  identity)
- Identity events and their outbox

`identity-platform` does **not** own:

- Gongzzang B2C product users, product sessions, or product auth flows.
  They remain `gongzzang`-owned; moving them requires a separate ADR
  (ADR-0048 rule restated).
- Authentication itself. Zitadel remains the IdP (OIDC issuance, JWKS).
  identity-platform is the principal/policy/contract layer *on top* — a
  buy-authentication, own-authorization split, the same shape
  Zanzibar-style adopters use (see References).
- Product-local exposure policy. Product registries such as
  `gongzzang.traffic_auth_policy_registry.v1` stay product-owned; only
  their `service_to_service` class must reference identity-platform-owned
  policy IDs.
- Domain audit *records*. Owning platforms keep their own audit rows;
  identity-platform only resolves the principals in them.

**Principal-reference vs principal-resolution.** The Catalog `ActorDto`
ACL is the mandated pattern for every platform: an owning platform stores
principal *references* (opaque `staff_id` UUIDs such as
`reviewer_staff_id`) inside its own data, never imports identity domain
types, and calls identity-platform when it needs *resolution* (id →
email/display_name/roles) for rendering or verification. No platform other
than identity-platform may read or join `workforce.*` (future
`identity.*`) tables — cross-service direct DB access stays forbidden
(ADR-0048 non-goal).

### 2. Contract surfaces v1

The v1 contract is mechanically derived from the verified current surface.
No new capability is added except one minimal read (principal lookup),
which closes the loop the ActorDto pattern requires.

#### 2.1 Staff API — `identity-platform.staff.v1`

Successor to `workforce.v1` (OpenAPI successor document:
`docs/openapi/identity.v1.json` in the implementing repo):

| Operation | Route | Semantics |
|---|---|---|
| `verifySession` | `POST /identity/v1/sessions/verify` | id_token → staff_id, session_id, email, display_name, roles[], expires_at. Same JWKS verification, JTI revocation check, and error mapping as today. |
| `assignRole` | `POST /identity/v1/staff/{id}/roles` | Bearer actor token; only `MASTER_ADMIN` grants (`can_grant_roles()`); 400/401/403/404/409 unchanged. |
| `getStaffPrincipal` | `GET /identity/v1/staff/{id}` | **New, minimal.** staff_id → {staff_id, email, display_name, roles[]}. Read-only, for audit rendering by platforms holding principal references. Registered in the deny-by-default service route table like every other cross-service route. Scope name reserved: `identity:read`; policy-id and allowed-call-id are assigned in the implementation slice (this route returns staff PII cross-service, so it enters the same deny-by-default table). |

The error model (StaffNotFound … Infrastructure and its HTTP mapping) is
carried over unchanged as part of the v1 contract.

Compatibility rule: `POST /workforce/v1/sessions/verify` and
`POST /workforce/v1/staff/{id}/roles` remain accepted **aliases** of the
`/identity/v1/*` routes until every consumer compiles against the new
contract — the same alias-plus-telemetry discipline the naming migration
uses for `/foundation-platform/events` vs `/foundation-platform/events`. Alias
usage must be measurable before deprecation.

#### 2.2 Service-identity verification — `identity-platform.service-auth.v1`

The verification semantics are pinned as a contract, exactly as
implemented today (code-verified 2026-07-02,
`services/gongzzang-api/src/routes/service_identity.rs:350-384`):

- Credential: static bearer token **or** workload-identity token file
  re-read per request; file preferred when both are configured.
  Constant-time comparison. No server-side token-length enforcement in v1;
  tightening is a candidate for the implementation slice.
- Required metadata headers (per family): policy-id, source, target,
  allowed-call-id always required on all lanes. Scope header required on
  dawneer and intelligence lanes; optional but validated when present on
  gongzzang lanes. Mismatch or missing required header is a deny. The v1
  contract records this asymmetry as-is; uniform scope enforcement is
  deferred to the implementation slice.
- Deny-by-default: a route is callable service-to-service only if listed
  in the route policy table with a matching policy.
- Token metadata discipline (client side, `crates/auth/src/foundation_platform_service.rs:333-342`):
  minimum token length of 16 chars, plus scope, issued_at, expires_at in
  RFC 3339 with TTL capped at 90 days, and a named rotation_owner.

Registry ownership moves: the shared consumer policy registry (today
`foundation_platform.traffic_auth_policy_registry.v1`) becomes
identity-platform-owned, with successor ID
`identity-platform.traffic_auth_policy_registry.v1` published in a
versioned slice. Consuming platforms (foundation, gongzzang, dawneer,
intelligence) consume this registry; they do not fork or own it.

**No new header family now.** The existing header families are historical
wire prefixes: some are source-named (`x-gongzzang-*` names the calling
product), others are target-named (`x-foundation-platform-*` names the
destination API). There is no uniform naming rule across families. The
decision not to introduce `x-identity-platform-*` rests on a different
basis: no consumer need exists, and policy-id/allowed-call-id values are
pinned contract IDs that would break consumers if renamed. Adding a fourth
alias family for its own sake is pure ceremony. Likewise, policy-id/target
*values* keep their pinned legacy contract IDs until the versioned-contract
slice, because renaming values inside a pinned contract breaks consumers
without buying anything.

#### 2.3 Events — `identity-platform.staff.*.v1`

Successor event names, payloads field-for-field identical to today's
workforce corpus:

| Successor | Legacy alias | Payload |
|---|---|---|
| `identity-platform.staff.invited.v1` | `workforce.staff.invited.v1` | schema_version, staff_id, email, invited_at, invited_by |
| `identity-platform.staff.role_assigned.v1` | `workforce.staff.role_assigned.v1` | schema_version, staff_id, role_code, assigned_at, assigned_by |
| `identity-platform.staff.session_revoked.v1` | `workforce.staff.session_revoked.v1` | schema_version, staff_id, jti, revoked_at, reason: logout\|admin_revoke\|role_changed\|security |

Compatibility rule: the `workforce.*.v1` names remain the wire format
until a versioned publication slice; the successor names are reserved by
this ADR. When the switch happens, the compatibility corpus must cover
both names, consumers must accept both during the transition, and legacy
names are retired only per the sequencing in §5 step 7. Events continue to
flow through the existing outbox table (renamed only with the DB migration
approval in §5 step 5).

### 3. Service identity staging

Following SPIFFE's own adoption framing — static secrets → platform-issued
short-lived identities — service identity evolves in three stages. Stages
1–2 exist today; stage 3 is trigger-gated.

- **Stage 1 (today): static token + metadata discipline.** Static bearer
  tokens with constant-time compare, metadata headers (4 always required;
  scope lane-dependent), and the 90-day TTL cap + rotation_owner
  requirement. This is acceptable pre-launch because token count is small
  (4 consumers), rotation is owned, and TTL is bounded.
- **Stage 2 (today, partial): workload-identity token file.** Token read
  from a file per request, preferred over static env tokens. This
  decouples credential delivery from process environment and is the
  stepping stone to platform-issued credentials. New consumers should
  onboard at stage 2, not stage 1.
- **Stage 3 (trigger-gated): SPIFFE/SPIRE-style workload identity.**
  Short-lived, automatically rotated SVIDs (on the order of one hour) with
  mTLS, replacing shared secrets entirely. This is the CNCF-standardized
  model run in production at Uber and Netflix (see References).
  **Triggers:** Kubernetes adoption (which ADR-0046 itself defers behind
  its own triggers) or more than two deployment environments. Building
  SPIRE infrastructure before either trigger is the infra-before-users
  trap ADR-0044 reversed.
- **Delegation (trigger-gated): RFC 8693 token exchange.** When a service
  must act *on behalf of* a staff principal across a service boundary —
  e.g., foundation-platform proving to identity-platform (or an auditor)
  *which staff member* approved a normalization proposal — the answer is
  OAuth 2.0 Token Exchange with the `act` claim, which preserves the full
  delegation chain in the token itself instead of shipping raw staff
  tokens between services. **Trigger:** the first cross-service call that
  must carry a staff principal's authority rather than a mere audit
  reference. Until then, audit-reference fields (`reviewer_staff_id` etc.)
  are sufficient and correct.

### 4. Authorization model

**Decision: centralize authorization decisions in identity-platform; keep
the model itself deliberately small (deny-by-default route/policy registry
plus role codes); defer relationship-based access control behind named
triggers.**

*Why centralize:* Google's Zanzibar demonstrated at the largest published
scale that authorization as a dedicated, uniform service — rather than
per-service ad-hoc role checks — is what keeps policy consistent and
auditable across many products (Calendar, Cloud, Drive, Maps, Photos,
YouTube all call one system). The same conclusion drove Airbnb's Himeji
and the open-source successors (SpiceDB, OpenFGA, Ory Keto). Our v1
equivalent of "one place answers *may X do Y*" is: identity-platform owns
`verifySession`, the role model, and the traffic-auth policy registry;
other platforms *ask*, they never fork the policy. In v1, identity-platform
centralizes verification and policy *data* (session state, role model,
policy registry); per-request allow/deny evaluation runs inside each target
service's enforcement middleware. Full decision-as-a-service — where
callers ask identity-platform to evaluate a policy and return allow/deny —
is part of the later extraction, not v1.

*Why NOT ReBAC now:* Zanzibar exists to answer relationship questions
("is this photo shared with a group the viewer is in?") across billions of
objects. Our current authorization universe is a handful of role codes
(`[A-Z0-9_]+`) held by internal staff, plus four service-to-service
policies. Deploying a relationship-tuple engine for that is
Google-cosplay, not engineering — it violates the product-first rule.

*ReBAC adoption triggers* (adopt SpiceDB/OpenFGA-class engine, do not
build one): (a) fine-grained per-object sharing requirements — e.g.,
listing- or site-level grants to individual external users; or
(b) multi-tenant delegation — e.g., Dawneer B2B tenant admins managing
their own members' permissions per industrial complex or per site. Either
one makes role codes combinatorially explode, which is exactly the
signal that the model, not the enforcement point, must change. Because
decisions are already centralized behind identity-platform contracts, that
swap changes the engine behind the API, not the consumers.

### 5. Extraction sequencing

Refines the plan's seven steps. Cross-service direct DB access is
forbidden at every step. The physical repo split is *last*, per ADR-0048.

1. **Contract ADR** — this document. Names, surfaces, staging, and
   triggers are now decided; later slices implement, they do not
   re-decide.
2. **Publish read-only contracts as aliases.** The legacy repo keeps its
   DB and routes. `/identity/v1/*` routes, `identity.v1` OpenAPI, and the
   successor event names are added as aliases of the workforce
   implementations, with telemetry on alias usage. `workforce.v1` stays
   fully functional.
3. **Service-identity policy ownership moves.** The shared consumer policy
   registry is re-owned as `identity-platform.traffic_auth_policy_registry.v1`
   (versioned slice); foundation Catalog policy and product exposure
   registries reference it instead of embedding shared policy.
4. **Product/staff separation documented.** Gongzzang B2C users and
   product sessions are explicitly out of scope (this ADR, §1, is that
   documentation); staff/admin accounts, service principals, and
   cross-service permissions are identity-platform-owned.
5. **DB/API migration prepared — separately gated.** A `workforce.*` →
   `identity.*` schema migration plan requires its own owner approval
   before any migration is written. Compatibility views or dual-read only
   if an active consumer forces them.
6. **Consumer cutover.** Catalog admin routes verify staff/sessions via
   identity-platform contract names; gongzzang, dawneer, and intelligence
   consume the published identity APIs and successor event names.
7. **Legacy retirement.** `workforce.v1` routes, event names, and pins are
   removed only after all consumers have moved, tests cover both legacy
   and final names, alias telemetry shows zero legacy traffic, and
   rollback is documented.

Physical extraction of identity-platform into its own repository/deployment
happens only after steps 1–7 are stable, under a dedicated repo-local ADR
(ADR-0048 reassessment trigger).

## Non-Goals

- No move of Gongzzang B2C users, product sessions, or product auth flows
  (separate ADR required).
- No new IdP. Zitadel stays; this ADR adds no authentication technology.
- No ReBAC engine now (trigger-gated, §4) — and if triggered, adopt, don't
  build.
- No SPIFFE/SPIRE infrastructure now (trigger-gated, §3 stage 3).
- No immediate physical repo split or deployment change.
- No Kafka or Kubernetes requirement introduced by this contract.
- No new `x-identity-platform-*` header family (§2.2).
- No new CI guards or registries beyond what a real cutover slice needs at
  the moment it ships (product-first rule 3).

## Consequences

Positive:

- Identity has a named owner and a versioned contract before any code
  moves, so the eventual physical extraction is a re-homing of an already
  published API, not a redesign.
- Consumers (foundation Catalog admin, gongzzang, dawneer, intelligence)
  get one stable identity surface: session verification, role grant,
  principal lookup, service-auth policy — deny-by-default everywhere.
- The audit boundary is clean: platforms keep opaque principal references;
  only identity-platform resolves them. `getStaffPrincipal` closes the one
  gap that pattern had.
- Future hardening paths (SPIFFE, RFC 8693, ReBAC) are pre-decided with
  named triggers, so under pressure we upgrade deliberately instead of
  improvising.

Costs / risks:

- Alias duplication (workforce.v1 + identity.v1 routes and event names)
  must be carried until cutover, with telemetry, tests, and eventual
  retirement work.
- The new `getStaffPrincipal` read is new surface area — small, but it
  must be added to the deny-by-default route table and covered by tests in
  its implementation slice.
- Registry re-ownership (§5 step 3) touches pinned policy IDs; done
  carelessly it can break the four existing consumers, which is why values
  stay pinned until the versioned slice.
- Moving staff/session verification too early can break Catalog admin
  approval paths (plan residual risk restated); sequencing §5 exists to
  prevent exactly that.

## Reassessment Triggers

- **Kubernetes adoption or >2 deployment environments** → implement stage
  3 (SPIFFE/SPIRE workload identity, mTLS); cross-ref ADR-0046 triggers.
- **First cross-service call carrying staff authority** (not just an audit
  reference) → implement RFC 8693 token exchange with `act` claims.
- **Per-object sharing or multi-tenant delegation requirement** → adopt a
  Zanzibar-lineage engine (SpiceDB/OpenFGA-class) behind the existing
  identity-platform decision contract.
- **Identity-platform becomes independently deployable** → write the
  repo-local physical extraction ADR (per ADR-0048).
- **A second product needs staff-facing admin UI** (Dawneer workbench) →
  revisit whether `identity.v1` needs staff listing/search operations
  beyond the minimal v1 surface.

## References

- Pang et al., *Zanzibar: Google's Consistent, Global Authorization
  System*, USENIX ATC '19 —
  <https://www.usenix.org/conference/atc19/presentation/pang> ·
  <https://research.google/pubs/zanzibar-googles-consistent-global-authorization-system/>
  (centralized authorization serving Calendar, Cloud, Drive, Maps, Photos,
  YouTube; the case for one decision plane and for ReBAC *at scale*).
- Airbnb Engineering, *Himeji: A Scalable Centralized System for
  Authorization at Airbnb* —
  <https://medium.com/airbnb-engineering/himeji-a-scalable-centralized-system-for-authorization-at-airbnb-341664924574>
  (the Himeji centralized authorization system that informed the
  adopt-don't-build conclusion for ReBAC).
- AuthZed, *Google Zanzibar overview and lineage* —
  <https://authzed.com/learn/google-zanzibar> (SpiceDB/OpenFGA/Ory Keto
  successor ecosystem and Zanzibar design lineage; adopt-don't-build).
- SPIFFE/SPIRE (CNCF graduation 2022) — production adopters (Uber,
  Netflix): <https://github.com/spiffe/spire/blob/main/ADOPTERS.md> ·
  <https://www.cncf.io/announcements/2022/09/20/spiffe-and-spire-projects-graduate-from-cloud-native-computing-foundation-incubator/>
  (short-lived auto-rotated workload identities + mTLS replacing static
  shared secrets; our stage 3 target).
- RFC 8693, *OAuth 2.0 Token Exchange* —
  <https://www.rfc-editor.org/info/rfc8693/> ·
  <https://datatracker.ietf.org/doc/html/rfc8693> (delegation with the
  `act` claim preserving the audit trail; our trigger-gated delegation
  answer).
- ADR-0048 — horizontal platform redefinition (ownership assignment this
  ADR implements).
