# API Exchange Direction Contract

Status: Accepted

Owner: foundation-platform

Date: 2026-07-09

## Purpose

This contract fixes the direction of API exchanges so new integrations do not
mix acquisition, command submission, event fan-out, and analytical querying.

The rule is:

```text
The owner of scheduling, quota, idempotency, and truth chooses the direction.
```

## Direction Rules

### External provider acquisition is pull

Foundation Platform pulls public provider data from data.go.kr, V-World,
hub.go.kr, MOLIT real-transaction export surfaces, and provider download
surfaces.

Providers do not push raw catalog data into Foundation Platform.

Foundation-owned acquisition controls:

- schedule
- provider quota and backoff
- Bronze object commit
- checksum truth
- lineage
- retry and resume

### Product catalog reads are pull

Product services pull published Foundation contracts through read APIs. They do
not read Foundation databases or object-lake internals directly.

Current governed service surfaces:

- `GET /catalog/v1/parcels/by-pnu/:pnu`
- `GET /catalog/v1/parcels/by-pnu/:pnu/buildings`

Public read contracts may also exist, but they are still published contracts,
not direct storage access.

### Proposal intake is push

`intelligence-platform` generates AI normalization proposals and pushes them to
Foundation Platform for durable review-gated intake.

Current governed service surface:

- `POST /internal/normalization/proposals`

This push does not grant canonical write authority. It only creates a proposal
receipt in the Foundation proposal inbox. Review, apply, and rollback remain
Foundation staff/admin commands.

### Lakehouse artifact registration is push

Product-owned workers can push governed artifact-registration requests when the
product owns the produced artifact and Foundation owns the cross-service
registry record.

Current governed service surface:

- `POST /internal/lakehouse/artifacts`

The pushed request registers metadata. It does not give the caller direct
Foundation database access.

### Admin commands are command pushes

Staff/admin routes push commands to Foundation Platform. These commands are not
provider acquisition and not event fan-out. They must remain authenticated,
authorized, audited, and routed through Foundation application commands.

Examples:

- approve a normalization proposal
- reject a normalization proposal
- apply an approved proposal
- rollback an applied proposal
- promote or rollback a governed manifest

### Outbox fan-out is push

Foundation emits committed events through `catalog.outbox_event` and the outbox
publisher. Webhook is the current transport. Kafka can be added later as another
broadcaster, but the direction stays push from Foundation to subscribers.

Outbox events are for committed facts or durable platform events. They are not
request/response reads and not source acquisition.

### dbt/Trino modeling is pull/query

dbt models query lakehouse relations through Trino. dbt does not push source
data, does not call AI models, does not approve proposals, and does not publish
canonical state by itself.

dbt owns SQL modeling and SQL tests only.

## Boundary Rules

- cross-service direct database access is forbidden.
- cross-service direct object-lake internals are forbidden unless the object is
  an explicitly published immutable artifact.
- Pull APIs must be idempotent reads or Foundation-owned acquisition workers.
- Push APIs must return durable receipts or accepted commands.
- Fan-out must use outbox transport, not ad-hoc synchronous callbacks.
- AI can push proposals, never canonical writes.
- Product services can pull published contracts, never Foundation internals.

## Current Direction Matrix

| Flow | Direction | Owner of Truth | Current Mechanism |
|---|---|---|---|
| Public data collection | Pull | Foundation Platform | provider clients, BronzeCommitter |
| Gongzzang/Dawneer catalog lookup | Pull | Foundation Platform | service read APIs |
| AI normalization proposal submit | Push | Foundation Platform | `POST /internal/normalization/proposals` |
| Product artifact registration | Push | Foundation Platform registry | `POST /internal/lakehouse/artifacts` |
| Staff review/apply/rollback | Command push | Foundation Platform | staff/admin APIs |
| Event fan-out | Push | Foundation Platform | `catalog.outbox_event` -> webhook, future Kafka |
| SQL modeling | Pull/query | Foundation Platform | dbt -> Trino |

## Not Allowed

- A product service polling Foundation internal PostgreSQL tables.
- A product service writing Foundation canonical tables.
- `intelligence-platform` applying a normalization proposal directly.
- dbt issuing admin commands or publishing Gold pointers.
- Provider data being pushed into Foundation by Gongzzang as if Gongzzang owned
  the public catalog source.
- Synchronous callback chains replacing durable outbox events.

