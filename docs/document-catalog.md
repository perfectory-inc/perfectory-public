<!-- GENERATED FILE. Do not edit by hand. -->
<!-- Render with: python3 scripts/catalog/render-document-catalog.py -->

# perfectory 전체 문서 색인

> 이 문서는 현재 작업 트리의 문서 파일을 자동으로 세어 만든 탐색용 색인입니다. 문서 내용의 정본이 아닙니다.
> 새 문서는 설명하는 코드 또는 책임 영역 가까이에 두고, README에는 이 색인과 정본 문서 링크만 추가합니다.

## 문서 규모

- 문서 파일: **403개**
- 소유 영역: **6개**

### 소유 영역별

| 소유 영역 | 문서 수 |
|---|---:|
| Foundation Platform | 108 |
| Gongzzang 제품 | 162 |
| Identity Platform | 16 |
| Intelligence Platform | 15 |
| Monorepo | 101 |
| Repository tooling | 1 |

### 유형별

| 유형 | 문서 수 |
|---|---:|
| ADR | 160 |
| README | 107 |
| agent rules | 5 |
| architecture | 26 |
| contract | 3 |
| convention | 10 |
| documentation | 32 |
| draft | 2 |
| fixture | 9 |
| guide | 2 |
| reference | 15 |
| roadmap | 3 |
| runbook | 29 |

## 책임별 문서 트리

### Foundation Platform

```text
platforms/foundation-platform/AGENTS.md
platforms/foundation-platform/CLAUDE.md
platforms/foundation-platform/crates/catalog/README.md
platforms/foundation-platform/crates/collection/README.md
platforms/foundation-platform/crates/foundation-contracts/README.md
platforms/foundation-platform/crates/foundation-outbox/README.md
platforms/foundation-platform/crates/foundation-shared-kernel/README.md
platforms/foundation-platform/crates/lakehouse/README.md
platforms/foundation-platform/crates/normalization/README.md
platforms/foundation-platform/crates/technical/README.md
platforms/foundation-platform/docs/adr/0001-inherit-gongzzang-adrs.md
platforms/foundation-platform/docs/adr/0002-r2-primary-object-storage.md
platforms/foundation-platform/docs/adr/0003-industrial-complex-catalog-ssot.md
platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md
platforms/foundation-platform/docs/adr/0005-object-lake-layout-and-indexing.md
platforms/foundation-platform/docs/adr/0006-lakehouse-table-format-and-serving-architecture.md
platforms/foundation-platform/docs/adr/0007-netflix-style-lakehouse-compute-architecture.md
platforms/foundation-platform/docs/adr/0008-pnu-anchor-pbf-marker-tile-contract.md
platforms/foundation-platform/docs/adr/0009-cross-service-lakehouse-registry-control-plane.md
platforms/foundation-platform/docs/adr/0010-cargo-build-ssot-and-bazel-freeze.md
platforms/foundation-platform/docs/adr/0011-true-bazel-build-ssot-transition.md
platforms/foundation-platform/docs/adr/0012-adopt-cross-repo-bazel-reconciliation.md
platforms/foundation-platform/docs/adr/0013-adopt-collection-event-fabric.md
platforms/foundation-platform/docs/adr/0014-bronze-source-slug-canonical-naming.md
platforms/foundation-platform/docs/adr/0015-bronze-object-key-content-addressed-layout.md
platforms/foundation-platform/docs/adr/0016-bronze-commit-protocol.md
platforms/foundation-platform/docs/adr/0017-bronze-collection-protocol.md
platforms/foundation-platform/docs/adr/0018-vworld-collection-channel-strategy.md
platforms/foundation-platform/docs/adr/0019-bronze-readable-object-lake-postgres-catalog-ssot.md
platforms/foundation-platform/docs/adr/0020-real-transaction-bronze-source-strategy.md
platforms/foundation-platform/docs/adr/0021-adopt-horizontal-platform-redefinition.md
platforms/foundation-platform/docs/adr/0022-lakehouse-handoff-vs-storage-format-boundary.md
platforms/foundation-platform/docs/adr/0023-standard-pnu-canonical-dialect.md
platforms/foundation-platform/docs/adr/0024-foundation-dbt-sql-modeling-layer.md
platforms/foundation-platform/docs/adr/0025-bronze-catalog-recovery-evidence-sealing.md
platforms/foundation-platform/docs/adr/0026-lakehouse-capability-ownership.md
platforms/foundation-platform/docs/adr/0027-normalization-capability-ownership.md
platforms/foundation-platform/docs/adr/0028-foundation-kafka-raw-written-design.md
platforms/foundation-platform/docs/adr/0029-runtime-environment-backend-separation.md
platforms/foundation-platform/docs/adr/README.md
platforms/foundation-platform/docs/architecture/ai-driven-maintenance-model.md
platforms/foundation-platform/docs/architecture/api-exchange-direction-contract.md
platforms/foundation-platform/docs/architecture/bronze-key-naming-and-catalog-principle.md
platforms/foundation-platform/docs/architecture/README.md
platforms/foundation-platform/docs/architecture/traffic-auth-policy-registry.v1.json
platforms/foundation-platform/docs/canonical-property-data-platform-northstar.md
platforms/foundation-platform/docs/catalog/bronze-source-slug-rename.v1.md
platforms/foundation-platform/docs/catalog/building-register-consistency-rules.v1.draft.md
platforms/foundation-platform/docs/catalog/building-register-field-mapping.v1.draft.md
platforms/foundation-platform/docs/catalog/building-register-floor-normalization-rules.v1.md
platforms/foundation-platform/docs/catalog/industrial-complex-lakehouse-poc.md
platforms/foundation-platform/docs/catalog/industrial-complex-ssot-model.md
platforms/foundation-platform/docs/catalog/lakehouse-industry-reference.md
platforms/foundation-platform/docs/catalog/national-data-normalization-contract.v1.json
platforms/foundation-platform/docs/catalog/pipeline-graph-control-plane.md
platforms/foundation-platform/docs/catalog/pipeline-graph.v1.example.json
platforms/foundation-platform/docs/catalog/pipeline-graph.v1.json
platforms/foundation-platform/docs/catalog/provider-rate-policy.v1.json
platforms/foundation-platform/docs/catalog/public-data-bronze-lane-registry.v1.json
platforms/foundation-platform/docs/catalog/public-data-collection-catalog.md
platforms/foundation-platform/docs/catalog/public-source-endpoint-catalog.v1.json
platforms/foundation-platform/docs/catalog/README.md
platforms/foundation-platform/docs/catalog/source-change-detection-policy.md
platforms/foundation-platform/docs/catalog/vworld-data-catalog-reference.md
platforms/foundation-platform/docs/catalog/vworld/README.md
platforms/foundation-platform/docs/data-quality/lakehouse-quality-rules.v1.example.json
platforms/foundation-platform/docs/db/catalog-schema-contract.v1.example.json
platforms/foundation-platform/docs/events/event-fabric-registry.v1.example.json
platforms/foundation-platform/docs/events/lineage/lakehouse-lineage-event.v1.example.json
platforms/foundation-platform/docs/events/webhook/outbox-webhook-envelope.v1.example.json
platforms/foundation-platform/docs/events/webhook/parcel-marker-anchor-snapshot-envelope.v1.example.json
platforms/foundation-platform/docs/events/webhook/receiver-contract.v1.example.json
platforms/foundation-platform/docs/observability/slo-policy.v1.example.json
platforms/foundation-platform/docs/openapi/catalog.v1.json
platforms/foundation-platform/docs/openapi/pipeline-graph.v1.json
platforms/foundation-platform/docs/README.md
platforms/foundation-platform/docs/runbooks/building-hub-bulk-bronze-ingest.md
platforms/foundation-platform/docs/runbooks/canonical-release-proof.md
platforms/foundation-platform/docs/runbooks/foundation-kafka-outbox-contract-test.md
platforms/foundation-platform/docs/runbooks/foundation-platform-low-cost-production-hardening.md
platforms/foundation-platform/docs/runbooks/iceberg-snapshot-rollback.md
platforms/foundation-platform/docs/runbooks/lakehouse-backfill-and-schema-rebuild.md
platforms/foundation-platform/docs/runbooks/lakehouse-catalog-smoke.md
platforms/foundation-platform/docs/runbooks/lakehouse-compute-engines.md
platforms/foundation-platform/docs/runbooks/lakehouse-incident-response.md
platforms/foundation-platform/docs/runbooks/lakehouse-registry.md
platforms/foundation-platform/docs/runbooks/outbox-webhook-fanout.md
platforms/foundation-platform/docs/runbooks/postgres-jobbus-contract-test.md
platforms/foundation-platform/docs/runbooks/production-orchestrator-cutover.md
platforms/foundation-platform/docs/runbooks/provider-acquisition-fargate.md
platforms/foundation-platform/docs/runbooks/provider-outage-and-quota.md
platforms/foundation-platform/docs/runbooks/public-data-bronze-lane-orchestration.md
platforms/foundation-platform/docs/runbooks/r2-inventory-audit.md
platforms/foundation-platform/docs/runbooks/r2-lakehouse-live-verification.md
platforms/foundation-platform/docs/runbooks/r2-namespace-contamination-recovery.md
platforms/foundation-platform/docs/runbooks/r2-vector-tile-manifest-smoke.md
platforms/foundation-platform/docs/runbooks/README.md
platforms/foundation-platform/docs/runbooks/remote-lakehouse-job-runner.md
platforms/foundation-platform/docs/runbooks/runtime-environment-separation.md
platforms/foundation-platform/docs/runbooks/slo-alert-policy.md
platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md
platforms/foundation-platform/docs/runbooks/vworld-dataset-file-bronze-ingest.md
platforms/foundation-platform/infra/lakehouse/dbt/README.md
platforms/foundation-platform/README.md
platforms/foundation-platform/services/foundation-api/README.md
platforms/foundation-platform/services/foundation-outbox-publisher/README.md
platforms/foundation-platform/services/foundation-profile-gateway/README.md
platforms/foundation-platform/services/foundation-provider-acquisition-worker/README.md
```

### Gongzzang 제품

```text
products/gongzzang/AGENTS.md
products/gongzzang/apps/platform-web/README.md
products/gongzzang/apps/web/README.md
products/gongzzang/CLAUDE.md
products/gongzzang/crates/admin-action-domain/README.md
products/gongzzang/crates/analysis-report-domain/README.md
products/gongzzang/crates/api-types/README.md
products/gongzzang/crates/audit-log-domain/README.md
products/gongzzang/crates/bookmark-domain/README.md
products/gongzzang/crates/business-verification-domain/README.md
products/gongzzang/crates/circuit-breaker/README.md
products/gongzzang/crates/court-auction-domain/README.md
products/gongzzang/crates/data-clients/README.md
products/gongzzang/crates/embedding/README.md
products/gongzzang/crates/featured-content-domain/README.md
products/gongzzang/crates/foundation-platform-client/README.md
products/gongzzang/crates/gongzzang-outbox/README.md
products/gongzzang/crates/gongzzang-persistence/README.md
products/gongzzang/crates/listing-domain/README.md
products/gongzzang/crates/listing-photo-domain/README.md
products/gongzzang/crates/listing-report-domain/README.md
products/gongzzang/crates/listing-review-domain/README.md
products/gongzzang/crates/notification-domain/README.md
products/gongzzang/crates/outbox-event-domain/README.md
products/gongzzang/crates/parcel-lookup/README.md
products/gongzzang/crates/product-identity-infrastructure/README.md
products/gongzzang/crates/real-transaction-domain/README.md
products/gongzzang/crates/repo-guard/README.md
products/gongzzang/crates/search-history-domain/README.md
products/gongzzang/crates/shared-kernel/README.md
products/gongzzang/crates/system-alert-domain/README.md
products/gongzzang/crates/user-domain/README.md
products/gongzzang/db/migration/README.md
products/gongzzang/docs/adr/0001-language-rust-ts.md
products/gongzzang/docs/adr/0002-monorepo-cargo-pnpm-turbo.md
products/gongzzang/docs/adr/0003-frontend-nextjs-react19.md
products/gongzzang/docs/adr/0004-db-postgres-postgis.md
products/gongzzang/docs/adr/0005-auth-zitadel.md
products/gongzzang/docs/adr/0006-api-rest-openapi.md
products/gongzzang/docs/adr/0007-cache-moka-valkey.md
products/gongzzang/docs/adr/0008-observability-grafana-otel-sentry.md
products/gongzzang/docs/adr/0009-iac-pulumi.md
products/gongzzang/docs/adr/0010-scope-information-platform-option-a.md
products/gongzzang/docs/adr/0011-embedding-gemini-pgvector.md
products/gongzzang/docs/adr/0012-pipeline-visualization-react-flow.md
products/gongzzang/docs/adr/0013-listing-search-naver-maps.md
products/gongzzang/docs/adr/0014-base-layer-defer-pmtiles.md
products/gongzzang/docs/adr/0015-v-world-acl-rearchitecture.md
products/gongzzang/docs/adr/0016-medallion-base-layer-postgis-silver-pmtiles-gold.md
products/gongzzang/docs/adr/0017-listing-marker-render-canvas-bitmap-stamp.md
products/gongzzang/docs/adr/0018-pnu-first-identity-no-coordinates.md
products/gongzzang/docs/adr/0019-pmtiles-source-via-addsourcetype.md
products/gongzzang/docs/adr/0020-naver-vector-interaction-model.md
products/gongzzang/docs/adr/0021-static-vector-tile-decomposition.md
products/gongzzang/docs/adr/0022-bronze-scraping-isolated-python-service.md
products/gongzzang/docs/adr/0023-audit-2026-05-08-hardening.md
products/gongzzang/docs/adr/0024-etl-cancel-protocol-immediate-abort.md
products/gongzzang/docs/adr/0025-bronze-scraping-workflow-orchestrator-not-rust-spawn.md
products/gongzzang/docs/adr/0026-bronze-api-archive-r2-not-postgres-jsonb.md
products/gongzzang/docs/adr/0027-admin-complex-layer-source-deferred.md
products/gongzzang/docs/adr/0028-supply-chain-sha-pin-and-cleanup-cron.md
products/gongzzang/docs/adr/0029-explicit-environment-separation.md
products/gongzzang/docs/adr/0030-three-service-architecture.md
products/gongzzang/docs/adr/0031-foundation-platform-bounded-contexts.md
products/gongzzang/docs/adr/0032-eventual-consistency-strategy.md
products/gongzzang/docs/adr/0033-seven-guardrails-enforcement.md
products/gongzzang/docs/adr/0034-catalog-ownership-handover-to-foundation-platform.md
products/gongzzang/docs/adr/0035-legacy-r2-removal-and-atomic-namespace.md
products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md
products/gongzzang/docs/adr/0037-pnu-anchor-pbf-marker-tiles.md
products/gongzzang/docs/adr/0038-listing-marker-serving-index-filter-mask.md
products/gongzzang/docs/adr/0039-service-owned-lakehouse-registry-integration.md
products/gongzzang/docs/adr/0040-bazel-first-build-verification-control-plane.md
products/gongzzang/docs/adr/0041-hermetic-javascript-package-bazel-rules.md
products/gongzzang/docs/adr/0042-cross-repo-bazel-native-build-graph.md
products/gongzzang/docs/adr/0043-bazel-transition-provisioning-decisions.md
products/gongzzang/docs/adr/0044-bazel-transition-reconciliation.md
products/gongzzang/docs/adr/0045-adr-placement-cross-repo-governance.md
products/gongzzang/docs/adr/0046-kafka-kubernetes-preliminary-design.md
products/gongzzang/docs/adr/0047-collection-event-fabric.md
products/gongzzang/docs/adr/0048-horizontal-platform-redefinition.md
products/gongzzang/docs/adr/0049-identity-platform-contract-design.md
products/gongzzang/docs/adr/0050-dawneer-workbench-and-internal-admin-surface.md
products/gongzzang/docs/adr/README.md
products/gongzzang/docs/architecture/caching.md
products/gongzzang/docs/architecture/data-flow.md
products/gongzzang/docs/architecture/foundation-platform-boundary.v1.json
products/gongzzang/docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json
products/gongzzang/docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json
products/gongzzang/docs/architecture/geo-pipeline.md
products/gongzzang/docs/architecture/layers.md
products/gongzzang/docs/architecture/mcp-vs-api.md
products/gongzzang/docs/architecture/observability.md
products/gongzzang/docs/architecture/partner-listing-exchange-boundary.md
products/gongzzang/docs/architecture/platform-integration/allowed-call-matrix.v1.json
products/gongzzang/docs/architecture/platform-integration/exception-policy.v1.json
products/gongzzang/docs/architecture/platform-integration/index.v1.json
products/gongzzang/docs/architecture/platform-integration/lakehouse-registry-policy.v1.json
products/gongzzang/docs/architecture/platform-integration/operations-policy.v1.json
products/gongzzang/docs/architecture/platform-integration/route-exposure-policy.v1.json
products/gongzzang/docs/architecture/platform-integration/service-auth-policy.v1.json
products/gongzzang/docs/architecture/platform-integration/supply-chain-policy.v1.json
products/gongzzang/docs/architecture/platform-integration/webhook-policy.v1.json
products/gongzzang/docs/architecture/README.md
products/gongzzang/docs/architecture/traffic-auth-policy-registry.v1.json
products/gongzzang/docs/architecture/traffic-auth-policy-registry/README.md
products/gongzzang/docs/auth/frontend-integration.md
products/gongzzang/docs/auth/README.md
products/gongzzang/docs/auth/staging-zitadel-integration.md
products/gongzzang/docs/backend/circuit-breaker.md
products/gongzzang/docs/backend/README.md
products/gongzzang/docs/compliance/README.md
products/gongzzang/docs/conventions/comments.md
products/gongzzang/docs/conventions/enforcement-flow.md
products/gongzzang/docs/conventions/error-format.md
products/gongzzang/docs/conventions/git-and-pr.md
products/gongzzang/docs/conventions/naming-and-ids.md
products/gongzzang/docs/conventions/README.md
products/gongzzang/docs/conventions/rust.md
products/gongzzang/docs/conventions/sql.md
products/gongzzang/docs/conventions/testing.md
products/gongzzang/docs/conventions/typescript.md
products/gongzzang/docs/conventions/ui-writing-korean.md
products/gongzzang/docs/cost/README.md
products/gongzzang/docs/data-sources/data-go-kr.md
products/gongzzang/docs/data-sources/korean-land-mcp.md
products/gongzzang/docs/data-sources/korean-law-mcp.md
products/gongzzang/docs/data-sources/korean-law.md
products/gongzzang/docs/data-sources/naver-maps.md
products/gongzzang/docs/data-sources/nice-identity.md
products/gongzzang/docs/data-sources/README.md
products/gongzzang/docs/data-sources/v-world.md
products/gongzzang/docs/database/er-diagram-v001.md
products/gongzzang/docs/database/migrations.md
products/gongzzang/docs/database/README.md
products/gongzzang/docs/frontend/listings-search.md
products/gongzzang/docs/frontend/panel-sss-axes.md
products/gongzzang/docs/frontend/README.md
products/gongzzang/docs/glossary.md
products/gongzzang/docs/governance/README.md
products/gongzzang/docs/README.md
products/gongzzang/docs/runbooks/foundation-platform-integration-operations.md
products/gongzzang/docs/runbooks/foundation-platform-workload-identity.md
products/gongzzang/docs/runbooks/README.md
products/gongzzang/docs/runbooks/supply-chain-provenance-and-deploy-gate.md
products/gongzzang/docs/ssot-matrix.md
products/gongzzang/docs/sss-charter.md
products/gongzzang/docs/testing/load.md
products/gongzzang/docs/testing/playwright-runtime.md
products/gongzzang/docs/testing/README.md
products/gongzzang/infrastructure/docker/README.md
products/gongzzang/infrastructure/README.md
products/gongzzang/infrastructure/security/README.md
products/gongzzang/migrations/README.md
products/gongzzang/packages/api-types/README.md
products/gongzzang/packages/map/README.md
products/gongzzang/packages/ui/README.md
products/gongzzang/README.md
products/gongzzang/reference/README.md
products/gongzzang/services/gongzzang-api/README.md
products/gongzzang/services/gongzzang-outbox-publisher/README.md
products/gongzzang/tests/load/README.md
```

### Identity Platform

```text
platforms/identity-platform/AGENTS.md
platforms/identity-platform/CLAUDE.md
platforms/identity-platform/crates/authorization/README.md
platforms/identity-platform/crates/identity-contracts/README.md
platforms/identity-platform/crates/identity-shared-kernel/README.md
platforms/identity-platform/crates/service-identity/README.md
platforms/identity-platform/crates/staff-identity/README.md
platforms/identity-platform/docs/adr/0001-inherit-monorepo-conventions.md
platforms/identity-platform/docs/adr/README.md
platforms/identity-platform/docs/openapi/identity.v1.json
platforms/identity-platform/docs/README.md
platforms/identity-platform/docs/runbooks/README.md
platforms/identity-platform/docs/runbooks/workload-identity-provisioning.md
platforms/identity-platform/README.md
platforms/identity-platform/services/identity-api/README.md
platforms/identity-platform/services/identity-policy-worker/README.md
```

### Intelligence Platform

```text
platforms/intelligence-platform/AGENTS.md
platforms/intelligence-platform/CLAUDE.md
platforms/intelligence-platform/crates/intelligence-contracts/README.md
platforms/intelligence-platform/crates/knowledge/README.md
platforms/intelligence-platform/crates/messaging/README.md
platforms/intelligence-platform/crates/normalization/README.md
platforms/intelligence-platform/docs/adr/0001-canonical-implementation-rust.md
platforms/intelligence-platform/docs/adr/0002-canonical-release-rag-design.md
platforms/intelligence-platform/docs/adr/README.md
platforms/intelligence-platform/docs/architecture.md
platforms/intelligence-platform/docs/README.md
platforms/intelligence-platform/README.md
platforms/intelligence-platform/schemas/README.md
platforms/intelligence-platform/services/intelligence-api/README.md
platforms/intelligence-platform/services/intelligence-worker/README.md
```

### Monorepo

```text
AGENTS.md
CLAUDE.md
CONTRIBUTING.md
docs/adr/0001-monorepo-governance-and-conventions.md
docs/adr/0002-docs-taxonomy-and-archive.md
docs/adr/0003-docs-physical-taxonomy.md
docs/adr/0004-verification-ssot.md
docs/adr/0005-hooks-advisory-ci-authoritative.md
docs/adr/0006-object-storage-first-serving.md
docs/adr/0007-public-code-private-operations-boundary.md
docs/adr/0008-manual-dependency-updates-and-organization-branches.md
docs/adr/0009-korean-first-documentation-and-multilingual-readiness.md
docs/adr/0010-live-resource-test-lanes.md
docs/adr/0011-test-execution-set-completeness.md
docs/adr/0012-verification-results-must-mean-what-they-say.md
docs/adr/0013-release-uniqueness-admits-both-source-kinds.md
docs/adr/0014-serving-generation-tracks-one-unit-source-selection.md
docs/adr/0015-one-idempotency-ledger-for-keyed-catalog-mutations.md
docs/adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md
docs/adr/0017-a-data-revision-belongs-to-the-unit-it-revises.md
docs/adr/0018-a-vocabulary-written-in-two-languages-is-compared.md
docs/adr/0019-membership-is-a-dated-fact-not-a-column.md
docs/adr/0020-geometry-is-not-evidence-for-a-fact.md
docs/adr/0021-an-unread-surface-is-deleted-not-migrated.md
docs/adr/0022-current-means-today-and-one-view-says-so.md
docs/adr/0023-an-edit-is-a-row-in-the-ledger-not-only-in-the-row.md
docs/adr/0024-the-serving-projection-carries-only-what-the-tile-contract-names.md
docs/adr/0025-parcel-publication-names-one-sealed-iceberg-evidence.md
docs/adr/0026-parcel-evidence-sealer-is-the-only-append-boundary.md
docs/adr/0027-every-guard-declares-its-threat-model.md
docs/adr/0028-supply-chain-vulnerability-gate-uses-an-osv-ratchet.md
docs/adr/0029-parcel-publication-evidence-is-written-from-the-terminal-run.md
docs/adr/0030-parcel-publication-evidence-requires-two-distinct-approvals.md
docs/adr/0031-parcel-mirror-run-seals-publication-scope.md
docs/adr/0032-provider-identity-is-derived-from-domain-label.md
docs/adr/0033-an-industrial-complex-without-a-sourced-address-is-not-representable.md
docs/adr/0034-an-administrative-code-carries-its-own-granularity.md
docs/adr/0035-a-region-the-pipeline-does-not-use-is-not-required.md
docs/adr/0036-a-pointed-at-object-has-the-command-that-wrote-it.md
docs/adr/0037-a-pointer-carries-the-address-template-with-its-object-key.md
docs/adr/0038-a-fetchable-artifact-does-not-share-a-bucket-with-the-canonical-bytes.md
docs/adr/0039-gold-serving-artifacts-live-in-the-lakehouse-bucket-and-tiles-do-not.md
docs/adr/0040-a-column-no-producer-fills-cannot-be-required.md
docs/adr/0042-a-silver-boundary-carries-its-source-crs.md
docs/adr/0043-a-canonical-id-is-read-not-recomputed.md
docs/adr/0044-a-column-named-for-a-fact-must-hold-that-fact.md
docs/adr/0045-a-serving-projection-row-names-its-load-not-its-revision.md
docs/adr/0046-a-publication-names-the-object-it-was-collected-from.md
docs/adr/0047-a-repairable-boundary-is-repaired-not-dropped.md
docs/adr/0048-a-published-feature-id-needs-a-read-keyed-on-it.md
docs/adr/0049-a-browsable-collection-is-paged-filtered-and-counted.md
docs/adr/0050-provider-escaped-text-is-unescaped-once-in-one-place.md
docs/adr/0051-a-pointer-is-published-only-against-bytes-that-were-read-back.md
docs/adr/0052-a-static-archive-reads-its-build-conditions-from-the-source-it-replaces.md
docs/adr/0053-a-static-tile-pointer-is-derived-from-build-ledger-facts.md
docs/adr/0054-static-release-tools-have-one-executable-identity-contract.md
docs/adr/0055-private-r2-profile-gateway.md
docs/adr/0056-heavy-foundation-gates-run-only-for-owned-inputs.md
docs/adr/0057-lakehouse-inventory-reads-current-iceberg-metadata.md
docs/adr/0058-repository-owned-environment-names-have-one-contract.md
docs/adr/0059-shapefile-files-have-a-first-class-streaming-ingress.md
docs/adr/0060-gold-artifact-identity-is-resolved-at-the-catalog-write-boundary.md
docs/adr/0061-refused-parcel-numbers-are-named-not-totalled.md
docs/adr/0062-an-ingest-batch-records-itself-in-the-table-it-writes.md
docs/adr/0063-a-partition-that-cannot-narrow-a-search-only-splits-files.md
docs/adr/0064-the-parcel-table-is-read-without-vectorization.md
docs/adr/0065-an-engine-version-is-written-once.md
docs/adr/0066-a-table-that-fits-in-one-file-is-not-split.md
docs/adr/0067-the-parcel-source-covers-the-country-twice.md
docs/adr/0068-the-command-names-the-object-it-read.md
docs/adr/0069-one-column-holds-five-kinds-of-thing.md
docs/adr/0070-the-boundary-source-carries-neither-use-nor-area.md
docs/adr/0071-a-deploy-that-leaves-the-schema-behind-has-not-finished.md
docs/adr/0072-units-attach-to-parcels-by-pnu-and-orphans-are-counted.md
docs/adr/0073-the-title-register-fills-the-building-between-parcel-and-unit.md
docs/adr/0074-a-unit-hangs-off-its-building-and-null-is-an-answer.md
docs/adr/0075-the-unit-load-fills-its-own-link.md
docs/adr/0076-a-building-answers-for-its-units.md
docs/adr/0077-the-pipe-looks-at-its-sources-every-day.md
docs/adr/0078-gongzzang-serves-the-lineage-and-tells-the-truth-about-blanks.md
docs/adr/0079-the-outbox-gets-a-postman-on-a-schedule.md
docs/adr/README.md
docs/architecture/administrative-boundary-versioning.md
docs/architecture/README.md
docs/architecture/single-source-spatial-publication.md
docs/glossary.md
docs/guides/documentation-lifecycle.md
docs/guides/README.md
docs/guides/single-source-spatial-publication-implementation.md
docs/public-data-catalog.md
docs/README.md
docs/reference/design-system-benchmarks.md
docs/reference/README.md
docs/roadmap/foundation-baseline.md
docs/roadmap/foundation-goals.md
docs/roadmap/production-readiness.md
docs/roadmap/README.md
docs/technology-stack.md
README.md
SECURITY.md
THIRD_PARTY_NOTICES.md
```

### Repository tooling

```text
tools/github/README.md
```

## 전체 문서 목록

| 경로 | 소유자 | 유형 | 상태 |
|---|---|---|---|
| `AGENTS.md` | Monorepo | agent rules | current |
| `CLAUDE.md` | Monorepo | documentation | current |
| `CONTRIBUTING.md` | Monorepo | documentation | current |
| `docs/adr/0001-monorepo-governance-and-conventions.md` | Monorepo | ADR | current |
| `docs/adr/0002-docs-taxonomy-and-archive.md` | Monorepo | ADR | current |
| `docs/adr/0003-docs-physical-taxonomy.md` | Monorepo | ADR | current |
| `docs/adr/0004-verification-ssot.md` | Monorepo | ADR | current |
| `docs/adr/0005-hooks-advisory-ci-authoritative.md` | Monorepo | ADR | current |
| `docs/adr/0006-object-storage-first-serving.md` | Monorepo | ADR | current |
| `docs/adr/0007-public-code-private-operations-boundary.md` | Monorepo | ADR | current |
| `docs/adr/0008-manual-dependency-updates-and-organization-branches.md` | Monorepo | ADR | current |
| `docs/adr/0009-korean-first-documentation-and-multilingual-readiness.md` | Monorepo | ADR | current |
| `docs/adr/0010-live-resource-test-lanes.md` | Monorepo | ADR | Accepted |
| `docs/adr/0011-test-execution-set-completeness.md` | Monorepo | ADR | current |
| `docs/adr/0012-verification-results-must-mean-what-they-say.md` | Monorepo | ADR | current |
| `docs/adr/0013-release-uniqueness-admits-both-source-kinds.md` | Monorepo | ADR | current |
| `docs/adr/0014-serving-generation-tracks-one-unit-source-selection.md` | Monorepo | ADR | current |
| `docs/adr/0015-one-idempotency-ledger-for-keyed-catalog-mutations.md` | Monorepo | ADR | current |
| `docs/adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md` | Monorepo | ADR | current |
| `docs/adr/0017-a-data-revision-belongs-to-the-unit-it-revises.md` | Monorepo | ADR | current |
| `docs/adr/0018-a-vocabulary-written-in-two-languages-is-compared.md` | Monorepo | ADR | current |
| `docs/adr/0019-membership-is-a-dated-fact-not-a-column.md` | Monorepo | ADR | current |
| `docs/adr/0020-geometry-is-not-evidence-for-a-fact.md` | Monorepo | ADR | current |
| `docs/adr/0021-an-unread-surface-is-deleted-not-migrated.md` | Monorepo | ADR | current |
| `docs/adr/0022-current-means-today-and-one-view-says-so.md` | Monorepo | ADR | current |
| `docs/adr/0023-an-edit-is-a-row-in-the-ledger-not-only-in-the-row.md` | Monorepo | ADR | current |
| `docs/adr/0024-the-serving-projection-carries-only-what-the-tile-contract-names.md` | Monorepo | ADR | current |
| `docs/adr/0025-parcel-publication-names-one-sealed-iceberg-evidence.md` | Monorepo | ADR | current |
| `docs/adr/0026-parcel-evidence-sealer-is-the-only-append-boundary.md` | Monorepo | ADR | current |
| `docs/adr/0027-every-guard-declares-its-threat-model.md` | Monorepo | ADR | current |
| `docs/adr/0028-supply-chain-vulnerability-gate-uses-an-osv-ratchet.md` | Monorepo | ADR | current |
| `docs/adr/0029-parcel-publication-evidence-is-written-from-the-terminal-run.md` | Monorepo | ADR | current |
| `docs/adr/0030-parcel-publication-evidence-requires-two-distinct-approvals.md` | Monorepo | ADR | current |
| `docs/adr/0031-parcel-mirror-run-seals-publication-scope.md` | Monorepo | ADR | current |
| `docs/adr/0032-provider-identity-is-derived-from-domain-label.md` | Monorepo | ADR | current |
| `docs/adr/0033-an-industrial-complex-without-a-sourced-address-is-not-representable.md` | Monorepo | ADR | current |
| `docs/adr/0034-an-administrative-code-carries-its-own-granularity.md` | Monorepo | ADR | current |
| `docs/adr/0035-a-region-the-pipeline-does-not-use-is-not-required.md` | Monorepo | ADR | current |
| `docs/adr/0036-a-pointed-at-object-has-the-command-that-wrote-it.md` | Monorepo | ADR | Accepted |
| `docs/adr/0037-a-pointer-carries-the-address-template-with-its-object-key.md` | Monorepo | ADR | Accepted |
| `docs/adr/0038-a-fetchable-artifact-does-not-share-a-bucket-with-the-canonical-bytes.md` | Monorepo | ADR | Superseded by ADR-0039 |
| `docs/adr/0039-gold-serving-artifacts-live-in-the-lakehouse-bucket-and-tiles-do-not.md` | Monorepo | ADR | Accepted |
| `docs/adr/0040-a-column-no-producer-fills-cannot-be-required.md` | Monorepo | ADR | Accepted |
| `docs/adr/0042-a-silver-boundary-carries-its-source-crs.md` | Monorepo | ADR | Accepted |
| `docs/adr/0043-a-canonical-id-is-read-not-recomputed.md` | Monorepo | ADR | Accepted |
| `docs/adr/0044-a-column-named-for-a-fact-must-hold-that-fact.md` | Monorepo | ADR | Accepted |
| `docs/adr/0045-a-serving-projection-row-names-its-load-not-its-revision.md` | Monorepo | ADR | Accepted |
| `docs/adr/0046-a-publication-names-the-object-it-was-collected-from.md` | Monorepo | ADR | Accepted |
| `docs/adr/0047-a-repairable-boundary-is-repaired-not-dropped.md` | Monorepo | ADR | Accepted |
| `docs/adr/0048-a-published-feature-id-needs-a-read-keyed-on-it.md` | Monorepo | ADR | Accepted |
| `docs/adr/0049-a-browsable-collection-is-paged-filtered-and-counted.md` | Monorepo | ADR | Accepted |
| `docs/adr/0050-provider-escaped-text-is-unescaped-once-in-one-place.md` | Monorepo | ADR | Accepted |
| `docs/adr/0051-a-pointer-is-published-only-against-bytes-that-were-read-back.md` | Monorepo | ADR | Accepted |
| `docs/adr/0052-a-static-archive-reads-its-build-conditions-from-the-source-it-replaces.md` | Monorepo | ADR | Accepted |
| `docs/adr/0053-a-static-tile-pointer-is-derived-from-build-ledger-facts.md` | Monorepo | ADR | Accepted |
| `docs/adr/0054-static-release-tools-have-one-executable-identity-contract.md` | Monorepo | ADR | Accepted |
| `docs/adr/0055-private-r2-profile-gateway.md` | Monorepo | ADR | Accepted |
| `docs/adr/0056-heavy-foundation-gates-run-only-for-owned-inputs.md` | Monorepo | ADR | current |
| `docs/adr/0057-lakehouse-inventory-reads-current-iceberg-metadata.md` | Monorepo | ADR | current |
| `docs/adr/0058-repository-owned-environment-names-have-one-contract.md` | Monorepo | ADR | current |
| `docs/adr/0059-shapefile-files-have-a-first-class-streaming-ingress.md` | Monorepo | ADR | current |
| `docs/adr/0060-gold-artifact-identity-is-resolved-at-the-catalog-write-boundary.md` | Monorepo | ADR | current |
| `docs/adr/0061-refused-parcel-numbers-are-named-not-totalled.md` | Monorepo | ADR | Accepted |
| `docs/adr/0062-an-ingest-batch-records-itself-in-the-table-it-writes.md` | Monorepo | ADR | Accepted |
| `docs/adr/0063-a-partition-that-cannot-narrow-a-search-only-splits-files.md` | Monorepo | ADR | Accepted |
| `docs/adr/0064-the-parcel-table-is-read-without-vectorization.md` | Monorepo | ADR | Superseded by ADR-0065 |
| `docs/adr/0065-an-engine-version-is-written-once.md` | Monorepo | ADR | Accepted |
| `docs/adr/0066-a-table-that-fits-in-one-file-is-not-split.md` | Monorepo | ADR | Accepted |
| `docs/adr/0067-the-parcel-source-covers-the-country-twice.md` | Monorepo | ADR | Accepted |
| `docs/adr/0068-the-command-names-the-object-it-read.md` | Monorepo | ADR | Accepted |
| `docs/adr/0069-one-column-holds-five-kinds-of-thing.md` | Monorepo | ADR | Accepted |
| `docs/adr/0070-the-boundary-source-carries-neither-use-nor-area.md` | Monorepo | ADR | Accepted |
| `docs/adr/0071-a-deploy-that-leaves-the-schema-behind-has-not-finished.md` | Monorepo | ADR | Accepted |
| `docs/adr/0072-units-attach-to-parcels-by-pnu-and-orphans-are-counted.md` | Monorepo | ADR | Accepted |
| `docs/adr/0073-the-title-register-fills-the-building-between-parcel-and-unit.md` | Monorepo | ADR | Accepted |
| `docs/adr/0074-a-unit-hangs-off-its-building-and-null-is-an-answer.md` | Monorepo | ADR | Accepted |
| `docs/adr/0075-the-unit-load-fills-its-own-link.md` | Monorepo | ADR | Accepted |
| `docs/adr/0076-a-building-answers-for-its-units.md` | Monorepo | ADR | Accepted |
| `docs/adr/0077-the-pipe-looks-at-its-sources-every-day.md` | Monorepo | ADR | Accepted |
| `docs/adr/0078-gongzzang-serves-the-lineage-and-tells-the-truth-about-blanks.md` | Monorepo | ADR | Accepted |
| `docs/adr/0079-the-outbox-gets-a-postman-on-a-schedule.md` | Monorepo | ADR | Accepted |
| `docs/adr/README.md` | Monorepo | README | current |
| `docs/architecture/administrative-boundary-versioning.md` | Monorepo | architecture | current |
| `docs/architecture/README.md` | Monorepo | README | current |
| `docs/architecture/single-source-spatial-publication.md` | Monorepo | architecture | current |
| `docs/glossary.md` | Monorepo | documentation | current |
| `docs/guides/documentation-lifecycle.md` | Monorepo | guide | current |
| `docs/guides/README.md` | Monorepo | README | current |
| `docs/guides/single-source-spatial-publication-implementation.md` | Monorepo | guide | proposed |
| `docs/public-data-catalog.md` | Monorepo | documentation | current |
| `docs/README.md` | Monorepo | README | current |
| `docs/reference/design-system-benchmarks.md` | Monorepo | reference | current |
| `docs/reference/README.md` | Monorepo | README | current |
| `docs/roadmap/foundation-baseline.md` | Monorepo | roadmap | current |
| `docs/roadmap/foundation-goals.md` | Monorepo | roadmap | current |
| `docs/roadmap/production-readiness.md` | Monorepo | roadmap | current |
| `docs/roadmap/README.md` | Monorepo | README | current |
| `docs/technology-stack.md` | Monorepo | documentation | current |
| `platforms/foundation-platform/AGENTS.md` | Foundation Platform | agent rules | current |
| `platforms/foundation-platform/CLAUDE.md` | Foundation Platform | documentation | current |
| `platforms/foundation-platform/crates/catalog/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/crates/collection/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/crates/foundation-contracts/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/crates/foundation-outbox/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/crates/foundation-shared-kernel/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/crates/lakehouse/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/crates/normalization/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/crates/technical/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/docs/adr/0001-inherit-gongzzang-adrs.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0002-r2-primary-object-storage.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0003-industrial-complex-catalog-ssot.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0005-object-lake-layout-and-indexing.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0006-lakehouse-table-format-and-serving-architecture.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0007-netflix-style-lakehouse-compute-architecture.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0008-pnu-anchor-pbf-marker-tile-contract.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0009-cross-service-lakehouse-registry-control-plane.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0010-cargo-build-ssot-and-bazel-freeze.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0011-true-bazel-build-ssot-transition.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0012-adopt-cross-repo-bazel-reconciliation.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0013-adopt-collection-event-fabric.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0014-bronze-source-slug-canonical-naming.md` | Foundation Platform | ADR | Superseded in part by ADR-0032 (D2 및 D6의 제공기관 유예만; D1·D3·D4·D5는 유효) |
| `platforms/foundation-platform/docs/adr/0015-bronze-object-key-content-addressed-layout.md` | Foundation Platform | ADR | Superseded by ADR 0016 and |
| `platforms/foundation-platform/docs/adr/0016-bronze-commit-protocol.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0017-bronze-collection-protocol.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0018-vworld-collection-channel-strategy.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0019-bronze-readable-object-lake-postgres-catalog-ssot.md` | Foundation Platform | ADR | Accepted |
| `platforms/foundation-platform/docs/adr/0020-real-transaction-bronze-source-strategy.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0021-adopt-horizontal-platform-redefinition.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0022-lakehouse-handoff-vs-storage-format-boundary.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0023-standard-pnu-canonical-dialect.md` | Foundation Platform | ADR | Accepted |
| `platforms/foundation-platform/docs/adr/0024-foundation-dbt-sql-modeling-layer.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0025-bronze-catalog-recovery-evidence-sealing.md` | Foundation Platform | ADR | Accepted |
| `platforms/foundation-platform/docs/adr/0026-lakehouse-capability-ownership.md` | Foundation Platform | ADR | Accepted |
| `platforms/foundation-platform/docs/adr/0027-normalization-capability-ownership.md` | Foundation Platform | ADR | Accepted |
| `platforms/foundation-platform/docs/adr/0028-foundation-kafka-raw-written-design.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/0029-runtime-environment-backend-separation.md` | Foundation Platform | ADR | current |
| `platforms/foundation-platform/docs/adr/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/docs/architecture/ai-driven-maintenance-model.md` | Foundation Platform | architecture | current |
| `platforms/foundation-platform/docs/architecture/api-exchange-direction-contract.md` | Foundation Platform | architecture | current |
| `platforms/foundation-platform/docs/architecture/bronze-key-naming-and-catalog-principle.md` | Foundation Platform | architecture | current |
| `platforms/foundation-platform/docs/architecture/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/docs/architecture/traffic-auth-policy-registry.v1.json` | Foundation Platform | architecture | current |
| `platforms/foundation-platform/docs/canonical-property-data-platform-northstar.md` | Foundation Platform | documentation | current |
| `platforms/foundation-platform/docs/catalog/bronze-source-slug-rename.v1.md` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/building-register-consistency-rules.v1.draft.md` | Foundation Platform | draft | review required |
| `platforms/foundation-platform/docs/catalog/building-register-field-mapping.v1.draft.md` | Foundation Platform | draft | review required |
| `platforms/foundation-platform/docs/catalog/building-register-floor-normalization-rules.v1.md` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/industrial-complex-lakehouse-poc.md` | Foundation Platform | reference | proposed |
| `platforms/foundation-platform/docs/catalog/industrial-complex-ssot-model.md` | Foundation Platform | reference | proposed |
| `platforms/foundation-platform/docs/catalog/lakehouse-industry-reference.md` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/national-data-normalization-contract.v1.json` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/pipeline-graph-control-plane.md` | Foundation Platform | reference | proposed |
| `platforms/foundation-platform/docs/catalog/pipeline-graph.v1.example.json` | Foundation Platform | fixture | fixture |
| `platforms/foundation-platform/docs/catalog/pipeline-graph.v1.json` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/provider-rate-policy.v1.json` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/public-data-bronze-lane-registry.v1.json` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/public-data-collection-catalog.md` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/public-source-endpoint-catalog.v1.json` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/docs/catalog/source-change-detection-policy.md` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/vworld-data-catalog-reference.md` | Foundation Platform | reference | current |
| `platforms/foundation-platform/docs/catalog/vworld/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/docs/data-quality/lakehouse-quality-rules.v1.example.json` | Foundation Platform | fixture | fixture |
| `platforms/foundation-platform/docs/db/catalog-schema-contract.v1.example.json` | Foundation Platform | fixture | fixture |
| `platforms/foundation-platform/docs/events/event-fabric-registry.v1.example.json` | Foundation Platform | fixture | fixture |
| `platforms/foundation-platform/docs/events/lineage/lakehouse-lineage-event.v1.example.json` | Foundation Platform | fixture | fixture |
| `platforms/foundation-platform/docs/events/webhook/outbox-webhook-envelope.v1.example.json` | Foundation Platform | fixture | fixture |
| `platforms/foundation-platform/docs/events/webhook/parcel-marker-anchor-snapshot-envelope.v1.example.json` | Foundation Platform | fixture | fixture |
| `platforms/foundation-platform/docs/events/webhook/receiver-contract.v1.example.json` | Foundation Platform | fixture | fixture |
| `platforms/foundation-platform/docs/observability/slo-policy.v1.example.json` | Foundation Platform | fixture | fixture |
| `platforms/foundation-platform/docs/openapi/catalog.v1.json` | Foundation Platform | contract | current |
| `platforms/foundation-platform/docs/openapi/pipeline-graph.v1.json` | Foundation Platform | contract | current |
| `platforms/foundation-platform/docs/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/docs/runbooks/building-hub-bulk-bronze-ingest.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/canonical-release-proof.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/foundation-kafka-outbox-contract-test.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/foundation-platform-low-cost-production-hardening.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/iceberg-snapshot-rollback.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/lakehouse-backfill-and-schema-rebuild.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/lakehouse-catalog-smoke.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/lakehouse-compute-engines.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/lakehouse-incident-response.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/lakehouse-registry.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/outbox-webhook-fanout.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/postgres-jobbus-contract-test.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/production-orchestrator-cutover.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/provider-acquisition-fargate.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/provider-outage-and-quota.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/public-data-bronze-lane-orchestration.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/r2-inventory-audit.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/r2-lakehouse-live-verification.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/r2-namespace-contamination-recovery.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/r2-vector-tile-manifest-smoke.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/docs/runbooks/remote-lakehouse-job-runner.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/runtime-environment-separation.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/slo-alert-policy.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/docs/runbooks/vworld-dataset-file-bronze-ingest.md` | Foundation Platform | runbook | current |
| `platforms/foundation-platform/infra/lakehouse/dbt/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/services/foundation-api/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/services/foundation-outbox-publisher/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/services/foundation-profile-gateway/README.md` | Foundation Platform | README | current |
| `platforms/foundation-platform/services/foundation-provider-acquisition-worker/README.md` | Foundation Platform | README | current |
| `platforms/identity-platform/AGENTS.md` | Identity Platform | agent rules | current |
| `platforms/identity-platform/CLAUDE.md` | Identity Platform | documentation | current |
| `platforms/identity-platform/crates/authorization/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/crates/identity-contracts/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/crates/identity-shared-kernel/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/crates/service-identity/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/crates/staff-identity/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/docs/adr/0001-inherit-monorepo-conventions.md` | Identity Platform | ADR | Accepted |
| `platforms/identity-platform/docs/adr/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/docs/openapi/identity.v1.json` | Identity Platform | contract | current |
| `platforms/identity-platform/docs/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/docs/runbooks/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/docs/runbooks/workload-identity-provisioning.md` | Identity Platform | runbook | current |
| `platforms/identity-platform/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/services/identity-api/README.md` | Identity Platform | README | current |
| `platforms/identity-platform/services/identity-policy-worker/README.md` | Identity Platform | README | current |
| `platforms/intelligence-platform/AGENTS.md` | Intelligence Platform | agent rules | current |
| `platforms/intelligence-platform/CLAUDE.md` | Intelligence Platform | documentation | current |
| `platforms/intelligence-platform/crates/intelligence-contracts/README.md` | Intelligence Platform | README | current |
| `platforms/intelligence-platform/crates/knowledge/README.md` | Intelligence Platform | README | current |
| `platforms/intelligence-platform/crates/messaging/README.md` | Intelligence Platform | README | current |
| `platforms/intelligence-platform/crates/normalization/README.md` | Intelligence Platform | README | current |
| `platforms/intelligence-platform/docs/adr/0001-canonical-implementation-rust.md` | Intelligence Platform | ADR | current |
| `platforms/intelligence-platform/docs/adr/0002-canonical-release-rag-design.md` | Intelligence Platform | ADR | current |
| `platforms/intelligence-platform/docs/adr/README.md` | Intelligence Platform | README | current |
| `platforms/intelligence-platform/docs/architecture.md` | Intelligence Platform | documentation | current |
| `platforms/intelligence-platform/docs/README.md` | Intelligence Platform | README | current |
| `platforms/intelligence-platform/README.md` | Intelligence Platform | README | current |
| `platforms/intelligence-platform/schemas/README.md` | Intelligence Platform | README | current |
| `platforms/intelligence-platform/services/intelligence-api/README.md` | Intelligence Platform | README | current |
| `platforms/intelligence-platform/services/intelligence-worker/README.md` | Intelligence Platform | README | current |
| `products/gongzzang/AGENTS.md` | Gongzzang 제품 | agent rules | current |
| `products/gongzzang/apps/platform-web/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/apps/web/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/CLAUDE.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/crates/admin-action-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/analysis-report-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/api-types/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/audit-log-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/bookmark-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/business-verification-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/circuit-breaker/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/court-auction-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/data-clients/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/embedding/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/featured-content-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/foundation-platform-client/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/gongzzang-outbox/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/gongzzang-persistence/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/listing-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/listing-photo-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/listing-report-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/listing-review-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/notification-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/outbox-event-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/parcel-lookup/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/product-identity-infrastructure/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/real-transaction-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/repo-guard/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/search-history-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/shared-kernel/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/system-alert-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/crates/user-domain/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/db/migration/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/adr/0001-language-rust-ts.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0002-monorepo-cargo-pnpm-turbo.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0003-frontend-nextjs-react19.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0004-db-postgres-postgis.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0005-auth-zitadel.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0006-api-rest-openapi.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0007-cache-moka-valkey.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0008-observability-grafana-otel-sentry.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0009-iac-pulumi.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0010-scope-information-platform-option-a.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0011-embedding-gemini-pgvector.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0012-pipeline-visualization-react-flow.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0013-listing-search-naver-maps.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0014-base-layer-defer-pmtiles.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0015-v-world-acl-rearchitecture.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0016-medallion-base-layer-postgis-silver-pmtiles-gold.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0017-listing-marker-render-canvas-bitmap-stamp.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0018-pnu-first-identity-no-coordinates.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0019-pmtiles-source-via-addsourcetype.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0020-naver-vector-interaction-model.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0021-static-vector-tile-decomposition.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0022-bronze-scraping-isolated-python-service.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0023-audit-2026-05-08-hardening.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0024-etl-cancel-protocol-immediate-abort.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0025-bronze-scraping-workflow-orchestrator-not-rust-spawn.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0026-bronze-api-archive-r2-not-postgres-jsonb.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0027-admin-complex-layer-source-deferred.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0028-supply-chain-sha-pin-and-cleanup-cron.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0029-explicit-environment-separation.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0030-three-service-architecture.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0031-foundation-platform-bounded-contexts.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0032-eventual-consistency-strategy.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0033-seven-guardrails-enforcement.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0034-catalog-ownership-handover-to-foundation-platform.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0035-legacy-r2-removal-and-atomic-namespace.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0037-pnu-anchor-pbf-marker-tiles.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0038-listing-marker-serving-index-filter-mask.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0039-service-owned-lakehouse-registry-integration.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0040-bazel-first-build-verification-control-plane.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0041-hermetic-javascript-package-bazel-rules.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0042-cross-repo-bazel-native-build-graph.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0043-bazel-transition-provisioning-decisions.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0044-bazel-transition-reconciliation.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0045-adr-placement-cross-repo-governance.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0046-kafka-kubernetes-preliminary-design.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0047-collection-event-fabric.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0048-horizontal-platform-redefinition.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0049-identity-platform-contract-design.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/0050-dawneer-workbench-and-internal-admin-surface.md` | Gongzzang 제품 | ADR | current |
| `products/gongzzang/docs/adr/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/architecture/caching.md` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/data-flow.md` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/foundation-platform-boundary.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/geo-pipeline.md` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/layers.md` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/mcp-vs-api.md` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/observability.md` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/partner-listing-exchange-boundary.md` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/platform-integration/allowed-call-matrix.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/platform-integration/exception-policy.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/platform-integration/index.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/platform-integration/lakehouse-registry-policy.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/platform-integration/operations-policy.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/platform-integration/route-exposure-policy.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/platform-integration/service-auth-policy.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/platform-integration/supply-chain-policy.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/platform-integration/webhook-policy.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/architecture/traffic-auth-policy-registry.v1.json` | Gongzzang 제품 | architecture | current |
| `products/gongzzang/docs/architecture/traffic-auth-policy-registry/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/auth/frontend-integration.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/auth/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/auth/staging-zitadel-integration.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/backend/circuit-breaker.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/backend/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/compliance/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/conventions/comments.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/conventions/enforcement-flow.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/conventions/error-format.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/conventions/git-and-pr.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/conventions/naming-and-ids.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/conventions/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/conventions/rust.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/conventions/sql.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/conventions/testing.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/conventions/typescript.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/conventions/ui-writing-korean.md` | Gongzzang 제품 | convention | current |
| `products/gongzzang/docs/cost/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/data-sources/data-go-kr.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/data-sources/korean-land-mcp.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/data-sources/korean-law-mcp.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/data-sources/korean-law.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/data-sources/naver-maps.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/data-sources/nice-identity.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/data-sources/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/data-sources/v-world.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/database/er-diagram-v001.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/database/migrations.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/database/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/frontend/listings-search.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/frontend/panel-sss-axes.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/frontend/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/glossary.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/governance/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/runbooks/foundation-platform-integration-operations.md` | Gongzzang 제품 | runbook | current |
| `products/gongzzang/docs/runbooks/foundation-platform-workload-identity.md` | Gongzzang 제품 | runbook | current |
| `products/gongzzang/docs/runbooks/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/docs/runbooks/supply-chain-provenance-and-deploy-gate.md` | Gongzzang 제품 | runbook | current |
| `products/gongzzang/docs/ssot-matrix.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/sss-charter.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/testing/load.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/testing/playwright-runtime.md` | Gongzzang 제품 | documentation | current |
| `products/gongzzang/docs/testing/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/infrastructure/docker/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/infrastructure/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/infrastructure/security/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/migrations/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/packages/api-types/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/packages/map/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/packages/ui/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/reference/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/services/gongzzang-api/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/services/gongzzang-outbox-publisher/README.md` | Gongzzang 제품 | README | current |
| `products/gongzzang/tests/load/README.md` | Gongzzang 제품 | README | current |
| `README.md` | Monorepo | README | current |
| `SECURITY.md` | Monorepo | documentation | current |
| `THIRD_PARTY_NOTICES.md` | Monorepo | documentation | current |
| `tools/github/README.md` | Repository tooling | README | current |

## 유지 규칙

1. 이 파일을 직접 편집하지 않습니다. `render-document-catalog.py`가 생성합니다.
2. 계약·fixture·코드가 읽는 경로는 참조를 확인하지 않고 이동하거나 삭제하지 않습니다.
3. 같은 사실을 여러 문서에 복사하지 말고 소유 영역의 SSOT를 링크합니다.
4. `review required`·`evidence` 문서는 현재 운영 지침으로 사용하지 않고 정리 상태를 확인합니다.
