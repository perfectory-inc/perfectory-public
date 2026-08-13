---
status: current
owner: gongzzang
doc_type: README
last_reviewed: 2026-07-29
---

# 아키텍처 결정 기록(ADR)

모든 기술·아키텍처 결정의 영구 기록.

## 작성 원칙

- 시간 순서가 의미 → `NNNN-` prefix 유지
- 한 결정 = 한 파일
- 승인 후 *수정 금지*. 변경은 새 ADR로 남기고 `Supersedes`를 표시한다.
- 재검토 / 보류는 *trigger 명시*

## 템플릿

```markdown
# ADR-NNNN: <제목>

| | |
|---|---|
| 작성일 | YYYY-MM-DD |
| 상태 | 제안 / 승인 / 폐기 / ADR-XXX로 대체 |
| 결정자 | <이름 또는 역할> |

## 컨텍스트
<왜 이 결정이 필요한가, 어떤 제약이 있는가>

## 결정
<무엇을 정했는가, 한 문장>

## 대안
- 대안 1: <왜 안 함>
- 대안 2: <왜 안 함>

## 결과
- 긍정: <이 결정으로 얻는 것>
- 부정: <이 결정의 비용>
- 영향 받는 영역: <crate / 폴더 / 시스템>

## 재검토 트리거
- <조건 1>
- <조건 2>

## 참조

각 ADR의 관련 문서는 해당 ADR 파일 안의 상대 Markdown 링크로 연결한다.
```

## 인덱스

| # | 제목 | 상태 |
|---|------|------|
| [0001](./0001-language-rust-ts.md) | 언어 — Rust + TypeScript | Accepted |
| [0002](./0002-monorepo-cargo-pnpm-turbo.md) | 모노레포 — Cargo + pnpm + Turborepo | Accepted |
| [0003](./0003-frontend-nextjs-react19.md) | 프론트엔드 — Next.js 16 + React 19 | Accepted |
| [0004](./0004-db-postgres-postgis.md) | DB — PostgreSQL 17 + PostGIS | Accepted |
| [0005](./0005-auth-zitadel.md) | 인증 IdP — Zitadel | Accepted |
| [0006](./0006-api-rest-openapi.md) | API — REST + OpenAPI (utoipa) | Accepted |
| [0007](./0007-cache-moka-valkey.md) | 캐시 — moka L1 + Valkey L2 | Accepted |
| [0008](./0008-observability-grafana-otel-sentry.md) | 관측성 — Grafana + OTel + Sentry | Accepted |
| [0009](./0009-iac-pulumi.md) | IaC — Pulumi (TypeScript) | Accepted |
| [0010](./0010-scope-information-platform-option-a.md) | 범위 — 산업용 부동산 정보 플랫폼 (옵션 A) | Accepted |
| [0011](./0011-embedding-gemini-pgvector.md) | 임베딩 — Gemini + pgvector (Phase 3+) | Accepted |
| [0012](./0012-pipeline-visualization-react-flow.md) | 파이프라인 시각화 — React Flow (xyflow) | Accepted |
| [0013](./0013-listing-search-naver-maps.md) | 매물 검색 지도 vendor — Naver Maps (SP6-ii) | Accepted |
| [0014](./0014-base-layer-defer-pmtiles.md) | 지도 base layer (전국 필지 polygon) — 보류 (R2 PMTiles SSS 부적합) | **Superseded by 0016** |
| [0015](./0015-v-world-acl-rearchitecture.md) | V-World ACL 재설계 — fixture-driven, layer-decomposed, envelope-aware | Accepted |
| [0016](./0016-medallion-base-layer-postgis-silver-pmtiles-gold.md) | 지도 base layer — 과거 PMTiles 직접/no-PostGIS 결정 | **0036으로 대체** |
| [0017](./0017-listing-marker-render-canvas-bitmap-stamp.md) | 매물 마커 렌더링 — Naver Marker + Canvas content + BitmapStampCache (단일 렌더 박자) | Accepted |
| [0018](./0018-pnu-first-identity-no-coordinates.md) | 매물 정체성 — PNU-First (좌표는 매칭/검색에 사용 안 함) | Accepted |
| [0019](./0019-pmtiles-source-via-addsourcetype.md) | PMTiles 통합 — VectorTileSource subclass + Service Worker transport | **Superseded by 0021** |
| [0020](./0020-naver-vector-interaction-model.md) | Naver gl SDK vector 한계와 플랫폼 상호작용 모델(probe 범위 = polygon 전용) | Accepted |
| [0021](./0021-static-vector-tile-decomposition.md) | 과거 PMTiles를 flat `{z}/{x}/{y}.pbf`로 분해한 결정 | **0036으로 대체** |
| [0022](./0022-bronze-scraping-isolated-python-service.md) | Bronze HTML scraping = 격리 Python service (`services/scraper-py/`) + Scrapling | Accepted |
| [0023](./0023-audit-2026-05-08-hardening.md) | Codex audit 2026-05-08 hardening — `/internal/auth/event` shared secret + production fail-fast + JTI fail-closed + structured map errors | Accepted (partial — handoff) |
| [0024](./0024-etl-cancel-protocol-immediate-abort.md) | ETL cancel protocol — 즉시 abort + L3 staging atomicity 보호 (state machine 거부) | Accepted |
| [0025](./0025-bronze-scraping-workflow-orchestrator-not-rust-spawn.md) | Bronze producer 격리와 Foundation Platform으로의 수집 orchestration 이전 | 운영상 0034/0048로 대체 |
| [0026](./0026-bronze-api-archive-r2-not-postgres-jsonb.md) | Bronze API archive — R2 (S3-호환 객체 저장소) 로 이전, Postgres jsonb 폐기 (cost + UPSERT 손실 + 시계열 보존) | Accepted |
| [0027](./0027-admin-complex-layer-source-deferred.md) | 정적 layer는 source별 데이터를 사용하고 가용성은 Foundation manifest에서 결정 | 운영상 0034/0036/0048로 대체 |
| [0028](./0028-supply-chain-sha-pin-and-cleanup-cron.md) | Supply-chain SHA pin; manifest cleanup portion superseded by ADR 0036/foundation-platform ADR 0004 | Accepted |
| [0029](./0029-explicit-environment-separation.md) | Explicit environment and atomic secret separation | Accepted invariant (legacy path superseded by 0035) |
| [0030](./0030-three-service-architecture.md) | Historical three-service architecture; core-centered naming superseded by ADR 0048 | Superseded in part by 0048 |
| [0031](./0031-foundation-platform-bounded-contexts.md) | Historical Catalog/Workforce boundary; split into Foundation/Identity by ADR 0048 | Superseded by 0048 |
| [0032](./0032-eventual-consistency-strategy.md) | Cross-service Eventual Consistency 전략 | Accepted |
| [0033](./0033-seven-guardrails-enforcement.md) | 7 Guardrails — cross-service 코드 리뷰 자동 강제 | Accepted |
| [0034](./0034-catalog-ownership-handover-to-foundation-platform.md) | 과거 Catalog 추출 기록. 현재 소유자 명칭은 ADR 0048에 따라 Foundation Platform | 승인(역사 명칭) |
| [0035](./0035-legacy-r2-removal-and-atomic-namespace.md) | Legacy `R2_*` + `ETL_BUILD_ENV` 완전 제거 + atomic namespace 강제 (ADR 0029 backward-compat 제거) | Accepted |
| [0036](./0036-static-vector-tile-runtime-contract.md) | Foundation vector tile runtime — 정확한 v1/v2 dispatch, publication unit당 완전한 Martin source 하나 | Accepted |
| [0037](./0037-pnu-anchor-pbf-marker-tiles.md) | PNU Anchor PBF Marker Tiles — Foundation anchor 위치와 Gongzzang PBF marker runtime | Accepted |
| [0038](./0038-listing-marker-serving-index-filter-mask.md) | 매물 marker serving index·filter mask — 확장 가능한 동적 제공 | Accepted |
| [0039](./0039-service-owned-lakehouse-registry-integration.md) | 서비스 소유 Lakehouse와 Foundation registry 통합 | Accepted |
| [0040](./0040-bazel-first-build-verification-control-plane.md) | Bazel 우선 build·검증 제어면 | 0044로 대체 |
| [0041](./0041-hermetic-javascript-package-bazel-rules.md) | Hermetic JavaScript package Bazel 규칙 | 0044로 대체 |
| [0042](./0042-cross-repo-bazel-native-build-graph.md) | 영역 간 Bazel-native build graph | 0044로 대체 |
| [0043](./0043-bazel-transition-provisioning-decisions.md) | Bazel 전환 provisioning 결정 | 0044로 대체 |
| [0044](./0044-bazel-transition-reconciliation.md) | Native build·검증 SSOT | Accepted |
| [0045](./0045-adr-placement-cross-repo-governance.md) | ADR 배치와 영역 간 거버넌스 | 루트 ADR-0001로 대체 |
| [0046](./0046-kafka-kubernetes-preliminary-design.md) | Kafka·Kubernetes 예비 설계 — trigger 전까지 보류 | Accepted(보류) |
| [0047](./0047-collection-event-fabric.md) | Collection Event Fabric — broker 없이 유지하는 Bronze 수집 제어면 | Accepted(broker 보류) |
| [0048](./0048-horizontal-platform-redefinition.md) | 수평 플랫폼 재정의 | Accepted |
| [0049](./0049-identity-platform-contract-design.md) | Identity Platform 계약 설계 | Accepted |
| [0050](./0050-dawneer-workbench-and-internal-admin-surface.md) | Dawneer workbench와 내부 admin 화면 | Accepted |
