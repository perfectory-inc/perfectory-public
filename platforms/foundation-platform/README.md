---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-11-05
---

# Foundation Platform

산업단지·필지·건물·제조사 canonical 데이터의 SSOT 플랫폼. 공공데이터 수집(collection),
Bronze/Silver/Gold 레이크하우스, 정규화(normalization), Catalog API와 공간·서빙 자산(PBF 타일,
by-PNU 문서)을 소유한다. perfectory 모노레포의 `platforms/foundation-platform` 영역이며,
제품(Gongzzang)은 published HTTP 계약/이벤트로만 소비한다. Staff/서비스 인증·인가는
`platforms/identity-platform` 소유 — Identity DB 직접 읽기 금지, published 계약만 사용한다.

## 책임 트리

```text
foundation-platform/
├── crates/collection/       공공데이터 수집·Bronze
├── crates/catalog/          카탈로그 원장·계약
├── crates/lakehouse/        Silver/Gold 처리 경계
├── crates/normalization/    Foundation 검토·적용
├── crates/stewardship/      자동 도출이 못 정한 검토 항목의 steward 결정
├── crates/publisher/        outbox-publisher 레인 라이브러리 (루트 ADR-0200)
├── crates/ops/              정기 작업 목록(orchestration/jobs.v1.json)과 그 렌더링
├── services/foundation-api  published HTTP API
├── services/foundation-outbox-publisher  수집·outbox 운영 CLI
├── services/foundation-*-gateway         Cloudflare Worker 서빙 관문 (by-pnu·map-edit·profile·tile)
├── orchestration/           Airflow DAG·작업 목록
└── infra/                   DB·lakehouse·관측성·호스트 실행 구성
```

상세 문서는 [Foundation 문서 지도](./docs/README.md)와
[전체 문서 색인 생성 안내](../../docs/README.md)에서 찾는다.

## Workspace

Cargo workspace 멤버의 정본은 [`Cargo.toml`](./Cargo.toml) `[workspace].members` 다. 묶음별로 보면:

```text
crates/
  foundation-shared-kernel        # 공용 타입
  foundation-contracts            # published wire 계약
  foundation-outbox               # outbox·R2 object writer·JobBus
  foundation-workspace-hack       # hakari
  catalog/        {catalog-domain, catalog-application, catalog-infrastructure}
  collection/     {collection-domain, collection-application, collection-infrastructure}
  lakehouse/      {lakehouse-domain, lakehouse-application, lakehouse-infrastructure}
  normalization/  {foundation-normalization-domain, -application, -infrastructure}
  stewardship/    {stewardship-domain, stewardship-application, stewardship-infrastructure}
  technical/      {outbound-http-infrastructure, foundation-shapefile, foundation-disposable-database, foundation-media-renditions}
  ops/foundation-jobs
  publisher/      {foundation-publisher-core, -by-pnu, -collection, -silver, -tiles}
services/
  foundation-api                          # Axum HTTP API (/healthz, /readyz, /metrics)
  foundation-outbox-publisher             # outbox 발행 + 운영 서브커맨드 CLI
```

Cargo workspace 밖: `services/foundation-provider-acquisition-worker`(Python)와
`services/foundation-{by-pnu,map-edit,profile,tile}-gateway`(TypeScript Worker, 각자 `package.json`).

## 로컬 개발

Rust 버전은 `tools/technology-versions.contract.json` 의 `rust_toolchain` 이 정본이고, 모노레포 루트
`rust-toolchain.toml` 단일 핀이 자동 적용된다(영역 내 toolchain 파일 생성 금지). Docker Desktop 필요.

```bash
cp .env.example .env   # placeholder를 로컬 시크릿으로 교체
docker compose up -d   # Postgres+PostGIS(127.0.0.1:15434)·Valkey — bootstrap→migrate→grants→finalize→api
```

compose 스택이 마이그레이션과 least-privilege role 부여(foundation_migrator/foundation_api)까지
수행한다. 이미지 태그·digest 는 `tools/technology-versions.contract.json` 의 `container_images` 가
정본이다. 포스트컨디션 검사를 포함한 스모크 기동:

```bash
bash scripts/compose-smoke.sh -- start-api
```

compose 파일은 `docker-compose.yml`(로컬 개발 스택)과 `compose.*.yml`(lakehouse·lakehouse-native·
observability·recovery·orchestration 과 identity·metadata·orchestration bridge)이다. 각 파일의 용도와
실행 방법은 그 파일 머리 주석이 정본이다.

## 검증

CI와 동일한 fmt + clippy + test (Docker 필요, 모노레포 루트에서):

```bash
bash scripts/verify/cargo-verify.sh platforms/foundation-platform
```

## 문서 라우팅

- [AGENTS.md](./AGENTS.md) — AI 에이전트 규칙 (영역 경계·Bronze 설계 SSOT·Cargo 빌드 SSOT)
- [docs/adr/](./docs/adr/) — 영역 결정 기록
- [docs/openapi/](./docs/openapi/) — published 계약 (`catalog.v1.json`·`data-catalog.v1.json`·
  `lineage-review.v1.json`·`pipeline-graph.v1.json`)
- [docs/runbooks/](./docs/runbooks/) — 운영 런북
- [docs/adr/0029-runtime-environment-backend-separation.md](./docs/adr/0029-runtime-environment-backend-separation.md) — local/CI/staging/production backend boundary
- [docs/runbooks/runtime-environment-separation.md](./docs/runbooks/runtime-environment-separation.md) — R2/Redpanda environment runbook
- [docs/runbooks/postgres-jobbus-contract-test.md](./docs/runbooks/postgres-jobbus-contract-test.md) — real-Postgres JobBus contract test and rollout boundary
- [루트 ADR-0007](../../docs/adr/0007-public-code-private-operations-boundary.md) — 공개 코드와
  비공개 역사·운영 증거의 경계
