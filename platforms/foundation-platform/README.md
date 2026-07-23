# Foundation Platform

산업단지·필지·건물·제조사 canonical 데이터의 SSOT 플랫폼. 공공데이터 수집(collection),
Bronze/Silver/Gold 레이크하우스, 정규화(normalization), Catalog API와 공간 자산(마커 앵커,
PBF 타일)을 소유한다. perfectory 모노레포의 `platforms/foundation-platform` 영역이며,
제품(Gongzzang)은 published HTTP 계약/이벤트로만 소비한다. Staff/서비스 인증·인가는
`platforms/identity-platform` 소유 — Identity DB 직접 읽기 금지, published 계약만 사용한다.

## Workspace

Cargo workspace 18 members + Python 워커 1:

```text
crates/
  foundation-shared-kernel        # 공용 타입
  foundation-contracts            # published wire 계약
  catalog/        {catalog-domain, catalog-application, catalog-infrastructure}
  collection/     {collection-domain, collection-application, collection-infrastructure}
  lakehouse/      {lakehouse-domain, lakehouse-application, lakehouse-infrastructure}
  normalization/  {foundation-normalization-domain, -application, -infrastructure}
  technical/outbound-http-infrastructure
  foundation-outbox
services/
  foundation-api                          # Axum HTTP API (/healthz, /readyz 포함)
  foundation-outbox-publisher             # outbox 발행 + 운영 서브커맨드 CLI
  foundation-provider-acquisition-worker  # provider 원천 수집 워커 (Python, Cargo workspace 밖)
```

## 로컬 개발

Rust 1.96.0 — 모노레포 루트 `rust-toolchain.toml` 단일 핀이 자동 적용된다 (영역 내
toolchain 파일 생성 금지). Docker Desktop 필요.

```bash
cp .env.example .env   # placeholder를 로컬 시크릿으로 교체
docker compose up -d   # Postgres 17+PostGIS(127.0.0.1:15434) — bootstrap→migrate→grants→finalize→api
```

compose 스택이 마이그레이션과 least-privilege role 부여(foundation_migrator/foundation_api)까지
수행한다. 포스트컨디션 검사를 포함한 스모크 기동:

```bash
bash scripts/compose-smoke.sh -- start-api
```

compose 파일 4종:

| 파일 | 용도 |
|---|---|
| `docker-compose.yml` | 로컬 개발 (Postgres/Redis + 부트스트랩·마이그레이션·API·스모크) |
| `compose.lakehouse.yml` | lakehouse compute (Trino 등 — API/DB 자격 없이 단독 실행 가능) |
| `compose.observability.yml` | Prometheus 등 관측성 스택 |
| `compose.recovery.yml` | pgBackRest 백업/복구 리허설 (R2) |

## 검증

CI와 동일한 fmt + clippy + test (Docker 필요, 모노레포 루트에서):

```bash
bash scripts/verify/cargo-verify.sh platforms/foundation-platform
```

## 문서 라우팅

- [AGENTS.md](./AGENTS.md) — AI 에이전트 규칙 (영역 경계·Bronze 설계 SSOT·Cargo 빌드 SSOT)
- [docs/adr/](./docs/adr/) — 영역 결정 기록 (ADR 27편)
- [docs/openapi/catalog.v1.json](./docs/openapi/catalog.v1.json) ·
  [docs/openapi/pipeline-graph.v1.json](./docs/openapi/pipeline-graph.v1.json) — published 계약
- [docs/runbooks/](./docs/runbooks/) — 운영 런북
- [루트 ADR-0007](../../docs/adr/0007-public-code-private-operations-boundary.md) — 공개 코드와
  비공개 역사·운영 증거의 경계
