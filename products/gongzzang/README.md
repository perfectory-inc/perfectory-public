# 공짱 (Gongzzang)

산업용 부동산 정보 플랫폼 — 공장·창고·산업단지·지식산업센터 매물과 입지·규제 정보를
매수자(투자자·기업), 매도자, 공인중개사, 시행사에게 제공한다. 한국 시장 고정(한국어·KRW·KST),
반응형 웹 + PWA. 필지·건물·산단 카탈로그 데이터는 Foundation Platform published contract로만
소비한다 — 공공 API 직접 수집 금지 ([AGENTS.md](./AGENTS.md) §0.5/§3).

## 모노레포 내 위치

perfectory 모노레포의 `products/gongzzang` 영역이다. 전 영역 공통 규칙(툴체인 단일 핀,
헬스 라우트, 마이그레이션 네이밍 등)은 [루트 README](../../README.md) →
[루트 ADR-0001](../../docs/adr/0001-monorepo-governance-and-conventions.md)이 SSOT다.

## 빠른 시작

Rust 툴체인은 모노레포 루트 `rust-toolchain.toml`(1.96.0 단일 핀)이 rustup의 상위 디렉토리
탐색으로 자동 적용된다 — 별도 install/default 명령 불필요, 영역 내 toolchain 파일 생성 금지.

```bash
pnpm install                  # JS 의존성 (pnpm 9.12, Node 20.19+/22.12+)
cp .env.example .env          # 로컬 환경 변수 채우기

# DB: PostgreSQL 17 + PostGIS. .env의 DATABASE_URL 기준으로 생성+마이그레이션(migrations/ 20편)
bash scripts/sqlx-migrate.sh

cargo run -p gongzzang-api    # API 서버 — API_LISTEN_ADDR, 기본 0.0.0.0:8080
pnpm dev                      # 프론트엔드 — turbo run dev → apps/web `next dev`
```

CI와 동일한 Rust 검증(fmt + clippy + 2단계 테스트, Docker 필요) — 모노레포 루트에서:

```bash
bash scripts/verify/cargo-verify.sh products/gongzzang
```

(이 디렉토리에서는 `bash ../../scripts/verify/cargo-verify.sh products/gongzzang`.)

## 워크스페이스 구조

Cargo workspace **27 members** (`Cargo.toml`):

- `crates/shared-kernel` — 공용 타입/에러
- 도메인 crate 17종 (`crates/*-domain`) — B2C(user·listing·listing-photo·real-transaction·
  court-auction·bookmark·search-history·analysis-report·notification) +
  운영(audit-log·outbox-event·admin-action·business-verification·listing-review·
  listing-report·featured-content·system-alert)
- 지원 crate 7종 — `gongzzang-persistence`·`gongzzang-outbox`·`foundation-platform-client`·
  `product-identity-infrastructure`·`circuit-breaker`·`parcel-lookup`·`repo-guard`
- `services/` 2종 — [`gongzzang-api`](./services/gongzzang-api/README.md)(HTTP API)·
  `gongzzang-outbox-publisher`

pnpm workspace — `apps/web`(Next.js 16 실구현; admin·admin-web·platform-web은 README 스캐폴드),
`packages/` 7종(api-client·api-types·map·shared·tsconfig·ui·ui-web), `infrastructure`(Pulumi), `tools`.

## 기술 스택 (요약)

Rust + Axum + SQLx · Next.js 16 + React 19 + TypeScript · PostgreSQL 17 + PostGIS ·
Naver Maps · Zitadel(OIDC) · moka + Valkey · Biome · Turborepo · Pulumi ·
Grafana/Prometheus/Loki/Tempo/Sentry/OpenTelemetry. 상세와 SSOT 맵: [TECH.md](./TECH.md)

## 진입점 (라우팅)

- [AGENTS.md](./AGENTS.md) — AI 에이전트 규칙 (절대 규칙·영역 경계·작업별 라우팅 표)
- [CLAUDE.md](./CLAUDE.md) — Claude Code 1줄 위임
- [TECH.md](./TECH.md) — 기술 스택 + SSOT 맵
- [docs/](./docs/README.md) — 도메인별 SSOT 문서 (adr·backend·frontend·auth·data·…)
- [docs/glossary.md](./docs/glossary.md) — 도메인 용어 (코드 식별자 SSOT)
- [docs/sss-charter.md](./docs/sss-charter.md) — SSS 품질 헌장

## 라이선스

[루트 LICENSE](../../LICENSE) — All Rights Reserved. 공개 열람은 사용·복제·배포 허가가
아니다. 의존성 라이선스는 `deny.toml`(cargo-deny)로 검증한다.
