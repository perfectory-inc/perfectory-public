---
status: current
owner: gongzzang
doc_type: README
last_reviewed: 2026-11-05
---

# 공짱 (Gongzzang)

산업용 부동산 정보 플랫폼 — 공장·창고·산업단지·지식산업센터 매물과 입지·규제 정보를
매수자(투자자·기업), 매도자, 공인중개사, 시행사에게 제공한다. 한국 시장 고정(한국어·KRW·KST),
반응형 웹 + PWA. 필지·건물·산단 카탈로그 데이터는 파운데이션이 공개한 계약으로만
소비한다 — 공공 API 직접 수집 금지 ([데이터 원천 경계](./docs/data-sources/README.md#카탈로그-원천-경계)).

## 책임 트리

```text
gongzzang/
├── apps/web/                 B2C 웹
├── services/gongzzang-api    제품 HTTP API
├── services/gongzzang-outbox-publisher 이벤트 발행
├── crates/*-domain           제품 도메인
├── packages/                 웹 공유 패키지
├── infrastructure/           Pulumi(레거시 WAF)·로컬 Docker
├── infrastructure-v2/        그린필드 플랫폼 Pulumi
└── docs/                     제품 문서 SSOT
```

문서 지도: [Gongzzang docs](./docs/README.md) ·
[전체 문서 색인 생성 안내](../../docs/README.md)

## 모노레포 내 위치

perfectory 모노레포의 `products/gongzzang` 영역이다. 모든 영역의 공통 규칙(도구 사슬 단일 핀,
상태 점검 라우트, 마이그레이션 이름 규칙 등)은 [루트 README](../../README.md) →
[루트 ADR-0001](../../docs/adr/0001-monorepo-governance-and-conventions.md)이 SSOT다.

## 빠른 시작

Rust 도구 사슬은 모노레포 루트 `rust-toolchain.toml`(단일 핀, 값은 `tools/technology-versions.contract.json` 의 `rust_toolchain`)이 rustup 의 상위 디렉터리
탐색으로 자동 적용된다 — 따로 설치·기본값 명령이 필요 없고, 영역 안에 도구 사슬 파일을 만들면 안 된다.

```bash
pnpm install                  # JS 의존성 (Node·pnpm 버전: package.json engines ← tools/technology-versions.contract.json)
cp .env.example .env          # 로컬 환경 변수 채우기

# DB: PostgreSQL + PostGIS(로컬은 infrastructure/docker/docker-compose.yml). .env의 DATABASE_URL 기준으로 생성+마이그레이션(migrations/)
bash scripts/sqlx-migrate.sh

cargo run -p gongzzang-api    # API 서버 — API_LISTEN_ADDR, 기본 0.0.0.0:8080
pnpm dev                      # 프론트엔드 — turbo run dev → apps/web `next dev`
```

CI 와 같은 Rust 검증(형식 + clippy + 2단계 시험, Docker 필요) — 모노레포 루트에서:

```bash
bash scripts/verify/cargo-verify.sh products/gongzzang
```

(이 디렉터리에서는 `bash ../../scripts/verify/cargo-verify.sh products/gongzzang`.)

## 워크스페이스 구조

Cargo 워크스페이스 멤버의 정본은 [`Cargo.toml`](./Cargo.toml) `[workspace].members` 다. 묶음별로 보면:

- `crates/shared-kernel` — 공용 타입·오류
- 도메인 크레이트 17종 (`crates/*-domain`) — 소비자용(사용자·매물·매물 사진·실거래·
  법원경매·관심 매물·검색 기록·분석 보고서·알림) +
  운영(감사 기록·아웃박스 이벤트·관리자 조치·사업자 확인·매물 검토·
  매물 신고·추천 콘텐츠·시스템 경보)
- 지원 크레이트 — `gongzzang-persistence`·`gongzzang-outbox`·`foundation-platform-client`·
  `product-identity-infrastructure`·`circuit-breaker`·`gongzzang-r2-object-storage`·`parcel-lookup`·`repo-guard`
  (+ hakari `gongzzang-workspace-hack`)
- `services/` — [`gongzzang-api`](./services/gongzzang-api/README.md)(HTTP API)·
  [`gongzzang-outbox-publisher`](./services/gongzzang-outbox-publisher/README.md)

`crates/{api-types,data-clients,embedding}`는 README 만 있는 디렉터리로 워크스페이스 멤버가 아니다.

pnpm 워크스페이스(정본 [`pnpm-workspace.yaml`](./pnpm-workspace.yaml)) — `apps/web`(Next.js 실제 구현;
`apps/platform-web`은 README 뼈대), `packages/`(`api-types`·`map`·`ui`), `infrastructure`(Pulumi).

## 기술 구성 (요약)

Rust + Axum + SQLx · Next.js + React + TypeScript · PostgreSQL + PostGIS · Zitadel(OIDC) · Valkey ·
Biome · Turborepo · Pulumi. 버전·전체 목록과 정본 지도는 [TECH.md](./TECH.md)가 정본이다.

## 진입점 (라우팅)

- [AGENTS.md](./AGENTS.md) — AI 에이전트 규칙 (소유 범위·규칙과 강제 장치·검증 명령)
- [CLAUDE.md](./CLAUDE.md) — Claude Code 한 줄 위임
- [TECH.md](./TECH.md) — 기술 구성 + 정본 지도
- [docs/](./docs/README.md) — 도메인별 정본 문서 (ADR·백엔드·프런트엔드·인증·데이터·…)
- [docs/glossary.md](./docs/glossary.md) — 도메인 용어 (코드 식별자의 정본)
- [docs/sss-charter.md](./docs/sss-charter.md) — SSS 품질 헌장

## 라이선스

[루트 LICENSE](../../LICENSE) — 모든 권리 보유(All Rights Reserved). 공개 열람은 사용·복제·배포 허가가
아니다. 의존성 라이선스는 `deny.toml`(cargo-deny)로 검증한다.
