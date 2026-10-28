# AGENTS.md — gongzzang

공짱(B2C 산업용 부동산 정보 제품) 영역에서 일하는 AI 에이전트의 진입점이다. 모노레포 공통 규칙(근본 해결 원칙,
툴체인 단일 핀, 헬스 라우트, 마이그레이션 이름, 영역 간 결합, 시크릿 스캔, 검증 SSOT, ADR 번호)은
[루트 AGENTS.md](../../AGENTS.md)가 소유하며 여기에 다시 적지 않는다. 이 파일은 그 위에 영역 규칙만 더한다.

## 소유하는 것과 소유하지 않는 것

| 공짱이 소유 | 공짱이 소유하지 않음 (소유자) |
|---|---|
| 매물·사용자·북마크·검색 기록·알림·분석 보고서·운영(감사·검수·신고·추천) 도메인 | 필지·건물·산단·제조사 카탈로그, 공공데이터 수집과 원자료 계보 (Foundation — [GZ-ADR-0034](./docs/adr/0034-catalog-ownership-handover-to-foundation-platform.md), [GZ-ADR-0048](./docs/adr/0048-horizontal-platform-redefinition.md), [FP-ADR-0021](../../platforms/foundation-platform/docs/adr/0021-adopt-horizontal-platform-redefinition.md)) |
| 매물 의미와 공짱 매물 PBF 마커 타일 ([GZ-ADR-0037](./docs/adr/0037-pnu-anchor-pbf-marker-tiles.md), [GZ-ADR-0038](./docs/adr/0038-listing-marker-serving-index-filter-mask.md)) | 필지 geometry, PNU 마커 앵커, 공개·기준 공간 레이어 (Foundation — 같은 ADR) |
| B2C 사용자 계정·세션 ([GZ-ADR-0005](./docs/adr/0005-auth-zitadel.md)) | 직원·서비스 신원, Zitadel 기동·구성 정의 (Identity — [루트 ADR-0228](../../docs/adr/0228-one-zitadel-setup-in-identity-platform-and-one-gongzzang-infrastructure-folder.md)) |
| 공짱 웹·API·이벤트 발행 | 직원 콘솔 화면 (더니어 — [루트 ADR-0114](../../docs/adr/0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md)) |

Catalog 데이터는 Foundation Platform 공개 계약(API·이벤트·불변 산출물)으로만 소비한다. 허용·금지 목록은
[데이터 원천 README](./docs/data-sources/README.md#catalog-원천-경계), 파일 단위 경로 소유권은
[foundation-platform-boundary.v1.json](./docs/architecture/foundation-platform-boundary.v1.json)이 정본이다.

## 코드 지도

| 경로 | 내용 |
|---|---|
| `apps/web` | Next.js 웹 (`@gongzzang/web`). `apps/platform-web` 은 README 뿐이다 |
| `services/gongzzang-api` | 제품 HTTP API |
| `services/gongzzang-outbox-publisher` | outbox 이벤트 발행 |
| `crates/*-domain` | 제품 도메인. 목록은 경계 계약의 `path_ownership`(`classification: product_domain`)이 정본 |
| `crates/foundation-platform-client` | Foundation Catalog 계약 소비자와 고정 OpenAPI 스냅숏 |
| `crates/gongzzang-persistence`, `crates/shared-kernel`, 그 밖의 지원 crate | 저장소 구현·공용 타입 |
| `crates/repo-guard` | 영역 가드(파일 길이·crate 배치·마이그레이션 접두) |
| `packages/` | 웹 공유 패키지 (`api-types`, `map`, `ui`) |
| `migrations/` | 제품 DB 마이그레이션 |
| `infrastructure/` | 인프라 코드(Pulumi, [GZ-ADR-0009](./docs/adr/0009-iac-pulumi.md))와 생성된 트래픽·인증 정책 |

## 영역 규칙과 강제 장치

여기 적힌 규칙은 모두 가드·CI·ADR 중 하나가 받친다. 받침이 없는 규칙은 이 파일에 두지 않는다.

| 규칙 | 받침 |
|---|---|
| Foundation 소유 Catalog crate 재도입, Catalog 표 직접 쓰기, Catalog 변경 route 금지 | `scripts/lefthook/foundation-ownership-boundary.sh` (CI `gongzzang-ci.yml`) |
| 도메인 crate 는 경계 계약 `path_ownership` 에 선언되고, crate 는 `crates/` 바로 아래에 둔다 | `cargo run -p repo-guard -- capability-layout` |
| Catalog API 소비 변경은 고정 계약 `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json` 을 함께 갱신한다 | `crates/foundation-platform-client/tests/catalog_contract_pin.rs` |
| 파일 1500줄 초과 금지(500줄부터 경고) | `cargo run -p repo-guard -- file-line-limit` |
| `TODO`·`HACK`·`XXX`·`TEMP`·`임시` 같은 미완 표식과 깨진 인코딩 금지(`.github`·`apps`·`services`·`crates`·`packages`) | `scripts/lefthook/check-forbidden-implementation-markers.sh` |
| 패널 프레임워크는 kind 를 import 하지 않고, URL 문법은 codec 에만, 패널 스택은 store 에 두지 않는다 | `scripts/lefthook/panel-no-*.sh --all` 과 `panel-guards.tests.sh`; 규범은 [panel-sss-axes.md](./docs/frontend/panel-sss-axes.md) |
| 트래픽·인증 정책은 레지스트리 `docs/architecture/traffic-auth-policy-registry.v1.json` 하나에서 생성한다 | CI `traffic-auth-policy-drift` (`cargo run -p gongzzang-api --bin generate-traffic-auth-policy`) |
| 공간 쿼리는 SRID 를 명시한다 | `crates/shared-kernel/src/srid.rs` 의 타입 |
| 매물 위치 정체성은 PNU 이며 매물 행이 정본 좌표를 갖지 않는다 | [GZ-ADR-0018](./docs/adr/0018-pnu-first-identity-no-coordinates.md), GZ-ADR-0037·0038 |
| 사용자에게 보이는 텍스트를 LLM 이 생성하지 않고, `apps`·`services`·`crates`·`packages` 는 MCP/LLM SDK 를 import 하지 않는다 | [GZ-ADR-0010](./docs/adr/0010-scope-information-platform-option-a.md) |
| 인프라는 코드로만 바꾼다 | GZ-ADR-0009 |

영역 ADR 번호(`docs/adr/`)는 0050에서 동결됐다. 새 결정은 루트 `docs/adr/` 의 다음 번호로 쓰고, 영역 ADR 은
`GZ-ADR-NNNN` 으로 인용한다([루트 ADR-0002](../../docs/adr/0002-docs-taxonomy-and-archive.md) 5항).

## 검증

CI 워크플로는 루트 `.github/workflows/gongzzang-*.yml` 다섯 개다(`gongzzang-ci`, `gongzzang-frontend`,
`gongzzang-db-migrations`, `gongzzang-sqlx-prepare`, `gongzzang-walking-skeleton`). 로컬에서 같은 검사를 부르는 법:

```bash
# 모노레포 루트에서
cargo xtask verify gongzzang                          # fmt + clippy + 테스트 (CI 는 --affected)
bash scripts/verify/cargo-verify.sh products/gongzzang # 같은 검증을 CI 와 같은 리눅스 컨테이너에서
DATABASE_URL=... cargo xtask integration gongzzang postgres  # postgres 레인(integration 기능 테스트)
bash scripts/verify/frontend-test.sh                  # 웹 단위 테스트를 격리 컨테이너에서

# products/gongzzang 에서
bash scripts/lefthook/foundation-ownership-boundary.sh
cargo run -p repo-guard -- file-line-limit             # capability-layout, migration-version-prefixes 도 같다
cargo sqlx prepare --workspace --check                 # 마이그레이션을 적용한 DB 가 필요하다
pnpm install --frozen-lockfile
pnpm biome check . && pnpm turbo typecheck && pnpm turbo test
pnpm --filter=@gongzzang/web run test:integration
pnpm markdownlint-cli2 "**/*.md" "#**/node_modules"
```

## 문서

- [docs/README.md](./docs/README.md) — 영역 문서 지도
- [docs/data-sources/README.md](./docs/data-sources/README.md) — 원천별 소유자와 Catalog 경계
- [docs/database/migrations.md](./docs/database/migrations.md) — 마이그레이션 작성
- [docs/glossary.md](./docs/glossary.md) — 도메인 용어 한·영 대응
- [docs/runbooks/README.md](./docs/runbooks/README.md) — 운영 런북
- [docs/adr/](./docs/adr/README.md) — 동결된 영역 결정 기록
