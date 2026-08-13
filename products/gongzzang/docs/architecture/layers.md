---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-07-29
---

# 계층

이 문서는 현재 Gongzzang의 의존성 방향을 설명한다.

## 1. 계층 규칙

의존성 방향:

```text
apps / services
  -> crates/gongzzang-persistence, adapters, route DTOs
  -> crates/*-domain ports and value objects
```

도메인 crate는 런타임 프레임워크·데이터베이스·HTTP client·제공기관 SDK·UI 코드에 의존하지 않는다.

## 2. Domain Layer

도메인 계층은 비즈니스 의미와 컴파일 시 규칙을 소유한다.

현재 예시:

- `crates/listing-domain`
- `crates/listing-photo-domain`
- `crates/user-domain`
- `crates/shared-kernel`
- `crates/real-transaction-domain`
- `crates/court-auction-domain`
- `crates/{bookmark,search-history,analysis-report,notification}-domain`
- `crates/{audit-log,outbox-event}-domain`

허용 의존성:

- shared value objects
- repository ports
- pure domain errors
- serializable DTOs when they are domain-owned

금지 의존성:

- `reqwest`
- `sqlx`
- Axum
- Next.js
- provider-specific response structs

## 3. Adapter Layer

어댑터는 도메인 port와 인프라 사이를 변환한다.

Current examples:

- `crates/gongzzang-persistence`
- `services/gongzzang-api/src/foundation_parcel_lookup.rs`
- `services/gongzzang-api/src/building_reader.rs`
- `services/gongzzang-api/src/photo_upload.rs`
- `services/gongzzang-outbox-publisher/src/foundation_lakehouse_registry.rs`

소유 경계가 요구할 때 어댑터는 `reqwest`, `sqlx`, S3/R2 client, Redis client를 사용할 수 있다.

## 4. Service Layer

서비스는 repository·어댑터·route 상태·middleware·시작 정책을 조합한다.

현재 서비스:

- `services/gongzzang-api`
- `services/gongzzang-outbox-publisher`

## 5. App Layer

프론트엔드 앱은 사용자 상호작용과 제품 UI를 소유한다.

현재 정본 앱:

- `apps/web`

중요한 프론트엔드 경계:

- 사용자 노출 문자열은 typed i18n을 거친다.
- public API 접근은 승인된 proxy/client 경로를 거친다.
- Foundation Platform event receiver는 좁은 통합 route이지 일반 Catalog client가 아니다.

## 6. Policy And Registry Layer

공통 규칙은 JSON/policy 파일에 등록하고 스크립트로 검사한다.

Important registries:

- `docs/architecture/traffic-auth-policy-registry.v1.json`
- `docs/architecture/foundation-platform-boundary.v1.json`
- `docs/architecture/platform-integration/index.v1.json`

생성·파생 런타임 파일은 해당 레지스트리를 따라야 한다.

## 7. Build/Verification Layer

`cargo` (Rust) and `pnpm` + `Turborepo` (frontend) are the build, test, and
verification SSOT (ADR-0002; ADR-0044 reversed the abandoned Bazel transition).

Current state:

- Rust is built/tested/linted with `cargo` (`cargo build`, `cargo test`, `cargo clippy`);
- the frontend is built/tested with `pnpm` + `turbo` (`turbo run build`, `turbo run test`, `turbo run typecheck`);
- off-the-shelf tools (gitleaks, lefthook, cargo-deny) and a small Rust `repo-guard` cover repo-specific guardrails.

목표는 native toolchain으로 재현 가능한 검증을 하는 것이며
ADR-0044에 따라 Bazel 빌드 그래프와 전환 래칫은 제거했다.

## 8. Guardrails

Layer 변경은 Foundation Platform dependency boundary와 platform-integration policy를 유지해야
한다. Foundation Platform catalog boundary는 다음이 강제한다.
`scripts/lefthook/foundation-ownership-boundary.sh` and the boundary contract
`docs/architecture/foundation-platform-boundary.v1.json`.
