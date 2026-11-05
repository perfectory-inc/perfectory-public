---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-11-05
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

## 2. 도메인 계층

도메인 계층은 비즈니스 의미와 컴파일 시 규칙을 소유한다.

위치: `crates/<capability>-domain` 과 `crates/shared-kernel` (예: `crates/listing-domain`,
`crates/listing-photo-domain`, `crates/user-domain`). 목록 정본은 디렉터리 자체이고, 각 도메인
crate의 소유자 선언은 [`foundation-platform-boundary.v1.json`](./foundation-platform-boundary.v1.json)
`path_ownership` 에 있다 — repo-guard `capability-layout` 이 둘이 어긋나면 실패시킨다.

허용 의존성:

- 공유 값 객체
- repository port
- 순수 도메인 에러
- 도메인이 소유하는 직렬화 DTO

금지 의존성:

- `reqwest`
- `sqlx`
- Axum
- Next.js
- 제공기관 전용 응답 구조체

## 3. 어댑터 계층

어댑터는 도메인 port와 인프라 사이를 변환한다.

현재 예시:

- `crates/gongzzang-persistence`
- `crates/foundation-platform-client`
- `crates/gongzzang-r2-object-storage`
- `services/gongzzang-api/src/foundation_parcel_lookup.rs`
- `services/gongzzang-api/src/photo_upload.rs`
- `services/gongzzang-outbox-publisher/src/foundation_lakehouse_registry.rs`

소유 경계가 요구할 때 어댑터는 `reqwest`, `sqlx`, S3/R2 client, Valkey(Redis 프로토콜) client를 사용할 수 있다.

## 4. 서비스 계층

서비스는 repository·어댑터·route 상태·middleware·시작 정책을 조합한다.

현재 서비스:

- `services/gongzzang-api`
- `services/gongzzang-outbox-publisher`

## 5. 앱 계층

프론트엔드 앱은 사용자 상호작용과 제품 UI를 소유한다.

현재 정본 앱:

- `apps/web`

중요한 프론트엔드 경계:

- 사용자 노출 문자열은 typed i18n(`apps/web/lib/i18n/`)을 거친다.
- public API 접근은 승인된 proxy/client 경로를 거친다.
- Foundation Platform event receiver(`apps/web/app/foundation-platform/events/route.ts`)는 좁은 통합 route이지 일반 Catalog client가 아니다.

## 6. 정책·레지스트리 계층

공통 규칙은 JSON 정책 파일에 등록한다.

- `docs/architecture/traffic-auth-policy-registry.v1.json` — 생성기 입력. 산출물 재현은 CI
  `Traffic/auth policy drift` 잡이 검사한다.
- `docs/architecture/foundation-platform-boundary.v1.json` — 경로 소유권. 도메인 crate 선언은
  repo-guard `capability-layout` 이 읽는다.
- `docs/architecture/platform-integration/index.v1.json` — 플랫폼 연동 정책 선언. 지금은 이 파일을
  읽는 검사가 없다.

생성·파생 런타임 파일은 해당 레지스트리를 따라야 한다.

## 7. 빌드·검증 계층

`cargo`(Rust)와 `pnpm` + Turborepo(프론트엔드)가 빌드·테스트·검증 SSOT다 (ADR-0002;
ADR-0044가 중단한 Bazel 전환을 되돌렸다).

현재 상태:

- Rust는 `cargo`로 빌드·테스트·린트하며, CI 입구는 `cargo xtask verify gongzzang` 이다.
- 프론트엔드는 `pnpm` + `turbo`로 빌드·테스트한다 (`turbo run build`, `turbo run test`, `turbo run typecheck`).
- 기성 도구(gitleaks, lefthook, cargo-deny)와 작은 Rust `repo-guard`가 저장소 전용 가드를 맡는다.

## 8. 가드

계층 변경은 Foundation Platform 의존 경계를 유지해야 한다. Foundation Platform Catalog 경계는
`scripts/lefthook/foundation-ownership-boundary.sh`(CI `gongzzang-ci.yml` `Repo guardrails`)가
강제한다 — Foundation 소유 crate 재도입, Catalog 표 직접 쓰기, Catalog 변경 route를 막는다.
