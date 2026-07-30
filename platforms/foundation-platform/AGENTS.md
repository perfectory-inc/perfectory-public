# AGENTS.md

Foundation Platform에서 일하는 AI 에이전트의 공용 진입점이다.

2026-07-02 갱신: Gongzzang ADR-0048과 이 영역 ADR-0021에 따라 이 저장소는
`foundation-platform` data responsibilities. Staff identity and authorization responsibilities are
`identity-platform` responsibilities and must not be
expanded as generic foundation data concerns.

## 현재 저장소 간 경계

플랫폼 이름·레이크하우스 이름·Catalog 소유권·직원 Identity·정규화 거버넌스를 바꾸기 전에 다음을 읽는다.

- [ADR 0021 - Adopt Horizontal Platform Redefinition](./docs/adr/0021-adopt-horizontal-platform-redefinition.md)
- [Gongzzang ADR 0048 - Horizontal Platform Redefinition](../../products/gongzzang/docs/adr/0048-horizontal-platform-redefinition.md)

마커·지도·Catalog 앵커·Gongzzang 연동 코드를 바꾸기 전에 다음을 읽는다.

- [ADR 0008 - PNU Anchor PBF Marker Tile Contract](./docs/adr/0008-pnu-anchor-pbf-marker-tile-contract.md)
- [Gongzzang ADR 0037 - PNU Anchor PBF Marker Tiles](../../products/gongzzang/docs/adr/0037-pnu-anchor-pbf-marker-tiles.md)
- [Gongzzang ADR 0038 - Listing Marker Serving Index and Filter Mask](../../products/gongzzang/docs/adr/0038-listing-marker-serving-index-filter-mask.md)

규칙:

- foundation-platform owns parcel geometry, parcel marker anchors, and public/reference spatial layers.
- Gongzzang owns listing semantics and Gongzzang-owned listing PBF marker tiles.
- foundation-platform must not own Gongzzang listing price, status, exposure rules, search filters, or
  detail payloads.
- foundation-platform anchor registry work must not be used as a reason to move product listing semantics
  into foundation-platform.
- Gongzzang listing marker runtime has a locally verified implementation slice; run the current
  verification SSOT (`cargo xtask verify foundation` and `cargo xtask verify gongzzang`) before
  making any fresh completion claim.
- Operational capabilities (local prelaunch evidence, bounded live data-collection proofs, Bronze
  raw preservation, Silver/Gold quality, PostGIS/anchor/PBF rebuild, regional load, national
  collection scope/manifest/plan/ledger) are invoked through the
  the Rust outbox-publisher subcommands, not PowerShell. None of these proofs amount to
  AWS/deployed cutover completion or full national rollout; `national_rollout_allowed` stays
  `false` until explicit operator approval.
- **Bronze collection pipeline design SSOT:** build the collection pipeline to
  [ADR 0013](./docs/adr/0013-adopt-collection-event-fabric.md) → gongzzang ADR-0047 (Collection Event
  Fabric). Kafka-shaped but broker-deferred; `JobBus` = job dispatch (new, Postgres-backed first),
  `EventBroadcaster` = `raw_written` fan-out only; raw bytes in R2 Bronze with messages carrying
  pointer + `sha256` + status; ledger stays SSOT; reuse `catalog.outbox_quarantine` for DLQ.
- Do not implement Gongzzang listing runtime behavior from foundation-platform.

## 일반 규칙

- Keep docs and source files below 1500 lines.
- Do not hardcode secrets or API keys.
- Do not use direct cross-service database access for product semantics.
- Keep infrastructure changes in code; do not rely on manual console changes.
- Do not claim completion without fresh verification evidence.

## 빌드 SSOT — Cargo(Bazel 폐기)

결정: [ADR 0012](./docs/adr/0012-adopt-cross-repo-bazel-reconciliation.md) → Gongzzang ADR-0044
(2026-06-21 되돌림). Bazel 전환은 **폐기**했고 이 저장소의 빌드·테스트·lint·릴리스 산출물 SSOT는
**Cargo로 고정**한다. ADR 0010/0011과 이전 Bazel 레지스트리·정책은 대체된 역사 기록이므로 구현하지 않는다.

- Build / test / lint / release evidence: **`cargo` only.** There are no Bazel files, targets, or registries.
- Partial builds (the original reason Bazel was considered) are native: `cargo build|test|check -p <crate>`.
- Standard partial verify: `SQLX_OFFLINE=true cargo test -p <crate>` / `cargo clippy -p <crate>`.
- Do NOT reintroduce Bazel, `MODULE.bazel`/`BUILD.bazel`, verification/projection/ratchet registries,
  generated-BUILD-fragment systems, or selector layers — these were deleted as ceremony (ADR-0044
  product-first). Verification logic goes in Rust or standard tools (cargo-deny, gitleaks), not new meta-machinery.
