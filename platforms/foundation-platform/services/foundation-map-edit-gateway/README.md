---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-09-28
---

# Foundation Map Edit Gateway

아직 R2 기본판 타일에 접히지 않은 폴리곤 편집을 담는 Cloudflare module Worker 와 D1 저장소다.
[ADR-0112](../../../../docs/adr/0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md)
의 작은 편집 저장소이며, 타일 경로(`foundation-tile-gateway`)는 이 저장소를 읽지 않는다.

## 계약

[`r2-connections.contract.json`](../../config/r2-connections.contract.json) 의 `map_edit_gateway`
가 Worker 이름, 공개 hostname, D1 binding·이름, 쓰기 토큰 binding, unit 별 feature id 문법과
속성 목록, 도형 한도, 편집 대기 상한, 캐시 정책, CORS 문법의 정본이다. `wrangler.jsonc` 는
`config:render` 산출물이고 `config:check` 가 drift 를 막는다. D1 의 `database_id` 는 저장소에
적지 않는다 — Wrangler 가 첫 배포에서 만들고 연결을 유지한다.

## 경로

- `GET|HEAD /overlay/{unit}` — 공개. 접히지 않은 편집의 feature 별 최신 상태를 돌려준다:
  `hidden_feature_ids`(upsert·delete 모두 — 웹이 기본판에서 가릴 id)와 `features`(upsert 만,
  GeoJSON FeatureCollection, 속성에 unit 의 id 속성 포함). ETag 는
  `{unit}-{folded_through_change_seq}-{latest_change_seq}` 이고 `If-None-Match` 가 맞으면 304 다.
  허용되지 않은 Origin 은 403, query 는 404.
- `POST /edits/{unit}` — 신뢰된 작성자(foundation-api) 전용. `Authorization: Bearer` 가 쓰기 토큰
  secret 과 같아야 하고 브라우저 Origin 이 붙으면 403 이다. 본문은
  `{feature_id, op: upsert|delete, geometry?, properties?, editor, idempotency_key}`. 구조 검사만
  한다(도형 종류·닫힌 고리·2차원 좌표·국토 범위·꼭짓점 상한·속성 목록). 자기교차 같은 위상 검사는
  작성자가 저장 전에 한다. 같은 idempotency key 의 재요청은 같은 `change_seq` 를 200 으로,
  다른 내용이면 409 다. 대기 편집이 상한에 닿으면 409 `fold_required` 다.
- `GET /edits/{unit}?after=&limit=` — 작성자 전용. 레이크하우스 편집 원장으로 내보낼 행을
  `change_seq` 순으로 준다.
- `POST /folds/{unit}` — 작성자 전용. `{folded_through_change_seq, release_id}`. 새 타일이 승격된
  **뒤에** 부른다. 접기 기록과 그 순번 이하 행의 은퇴가 한 트랜잭션이다. 뒤로 가는 접기와 없는
  편집을 주장하는 접기는 409 이고, 같은 접기의 재기록은 무해하다.

## 저장소 불변식

[`migrations/0001_map_edit.sql`](./migrations/0001_map_edit.sql) 의 트리거가 Worker 와 무관하게
막는다: 편집 행 수정 금지(append-only), 접기 기록이 덮지 않은 행의 삭제 금지, 접기 순번 후퇴 금지.

## 로컬 검증

```bash
pnpm install --frozen-lockfile
pnpm run config:check
pnpm run typecheck
pnpm test
pnpm run build:check
```

테스트는 Miniflare 의 실제 D1(SQLite)에 마이그레이션을 적용해 돌리며, 좌표는 공개 저장소의
합성 좌표 범위만 쓴다. 검증 SSOT 인 루트 `cargo xtask verify foundation` 이 다른 gateway 와 함께
실행한다.

## 배포

평소에는 데이터 호스트가 main 을 배포한 뒤 D1 마이그레이션과 이 Worker 를 스스로 배포한다(루트 ADR-0175,
[런북](../../docs/runbooks/worker-autodeploy.md)). 쓰기 토큰 secret 과 CORS 값은 처음 한 번 손으로 둔다.

배포와 Cloudflare 계정 변경은 로컬 구현·검증에 포함하지 않는다. 순서: `pnpm run config:render`
→ 검증 → `pnpm exec wrangler d1 migrations apply foundation-map-edits --remote` →
`pnpm exec wrangler secret put FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN` →
`pnpm exec wrangler deploy --var FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS:<값>`.
