---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-28
---

# R2 네임스페이스 오염 복구

## 목적

Foundation Platform R2 네임스페이스에 예상하지 않은 객체가 나타나거나, 레거시 prefix가 현재
객체 배치와 충돌하거나, 정리 후보를 검토해야 할 때 이 런북을 사용한다.

## 원칙

- R2 정리는 인벤토리와 분류부터 시작한다.
- R2 Data Catalog 메타데이터, 정본 manifest 포인터, 런타임 Gold 산출물, 정본 Bronze 계약
  객체는 보호한다.
- `bronze/source=<source>/ingest_date=<date>/run_id=<run_id>/...` 같은 날짜 분할 Bronze 키는
  레거시 객체다. 기존 키를 삭제하기 전에 `bronze/source=<source>/run_id=<run_id>/...`로
  복사하고 검증해야 한다.
- 명시적인 dry-run 계획·허용 prefix·확인 문구 없이는 객체를 삭제하지 않는다.

## 복구 절차

1. 인벤토리 감사를 실행한다.

```bash
cargo run -p foundation-outbox-publisher -- audit-r2-inventory
```

2. `review` 객체를 수동 검토하고 소유자를 지정한다.
3. dry-run 삭제 계획을 생성한다.

```bash
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_ALLOWED_PREFIXES=$'bronze/2026-05/\ngold/staging/' \
cargo run -p foundation-outbox-publisher -- delete-r2-candidates
```

4. `mode`가 `dry_run`이고 `executed_count`가 `0`이며 모든 키가 허용 prefix 안에 있는지
   확인한다.
5. 검토가 끝난 뒤에만 실행한다.

```bash
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_ALLOWED_PREFIXES=$'bronze/2026-05/\ngold/staging/' \
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_EXECUTE=true \
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_CONFIRM_PHRASE="DELETE FOUNDATION PLATFORM R2 CANDIDATES" \
cargo run -p foundation-outbox-publisher -- delete-r2-candidates
```

6. 두 번째 인벤토리 감사와 정리 검증을 실행한다.

## 보호 prefix

삭제 명령(`services/foundation-outbox-publisher/src/r2_delete_candidates.rs` 의 `is_protected_key`)이 스스로
거부하는 것은 다음뿐이다. 전체 규칙은 [R2 Inventory Audit](./r2-inventory-audit.md)의 "보호되는 것과 아닌 것"이다.

- 인벤토리 감사가 `keep` 으로 분류하는 모든 키(`audit_keeps`, 루트 ADR-0264): `__r2_data_catalog/`,
  `control/evidence/`, `gold/manifest.json`, by-PNU 서빙 키, `gold/vector-tiles/{artifacts,manifests,releases}/`,
  `gold/parcel-marker-anchors/artifacts/`, `silver-handoff/` 등
- `warehouse/` 전부
- 날짜 분할이 아닌 `bronze/source=…` 전부

**`gold/v*/` 는 보호되지 않는다.** 감사는 `gold/v<숫자>/…` 를 `legacy_gold_artifact`(`review`)로 두어 후보 파일에 넣지
않지만, 삭제 manifest 에 `delete_candidate` 로 적혀 있으면 지운다. 그래서 manifest 를 손으로 고치지 않고, 허용
prefix 를 `gold/` 처럼 넓게 쓰지 않으며, `gold/v*/` 정리는 그 키를 가리키는 Catalog·manifest 포인터가 없음을 먼저
확인한 뒤 그 prefix 하나로만 dry-run 을 검토하고 실행한다.

레거시 날짜 분할 Bronze 키는 현재 계약 객체로 보호되지는 않지만 정리 작업에서 직접 삭제하지
않는다. 먼저 `write-r2-bronze-key-migration-plan`과 `migrate-r2-bronze-keys`를 사용한다.

## 검증

R2 인벤토리·정리 서브커맨드 테스트를 실행한다.

```bash
cargo test -p foundation-outbox-publisher r2
```

다음 조건을 모두 충족할 때만 복구가 완료된다.

- 정리 후 계획된 삭제 후보가 모두 사라졌다.
- 사전 감사에서 보존한 객체가 같은 키와 크기로 남아 있다.
- 사후 감사 `review_count`가 0이다.
- 검증 보고서 상태가 `passed`다.
