---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
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

이 워크플로로 다음을 절대 삭제하지 않는다.

- `__r2_data_catalog/`
- `gold/manifest.json`
- `gold/v*/`
- `bronze/source=*/run_id=*/partition=*`

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
