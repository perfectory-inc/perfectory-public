---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-28
---

# R2 Inventory Audit

## 목적

Cloudflare R2 bucket의 현재 object를 읽기 전용으로 수집한 뒤 Foundation Platform 소유권 규칙에 따라
`keep`, `delete_candidate`, `review`로 분류한다.

이 runbook은 삭제를 수행하지 않는다. 삭제 전에는 반드시 `r2-delete-candidates.json`을 사람이 검토하고,
별도 삭제 workflow에서 명시적인 prefix allow-list와 최종 승인 절차를 거쳐야 한다.

## 분류 규칙

정본은 `services/foundation-outbox-publisher/src/r2_inventory_audit.rs` 의 `classify_key` 다(위에서부터 처음 맞는 규칙).
이 표는 그 요약이다.

| Prefix / Key | Classification | Action | 의미 |
|---|---|---|---|
| `__r2_data_catalog/` | `managed_iceberg_catalog` | `keep` | R2 Data Catalog / Iceberg 내부 메타데이터 |
| `control/evidence/bronze-catalog-recovery/<kind>/<file>` | `bronze_catalog_recovery_evidence` | `keep` | Bronze Catalog 복구 적용의 내용 주소 증거 |
| `control/evidence/` | `control_evidence` | `keep` | 운영 증거. 지우지 않는다 |
| `gold/manifest.json` | `runtime_manifest_pointer` | `keep` | canonical runtime manifest pointer |
| by-PNU 서빙 키(manifest·base 객체·patch 객체·교체된 manifest 이력, pack 레인 manifest·이력) | `*_by_pnu_serving_*` | `keep` | `r2_layout::by_pnu` 가 알아보는 키 (루트 ADR-0096·0141·0190) |
| `gold/vector-tiles/artifacts/`, `gold/vector-tiles/manifests/`, `gold/vector-tiles/releases/`, `gold/parcel-marker-anchors/artifacts/` | `runtime_spatial_artifact` | `keep` | 불변 공간 산출물·manifest |
| `gold/v<숫자>/…`, `gold/parcel-marker-anchor-pbf/`, `gold/parcel-marker-anchor-aggregate-pbf/`, `gold/parcel-marker-anchor-runtime/`, `artifacts/` 밖의 `gold/parcel-marker-anchors/` | `legacy_gold_artifact` | `review` | 옛 Gold 배치. **보호 대상이 아니다** — 아래 "보호되는 것과 아닌 것" |
| `silver-handoff/` | `silver_handoff_artifact` | `keep` | 재생 가능한 승격 입력·감사 출력 |
| `bronze/source=*/ingest_date=*/run_id=*/partition=*` | `legacy_date_partitioned_bronze` | `review` | legacy date-partitioned Bronze object; migrate before deletion |
| `bronze/source=…` 중 키 계약 위반 | `legacy_bronze_path_contract` | `review` | 메타데이터를 Catalog 로 옮기고 bytes 를 보존한 뒤에만 정리 |
| `bronze/source=*/run_id=*/partition=*` (`catalog.bronze_object` 에 있음) | `current_bronze_contract` | `keep` | source/run_id/partition Bronze object contract |
| 같은 모양이나 `catalog.bronze_object` 에 없음 | `bronze_catalog_metadata_missing` | `review` | 메타데이터 없는 Bronze bytes |
| 같은 모양, Catalog 에 없고 displaced 사건이 orphan 창(기본 24시간)보다 오래됨 | `bronze_orphan_candidate` | `delete_candidate` | 단, 삭제 명령은 `bronze/source=` 계약 키를 거부한다 |
| `warehouse/` 중 `smoke` 포함 | `iceberg_catalog_cleanup_required` | `review` | Catalog 로 지운다. raw R2 삭제 금지 |
| `warehouse/` | `managed_iceberg_table` | `keep` | Iceberg 테이블 데이터·메타데이터 |
| `gold/staging/` | `staging_gold_artifact` | `delete_candidate` | 장기 보존 대상이 아닌 staging artifact |
| `bronze/YYYY-MM/` | `legacy_uncontracted_bronze` | `delete_candidate` | 현재 Bronze 계약 밖의 legacy 날짜형 prefix |
| `*smoke*` | `smoke_artifact` | `delete_candidate` | 남아 있으면 정리 가능한 smoke artifact |
| 기타 (예: `gold/vector-tiles/runtime-manifest.json`) | `unknown` | `review` | 사람이 소유권을 확인해야 하는 object |

## 실행

실제 R2를 읽어 audit report를 생성한다.

```bash
cargo run -p foundation-outbox-publisher -- audit-r2-inventory
```

기본 출력:

```text
target/r2-inventory-audit/r2-inventory-audit.json
target/r2-inventory-audit/r2-delete-candidates.json
```

특정 prefix만 읽을 수도 있다.

```bash
FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_PREFIX="bronze/" \
cargo run -p foundation-outbox-publisher -- audit-r2-inventory
```

이미 저장한 `aws s3api list-objects-v2` JSON으로 offline audit를 실행할 수도 있다.

```bash
FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_INPUT_JSON=target/r2-inventory-audit/sample-list-objects.json \
FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_OUTPUT_DIR=target/r2-inventory-audit/offline \
FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_USAGE_METRICS_PATH=target/r2-inventory-audit/offline/r2-usage.prom \
FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_ESTIMATED_LIST_REQUEST_UNIT_COST_USD="0.000000" \
cargo run -p foundation-outbox-publisher -- audit-r2-inventory
```

## Billing Export Metrics

## Live Smoke Metrics

선택적인 R2 live smoke는 전용 smoke를 위한 Prometheus text artifact를 쓸 수 있다.
write/read/delete round trip:

```bash
export FOUNDATION_PLATFORM_R2_SMOKE_METRICS_PATH="target/r2-live-smoke/r2-smoke.prom"
cargo run -p foundation-outbox-publisher --bin foundation-outbox-publisher -- smoke-r2
```

metric 산출물은 다음을 포함한다.

- `foundation_platform_r2_smoke_request_total`
- `foundation_platform_r2_smoke_bytes_verified`

metric label에는 source와 operation만 들어간다. object key는 넣지 않으므로 smoke 실행마다
고카디널리티 series가 생기지 않는다. 이 smoke 를 돌리는 CI job 은 없다 — 필요할 때 사람이 위 명령으로 돌린다.

Inventory audit의 list request cost는 호출자가 넣은 단가 기반 estimate다. 실제 R2 request/cost
accounting은 Cloudflare billing export를 내려받아 normalized CSV 또는 JSON으로 저장한 뒤 별도
offline 변환 스크립트로 Prometheus text artifact를 생성한다.

collector dry-run은 signed URL query를 plan과 로그에서 redaction 하고, 실제 download를 수행하지 않는다.

```bash
FOUNDATION_PLATFORM_R2_BILLING_EXPORT_COLLECT_EXPORT_URL="https://billing.example.invalid/accounts/foundation-platform/r2.csv?signature=..." \
FOUNDATION_PLATFORM_R2_BILLING_EXPORT_COLLECT_OUTPUT_PATH=target/r2-billing/cloudflare-r2-billing.csv \
FOUNDATION_PLATFORM_R2_BILLING_EXPORT_COLLECT_PLAN_OUTPUT_PATH=target/r2-billing/collection-plan.json \
FOUNDATION_PLATFORM_R2_BILLING_EXPORT_COLLECT_AUTH_TOKEN_ENV=CF_BILLING_EXPORT_TOKEN \
cargo run -p foundation-outbox-publisher -- collect-r2-billing-export
```

실제 수집은 `EXECUTE=true` 와 `CONFIRM_BILLING_EXPORT_COLLECTION=true` 를 둘 다 명시해야 열린다.
Bearer token 이 필요한 export endpoint 는 `FOUNDATION_PLATFORM_R2_BILLING_EXPORT_COLLECT_AUTH_TOKEN_ENV` 로 process 환경변수 이름만 넘긴다.

필수 컬럼:

```text
provider,service,bucket,operation,request_count,usage_bytes,cost_usd,currency,period_start,period_end,source_export_id
```

실행 예:

```bash
FOUNDATION_PLATFORM_R2_BILLING_USAGE_METRICS_INPUT_CSV=target/r2-billing/cloudflare-r2-billing.csv \
FOUNDATION_PLATFORM_R2_BILLING_USAGE_METRICS_BUCKET_NAME="foundation-platform-lakehouse" \
FOUNDATION_PLATFORM_R2_BILLING_USAGE_METRICS_OUTPUT_DIR=target/r2-billing \
FOUNDATION_PLATFORM_R2_BILLING_USAGE_METRICS_METRICS_PATH=target/r2-billing/r2-billing.prom \
cargo run -p foundation-outbox-publisher -- r2-billing-usage-metrics
```

출력 metric:

- `foundation_platform_r2_billing_request_total`
- `foundation_platform_r2_billing_usage_bytes`
- `foundation_platform_r2_billing_cost_usd`

이 metric은 billing export artifact 기준의 실제 청구 데이터 변환이다. collector 는 HTTPS/loopback
download guard와 secret redaction baseline 이며, Cloudflare billing API 연동과 dashboard scrape 배포는
별도 운영 작업으로 남아 있다.

## 필요한 환경 변수

`.env.local` 또는 process 환경에 다음 값이 필요하다.

```text
FOUNDATION_PLATFORM_RUNTIME_ENV=local|ci|staging|production
FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-dev  # local process uses the remote dev bucket
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT=              # 또는 FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION=auto
FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY=
DATABASE_URL=                                           # 또는 FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_DATABASE_URL
```

`audit-r2-inventory` 는 offline(`INPUT_JSON`) 실행에서도 `catalog.bronze_object` 와 orphan 사건을 읽으므로
DB URL 이 없으면 거부한다(`bronze_orphan_database_url`). orphan 창은 `FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_BRONZE_ORPHAN_MIN_AGE_HOURS`
(기본 `24`).

스크립트는 secret 값을 출력하지 않는다.

## 삭제 전 체크리스트

1. `r2-inventory-audit.json`의 `review_count`가 0인지 확인한다.
2. `r2-delete-candidates.json`의 모든 key가 예상 prefix에만 속하는지 확인한다.
3. `r2-delete-candidates.json` 은 audit 가 쓴 그대로 쓴다 — 손으로 키를 더하거나 `action` 을 바꾸지 않는다.
   삭제 명령은 manifest 의 `action == "delete_candidate"` 를 믿고, 스스로 다시 거부하는 것은 아래 "보호되는 것과
   아닌 것"의 키뿐이다. 특히 `gold/v<숫자>/…` 는 audit 가 `review` 로 두지만 **삭제 명령은 막지 않는다**.
   Date-partitioned `bronze/source=*/ingest_date=*/...` keys must be migrated before deletion.
4. `FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_USAGE_METRICS_PATH` artifact 가 `foundation_platform_r2_inventory_object_count`,
   `foundation_platform_r2_inventory_total_size_bytes`, `foundation_platform_r2_inventory_delete_candidate_count`,
   `foundation_platform_r2_inventory_review_count`, `foundation_platform_r2_inventory_list_request_count` 를
   포함하는지 확인한다.
   `FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_ESTIMATED_LIST_REQUEST_UNIT_COST_USD` 를 지정했다면
   `foundation_platform_r2_inventory_estimated_list_request_cost_usd` 도 포함된다. 이 값은 호출자가
   제공한 단가로 계산한 estimate 이며 Cloudflare billing export 를 대체하지 않는다.
5. billing export를 확보한 경우 `r2-billing-usage-metrics` 출력이
   `foundation_platform_r2_billing_request_total`, `foundation_platform_r2_billing_usage_bytes`,
   `foundation_platform_r2_billing_cost_usd` 를 포함하는지 확인한다.
6. 삭제 workflow는 dry-run을 기본값으로 두고, 실제 삭제는 별도 명시 플래그와 승인 문구를 요구해야 한다.
7. 삭제 후에는 audit를 다시 실행해 candidate가 사라졌고 keep object가 유지되는지 확인한다.

## 삭제 계획 dry-run

`r2-delete-candidates.json`을 실제 삭제하지 않고 계획으로만 변환한다.

```bash
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_ALLOWED_PREFIXES=$'bronze/2026-05/\ngold/staging/' \
cargo run -p foundation-outbox-publisher -- delete-r2-candidates
```

`FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_ALLOWED_PREFIXES` 는 한 줄에 하나씩 prefix 를 적는다.

기본 출력:

```text
target/r2-delete-candidates/r2-delete-plan.json
```

`mode`는 `dry_run`이어야 하고, `executed_count`는 `0`이어야 한다.

## 실제 삭제 가드

실제 삭제는 다음 세 조건이 모두 맞아야 열린다.

1. `FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_EXECUTE=true`를 명시한다.
2. `FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_CONFIRM_PHRASE="DELETE FOUNDATION PLATFORM R2 CANDIDATES"`를 정확히 입력한다.
3. `FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_ALLOWED_PREFIXES`를 명시하고, 모든 삭제 후보 key가 해당 prefix 안에 있어야 한다.

예시:

```bash
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_ALLOWED_PREFIXES=$'bronze/2026-05/\ngold/staging/' \
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_EXECUTE=true \
FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_CONFIRM_PHRASE="DELETE FOUNDATION PLATFORM R2 CANDIDATES" \
cargo run -p foundation-outbox-publisher -- delete-r2-candidates
```

### 보호되는 것과 아닌 것

삭제 명령(`services/foundation-outbox-publisher/src/r2_delete_candidates.rs` 의 `assert_delete_candidate_object`)은
manifest 의 모든 키에 대해 다음을 거부하고, 하나라도 걸리면 아무것도 지우지 않는다.

- `action` 이 `delete_candidate` 가 아닌 항목, 빈 키, `/` 로 시작하거나 `..`·`\` 가 든 키
- audit 가 `keep` 으로 분류하는 모든 키(`is_protected_key` → `audit_keeps`, 루트 ADR-0264) — 위 분류 표의 `keep` 행 전부
- `warehouse/` 아래 전부(Iceberg 파일은 Catalog 로만 지운다)
- 날짜 분할이 아닌 `bronze/source=…` 전부(`bronze_orphan_candidate` 포함)
- 날짜 분할 Bronze 인데 classification 이 `legacy_date_partitioned_bronze` 가 아닌 항목
- 허용 prefix 밖의 키(`EXECUTE=true` 에는 허용 prefix 가 필수, dry-run 은 주었을 때만 검사)

**보호되지 않는 것:** audit 가 `review` 로 두는 나머지 — `gold/v<숫자>/…` 와 옛 `gold/parcel-marker-anchor*` 배치
(`legacy_gold_artifact`), `unknown`(예: `gold/vector-tiles/runtime-manifest.json`), 날짜 분할 Bronze. 이 키들은
audit 의 후보 파일에는 들어가지 않지만, manifest 에 `delete_candidate` 로 적혀 있으면 삭제 명령은 지운다(테스트
`delete_plan_allows_explicitly_audited_legacy_spatial_layouts` 가 `gold/v1/…` 삭제 허용을 고정한다). 그래서 운영자는:

1. manifest 는 방금 돌린 audit 의 `r2-delete-candidates.json` 만 쓴다(`FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES_MANIFEST_PATH` 로 다른 파일을 가리키지 않는다).
2. 허용 prefix 에 `gold/` 나 `gold/v1/` 같은 넓은 prefix 를 넣지 않는다 — 지울 prefix 만 정확히 적는다.
3. `gold/v<숫자>/` 를 정말 지워야 하면, 먼저 그 키를 가리키는 Catalog·manifest 포인터가 없음을 확인하고 그 증거를
   남긴 뒤, 그 prefix 하나만 허용해 dry-run 계획을 사람이 검토한다.

Date-partitioned Bronze keys under `bronze/source=*/ingest_date=*/...` are legacy migration
inputs, not current-contract keep objects.

## 삭제 후 검증

실제 삭제 후에는 같은 audit를 다른 출력 폴더로 다시 실행한다.

```bash
FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_OUTPUT_DIR=target/r2-inventory-audit-after-cleanup \
cargo run -p foundation-outbox-publisher -- audit-r2-inventory
```

그 다음 삭제 전 audit, execute plan, 삭제 후 audit를 비교한다.

```bash
FOUNDATION_PLATFORM_R2_CLEANUP_VERIFY_BEFORE_AUDIT_PATH=target/r2-inventory-audit/r2-inventory-audit.json \
FOUNDATION_PLATFORM_R2_CLEANUP_VERIFY_DELETE_PLAN_PATH=target/r2-delete-candidates/r2-delete-plan.json \
FOUNDATION_PLATFORM_R2_CLEANUP_VERIFY_AFTER_AUDIT_PATH=target/r2-inventory-audit-after-cleanup/r2-inventory-audit.json \
cargo run -p foundation-outbox-publisher -- verify-r2-cleanup
```

검증은 다음 조건을 모두 요구한다.

- delete plan 이 `execute` mode 이다.
- `executed_count`가 `object_count`와 같다.
- 삭제 계획의 모든 key가 after audit 에서 사라졌다.
- before audit 의 모든 `keep` object가 after audit 에 같은 key와 같은 size로 남아 있다.
- after audit 의 `review_count`가 0이다.

## 테스트

R2 inventory/delete/cleanup/billing 동작은 outbox-publisher 서비스의 cargo 테스트로 검증한다.

```bash
cargo test -p foundation-outbox-publisher r2
```
