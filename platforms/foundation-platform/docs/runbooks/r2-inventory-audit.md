# R2 Inventory Audit

## 목적

Cloudflare R2 bucket의 현재 object를 읽기 전용으로 수집한 뒤 Foundation Platform 소유권 규칙에 따라
`keep`, `delete_candidate`, `review`로 분류한다.

이 runbook은 삭제를 수행하지 않는다. 삭제 전에는 반드시 `r2-delete-candidates.json`을 사람이 검토하고,
별도 삭제 workflow에서 명시적인 prefix allow-list와 최종 승인 절차를 거쳐야 한다.

## 분류 규칙

| Prefix / Key | Classification | Action | 의미 |
|---|---|---|---|
| `__r2_data_catalog/` | `managed_iceberg_catalog` | `keep` | R2 Data Catalog / Iceberg 내부 메타데이터 |
| `gold/manifest.json` | `runtime_manifest_pointer` | `keep` | canonical runtime manifest pointer |
| `gold/v*/` | `runtime_gold_artifact` | `keep` | manifest가 참조할 수 있는 versioned Gold artifact |
| `bronze/source=*/run_id=*/partition=*` | `current_bronze_contract` | `keep` | source/run_id/partition Bronze object contract |
| `bronze/source=*/ingest_date=*/run_id=*/partition=*` | `legacy_date_partitioned_bronze` | `review` | legacy date-partitioned Bronze object; migrate before deletion |
| `gold/staging/` | `staging_gold_artifact` | `delete_candidate` | 장기 보존 대상이 아닌 staging artifact |
| `bronze/YYYY-MM/` | `legacy_uncontracted_bronze` | `delete_candidate` | 현재 Bronze 계약 밖의 legacy 날짜형 prefix |
| `*smoke*` | `smoke_artifact` | `delete_candidate` | 남아 있으면 정리 가능한 smoke artifact |
| 기타 | `unknown` | `review` | 사람이 소유권을 확인해야 하는 object |

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

Optional R2 live smoke can write a Prometheus text artifact for the dedicated smoke
write/read/delete round trip:

```bash
export FOUNDATION_PLATFORM_R2_SMOKE_METRICS_PATH="target/r2-live-smoke/r2-smoke.prom"
cargo run -p foundation-outbox-publisher --bin foundation-outbox-publisher -- smoke-r2
```

The metric artifact contains:

- `foundation_platform_r2_smoke_request_total`
- `foundation_platform_r2_smoke_bytes_verified`

The metric labels include the source and operation only. They do not include the object key, so each
smoke run does not create high-cardinality series. The optional GitHub Actions `r2-live-smoke` job
uploads this file as the `r2-live-smoke-metrics` artifact.

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
R2_BUCKET_NAME=
R2_ENDPOINT=              # 또는 R2_ACCOUNT_ID
R2_ACCOUNT_ID=
R2_REGION=auto
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
```

스크립트는 secret 값을 출력하지 않는다.

## 삭제 전 체크리스트

1. `r2-inventory-audit.json`의 `review_count`가 0인지 확인한다.
2. `r2-delete-candidates.json`의 모든 key가 예상 prefix에만 속하는지 확인한다.
3. `__r2_data_catalog/`, `gold/manifest.json`, `gold/v*/`, and canonical
   `bronze/source=*/run_id=*/partition=*` keys are not delete candidates. Date-partitioned
   `bronze/source=*/ingest_date=*/...` keys must be migrated before deletion.
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

스크립트는 다음 key를 항상 보호한다.

- `__r2_data_catalog/*`
- `gold/manifest.json`
- `gold/v*/*`
- `bronze/source=*/run_id=*/partition=*`

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
