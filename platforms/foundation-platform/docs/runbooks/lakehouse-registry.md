# Lakehouse Registry Runbook

## 목적

Lakehouse Registry는 R2 버킷에 저장된 데이터 본문을 PostgreSQL로 옮기는 시스템이 아니다.
PostgreSQL에는 운영에 필요한 메타데이터만 둔다.

- 어떤 서비스가 어떤 R2 버킷을 소유하는지
- 어떤 데이터 자산이 Bronze/Silver/Gold 중 어디에 있는지
- 어떤 데이터셋 버전이 active인지
- 어떤 object key, checksum, byte count가 해당 버전에 속하는지
- 품질 검사, lineage, 접근 정책

실제 대용량 원본과 분석 파일은 R2/Iceberg 계층에 남긴다.

## 현재 최소 구성

구현된 범위:

- `catalog.lakehouse_storage_namespace`
- `catalog.lakehouse_data_asset`
- `catalog.lakehouse_dataset_version`
- `catalog.lakehouse_object_artifact`
- `catalog.lakehouse_quality_check`
- `catalog.lakehouse_lineage_edge`
- `catalog.lakehouse_access_policy`
- Rust domain model: `lakehouse-domain::lakehouse_registry`
- Rust repository ports: `lakehouse-application::ports::{LakehouseRegistryRepository, LakehouseRegistryUnitOfWork}`
- PostgreSQL adapters: `lakehouse-infrastructure::{PgLakehouseRegistryRepository, PgLakehouseRegistryUnitOfWork}`
- CLI:
  - `seed-lakehouse-registry`
  - `verify-lakehouse-registry`
- CI/local preflight:
  - `foundation-outbox-publisher verify-lakehouse-registry`

아직 포함하지 않은 범위:

- Admin UI
- 내부 공개 API
- Gongzzang/Dawneer write client

## Seed

```bash
cargo run -p foundation-outbox-publisher -- seed-lakehouse-registry
```

이 명령은 production namespace 3개를 보장한다.

| owner_service | bucket |
|---|---|
| `foundation-platform` | `foundation-platform-lakehouse-prod` |
| `gongzzang` | `gongzzang-lakehouse-prod` |
| `dawneer` | `dawneer-lakehouse-prod` |

## Verify

```bash
cargo run -p foundation-outbox-publisher -- verify-lakehouse-registry
```

검증 항목:

- 3개 production namespace가 존재한다.
- 각 namespace의 bucket과 owner service가 맞다.
- namespace 상태가 `active`다.
- `R2_BUCKET_NAME`은 `foundation-platform-lakehouse-prod`와 일치한다.

## 전국 수집 전 Preflight

`foundation-outbox-publisher run-national-data-collection --execute`는 이제 public API 수집 전에
Lakehouse Registry preflight를 자동으로 실행한다.

실행 순서:

1. 승인 파일과 실행 확인 플래그를 검증한다.
2. `.env.local`을 로드하고 필수 env를 확인한다.
3. `foundation-outbox-publisher verify-lakehouse-registry`를 실행한다.
4. `target/audit/lakehouse-registry-verify.json` evidence를 남긴다.
5. Registry가 `ready`일 때만 public API quota metric과 Bronze 수집으로 넘어간다.

preflight가 실패하면 public API 호출과 Bronze object write는 시작하지 않는다.
이 경로에는 우회 스위치가 없다.

수동 확인 명령:

```bash
cargo run -p foundation-outbox-publisher -- verify-lakehouse-registry
```

정상 출력 예:

```text
lakehouse-registry-ok namespaces=3 output=target/audit/lakehouse-registry-verify.json
```

전국 수집 evidence에는 다음 필드가 포함된다.

```json
{
  "lakehouse_registry_preflight": {
    "status": "ready",
    "report_path": "target/audit/lakehouse-registry-verify.json",
    "namespace_count": 3,
    "blocker_count": 0
  }
}
```

## Bronze Evidence Registry Record

수집이 끝나면 runner는 `national-data-collection-run-evidence.json`을 먼저 작성한 뒤,
다음 Rust 명령으로 Bronze object metadata를 Registry에 등재한다.

```bash
export FOUNDATION_PLATFORM_LAKEHOUSE_BRONZE_RUN_EVIDENCE_PATH="target/audit/national-data-collection-run-evidence.json"

cargo run -p foundation-outbox-publisher -- record-lakehouse-bronze-run-evidence
```

이 명령이 하는 일:

- provider별 `source_slug`를 `foundation_platform.bronze.<source_slug>` asset 이름으로 정규화한다.
- asset을 `lakehouse_data_asset`에 upsert한다.
- ingestion run id를 dataset version으로 기록하고 active pointer를 갱신한다.
- Bronze object key, checksum, byte size, logical record count를 `lakehouse_object_artifact`에 기록한다.
- 같은 object key가 다른 checksum이나 version으로 다시 들어오면 덮어쓰지 않고 실패한다.

정상 출력 예:

```json
{
  "schema_version": "foundation-platform.lakehouse_bronze_evidence_registry_record.v1",
  "status": "ready",
  "provider_count": 1,
  "artifact_count": 18
}
```

최종 전국 수집 evidence에는 다음 필드도 포함된다.

```json
{
  "lakehouse_registry_record": {
    "status": "ready",
    "report_path": "target/audit/lakehouse-bronze-evidence-registry-record.json",
    "provider_count": 1,
    "artifact_count": 18
  }
}
```

> 참고: 별도 gate 커맨드였던 `check-national-data-collection-run-evidence`는 producer 자신의
> 출력을 재검사하는 ceremony로 판정되어 2026-06-22 self-verifying evidence-gate 정리에서
> 삭제되었다. `lakehouse_registry_preflight`와 `lakehouse_registry_record` 필드는 위의 producer
> 명령(`run-national-data-collection`, `record-lakehouse-bronze-run-evidence`)이 evidence에 직접 기록한다.
