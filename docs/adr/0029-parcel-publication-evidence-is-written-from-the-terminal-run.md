---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-13
---

# ADR 0029: 필지 발행 실행 증거는 terminal run에서 쓴다

- Status: Accepted
- Date: 2026-08-13
- Implements: [ADR-0025](./0025-parcel-publication-names-one-sealed-iceberg-evidence.md), [ADR-0026](./0026-parcel-evidence-sealer-is-the-only-append-boundary.md)

## 맥락

`seal-parcel-publication-evidence`는 R2 실행 증거 bytes, Iceberg REST catalog의 table UUID와
snapshot 이력, Postgres의 terminal mirror run과 run-scoped projection digest를 함께 검증한다.
그러나 strict contract인 `foundation-platform.parcel_publication_execution_evidence.v1`을 쓰는
생산자가 없다. 사람이 JSON을 만들지 않는 한 봉인자에 들어갈 객체가 없고, 사람이 만들면 검증할
사실과 운영자가 새로 주장한 값을 구별할 수 없다.

`postgis-parcel-boundary-mirror-national-rebuild`가 읽는
`foundation-platform.silver_gold_national_promotion_execution.v1`은 처리할 R2 handoff 객체 목록인
입력이다. 이 ADR의 실행 증거는 완료한 mirror run을 발행 사슬에 넘기는 출력이다. 두 문서는 역할과
schema가 다르며 서로를 변환하거나 같은 타입으로 해석하지 않는다.

근본 원인은 rebuild가 이미 확정한 run/source/quality와 봉인자가 읽는 R2 contract 사이에 소유된
write 경계가 없다는 것이다. 지켜야 할 불변식은 다음과 같다.

1. 성공하지 않은 run은 발행 실행 증거 객체를 만들 수 없다.
2. snapshot은 producer 실행 시점의 최신값이 아니라 rebuild가 읽은 terminal run의 값이다.
3. rebuild가 계산한 quality count는 다른 곳에서 다시 세지 않는다.
4. 같은 bytes의 재시도만 같은 content-addressed key를 재사용하며 다른 bytes는 덮지 않는다.
5. producer와 sealer는 서로 다른 책임을 유지한다. producer는 사실을 쓰고 sealer는 그 사실을 외부
   상태와 대조해 append 권한을 행사한다.

## 결정

### 1. 별도 producer 명령이 immutable terminal run을 읽는다

`foundation-outbox-publisher write-parcel-publication-evidence`를 별도 명령으로 둔다. 입력 선택값은
`mirror_rebuild_run_id` 하나이며, table UUID, snapshot id, source record/file asset, status, quality를
운영자가 각각 넘기는 환경변수는 만들지 않는다.

명령은 `DATABASE_URL`의 read-only 권한으로
`serving_postgis.parcel_boundary_mirror_rebuild_run`을 읽는다. run은 `succeeded`, non-null
`finished_at`, positive `loaded_row_count`, zero `rejected_row_count`, `EPSG:5179`, non-null
`source_record_id`와 `source_file_asset_id`, strict
`foundation-platform.parcel_publication_quality.v1`을 모두 만족해야 한다. quality의 count는 run의
typed JSON을 역직렬화해 검증할 뿐 재계산하지 않는다. 이 명령은 Postgres에 쓰지 않는다.

별도 명령은 R2 쓰기가 실패한 뒤 이미 성공한 rebuild를 다시 실행하지 않고 같은 run을 재시도할 수
있게 한다. rebuild 마지막 단계에 넣었을 때보다 in-memory 결박은 약하지만, terminal run tuple을
바꾸지 못하게 하는 `parcel_boundary_mirror_rebuild_run_state_guard`와 evidence가 그 tuple 전체를
가리키는 `parcel_publication_source_evidence_mirror_run_fkey`가 그 간격을 닫는다.

rebuild가 실패하거나 terminal success 조건을 만족하지 않으면 producer는 R2 PUT 전에 실패한다.
실패 문서는 따로 쓰지 않는다. 따라서 실패한 실행의 객체가 `status=succeeded`로 오인될 표면 자체가
없다.

### 2. snapshot은 run에서, table UUID와 snapshot 존재 확인은 catalog에서 읽는다

`iceberg_commit.snapshot_id`는 terminal run의 `source_snapshot_id`가 정확히
`iceberg:<선행 0 없는 양의 십진수>`일 때 그 숫자에서만 얻는다. producer 실행 시점의 Iceberg
`current-snapshot-id`를 선택하지 않는다. table이 rebuild 뒤 전진해도 실행이 실제로 읽은 snapshot은
바뀌지 않아야 하기 때문이다.

producer는 Iceberg REST catalog에서 `silver.parcel_boundaries`를 읽어 `table_uuid`를 얻고, run이
지명한 snapshot id가 그 table의 보존된 snapshot 목록에 실제로 존재하는지 확인한다. table이 없거나
snapshot 이력이 그 id를 포함하지 않으면 객체를 쓰지 않는다. sealer는 독립적으로 같은 대조를 다시
수행한다.

contract의 `scope={kind:national,complete:true}`, null object/row/shard limits, committed/production/
national true는 이 producer만 쓸 수 있는 고정값이다. 별도 override는 없다. 이것은 upstream run이
전국 completeness를 거짓으로 주장하는 경우까지 독립적으로 발견하지는 못하므로 §5의 신뢰 경계로
명시한다.

### 3. key는 bytes SHA-256이고 PUT은 별도로 create-only다

정본 key는 다음 하나다.

```text
control/evidence/parcel-publication/execution/sha256=<lowercase-sha256>.json
```

producer는 기존 `ParcelPublicationExecutionEvidence` struct를 한 번 직렬화한 exact bytes의
SHA-256을 계산하고 `r2_layout.rs`가 만든 key만 쓴다. content-addressed 이름은 같은 내용에 같은
주소를 줄 뿐 overwrite를 막지 않는다. 실제 write는 기존 `ObjectWriteMode::CreateOnly`를 사용해 R2
`If-None-Match: *` 조건부 PUT으로 수행한다.

이미 존재해 `412 Precondition Failed`가 나면 key나 저장 metadata만 믿지 않는다. producer가 객체
bytes를 실제로 GET하고 새로 만든 bytes와 완전히 같을 때만 idempotent reuse로 성공한다. 한 byte라도
다르거나 읽지 못하면 실패한다. 따라서 content address와 conditional write와 exact-byte reconcile
세 가지가 각각 identity, overwrite 방지, retry 판정을 맡는다.

### 4. 전용 evidence writer 이름을 요구한다

producer는 lakehouse의 account/endpoint/bucket/region을 재사용하되 다음 전용 credential 이름만
write에 사용한다.

```text
FOUNDATION_PLATFORM_R2_PARCEL_PUBLICATION_EVIDENCE_WRITER_ACCESS_KEY_ID
FOUNDATION_PLATFORM_R2_PARCEL_PUBLICATION_EVIDENCE_WRITER_SECRET_ACCESS_KEY
```

shared `FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_*`로 fallback하지 않는다. sealer는 write credential이
아니라 기존 `FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_*`로 객체를 읽는다. 저장소에는 credential
값을 두지 않고 이름과 소비 경로만 둔다.

Cloudflare R2의 장기 S3 API token은 확인된 dashboard 경로에서 bucket 단위 read/write 권한을
부여한다. prefix/object 제한은 최대 7일의 temporary credential에서 제공되며 발급·rotation 경로는
현재 저장소에 없다. 따라서 전용 이름과 코드 경계는 만들 수 있지만 production credential 발급,
`control/evidence/parcel-publication/` prefix 제한 temporary credential의 안전한 rotation, 기존
shared writer가 그 prefix를 쓸 수 없게 하는 분리는 실물 계정 승인이 필요하다. 승인 전 로컬 증명은
fixture credential만 사용하며 production R2 write 가능을 주장하지 않는다.

### 5. 위협 모델

이 경계는 데이터 무결성에 대한 deliberate-bypass prevention과 honest-mistake detection을 함께
다룬다.

**막는 것:** 명령을 통한 nonterminal/failed run 발행, 운영자가 run과 다른 snapshot/source/table을
조립하는 일, quality count의 두 번째 구현, producer 실행 시점의 최신 snapshot을 잘못 고르는 일,
정상 retry가 기존 object를 덮는 일, 충돌 key의 다른 bytes를 같은 증거로 재사용하는 일을 막는다.
run 불변성 trigger, 복합 FK, strict typed contract, catalog snapshot-history 대조, conditional PUT,
exact-byte GET 비교가 각각 권위 경계다.

**막지 못하는 것:** `DATABASE_URL`의 원장을 바꾸거나 trigger를 교체할 권한, Iceberg catalog
metadata를 바꿀 권한, R2 bucket 관리자 또는 기존 shared writer가 evidence prefix를 직접
overwrite/delete하는 권한, 전용 credential 소유자가 이 binary를 우회해 임의 JSON을 PUT하는 권한,
upstream이 실제 전국 completeness를 거짓으로 terminal run에 결박한 경우는 막지 못한다. R2는 S3
`PutObject` object-lock header를 제공하지 않으므로 create-only는 producer 요청의 조건이지 bucket의
영구 WORM 보장이 아니다. production에서 prefix-scoped temporary credential과 기존 writer의 prefix
차단이 확인될 때까지 R2 credential 소유자가 이 사슬의 신뢰 종점이다.

## 기각한 대안

### rebuild의 마지막 단계에서 직접 쓴다

in-memory 결과를 곧바로 직렬화하는 장점은 있다. 그러나 DB run을 `succeeded`로 닫은 뒤 R2가
실패하면 evidence만 재시도할 수 없고, rebuild 전체를 새 run으로 다시 수행해야 한다. immutable
terminal tuple과 복합 FK가 별도 reader의 결박을 제공하므로 복구 가능한 별도 명령을 택한다.

### rebuild summary 또는 새 handoff schema를 producer 입력으로 둔다

run 원장 옆에 같은 status/source/quality를 담은 두 번째 계약을 만들고 어느 것이 맞는지 다시
판정해야 한다. 현재 입력 evidence와 출력 contract를 섞는 오류도 재발한다. terminal run과 기존
`parcel_publication_contract.rs`만 SSOT로 남긴다.

### 봉인자가 문서를 스스로 만든다

봉인자가 자기가 만든 scope/cutover 주장을 다시 검증하게 되어 producer와 verifier의 독립성이
사라진다. 봉인자는 외부 bytes를 읽고 대조하는 현재 책임만 유지한다.

### 사람이 JSON을 만들어 올린다

snapshot/source/boolean을 서로 독립된 운영자 입력으로 되돌리고 exact serialization, content address,
create-only retry를 자동화할 수 없다. 운영 runbook이 아니라 binary의 typed 경계가 문서를 쓴다.

### 증거 없이 발행하는 우회 경로를 둔다

ADR-0025의 단일 evidence id와 FK를 무효화하고 QA projection이 runtime manifest로 승격될 수 있게
한다. 발행 불가를 명시적으로 유지하며 fallback이나 긴급 override를 두지 않는다.

## 결과

- producer와 sealer 사이의 R2 객체가 기존 typed contract 하나로 생긴다.
- non-success run에는 객체가 없고 success run의 R2 실패는 같은 run으로 안전하게 재시도한다.
- object key, write mode, retry 판정이 하나의 `r2_layout`/object-storage 경로를 공유한다.
- production R2 credential 발급과 prefix 격리는 별도 실물 승인 없이는 완료로 표시하지 않는다.

## 참고

- [Cloudflare R2 S3 API compatibility](https://developers.cloudflare.com/r2/api/s3/api/)
- [Cloudflare R2 temporary credentials](https://developers.cloudflare.com/r2/api/s3/temporary-credentials/)
