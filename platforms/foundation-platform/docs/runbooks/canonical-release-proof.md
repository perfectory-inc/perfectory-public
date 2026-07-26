# Canonical release proof

이 명령은 이미 수집된 Bronze가 만든 Silver·Gold 결과와 타일 manifest가 같은 원본
스냅샷을 가리키는지 확인한다. 원본 데이터를 다시 수집하거나 Postgres에 Bronze를 복사하지
않는다.

## 무엇을 검사하는가

- Silver summary가 `silver.industrial_complexes` Iceberg 쓰기인지 확인한다.
- Gold summary가 `gold.complex_catalog` Iceberg 쓰기이고 canonical Silver를 입력으로 삼았는지
  확인한다.
- 두 summary의 source snapshot 목록이 잘리지 않았고 비어 있지 않으며 fixture/synthetic ID가
  아닌지 확인한다.
- 타일 manifest의 `source_snapshot_id`가 Gold의 source snapshot 목록에 반드시 포함되는지
  확인한다.
- manifest의 모든 artifact가 자기 logical layer와 같은 `source_layer`를 갖고, 물리적
  `object_key_prefix`를 갖는지 확인한다.
- 검증을 통과한 경우에만 `canonical-release-proof.v1` 증거 파일을 임시 파일에서 atomic
  rename으로 공개한다. 이미 같은 경로의 증거가 있으면 덮어쓰지 않는다.

## 로컬 실행

Foundation workspace에서 pinned Rust Docker를 사용한다.

```text
docker run --rm -v "${PWD}:/workspace" -w /workspace/platforms/foundation-platform \
  rust:1.96.0-bookworm bash -lc \
  "source /usr/local/cargo/env && SQLX_OFFLINE=true \
   cargo run -p foundation-outbox-publisher --bin foundation-outbox-publisher -- \
   write-canonical-release-proof"
```

기본 입력 경로는 다음과 같다.

- `target/lakehouse/smoke/summaries/industrial_complexes_iceberg.json`
- `target/lakehouse/smoke/summaries/gold_complex_catalog_iceberg.json`
- `target/canonical/vector-tile-manifest.json`

경로를 바꿔야 하면 `FOUNDATION_PLATFORM_CANONICAL_RELEASE_PROOF_*_PATH` 환경변수를
설정한다. `FOUNDATION_PLATFORM_REPO_ROOT` 밖의 출력 경로는 거부한다.

## 운영 승격 전 조건

이 명령의 통과는 national rollout 또는 production R2 쓰기를 의미하지 않는다. 다음이
모두 별도로 승인되어야 한다.

1. Silver와 Gold가 실제 Iceberg catalog에서 read-back 검증을 통과한다.
2. Foundation Catalog에 tile manifest promotion이 성공하고, manifest의
   `source_snapshot_id`가 동일한 release의 Gold snapshot으로 기록된다.
3. R2 object key와 CDN namespace가 전용 staging/test prefix인지 확인한다.
4. `cargo xtask verify foundation`와 해당 변경 영역의 integration test가 통과한다.

실패하면 이전 증거·manifest·R2 object를 삭제하거나 덮어쓰지 말고, 잘못된 입력의 lineage를
수정한 뒤 새 release ID로 다시 실행한다.
