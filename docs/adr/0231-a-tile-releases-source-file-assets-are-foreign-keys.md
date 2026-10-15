# ADR 0231: 타일 릴리스의 원천 파일은 외래 키로 file_asset 을 가리킨다

- Status: Accepted
- Date: 2026-10-14
- Related: [FP-ADR-0004](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md) (`source_file_asset_ids` 계약)

## Context

`catalog.vector_tile_release.source_file_asset_ids` 는 `uuid[]` 다. PostgreSQL 은 배열 원소에 FOREIGN KEY 를
걸 수 없어서, 릴리스가 존재하지 않는 `catalog.file_asset` 를 가리켜도, 가리키는 자산이 지워져도 DB 가 막지 않았다.
같은 사실의 v1 표(`catalog.vector_tile_artifact_source_file_asset`)는 처음부터 행 단위 조인 표에 외래 키를 걸어 두었다.

실제로 막지 않았다: `catalog-infrastructure` 의 `spatial_tile_publication` 시험 고정값은 없는 자산 id 로 릴리스를
수십 번 만들고 있었다. 운영 쪽 쓰기 경로 셋 중 `foundation-outbox-publisher` `vector_tile_runtime_promote` 만 존재
여부를 앱에서 물었다.

2026-10-14 운영 측정: 릴리스 13행, 배열 원소 13개, 없는 자산을 가리키는 원소 0개, 중복 0개.

읽는 쪽: `sqlx_repository`·`unit_of_work`(Rust), `guard_vector_tile_readdress_release`(SQL, 배열 동일성 비교),
API·OpenAPI·gongzzang/dawneer 타입이 배열 모양을 그대로 낸다. 배열의 **순서**는 요청 지문(`ports.rs`)의 일부다.

## Decision

1. `catalog.vector_tile_release_source_file_asset (release_id, ordinal, file_asset_id)` — PK `(release_id, ordinal)`,
   `file_asset_id` → `catalog.file_asset(id)` 외래 키 `vector_tile_release_source_file_asset_file_asset_fkey`,
   `release_id` → 릴리스 `ON DELETE CASCADE`.
2. 이 표는 트리거 `vector_tile_release_source_file_asset_sync`(릴리스 `AFTER INSERT OR UPDATE OF source_file_asset_ids`)
   만 쓴다. 배열은 그대로 값의 정본이고, 이 표는 그 원소마다 외래 키를 거는 자리다. 없는 자산을 가리키는 릴리스는
   자기 INSERT 에서 SQLSTATE 23503 으로 실패하고, 가리켜진 자산은 지울 수 없다.
3. 마이그레이션은 기존 행을 채운다. 없는 자산을 가리키는 원소는 배포를 실패시키지 않고 빼고, 릴리스마다
   `WARNING` 을 남긴다. 나중에 찾는 질의: `cardinality(source_file_asset_ids)` 가 이 표의 그 릴리스 행 수보다 큰 릴리스.
4. 시험: `spatial_tile_publication::source_file_asset` 의 두 시험이 운영 경로(`MarkTileLayerDynamic`)로 없는 자산을
   거부하는 것과 가리켜진 자산 삭제 거부를 확인한다.

## 기각한 대안

- **배열을 지우고 조인 표만 정본으로.** 쓰기 4곳·읽기 3곳·재주소 트리거·시드·시험 10여 개를 바꾸고, 배포 중 옛 바이너리가
  사라진 열을 읽는다. 순서도 따로 옮겨야 한다. 얻는 보장은 이 결정과 같다.
- **배열 원소를 트리거로 직접 검사(흉내 낸 외래 키).** 표가 하나 줄지만 `file_asset` 쪽 삭제 트리거와 `FOR KEY SHARE`
  잠금을 손으로 써야 한다. 엔진의 외래 키가 이미 하는 일을 다시 만드는 것이다.
- **앱 검사만.** 쓰기 경로마다 따로 묻는 지금 상태이고, 셋 중 하나만 묻고 있었다.

## Consequences

- 배열과 표가 같은 원소를 두 번 담는다. 표는 트리거만 쓰므로 직접 쓰기는 이 결정 위반이다.
- 새 고정값은 릴리스가 가리킬 `file_asset` 행을 먼저 만들어야 한다.
