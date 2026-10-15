# ADR 0254: 런타임 매니페스트는 R2 객체 하나이고, 타일 엣지가 그것을 낸다

- Status: Accepted
- Date: 2026-10-15
- Builds on: [ADR-0225](./0225-the-tile-manifest-has-one-version-and-v1-retires-in-four-steps.md) (v2 하나만 남김),
  [FP-ADR-0004](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md) (v2 런타임 계약),
  [ADR-0006](./0006-object-storage-first-serving.md) (오브젝트 스토리지 우선 서빙)

## Context

지도는 매니페스트(v2, `GET /catalog/v1/vector-tiles/runtime-manifest`)를 먼저 읽고, 그 안의 주소로 타일을 받는다.
타일, by-PNU 패널, 편집 오버레이는 이미 Cloudflare 엣지에서 R2로 나간다. 매니페스트만 아직 ai-server의 foundation-api가
요청마다 Postgres를 읽어 답한다. 그래서 지도를 열 때마다 데이터 호스트가 살아 있어야 한다.

2026-10-15에 코드를 대조한 결과다.

- 카탈로그는 v2 매니페스트마다 객체 키 `gold/vector-tiles/manifests/{id}.json`을 `file_asset`에 예약한다(크기 0).
  그런데 그 바이트를 쓰는 코드가 없다.
- 아웃박스는 30분마다 돈다. 승격마다 `catalog.vector_tile_runtime_manifest.published.v2` 이벤트를 받지만, R2에 쓰지 않고
  웹훅·Kafka 쪽으로만 넘긴다.
- 퍼블리셔가 R2에 쓰는 매니페스트는 v1 포인터 `gold/manifest.json` 하나다. v1은 ADR-0225로 은퇴 중이다.
- 타일 Worker는 타일 경로만 처리한다.

## Decision

1. **원천은 R2 객체 하나다.** 매니페스트가 활성이 되면 아웃박스의 카탈로그 소비자가 활성 v2 매니페스트를 읽어,
   타일 Worker가 읽는 버킷(`tile_derivatives`)에 두 번 쓴다.
   - 예약된 키 `gold/vector-tiles/manifests/{id}.json`에 create-only로 쓴다. 불변 사본이다.
   - 포인터 `gold/vector-tiles/runtime-manifest.json`를 덮어쓴다. 정본 상수는
     `VECTOR_TILE_RUNTIME_MANIFEST_POINTER_OBJECT_KEY`다.
   - 바이트는 API 응답과 같은 변환에서 나온다. `foundation-contracts`의
     `From<VectorTileRuntimeManifest> for VectorTileRuntimeManifestResponse`가 그 변환이고, API와 퍼블리셔가 함께 쓴다.
   - 이벤트의 매니페스트가 더는 활성이 아니면 쓰지 않는다. 나중 이벤트가 쓴다. 활성 매니페스트가 없으면 오류를 내고
     재시도한다.
   - 사본이 이미 있으면(재시도) 포인터만 다시 쓴다.
   - 이벤트는 그 뒤 원래대로 웹훅·Kafka로 넘어간다.
2. **엣지가 그 포인터를 낸다.** 타일 Worker에 `GET /runtime-manifest`를 더한다. 경로와 객체 키는
   `r2-connections.contract.json`의 `vector_tile_gateway`에 둔다. 다음 변경이다.
3. **API는 같은 객체를 읽는다.** `runtime-manifest` 라우트가 Postgres 대신 그 포인터를 읽는다. 두 번째 원천이 아니라
   읽는 쪽이다. 엣지 응답과 API 응답이 같은 바이트임을 확인한 뒤의 변경이다.
4. **웹은 엣지 주소를 읽는다.** 런타임 매니페스트 주소를 새 환경값으로 받는다. 웹 출시는 사장이 한다.

## Consequences

- 엣지의 매니페스트는 R2에 쓰인 뒤에야 바뀐다. 아웃박스 틱(30분) 안에 따라간다. 승격 직후 publish-once로 당기는 것은
  2단계에서 정한다. 옛 판의 객체는 불변이라, 늦어져도 지도가 깨지지는 않는다.
- 아웃박스 유닛이 `map-edit-fold.env`(타일 버킷 쓰기 키)를 읽는다. production에서 그 설정이 없으면 아웃박스가 시작하지
  않는다. 조용히 쓰지 않는 것보다 낫다.
- **남은 결정(2단계 전): 폴링 간격.** v2 계약은 `refresh_after_seconds`를 정확히 4초로 묶는다. Workers 무료 요금제는
  하루 10만 요청(타일 포함)이다. 4초 폴링이면 지도 하나를 한 시간 열어 둘 때 매니페스트 요청이 900건이다. 엣지로 옮기기
  전에 간격을 늘리거나, Worker를 거치지 않는 캐시 경로를 정해야 한다.
