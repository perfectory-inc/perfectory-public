# ADR 0037: 포인터는 객체 키와 함께 주소 틀을 싣는다

- Status: Accepted
- Date: 2026-08-18

## Context

`catalog.industrial_complex_gold_pointer` 와 그 API 응답
(`IndustrialComplexGoldPointerResponse`)은 `profile_object_key` 만 싣는다. 키는 **어떤 객체인지**
를 말할 뿐 **어디서 읽는지**를 말하지 않는다. 클라이언트가 그 키를 주소로 바꾸는 방법은 계약
어디에도 없었다. 화면은 키를 받고 아무것도 못 한다.

같은 문제를 타일 쪽은 이미 풀었다. `catalog.vector_tile_manifest.tiles_url_template` 이
발행 시점 입력으로 들어가 manifest 행에 남고, 런타임 manifest 문서에 실려 나가며, 클라이언트
(`products/gongzzang/apps/web/lib/map/vector-tile-manifest.ts`)가 자리표시자를 치환해 주소를
만든다. 템플릿은 검증된 값이다: 필수 자리표시자, https 우선, 질의문자열·프래그먼트 금지.

환경변수 하나를 API 프로세스에 두고 서빙 시점에 URL 을 조립하는 선택지도 있었다. 그러면 주소는
계약이 아니라 배선이 된다: 어느 발행이 어느 주소로 나갔는지 남지 않고, 배포 환경이 바뀌면 과거
발행의 의미가 소급해서 바뀐다. 참고로 `.env.local` 의
`FOUNDATION_PLATFORM_R2_LAKEHOUSE_PUBLIC_BASE_URL` 은 지금 비어 있다 — 배선으로 갔다면 이
결함은 런타임에야 드러났을 것이다.

## Decision

1. `foundation_shared_kernel::ObjectUrlTemplate` 이 "객체 키 하나를 주소로 바꾸는 틀"의 단일
   정의다. 규칙:
   - `{object_key}` 를 정확히 한 번 포함한다. 다른 자리표시자는 거부한다.
   - 절대 https URL 이어야 한다. 평문 http 는 loopback 호스트에만 허용한다.
   - 질의문자열·프래그먼트를 담지 않는다. 앞뒤 공백을 담지 않는다.
   - 자리표시자는 경로에 있어야 한다. 호스트 자리에 두는 것은 거부한다.
   막는 사고: 발행이 주소를 선언하지 않은 채로 통과해, 소비자가 키만 받고 아무것도 못 하는 상태.

2. `profile_url_template` 은 **포인터 발행 시점 입력**이며
   `catalog.industrial_complex_gold_pointer` 의 필수 컬럼이다. API 응답도 이 값을 싣는다.
   클라이언트가 `{object_key}` 를 치환해 `profile_object_key` 든
   `spatial_locator_object_key` 든 주소로 만든다. 그래서 서버는 완성된 URL 을 싣지 않는다 —
   틀 하나가 두 키를 모두 처리하고, 같은 사실이 두 필드에 복제되지 않는다.

3. 형식 규칙은 **Rust 값 객체 한 곳에만** 산다. SQL 제약은 `<> ''` 만 본다.
   근거: 같은 규칙을 SQL 과 Rust 에 각각 적으면 두 사본이 어긋난다
   (`r2_layout.rs` 의 `vector_tile_release_key` 주석이 기록한 실제 사고).

4. 마이그레이션은 컬럼을 NOT NULL 로 추가하되, 기존 행이 있으면 **실패한다**.
   과거 행에 넣을 주소를 지어내지 않기 위해서다. Gold 프로필 생산자가 없었으므로(ADR-0036)
   정당한 포인터 행은 존재할 수 없다.

5. `export-industrial-complex-gold-profiles` 에서는 이 템플릿이 **선택 입력**이다. 주면 요약
   JSON 이 템플릿과 각 산출물의 완성된 URL 을 함께 내고, 주지 않으면 두 필드가 `null` 이며
   커맨드가 경고를 남긴다. 객체를 쓰는 것은 사실을 적는 일이고 주소를 붙이는 것은 발행이다.
   서빙 호스트가 아직 없다는 이유로 Gold 산출물 생산을 막지 않되(그건 지금 실제 상황이다 —
   `FOUNDATION_PLATFORM_R2_LAKEHOUSE_PUBLIC_BASE_URL` 이 비어 있다), 소비자를 그 객체로
   보내는 일은 주소 없이 진행되지 않는다.

6. **이번 범위가 아닌 것:** 이벤트
   `catalog.industrial_complex.gold_pointer.published.v1` 은 바꾸지 않는다. 그 wire 형식은
   바이트 단위로 고정된 계약이고, 주소를 이벤트에 실을지는 별도 결정이다. 이벤트 소비자는
   Catalog API 로 포인터를 읽는다.

## Consequences

- 포인터를 발행하려면 그 산출물이 어디서 읽히는지 말해야 한다. 말할 수 없으면 발행이 거부된다.
- 어느 발행이 어느 주소로 나갔는지 행에 남는다. 서빙 주소 변경은 새 발행이지 배포 설정 변경이
  아니다.
- 같은 형태의 문제가 다른 Gold 포인터에도 있다면 같은 값 객체를 재사용한다.
  `TilesUrlTemplate` 은 자리표시자 계약이 다르므로(`{object_key_prefix}/{z}/{x}/{y}`) 별도
  타입으로 남는다.
- 1,442 행이 같은 템플릿 문자열을 각각 싣는다. 포인터 표는 이미 단지당 한 행으로 비정규화되어
  있고(`source_snapshot_id` 도 같은 값이 반복된다), 템플릿은 발행 단위의 사실이므로 받아들인다.
