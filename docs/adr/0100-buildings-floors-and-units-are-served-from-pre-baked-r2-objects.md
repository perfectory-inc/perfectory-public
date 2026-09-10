# ADR 0100: 건물·층·호는 독립된 by-PNU R2 객체로 서빙한다

- Status: Accepted
- Date: 2026-09-10

## Context

ADR-0072·0073·0074·0075의 건물과 호 원천은 이미 레이크하우스에 있다. 이 공개 데이터를
패널마다 Catalog PostgreSQL과 제품 API를 거쳐 읽으면 ADR-0096이 제거한 대량 서빙
투영 의존성이 건물 경로에 남는다. 필지 문서에 건물 섹션을 추가하면 건물만 바뀌어도
필지 전체 문서를 다시 구워야 한다. 변경 주기가 다른 데이터를 하나의 객체로 묶는 것이
구조적 원인이다.

불변식은 **건물·층·호의 변경과 필지 속성 변경을 독립적으로 발행**하면서, 같은 Gold
스냅숏과 행은 같은 JSON 바이트를 만들고 검증된 세대만 공개하는 것이다. 원천의
미기재 값과 미연결 호를 삭제하거나 추측한 값으로 채우지 않는다.

## Decision

1. `gold.building_panel`은 PNU당 한 행이며 `buildings_json`에 건물과 그 `floors`·`units`를
   중첩한다. 건물에 연결되지 않은 호는 `unlinked_units_json`으로 명시한다. 건물·호의
   공개 필드는 `foundation-contracts`를 따르며 Catalog 실행 시각은 문서에 넣지 않는다.
   Silver의 단일 스냅숏·연결·중복 검증을 통과한 행만 생산하고 `--validate-only`도 같은
   검증을 실행한다. `--region-prefix`와 `--pnu-prefix`는 읽기 범위를 명시한다.
2. `row_digest`는 `pnu`·`buildings_json`·`unlinked_units_json`만의 결정적 SHA-256이다.
   `source_snapshot_id`·`published_at_utc`는 제외한다. 내용과 계보를 분리하는 ADR-0099를
   그대로 적용하며 배열 순서도 결정적이어야 한다.
3. 객체 키는 `serving/buildings/by-pnu/v{generation}/{pnu}.json`, 세대 포인터는
   `serving/buildings/by-pnu/manifest.json`이다. `r2-connections.contract.json`의
   `building_by_pnu_gateway`가 주소·문법·CORS·캐시 정책의 단일 출처다. Rust 타입드 리더,
   키 빌더와 왕복 판정 함수, Worker 구성 렌더러가 같은 계약을 읽는다.
4. `export-building-by-pnu-serving`과 `publish-building-by-pnu-serving-manifest`는 별도
   명령이다. 환경변수 접두사는 `FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_`이다.
   굽기는 create-only이며 기존 객체는 바이트가 같을 때만 재사용한다. 목록 기반 재개는
   지정한 PNU 샤드의 키 범위만 읽고, 쓰기 순서는 PNU 해시로 분산한다. 델타 덮어쓰기와
   같은 세대 재발행은 각각 명시된 플래그가 필요하다.
5. 발행은 기대 객체 수와 Gold 스냅숏을 대조하고 객체를 읽어 검증한 뒤 포인터를 바꾼다.
   목록은 존재 증거이며 전체 내용의 검증으로 표현하지 않는다. 검증 표본 범위와 결과를
   명시한다. 첫 발행과 같은 세대 재발행은 별도 선언이며 세대 역행은 거부한다.
6. 브라우저는 `/buildings/by-pnu/{pnu}`의 공개 문서를 받아 기존 패널 뷰모델로 순수 함수
   변환한다. 객체 부재는 typed 404 미서빙 오류다. manifest를 신뢰할 수 없으면
   `503`·`Cache-Control: no-store`로 거부한다. 객체 캐시는 1시간, manifest 엣지 캐시는
   60초이며 캐시 정책은 필지 경로와 같다.
7. 배치·R2 SDK·Iceberg·Cloudflare Worker는 필지 슬라이스의 채택 도구를 재사용한다.
   새 저장 엔진이나 데이터 플레인은 도입하지 않는다. 별도 건물 서빙 경로의 도메인
   어댑터와 검증만 추가한다. 이 변경은 코드 구현이며 수집·전국 굽기·배포를 실행하지 않는다.

## Alternatives

- 필지 JSON에 건물 섹션 추가: 변경 주기가 결합되어 기각한다.
- Catalog PostgreSQL 대량 투영 유지: ADR-0096의 저장 비용·운영 의존성을 남겨 기각한다.
- 새 KV·직렬화 엔진 도입: 이미 검증한 R2·JSON 경로로 충족되어 기각한다.
- 미연결 호 삭제 또는 임의 건물에 연결: ADR-0074의 측정된 미연결 의미를 훼손해 기각한다.

## Consequences

건물 변경은 건물 세대와 객체만 갱신한다. 필지 경로와 운영 중인 굽기는 독립적으로 유지된다.
Gold 칼럼·문서 DTO·지문·파이프라인 그래프를 같은 변경에서 갱신하고, 합성 PNU(`99999`
접두)로 동일 바이트 재사용·키 경계·미서빙 오류·매핑을 검증한다. 전체 재굽기와 운영
포인터 승격은 이 코드의 검증 후 별도 운영 작업이다.
