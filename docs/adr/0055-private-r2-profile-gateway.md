# ADR 0055: 비공개 lakehouse 프로필은 허용 목록 Worker만 통과한다

- Status: Accepted
- Date: 2026-08-25
- Amends: ADR-0039의 공개 경계 결정

## Context

ADR-0006은 카탈로그 단건 조회를 미리 만든 R2 JSON으로 제공하되 정본·원천 공간 데이터와 제공
경계를 분리한다고 정했다. ADR-0039는 산업단지 Gold 프로필의 물리 위치를 lakehouse 버킷의
`gold/industrial-complex/profiles/{artifact_id}.json`으로 바로잡았지만, 같은 버킷 안의 `gold/`
일부만 CDN/도메인 설정으로 공개할 수 있다고 잘못 전제했다. R2 공개 접근은 버킷 단위다. 그 설정을
켜면 같은 버킷의 Bronze 원본, Silver 정본과 Iceberg 메타데이터도 공개 경계 안으로 들어온다.

근본 원인은 저장 위치와 공개 권한을 같은 버킷 설정이 해결한다고 본 것이다. 지켜야 할 불변식은
다음과 같다.

1. 외부 URL 하나는 `r2_layout`이 만들 수 있는 정본 프로필 키 하나로만 대응한다.
2. 그 밖의 경로와 query는 R2를 조회하기 전에 404이고, 목록과 쓰기 능력은 Worker 타입에도 없다.
3. 브라우저 CORS는 명시한 앱 origin만 허용하며 `*`를 표현할 수 없다.
4. 버킷 이름, 키 배치, Worker/binding 이름, CORS 문법과 캐시 정책은 계약 하나에서 나온다.
5. 로컬과 CI는 같은 `cargo xtask verify foundation`을 호출하고 실제 workerd/R2 시뮬레이터도 돈다.

Cloudflare의 공식 문서는 [R2 버킷은 기본적으로 비공개이며 `r2.dev`는 비운영 용도](https://developers.cloudflare.com/r2/buckets/public-buckets/),
[Worker의 R2 binding은 자격증명 없이 버킷을 연결](https://developers.cloudflare.com/r2/api/workers/workers-api-usage/),
[Wrangler 로컬 개발은 R2 binding을 로컬에서 시뮬레이션](https://developers.cloudflare.com/workers/development-testing/),
[Cache API는 Custom Domain에서 동작하고 `workers.dev`에서는 보장되지 않음](https://developers.cloudflare.com/workers/runtime-apis/cache/),
[Worker Custom Domain은 Worker 자체를 origin으로 둠](https://developers.cloudflare.com/workers/configuration/routing/custom-domains/),
[RFC 3986 URL 정규화가 Worker보다 먼저 적용될 수 있음](https://developers.cloudflare.com/rules/normalization/)
을 각각 명시한다.

## Decision

1. `platforms/foundation-platform/services/foundation-profile-gateway`에 Foundation 소유의 독립
   module Worker를 둔다. 이 위치는 데이터와 키 계약의 소유권을 Foundation에 유지하며 제품 영역에
   R2 지식을 복제하지 않는다.
2. `config/r2-connections.contract.json` schema 2의 `profile_gateway`가 Worker 이름,
   lakehouse 연결, 단일 R2 binding, 공개 URL 환경변수 이름, CORS binding과 문법 corpus,
   compatibility date, 객체 root/UUID pattern/suffix, content type과 cache-control의 SSOT다.
   Rust `profile_gateway_contract`, `r2_layout`, 업로드 요청, TypeScript Worker, Wrangler 투영기와
   deploy/guard 테스트가 같은 파일을 소비한다. Rust와 TypeScript 모두 계약 정규식을 실제로 컴파일한다.
3. 공개 경로는 query 없는
   `/gold/industrial-complex/profiles/{lowercase-hyphenated-uuid}.json` 정확히 하나다. Worker가 받은
   WHATWG URL 표현을 전체 일치시키며 Bronze/Silver, 중첩 경로, raw/encoded/double-encoded traversal은
   404다. 운영 zone은 **Rules > Settings > URL Normalization**에서 RFC-3986 incoming normalization도
   켠다. Worker는 원래 raw request-target을 제공받지 않으므로 실물 workerd 테스트가 전달된 표현의
   모든 공격 corpus를 검증한다.
4. `Env.LAKEHOUSE` 타입은 `Pick<R2Bucket, "get">`이다. list/put/delete와 S3 계정·키를 표현하지
   않는다. canonical 경로의 GET/HEAD/OPTIONS만 허용하고 다른 method는 405다.
5. 객체는 create-only이고 artifact_id가 바뀌면 URL도 바뀐다. 따라서 성공 응답은
   `Cache-Control: public, max-age=31536000, immutable`을 쓴다. 31,536,000초는 365일이며,
   immutable은 같은 URL 재검증을 생략해도 된다는 계약이다. 교체는 기존 키를 덮지 않고 새 artifact
   URL을 발행한다. Worker Cache API에는 CORS가 없는 origin-neutral 응답만 넣는다.
6. R2의 quoted `httpEtag`를 그대로 응답한다. cold GET/HEAD는 요청 Headers를 R2 `onlyIf`에 넘겨
   `If-None-Match` 판단을 provider에 맡기고 precondition 불일치는 304로 바꾼다. warm GET은 Cache
   API의 조건부 match를 사용한다. 자체 ETag 비교기를 만들지 않는다.
7. `FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS`는 쉼표 구분 serialized HTTP(S) origin 문법을 쓴다.
   공백과 빈 항목은 제거하고, path/query/fragment/credential/`*`나 문법 오류 값은 승인하지 않는다.
   설정 자체가 잘못되면 Worker는 500으로 닫히고, 유효한 빈 목록은 origin 없는 읽기만 허용한다.
   cache lookup 뒤 복제한 응답에만 `Vary: Origin`과 정확한 ACAO를 붙인다.
8. Wrangler 구성은 계약 투영기가 생성하고 정확히 한 R2 binding만 가지며 `workers_dev: false`로
   Cache API가 보장되지 않는 공개 보조 경로를 기계적으로 닫는다. `keep_vars: true`로 Dashboard의 실제
   origin 목록을 보존하되 production route/domain, origin 값과 계정 값은 저장소에 넣지 않는다. 로컬
   proof만 `--var`로 localhost origin을 주입하고, 임시 persistence 디렉터리와 local R2 두 객체를 사용한
   뒤 60초 제한 안에 프로세스 트리와 디렉터리를 정리한다.
9. Node 20.19.0에서 최신 Workers Vitest plugin은 실행할 수 없으므로, 같은 Workers SDK의 공식
   Miniflare/workerd 저수준 harness를 사용한다. Wrangler 4.86.0과 workerd 호환판을 lockfile로
   고정한다. `cargo xtask verify foundation`이 install/config/type/test/dry-run을 모두 소유하고 CI가
   별도 명령을 복제하지 않는다.

## Rejected alternatives

- R2 public access나 bucket-bound custom domain은 버킷 전체를 열어 허용 목록 불변식을 만족하지 않는다.
- `workers.dev`를 운영 URL로 삼으면 Custom Domain의 안정된 제품 URL과 Cache API 경계를 얻지 못한다.
- Foundation API를 통해 객체를 proxy하면 이미 채택한 object-storage-first 데이터 경로에 애플리케이션
  서버 병목과 장애 결합을 다시 넣는다.
- Gold 프로필을 두 번째 공개 버킷에 복제하면 승격·정합성·삭제 정책이 두 벌이 된다.
- 자체 R2 emulator, ETag parser나 cache plane을 만들면 Wrangler/Miniflare/provider가 이미 검증한
  데이터 플레인을 다시 구현한다.
- Worker regex와 Rust 상수를 따로 유지하면 어느 쪽이 정본인지 다시 모호해진다. 계약 field를 두
  runtime이 실제로 소비하고 guard가 중복 상수의 재도입을 막는다.

## Consequences

lakehouse 버킷은 계속 비공개이고 공개 권한은 Worker 코드의 정확한 key capability로 좁아진다. 프로필
경로 추가는 계약 schema와 허용 목록 테스트를 함께 바꾸는 별도 결정이며, 이 ADR은 타일·필지·건물
경로를 자동 승인하지 않는다. Cache API는 데이터센터별 최적화라 첫 요청은 R2를 읽을 수 있지만 긴
immutable cache와 ETag 304로 반복 전송을 줄인다.

롤백은 Worker Custom Domain/route를 제거하고
`FOUNDATION_PLATFORM_R2_LAKEHOUSE_PUBLIC_BASE_URL` 소비자 값을 비우는 것이다. 버킷 public access나
객체를 변경할 필요가 없다. 운영 승격 전에는 사용자가 Dashboard에서 비공개 상태, 단일 binding,
origin 변수, URL normalization과 Worker Custom Domain을 독립 검토한다.
