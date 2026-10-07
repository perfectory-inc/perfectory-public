---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-09-27
---

# Foundation Tile Gateway

비공개 R2의 불변 PMTiles v3 release 객체에서 MVT 타일을 읽는 Cloudflare module Worker다.
ADR-0111의 A1 범위이며, 전국 base release를 서빙한다. changed-tile patch 합성은 후속 범위다.

## 계약과 응답

[`r2-connections.contract.json`](../../config/r2-connections.contract.json)의
`vector_tile_gateway`가 Worker 이름, 공개 hostname·alias, 연결·binding, source ID 문법,
최대 zoom, content type, cache 정책과 CORS 문법의 정본이다. `object_key.root.expected_value`는
해당 연결의 `expected_values`에 있는 PREFIX 키를 참조하므로 객체 경로를 중복 정의하지 않는다.
`wrangler.jsonc`는 `config:render`의 산출물이며 `config:check`가 drift를 차단한다.

- 경로: `/{source_id}/{z}/{x}/{y}`. `source_id`는 `{unit}-{release_uuid}`이며 unit 목록은
  catalog 소유다. Worker는 특정 unit을 열거하지 않는다. 확장자·query·선행 0·범위 밖 좌표는 404다.
- 객체: 연결 PREFIX 아래 `{source_id}.pmtiles`. binding은 R2 `get` range 읽기만 노출한다.
  [`pmtiles`](https://github.com/protomaps/PMTiles/tree/main/js) 공식 라이브러리가 header,
  directory, Hilbert tile ID를 해석하며 isolate별 유계 `ResolvedValueCache`를 재사용한다.
- 메서드: `GET`, `HEAD`, `OPTIONS`. 나머지는 405이며 `Allow` 헤더를 반환한다.
- 타일: MVT는 200, archive 안에 없는 타일은 빈 204, 없는 archive는 404다. 다른 tile type,
  지원하지 않는 압축, 손상된 archive 또는 R2 오류는 본문 없는 500이다.
- 압축: 디렉터리는 라이브러리가 해제하고, gzip 타일은 압축된 바이트와 `Content-Encoding: gzip`을
  그대로 보낸다. 무압축 타일은 원본 바이트를 보낸다. `encodeBody: manual`을 응답과 캐시 복사에
  모두 지정해 workerd의 `Response.clone()` 이후 이중 압축을 막는다.
- 캐시: 불변 release이므로 계약의 1년 immutable 정책을 적용한다. 성공한 GET만 canonical URL로
  `caches.default`에 저장한다. 204 저장을 지원하지 않는 캐시 구현에서도 빈 타일을 재사용하도록
  내부에는 표식이 있는 빈 200을 저장하며, 클라이언트에는 표식을 제거하고 204로 복원한다.
- 조건부 요청: 객체 ETag와 z/x/y로 만든 ETag를 사용한다. GET/HEAD의 `If-None-Match`는
  단일 값·목록·weak tag·`*`를 지원하며 일치하면 본문 없는 304다. HEAD는 항상 본문이 없다.
- CORS: 계약이 지정한 allowed-origins binding을 엄격하게 파싱한다. 미허용 Origin은 캐시
  조회 전 403이다. 캐시는 origin 중립이며 각 응답에 `Vary: Origin`과 허용된 origin만 붙인다.
  preflight는 GET/HEAD와 선택적 `If-None-Match`만 허용한다.

## 로컬 검증

Node·pnpm 버전은 parcel gateway의 `package.json`과 동일하게 고정한다. `pnpm.overrides`는 손으로 고치지 않는다 — `tools/npm/security-overrides.contract.json`에서 렌더된다(루트 ADR-0158).

```bash
pnpm install --frozen-lockfile
pnpm run config:check
pnpm run typecheck
pnpm test
pnpm run build:check
pnpm run verify:local
```

`build:check`는 기존 gateway와 동일한 Wrangler dry-run 번들 검증이다. `verify:local`은
실제 workerd, 로컬 R2와 Cache API에서 synthetic PMTiles의 압축·캐시 동작을 검증한다.
테스트 archive는 header·root directory를 직접 인코딩하며 실제 지명·필지·운영 데이터를 담지 않는다.
config 테스트는 별도 임시 설정을 변조해 검사 실패도 증명한다.

검증 SSOT인 루트 `cargo xtask verify foundation`이 다른 gateway와 함께 config, typecheck,
test, build 검사를 실행한다. CI의 Node 검증 경로를 별도로 추가하지 않는다.

## 배포

운영자가 CORS binding을 설정하고 release 객체 존재를 확인한 뒤, 이 서비스 디렉터리에서
`pnpm run config:render`와 검증을 거쳐 `pnpm exec wrangler deploy`로 배포한다.
생성 구성은 계약의 hostname과 alias를 custom domain으로 붙이고 `workers_dev: false`로
닫는다. R2 bucket 이름은 `connections.tile_derivatives.expected_values`에서 읽는다.
`keep_vars: true`는 운영 CORS 값을 보존하며 저장소에 실제 허용 origin이나 자격증명을 넣지 않는다.

배포와 Cloudflare/R2 계정 변경은 로컬 구현·검증에 포함하지 않는다.
