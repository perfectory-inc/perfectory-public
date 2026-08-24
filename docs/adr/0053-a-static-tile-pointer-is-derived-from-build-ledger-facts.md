# ADR 0053: 정적 타일 포인터는 build ledger의 객체 사실에서 파생한다

- Status: Accepted
- Date: 2026-08-24

## Context

`catalog.vector_tile_build_job`은 입력 release와 frozen snapshot, 검증 증거만 담았고 생산자가
없었다. 반면 `PromoteTileLayerStaticCommand`는 release id, file asset id, 객체 checksum과 크기,
Martin URL을 다시 받았다. 따라서 객체를 실제로 create-only 업로드하고 GET으로 재해시한 실행과
Catalog 포인터를 옮기는 실행이 서로 다른 바이트를 주장할 수 있었다. 이는 ADR-0051이 Gold
포인터에서 제거한 것과 같은 신뢰 경계 결함이다.

Cloudflare R2의 S3 호환 API는 `If-None-Match: *` 조건부 쓰기를 지원하고 조건 불일치를 `412`로
응답한다. Martin 1.12는 원격 PMTiles 경로를 발견하고 PMTiles를 직접 읽으며, `martin-cp`,
MBTiles 검증, `pmtiles convert`, `pmtiles verify`가 저장 포맷 생성·검증의 채택된 데이터
플레인이다. 이 저장소의 `FileObjectStorage`는 create-only 쓰기와 exact readback rehash를 같은
포트로 구현하므로 로컬 증명에서 R2의 불변 조건을 재현할 수 있다.

참고한 1차 자료:

- Cloudflare R2 S3 API 호환성: <https://developers.cloudflare.com/r2/api/s3/api/>
- Martin PMTiles 및 원격 객체 저장소: <https://maplibre.org/martin/sources-pmtiles.html>
- Martin 타일 복사 도구: <https://maplibre.org/martin/martin-cp.html>
- RFC 9562 UUIDv8 custom space: <https://www.rfc-editor.org/rfc/rfc9562.html#section-5.8>

## Decision

1. `catalog.vector_tile_build_job`은 검증된 결과의 release id, PMTiles file asset id, object key,
   checksum, byte size, Martin URL template, 검증 증거와 결과 기록자를 함께 보관한다. `validated`
   또는 `promoted` 상태는 이 객체 사실이 전부 존재할 때만 표현할 수 있고, `failed`는 실패 이유를
   반드시 보관한다.
2. 정적 승격 명령은 `unit_key`, `build_job_id`, 관찰한 active release/generation, idempotency key,
   operator만 받는다. release/file asset/layer/객체 사실은 잠근 build row와 그 input release에서
   만든다. operator가 checksum이나 size를 입력하는 표면은 두지 않는다.
3. 발행기는 `PostGIS snapshot → martin-cp → MBTiles validate → pmtiles convert → pmtiles verify →
   create-only upload → exact GET rehash → Martin decode → build result record → manifest CAS` 순서를
   하나의 명령으로 실행한다. 외부 도구는 각각 시간 제한 안에 성공해야 하며, 도구 부재와 시간
   초과는 skip이 아니라 오류다. PostGIS projection table은 append-only이므로 발행기가 runtime
   manifest pointer table에 `SHARE` lock을 유지하는 동안 동적 Martin view의 row set은 고정된다.
   이 lock은 Martin의 `ACCESS SHARE` 읽기와 양립하고 CAS의 `SHARE ROW EXCLUSIVE` 전환만 막으며,
   archive/readback/decode가 끝난 직후 풀어 동시 전환은 이후 promotion의 active-release 비교가
   판정하게 한다.
4. 원격 쓰기는 `ObjectWriteMode::CreateOnly`만 사용한다. 등록 전에 같은 객체를 다시 읽어 SHA-256과
   byte size를 비교하고, Martin이 release-addressed route에서 대표 타일을 읽어 동적 타일과 같은
   바이트를 반환해야 한다. release/file asset UUID는 build job UUID에서 역할별 RFC 9562 UUIDv8로
   결정론적으로 파생한다. 따라서 같은 build idempotency key의 재실행은 같은 객체 주소를 사용하며,
   두 번째 create-only 요청은 저장소에서 거절된 뒤 exact GET 재해시가 이번 빌드 바이트와 같을 때만
   복구된다. 새 주소를 만들어 고아 객체를 숨기거나 overwrite로 바꾸지 않는다.
5. `scripts/tiles/boundary-slice-proof.sh`가 `FileObjectStorage`로 create-only 충돌, readback 불일치,
   unit source XOR, Martin read 불가의 네 무력화 실험을 소유한다. 별도 정적 증명 스크립트를 만들지
   않는다.

## Consequences

정적 release 등록과 포인터 승격은 실제로 읽어 본 객체 사실 하나만 소비한다. build ledger는 더
넓어지고 외부 도구와 Martin probe 때문에 발행 시간은 늘지만, Catalog가 업로드와 다른 checksum
또는 크기를 가리키는 상태는 명령 타입과 데이터베이스 CHECK 양쪽에서 만들 수 없다. R2 쓰기는
여전히 명시적 운영 승인 대상이며 로컬 증명은 `FileObjectStorage`만 사용한다.
