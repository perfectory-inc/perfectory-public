# ADR 0430: 공짱 매물 사진은 해시로 한 번 저장되고, 사다리는 그 사진의 권한 경로로 받는다

- Status: Accepted
- Date: 2026-11-06
- Builds on: [ADR-0427](./0427-a-photo-is-shown-from-a-rendition-ladder-made-once-from-its-stored-original.md)
  "이어지는 PR 사슬" 3(배포 순서 ①~⑤), [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md),
  [ADR-0261](./0261-each-language-reads-r2-through-one-path.md)

## Context

ADR-0427 은 모든 사진이 한 파이프라인(내용주소 원본 + 원본에서 만든 사다리 + 공용 사진 칸)에 모이게 했고, 공짱 매물
사진만 남았다: id 로 잡는 키(`media/listing-photo/listings/<lst>/photos/<lph>.<ext>`), 채워진 적 없는
`thumbnail_r2_key`, 따로 정의한 `PhotoContentType`(jpeg/png/webp), Next `next/image` 기본 최적화기.

두 가지가 공매·경매와 다르다.

- **영역 경계.** 공짱은 별도 Cargo 작업공간이고 "영역 간 결합은 published HTTP 계약/이벤트만, Cargo path 의존
  금지"(AGENTS.md) 다. `foundation-shared-kernel` `MediaFormat`·키 함수와 `foundation-media-renditions` 를 그대로
  쓸 수 없다.
- **사진이 비공개다.** 초안 매물의 사진은 소유자만 본다(`can_view_photo`). 공매·경매처럼 공개 엣지의 경로
  (`<base>/<prefix>/sha256/…`)로 줄 수 없고, 지금도 사진마다 권한 경로가 서명 URL 로 돌려보낸다.

## Decision

1. **형식·키는 공짱의 사본이고, 시험이 원본에 묶는다.** `PhotoContentType` 을 지우고
   `shared_kernel::media`(`MediaFormat`·`Sha256Hex`·`content_addressed_key`·`rendition_key`·`rendition_manifest_key`·
   사다리 version/폭/형식)를 둔다. `crates/shared-kernel/tests/media_mirrors_foundation.rs` 가 시험 때 foundation
   `media.rs` 의 형식·확장자·media type 과 `config/r2-connections.contract.json` `parcel_media.renditions` 를 읽어
   같은지 본다(컴파일 때 읽지 않는다 — 이미지 빌드 문맥에 다른 영역 파일이 없다). 매물이 받는 형식은 그중
   jpeg/png/webp(`listing_photo_domain::entity::UPLOAD_FORMATS`).
2. **새 업로드는 해시로 한 번.** 업로드 요청이 `content_sha256` 을 주면 키는 `media/listing-photo/sha256/<hex>.<ext>`
   이고, PUT 서명에 `x-amz-checksum-sha256` 이 들어가 저장소가 그 해시가 아닌 바이트를 거절한다(S3 체크섬 —
   클라이언트가 해시를 거짓으로 대도 키가 오염되지 않는다). 행은 `listing_photo.content_sha256` 을 갖는다. 해시가
   없는 요청은 옛 id 키로 그대로 된다(하위 호환: 옛 웹·앱이 계속 동작).
3. **옛 사진은 append-only 대응으로.** `listing_photo_content_address(legacy_r2_key → content_sha256, content_key,
   size_bytes)` 는 UPDATE·DELETE 를 거부하는 표다. 읽기는 `coalesce(행의 해시, 대응의 해시)` 이고 `r2_key` 는 옛 키
   그대로라 원본은 옛 객체로 계속 받는다. 백필 `listing_photo_content_address_backfill`(gongzzang-api bin)은 대응 없는
   확인된 사진만, run 당 `LISTING_PHOTO_BACKFILL_BATCH_LIMIT`(기본 200, 최대 5,000)까지 읽어 같은 바이트를
   `If-None-Match: *` + 체크섬으로 내용주소 키에 쓰고 대응을 쌓는다. 옛 객체를 지우지 않는다. 바이트가 행이 말한
   형식이 아니면 세고 남긴다. 공짱을 운영하는 사람이 ADR-0427 배포 순서 ② 뒤에 손으로 돌린다 — 어떤 배포도 돌리지
   않는다.
4. **사다리는 사진의 권한 경로로.** `GET /listings/:listing_id/photos/:photo_id?rendition=<width>.<format>` 이 사다리
   한 단계(`media/listing-photo/renditions/<version>/<hex>/<width>.<ext>`)의 서명 URL 로 돌려보낸다. 계약에 없는
   단계·해시 없는 사진은 404. 새 경로를 더하지 않아 트래픽 정책 레지스트리·생성물은 그대로다. 웹은 공용
   `MediaStore` 에 "파일·단계 URL 을 주는 저장소" 꼴을 더해(`listingPhotoStore`) 공매·경매와 같은 `PhotoTile`
   (`<picture>` AVIF/WebP `srcset`, 사다리 없으면 파일)을 쓴다. 해시가 아직 없는 사진은 같은 권한 경로의 파일을
   평범한 `<img>` 로 보인다. 두 경우 모두 `next/image` 기본 최적화기를 거치지 않는다(비공개 사진을 서버 캐시에
   두지 않는다).
5. **`thumbnail_r2_key` 는 읽지도 쓰지도 않는다.** 열은 지우지 않고 deprecated 주석만 단다(값을 잃는 행이 없다).
6. **사다리를 만드는 일은 아직 없다 — 소유자 판단.** foundation 의 `ensure_ladder`·렌더러는 경계 때문에 부를 수 없다.
   길은 둘이다: (a) 기술 크레이트 하나(`foundation-media-renditions`)에 한해 영역 간 path 의존을 허용하거나 공용
   위치로 옮긴다, (b) 공짱 outbox publisher 의 `listing_photo.upload_confirmed` 싱크가 같은 OSS 코덱(`image`·`webp`,
   계약의 같은 사양)으로 따로 만든다(코드 사본, 시험이 사양을 묶는다). 정해질 때까지 사다리 단계는 404 이고 화면은
   파일로 대체된다(ADR-0427 §3 의 같은 대체).

## Consequences

- 바꾸는 것은 코드뿐이다. AWS 자원·설정·버킷 쓰기는 이 저장소가 하지 않는다. 마이그레이션은 공짱 배포가 적용한다.
- 화면 바이트는 §6 전까지 그대로다(파일을 그대로 받는다). 사다리가 생기면 표지 한 장이 ADR-0427 표본 기준
  원본 평균 411 KB 에서 960 AVIF 57 KB(DPR 1)·1920 AVIF 139 KB(DPR 2)로 준다(계산, 매물 원본 크기는 재지 못함).
- 같은 바이트를 두 매물이 올리면 객체 하나를 같이 쓴다. 사진 삭제는 행의 soft delete 이고 객체는 지우지 않는다.
- 매물 사진 수·바이트는 운영(AWS) DB·버킷을 볼 수 없어 재지 못했다; 백필 run 수 = 사진 수 / 200.
