# ADR 0503: 화면이 못 읽는 사진(BMP·TIFF)은 받은 그대로 남기고, 무손실 PNG 표시 원본으로 보인다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md)(내용주소 write-once 저장소),
  [ADR-0424](./0424-a-public-auction-photo-is-stored-at-its-original-size-beside-the-providers-thumbnail.md)(원본을 썸네일 옆에),
  [ADR-0427](./0427-a-photo-is-shown-from-a-rendition-ladder-made-once-from-its-stored-original.md) §2·§4(사다리·원본 규칙 감시),
  [ADR-0486](./0486-a-public-auction-photos-original-rule-is-written-from-the-url-the-api-gives-and-the-screen-outcome-is-held-not-the-attempts.md)(원본 규칙을 API URL 로)
- Amends: ADR-0298 의 "판정 실패는 미저장" — BMP·TIFF 는 이제 판정되고 저장된다(아래 §1). ADR-0427 §4 의 "규칙이 원본에 닿지 않음"의 셈(아래 §4).

## Context

ADR-0486 이 원본 규칙을 고친 뒤 첫 run(11-11 05:07Z)은 원본 2,204 장을 물었고 그중 84 장(3.8 %)을 "규칙이 닿지 않음"
으로 셌다. 최신 item-detail Bronze(그 run 의 600 회차, 사진 2,204 장)를 끝까지 실측했다(공개 GET, 0.3초 간격):

| 사진 | 장 | 실측 |
| --- | ---: | --- |
| 원본 따로 적힘(`potoUrlListOriginal`) | 1,454 | 원본 규칙이 닿음 |
| 썸네일 URL 이 이미 원본 크기 | 653 | 긴 변 > 178(예 1802×1900 JPEG); 원본 = 같은 바이트라 따로 적지 않음 |
| **썸네일 URL·원본 URL 모두 BMP** | **75** | 제공자는 BMP 로 올라온 사진의 썸네일을 만들지 않고 BMP 그대로 준다(예 810×920 2,980,854 B, 1097×1321 5.8 MB); `MediaFormat::sniff` 가 BMP 를 몰라 둘 다 "모르는 형식"으로 버림 → 화면에 끝내 없다 |
| 썸네일 JPEG 178×134, **원본 BMP** | 5 | 예 800×600 1,920,054 B, 1400×1050 5.9 MB; 원본을 버려 화면은 썸네일 |
| 썸네일·원본 모두 HTML 오류 페이지 | 4 | 제공자에게 파일이 없다(1,071 B `<html>`) |
| 썸네일 178×134, **원본 PNG 57~63 MB** | 8 | 5472×3648·5712×4284; `max_file_bytes` 50 MiB 를 넘어 Oversize → 화면은 썸네일 |
| 그 밖 | 5 | 저장된 파일이 jpg·png·gif 가 아님; 열어 본 1 장은 사진 칸의 PDF(제공자 자료) |

84 = 75 + 5 + 4: "규칙 불일치"의 전부가 규칙 탓이 아니었다. 80 장은 형식을 몰라서, 4 장은 제공자에게 파일이 없어서다.
BMP 를 모르는 형식으로 버리면 그 회차는 "영구 건너뜀"으로 완료되므로(ADR-0330 §1) 다시 묻지도 않는다.

BMP 를 그대로 서빙 형식에 넣는 길은 닫혀 있다. Gold 문서의 `media_format` 은 스키마 레지스트리 enum
(`jpg`·`png`·`gif`·`pdf`, 판 4)이고 웹은 생성된 zod 로 문서 전체를 거른다 — 값 하나가 늘면 새 판·호환 판정·웹·엣지
경로 규칙(`parcel_media.path_pattern`)·대조기(media_reconcile)가 함께 바뀌어야 하고, TIFF 는 Safari 밖 브라우저가 못
그린다. 업계의 이미지 서비스는 이 문제를 입력 정규화로 푼다: Cloudinary·imgix·Thumbor 는 BMP·TIFF 를 받아 원본은
보관하고, 보내는 것은 언제나 웹 형식의 파생본이다.

## Decision

1. **형식** — `foundation-shared-kernel` `MediaFormat` 에 `Bmp`(`BM` + 알려진 DIB 머리글 크기 12·40·52·56·64·108·124;
   `BM` 두 글자만으로는 판정하지 않는다)와 `Tiff`(`II*\0`·`MM\0*`)를 더한다. 둘 다 `is_image()` 이고 `is_served()`
   가 아니다. 디코더는 `image` 크레이트의 `bmp`·`tiff` 기능이다(손으로 쓴 코덱 없음).
2. **받은 그대로 남긴다** — 서빙 형식이 아닌 파일은 `<prefix>/sources/sha256/<받은 바이트 해시>.<bmp|tif>` 에 바이트
   그대로 write-once 로 쓴다(append-only, 지우지 않는다). 이 경로는 엣지 경로 규칙 밖이고 대조기가 세는 `sha256/`
   밖이다: 서빙되지 않는 보관본이다.
3. **표시 원본** — 같은 파일을 디코딩해 EXIF/TIFF 방향만 바로 세운 **무손실 PNG**(`foundation-media-renditions`
   `display_master`, `image` 의 PNG 인코더 기본값 — 같은 픽셀이면 같은 바이트라 다시 봐도 중복 저장 없음)를
   `<prefix>/sha256/<PNG 해시>.png` 에 쓴다. 저장소의 답(`StoredMedia`)은 이 PNG 이고, 받은 파일은 `source` 로 곁에
   있다. 그래서 Bronze 줄·Silver 사진 색인·사다리(PNG 에서 만든다 — 픽셀이 원본과 같다)·Gold·웹·엣지·공짱 미러
   어디에도 BMP·TIFF 가 이름 불리지 않는다. Bronze 줄은 그 항목에 `source_sha256`·`source_media_format`·
   `source_size_bytes` 를 더해 출처를 적는다(공매 `onbid_media.rs`, 경매 `courtauction_go_kr_collect/detail.rs` 둘 다
   `media_store::store_bytes` 의 답을 그대로 적는다 — 경매는 그동안 자기 해시를 따로 셌다). 디코딩이 안 되는
   BMP 는 보관본만 남고 "모르는 형식"으로 센다. 픽셀 상한은 사다리와 같은 `parcel_media.renditions.max_source_pixels`.
   이것은 사진 길(`store_bytes`)의 일이다. 산업단지 고시 첨부(ADR-0484, `store_bytes_as`)는 화면에 사진으로 그리지
   않으므로 BMP 라도 받은 그대로 `sha256/` 에 담는다.
4. **규칙 불일치의 셈** — 원본 URL 의 답이 원본이 아닐 때, 그 썸네일 URL 도 저장되지 않았으면(제공자에게 파일이
   없다) `originals_rule_mismatched` 가 아니라 `originals_provider_missing` 으로 센다. 규칙이 닿을 파일이 없기 때문이다.
   썸네일이 저장됐는데 원본이 HTML 이거나 178 이하이면 그대로 규칙 불일치다(시험이 둘을 가른다).
5. **큰 원본** — `data-go-kr-onbid.contract.json` `media.max_file_bytes` 를 50 MiB → **128 MiB**. 제공자가
   Content-Length 를 주므로 본문을 읽기 전에 거르고, 59 MB 받기는 5.7 초(실측) < `download_timeout_seconds` 20.
   메모리 최악(계산) ≈ 2.2 GB < 수집 유닛 `MemoryMax=3G`.
6. **다시 묻기** — `coverage::MEDIA_FIELDS` 4 → 5: BMP 를 버려 "완료"된 회차가 한 번 더 받아진다. 저장된 미디어는
   다시 받지 않는다(메모). 4 로의 한 바퀴(ADR-0486)는 11-11 05:07Z 에 시작했으므로 겹쳐 더 드는 몫은 그 사이 완료된
   회차뿐이다.

## Consequences

- 측정(최신 Bronze 2,204 장 기준, 계산): 화면에 원본 크기로 보이는 사진 2,107 → 2,195(+75 BMP, +5 BMP 원본, +8 큰
  PNG), 보이지 않던 BMP 75 장이 보인다. 규칙 불일치 84 → 0(나머지 4 는 `provider_missing`).
- 비용(계산): BMP 80 장 ≈ 200 MB 보관 + PNG 표시 원본(BMP 의 약 40~60 %) ≈ 100 MB, 사진 전체(4.5만 회차 × 3.6 %)로
  넓히면 보관 ~8 GB·PNG ~4 GB ≈ R2 $0.18/월. CPU: BMP 한 장 디코딩+PNG 인코딩 ~0.1~0.3 초.
- 시험: `media.rs`(판정·키), `foundation-media-renditions`(BMP·TIFF 사다리, PNG 가 무손실·결정적, 상한 거부),
  `onbid_media`(BMP 를 두 URL 에서 받아 보관본·PNG·사다리·Bronze 출처 — 이번 사고 재연; HTML 두 URL = 제공자 결손),
  게이트웨이(`sources/` 와 `.bmp` 는 404).
- 남은 것: 사진 칸의 PDF 같은 제공자 자료(위 "그 밖" 5 장)는 그대로다 — 웹이 사진으로 그리지 못하므로 링크로 보이는 일은 따로.
