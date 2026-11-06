# ADR 0426: 사진은 저장된 원본에서 한 번 만든 표시 사다리로 보이고, 원본 규칙이 어긋나면 세어 알린다

- Status: Accepted
- Date: 2026-11-06
- Builds on: [ADR-0297](./0297-parcel-media-captures-originals-first-and-pre-generates-display-sizes.md) §2·§3·§8(이 ADR 이 그 이행이다),
  [ADR-0424](./0424-a-public-auction-photo-is-stored-at-its-original-size-beside-the-providers-thumbnail.md),
  [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md),
  [ADR-0330](./0330-a-public-auction-round-is-detailed-until-its-media-landed-open-rounds-first.md),
  ADR-0384(경매 사진, #1100 에서 열린 채)
- Amends: ADR-0424 §6 — 화면은 원본 파일 자체가 아니라 원본에서 만든 사다리를 보인다.

## Context

ADR-0424 로 공매 사진의 원본(평균 0.87 MB, 최대 3.6 MB·4000×3000)을 받게 됐고, 화면은 그 원본을 160px 칸에 그대로
내려받는다. ADR-0297 §2 는 "표시 크기는 사다리로 미리 만든다"고 정했지만 크기·형식·키를 열어 두었다. 2026-11-06 사장
지시: 원본 하나가 정본이고, 화면 크기는 우리가 원본에서 만든다; 제공자 썸네일은 화면 단계가 아니라 원천 증거다.

실측(2026-11-06, 읽기만):

| 무엇 | 결과 |
| --- | --- |
| Silver `onbid_real_estate_item_photo` 사진 원천(서로 다른 해시) | **29,180**, 평균 60 KB, p50 8.7 KB(대부분 178×134 썸네일) |
| 저장된 원본 크기 사진 40 장(Silver 에서 해시 순 표본, catalog 엣지 GET·해시 대조) | 원본 평균 **411 KB**(p50 381 KB), 픽셀 p50 1800×1900, 최대 긴 변 2850 |
| 같은 40 장의 사다리(EXIF 방향 적용, Lanczos, AVIF q60 speed 8·WebP q75; 랩톱 Pillow 12.2 의 libavif·libwebp 를 대리로) | 400px AVIF 평균 **16.0 KB**·WebP 17.7 KB; 960px 57.3 / 57.3 KB; 1920px 139 / 126 KB; 원천 하나의 사다리 6 파일 합 413 KB |
| 같은 사다리를 만든 시간 | 원천 하나 평균 1.81 초, 최대 13.8 초(8 코어 랩톱) |
| ai-server | 20 코어, 부하 평균 1.5 |
| caniuse WebP(2026-11) | 전 세계 **97.3 %**, iOS Safari 14+, Safari 16+ |
| Cloudflare Images 변환 요금(2026-11, 공식 가격표) | 매달 고유 변환 5,000 무료, 그 뒤 **1,000 당 $0.50/월**; 같은 원천·같은 크기는 형식·품질이 달라도 하나로 센다; R2 등 바깥 저장소 이미지에도 같다 |

## Decision

1. **원본이 정본이다.** 저장은 ADR-0298 의 내용주소 한 번(`onbid/media/sha256/<해시>.<확장자>`) 그대로다. 제공자
   썸네일은 Bronze 증거로 계속 남기고(append-only), 원본이 있는 사진의 화면 단계로는 쓰지 않는다. 원본이 없는 사진
   (규칙이 닿지 않았거나 아직 못 받은 것)만 썸네일이 원천이 된다.
2. **사다리는 계약 하나에 있다.** `definitions/platform.definition.json` → 생성되는 `config/r2-connections.contract.json`
   `parcel_media.renditions`: `version: r1`, 긴 변 **400·960·1920**, 형식 **AVIF 다음 WebP**(JPEG 대체 없음 — WebP
   97.3 %, 둘 다 못 읽는 브라우저는 §3 의 오류 대체로 원본 파일을 본다), `max_source_pixels` 5,000만(디코딩 전에 거부하는
   압축 폭탄 상한). 원본보다 크게 늘리지 않는다: 원천보다 큰 단계는 원천 크기로, 아래 단계와 같은 파일을 쓴다.
   만들기는 새 크레이트 `crates/technical/foundation-media-renditions` 하나다 — `image`(디코딩·EXIF 방향·Lanczos3·AVIF
   = ravif/rav1e, nasm 없이)와 `webp`(libwebp) 크레이트, 손으로 쓴 코덱 없음. EXIF 방향으로 세운 뒤 줄이고, 파일은
   디코딩한 픽셀에서만 쓰므로 EXIF·GPS 가 남지 않는다(시험이 합성 EXIF·GPS 문자열로 확인한다).
3. **키는 (원본 해시, version) 이다.** 단계는 `<prefix>/renditions/<version>/<원본 sha256>/<width>.<확장자>`, 다 쓴 뒤
   마지막에 `<prefix>/renditions/<version>/<원본 sha256>.json`(단계·픽셀·바이트·각 파일 sha256)을 쓴다 — 그 파일이
   있으면 사다리가 다 있다(write-audit-publish 의 publish 표시). 전부 CreateOnly·`immutable` 1 년 캐시. 값을 바꾸면
   version 을 올린다: 새 version 만 새로 만들고 옛 것은 지우지 않는다. 웹은 엣지 계약 시험(`edge-contract.test.ts`)으로
   같은 version·단계·형식에 묶인 사본으로 `<picture>` 의 AVIF `srcset` 과 WebP `<img srcset>`·`sizes` 를 만들고
   `loading="lazy"` 다; 원본 파일은 사진을 눌러야만 받는다. 사다리가 아직 없는 사진(그 run 의 사다리 상한에 걸린 새
   사진)은 이미지 오류에서 원천 파일로 바꿔 보인다 — 그 수는 §5 지표로 보인다.
4. **수집 때, 바뀐 것만 만든다.** 공매 수집기의 미디어 단계(`onbid_media.rs`) 뒤 같은 run 에서, 회차(상세 한 줄)마다
   화면에 보일 원천(원본, 없으면 썸네일)의 사다리를 만든다(`media_renditions.rs`, 경매 수집기가 같은 함수를 쓴다).
   원천은 다시 내려받지 않고 저장소에서 읽는다(`ObjectStorageService::read_object_bytes`). 사다리 표시가 이미 있으면
   HEAD 한 번이다. 상한은 수집 계약 `media.renditions`(원천 600·동시 4·300 초)이고, 상한에 걸린 회차와 저장소가
   거절한 회차는 미완료로 남아 다음 run 이 다시 받는다(ADR-0330 §1). 디코딩되지 않는 원천은 영구 제외(화면은 그 파일).
   이미 완료된 열린 물건은 `coverage::MEDIA_FIELDS` 2 → 3 으로 한 번 더 지나가며 사다리를 얻는다(ADR-0424 §4 와 같은 길).
5. **원본 규칙 감시 — 조용히 나빠지지 않는다.** 원본 규칙(`media.fields.originals`)으로 받은 답이 HTML 이거나 머리글의
   긴 변이 규칙의 `thumbnail_longest_px`(178) 이하이면 원본으로 적지 않고(화면은 썸네일), run 의
   `originals_rule_mismatched` 로 센다. 수집기는 그 수와 시도 수, 미룬 사다리 수를 기존 커버리지 지표 파일
   (`onbid-item-detail.prom`)에 쓰고, 기존 Prometheus 규칙 파일의 `FoundationOnbidPhotoOriginalRuleMismatch` 가
   한 run 에 10 건 이상 시도해 절반 넘게 어긋나면 1 시간 뒤 알린다. 막는 사고: 2026-11-06 까지 저장된 공매 사진
   전부가 178×134 썸네일이었는데 아무것도 그것을 말하지 않았다(ADR-0424 Context).
6. **출처 표시.** 공짱 공매 패널 아래에 "출처: 온비드(한국자산관리공사)" 를 단다(공공누리 제1유형, 출처표시).
   ADR-0424 가 소유자 판단으로 남긴 부분이며, 2026-11-06 사장이 표시하라고 정했다.
7. **경매도 같은 파이프라인이다.** 경매 사진(ADR-0384, #1100)은 같은 저장소(`photos.object_prefix_from` = 공매의
   `media.object_prefix`)에 담기므로, 저장한 사진마다 같은 `media_renditions::ensure_ladder` 를 부른다 — 모듈을 복사하지
   않는다. #1100 이 병합된 뒤 그 수집기에 붙인다(그 PR 이 아직 열려 있어 이 PR 에는 공매만 있다).

### 미리 만들기 대 엣지 즉석 변환

| 축 | 미리 만들기(이 결정) | Cloudflare Images 즉석 변환 |
| --- | --- | --- |
| 돈(계산) | 저장 29,180 × 413 KB ≈ **12 GB → $0.18/월**; 쓰기 29,180 × 7 객체 ≈ 20.4 만 Class A → **$0.92 한 번**; 원본 읽기 2.9 만 Class B ≈ $0.01; CPU 는 우리 호스트 | 원천 29,180 × 3 크기 = 87,540 고유 변환/월(화면에 한 달에 한 번이라도 보이면) → (87,540 − 5,000) × $0.50/1,000 ≈ **$41/월, 매달** |
| 첫 화면 지연 | R2 에서 16 KB 객체 하나 | 캐시가 비면 원본(평균 411 KB~0.87 MB)을 R2 에서 읽고 그 자리에서 변환 |
| 부분 처리 | 새 원본만, version 이 바뀌면 그 version 만 | 매달 다시 센다(같은 변환도 다음 달 새로 과금) |
| append-only·정본 | 사다리도 R2 의 내용주소 객체, version 별로 쌓인다 | 결과는 CDN 캐시뿐, 정본에 없다 |
| 잠김 | 평범한 R2(S3 API) 객체, 어느 CDN 이든 | `/cdn-cgi/image/…` URL 규칙은 Cloudflare 전용 |
| CI 비용 | 0(시험은 합성 8~64px 이미지) | 0 |

우리 원칙(부분 처리·append-only·레이크하우스가 정본)에서 미리 만들기가 이긴다. 같은 꼴: Airbnb·Zillow 는 업로드 때
고정 크기 사다리를 만들어 CDN 에 둔다; imgproxy·thumbor 는 즉석 변환 서버이고 앞에 CDN 캐시를 두는데, ADR-0297 이
서버 운영 부담으로 이미 기각했다. 즉석 변환은 ADR-0297 §4 대로 "슬롯을 미리 모르는 곳"에만 남는다.

## Consequences

- **화면 바이트(실측 표본 + 계산)**: 물건 한 번 보기(평균 사진 3.3 장, 160px 칸, DPR 2 → 400 단계).
  지금 main(썸네일) 3.3 × ~9 KB ≈ 30 KB 지만 178×134 라 흐리다; ADR-0424 대로 원본을 보이면 3.3 × 411 KB ≈ **1.36 MB**
  (표본 평균; ADR-0424 의 16 장 평균 0.87 MB 면 2.87 MB); 이 결정 뒤 3.3 × 16.0 KB ≈ **53 KB**(AVIF; WebP 58 KB) —
  원본 보이기의 1/25~1/54.
- **백필(계산)**: 사진 원천 29,180. rav1e(asm 없음)를 Pillow·libavif 대리 측정 1.81 초의 2~3 배인 ~4.5 초로 잡으면
  run 당 300 초 × 동시 4 ≈ 270 원천 → ~108 run(시간마다 ~4.5 일), CPU ~36 시간. 그동안 사다리가 없는 사진은 원천
  파일로 보인다. 미완료 물건은 상세를 다시 받으므로 그 기간 물건상세 호출이 run 당 최대 600(하루 한도 20,000 안).
- **돈(계산)**: 위 표 — 한 번 ~$0.93, 매달 ~$0.18. 사장 승인 범위(소액) 안이다.
- **작업 시간**: 미디어 단계 600 초 + 사다리 300 초 + 한 회차(≤ 70 초)로 작업 한도 40 분 안이다.
- **검증하지 못한 것**: rav1e 의 실제 속도와 파일 크기는 CI 시험(합성 이미지)과 첫 운영 run 의 `onbid-media-summary-json`
  (`ladders_written`·`ladder_bytes_stored`)으로 처음 보인다; 표의 크기는 libavif(aom) 대리값이다.
- 판정되지 않는 형식(PDF 감정평가서·지도 외)은 사다리를 만들지 않는다. 색 프로필(ICC) 변환은 하지 않는다(원본
  대부분 sRGB JPEG). 지각 해시 중복 제거는 하지 않는다.
- 문서 스키마는 바뀌지 않는다: 사다리 키는 문서가 이미 싣는 원천 해시와 계약의 version 으로 정해진다.
