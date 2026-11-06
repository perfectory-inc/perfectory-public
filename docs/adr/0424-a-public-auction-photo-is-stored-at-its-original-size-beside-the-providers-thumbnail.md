# ADR 0424: 공매 사진은 제공자 썸네일 옆에 원본 크기로도 담고, 화면은 원본을 보인다

- Status: Accepted
- Date: 2026-11-06
- Builds on: [ADR-0296](./0296-public-auction-serves-full-info-and-shares-one-content-addressed-media-store.md) §2·§5,
  [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md),
  [ADR-0339](./0339-a-public-auction-item-is-detailed-once-for-all-its-rounds.md),
  [ADR-0340](./0340-every-onbid-field-is-carried-to-the-screen-or-says-why-not.md) §4,
  [ADR-0336](./0336-every-published-schema-lives-in-one-registry-and-ci-holds-every-consumer-to-it.md)

## Context

공짱 공매 화면의 사진이 작다. 2026-11-06 실측(읽기만: Trino `r2.silver`, 우리 catalog 엣지 GET, 온비드 공개 화면과 같은
공개 GET 을 0.5~0.7초 간격으로; 로그인·CAPTCHA 우회 없음):

| 무엇 | 결과 |
| --- | --- |
| 저장된 사진 b7ba8489…(PNU 1111012600100240000 물건의 첫 사진) | **178×134**, 9,897 B |
| Silver 미디어 색인의 `photo` jpg 25,000 여 개(서로 다른 해시 24,899) | 크기 p10 4.9 KB · **p50 7.9 KB** · p90 140 KB |
| 그 물건의 첨부(`dnldImgFile.do?atchFileLstNo=…&atchSn=N`)를 `thnImgDownloadFlag=true` 로 GET | 저장된 사진 3 장과 **sha256 일치**, 모두 178×134 |
| 같은 첨부를 `thnImgDownloadFlag=false&downloadImageKind=CLG_FILE_NM` 로 GET | **원본**: 747×643 489,437 B, 1024×576 596,636 B, 1024×576 373,291 B |
| 같은 첨부를 `downloadImageKind=ELGM_FILE_NM`(플래그 false)로 GET | 640×480 — 온비드 물건 상세 화면이 쓰는 크기 |
| 온비드 목록 화면의 다른 물건 첨부 16 개, 원본과 썸네일 | 원본 30 KB~3.6 MB(평균 0.87 MB, 440×440~4000×3000), 썸네일은 **언제나 178×134** |
| `www.onbid.co.kr/robots.txt`, `m.onbid.co.kr/robots.txt` | 둘 다 404(없음) |
| 온비드 사이트이용정책(저작권및공공데이터이용정책) | 공사가 저작재산권 전부를 가진 저작물은 공공누리 제1유형(출처표시); 공공데이터는 공공데이터법에 따라 영리 목적 포함 자유 이용, 단 제3자 권리가 포함된 정보는 제외 |
| 온비드 이용약관(회원 약관) | "회원은 공사가 제공한 정보를 공사의 사전승낙 없이 … 영리목적으로 이용하거나 제3자에게 이용하게 해서는 안 됩니다" — 회원에 대한 조항이고 우리는 로그인하지 않는다 |

즉 물건상세 API(`getRlstDtlInf2`, data.go.kr 15157247)의 `potoUrlList[].urlAdr` 는 제공자가 만든 **썸네일**을 가리킨다.
같은 URL 가족(`www.onbid.co.kr` 의 `dnldImgFile.do`)이 질의 값만 바꾸면 원본을 공개로 준다 — 온비드 자신의 목록 화면이
`downloadImageKind=CLG_FILE_NM` 로 원본을 보인다. API 응답에는 원본 URL 칸이 없다(인벤토리
`data-go-kr-onbid.fields.json` 의 물건상세 필드 전부를 대조).

## Decision

1. **원본을 묻는 규칙은 계약에 둔다.** `data-go-kr-onbid.contract.json` `media.fields.originals` 가 목록 필드마다
   원본의 `media_kind`, 적용할 경로 끝(`path_suffix`)과 바꿀 질의 값(`query`)을 적는다. 사진은
   `potoUrlList` → `photo_original`, `/dnldImgFile.do`, `thnImgDownloadFlag=false`·`downloadImageKind=CLG_FILE_NM`.
   다른 질의 값(첨부 번호)은 그대로 둔다. 규칙이 맞지 않는 URL 은 원본이 없다(받지 않는다). 수집기
   (`onbid_media.rs` `original_url`)와 Silver·Gold 적재가 같은 규칙을 읽는다 — 하드코딩한 URL 은 없다.
2. **둘 다 남긴다(append-only, ADR-0298).** 원본은 썸네일과 같은 내용주소 저장소(`onbid/media/sha256/<해시>.<확장자>`)에
   한 번 쓰고, 상세 Bronze 줄의 물건에 `potoUrlListOriginal: [{sequence, sha256, media_format, size_bytes}]` 로 적는다
   (`sequence` = 그 썸네일의 목록 순서; ADR-0340 §4 의 `<필드>Media` 와 같은 꼴). 썸네일 항목은 그대로다. 원본의 바이트가
   썸네일과 같으면(제공자가 썸네일을 안 만든 첨부) 적지 않는다. Silver `onbid_real_estate_item_photo` 는 표 모양을
   바꾸지 않고 `media_kind = photo_original` 행을 더한다.
3. **받는 순서와 상한.** 미디어 단계는 모든 줄의 썸네일 → 모든 줄의 원본 → 나머지(감정평가서·지도 …) 순이다(ADR-0339
   사진 먼저의 연장: 화면이 보일 것 먼저). 다운로드 한 번은 썸네일과 같은 상한(`per_run_limit`·`daily_download_limit`·
   `run_time_budget_seconds`·`max_file_bytes`)에 든다. 원본이 5xx·시간 초과면 그 물건은 미완료로 남아 다음 run 이 다시
   받는다(ADR-0330 §1); 4xx 는 영구 제외다.
4. **이미 완료된 물건의 원본은 기존 판 올림으로 받는다.** `coverage::MEDIA_FIELDS` 를 1 → 2 로 올린다(ADR-0340 §4 와 같은
   길): 그 전에 완료된 열린 물건은 상세를 한 번 다시 받고, 이미 저장된 썸네일은 `state.media` 로 다시 받지 않으며 원본만
   새로 받는다. 이미 닫힌 물건은 다시 받지 않는다(서빙되지 않는다).
5. **서빙 문서는 덧붙이기만 한다.** `parcel_public_auction_by_pnu` 판 4(레지스트리, FULL_TRANSITIVE): 회차 사진
   `rounds[].photos[]` 에 선택 칸 `original {sha256, media_format, size_bytes}` 를 더한다(원본이 아직 없으면 null).
   새 `media_kind` 값을 `photos[]` 에 넣지 않는다 — 닫힌 열거로 읽는 배포된 웹이 깨진다(#939 가 막는 꼴); 선택 객체는
   판 3 독자가 무시한다. Gold(`photos_by_item`)는 물건·순서가 같은 원본 행 중 가장 늦게 들어온 것을 붙이고, 원본 행은
   물건 `media` 에도 따로 넣지 않는다.
6. **화면은 원본을 보인다.** 공짱 공매 사진 칸은 `src` 가 원본, `srcset` 이 `썸네일 178w, 원본 1024w`, 배경이 썸네일
   (원본이 오는 동안 자리표시), 누르면 원본 파일을 새 탭에 연다. 원본이 없는 사진은 지금처럼 썸네일이다.

대기업·오픈소스의 같은 꼴: 원본을 보존하고 화면용 파생본을 따로 두는 것은 미디어 저장의 표준이다(Wikimedia Commons
의 원본 + 썸네일 파생, Cloudinary·imgix 의 원본 보존 + 변환 URL, HTML `srcset` 의 해상도 후보). 스키마는 Confluent
Schema Registry 의 FULL_TRANSITIVE 덧붙이기 규칙(선택 필드만 추가)을 따른다.

## Consequences

- **비용(계산)**: 백필 대상 = 열린 물건(10-29 실측 8,722)의 사진 ≈ 29,000 장(Silver 의 서로 다른 사진 해시 29,071 이 상한).
  평균 0.87 MB(위 16 표본) → **~25 GB**. R2 저장 ~$0.38/월, Class A ~29,000 건 ≈ $0.13 한 번. 온비드 측 내려받기는
  실측 0.8 MB/s 로 ~8.7 시간 = run 당 600초 예산으로 ~52 run(~2.2 일); 그동안 새 물건의 감정평가서는 원본 뒤로 밀린다.
  물건상세 재호출 ~8,700 번(하루 한도 20,000, run 당 600 → ~15 run). 그 뒤 하루 = 새 물건 ~200 × 3.3 장 ≈ 660 원본.
- **굽기(계산)**: 판 4 로 바뀐 `schema_revision` 때문에 공매 레인 문서 전부(11-06 Gold 4,217)가 한 세대 바뀐다; 그 뒤는
  원본이 들어온 물건의 PNU 만(ADR-0312 증분).
- **문서 크기**: 사진마다 `original` 객체 ~120 B. 256 KiB 상한(ADR-0193)은 쓰는 그대로 재므로 넘치는 회차는 지금처럼
  잘린다.
- **원본 크기**: 최대 3.6 MB(4000×3000)를 본 그대로 보인다. 화면 크기 파생본(예: 1024px)을 만들지는 않았다 — 필요하면
  같은 내용주소 저장소에 파생본 종류를 더하는 후속이다.
- **이용 조건**: 원본은 API 가 내준 URL 과 같은 호스트·같은 첨부의 공개 GET 이고, 받는 속도는 썸네일과 같은 상한
  (동시 6, run·일 상한) 안이다. 사진 저작물의 제3자 권리(감정평가기관·위임기관이 찍은 사진)는 썸네일에도 이미 같은
  문제다. 문서는 `public_auction.source_datasets` 로 출처를 싣지만 공짱 화면은 아직 출처를 표기하지 않는다 —
  공공누리 제1유형(출처표시)을 화면에 다는 것은 후속이며, 회원 약관 조항의 해석과 함께 소유자 판단으로 남긴다.
- 제공자가 URL 규칙을 바꾸면 원본 다운로드가 4xx·형식 불명으로 영구 제외되고 썸네일은 그대로 서빙된다(퇴보 없음);
  `MediaReport` 의 `skipped_gone`·`skipped_unknown_format` 이 늘어난 것으로 보인다.

## 덧붙임: 같은 날 잰 공매 문서 수 4,374 와 4,217 의 차이

11-06 00:25Z 굽기가 4,217 문서를 발행했고 Gold 는 4,374 라고 보고됐다. 실측(Trino, 읽기만): 굽기가 읽은 Gold 스냅숏
3464237000452890770 자체가 **4,217 행**(PNU 중복·무효 0)이고 굽기 로그도 `4217 documents and 0 tombstones` 다. 4,374 는
그 앞 스냅숏 2101964891083258407(11-04 21:43Z)의 행 수다 — 굽기 전에 잰 숫자가 11-05 21:39Z Gold 재빌드로 낡았다.
재빌드의 차이(두 스냅숏 PNU 완전 외부 조인): 빠진 PNU 260, 새 PNU 103, 순 −157. 빠진 260 은 모두 설계대로다 —
서빙 회차의 입찰 마감이 다 지남 21(ADR-0287 §7), 물건이 낙찰돼 남은 회차가 목록에서 빠짐 153, 낙찰 기록 없이 완전한
목록 전수에서 빠짐 86(그중 72 는 압류재산, ADR-0275). `attached_via` 는 두 스냅숏 모두 null 이라 `servable()`
(ADR-0292)은 관여하지 않았다. 고칠 결함은 없다.
