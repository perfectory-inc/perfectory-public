# ADR 0486: 공매 사진 원본 규칙은 API 가 주는 URL 로 정하고, 시도 수가 아니라 화면의 결과를 지킨다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0424](./0424-a-public-auction-photo-is-stored-at-its-original-size-beside-the-providers-thumbnail.md)(원본을 썸네일 옆에),
  [ADR-0427](./0427-a-photo-is-shown-from-a-rendition-ladder-made-once-from-its-stored-original.md) §4·§5(사다리·원본 규칙 감시),
  [ADR-0480](./0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md)(결과로 잰다),
  [ADR-0330](./0330-a-public-auction-round-is-detailed-until-its-media-landed-open-rounds-first.md) §4(커버리지 지표)
- Amends: ADR-0424 §1 — 규칙의 `path_suffix`·`query` 값(규칙의 자리·모양은 그대로).

## Context

#1102(ADR-0424, 11-06 병합)와 #1104(ADR-0427)가 공매 사진을 원본 크기로 보이게 했어야 했는데, 11-11 화면은 여전히
178×134 썸네일이었다. 사슬을 끝에서부터 실측했다(읽기만):

| 고리 | 실측(2026-11-11) |
| --- | --- |
| 화면(web-preview, PNU 1111012600100240000 공매 탭) | 사진 칸 6 개 모두 `naturalWidth×Height` **178×134**(사다리 404 → 저장 파일로 대체) |
| catalog by-PNU 문서 | `schema_revision` 4, 사진 6 장 모두 `original: null` |
| Silver `onbid_real_estate_item_photo` | `media_kind = photo_original` 행 **0**(사진 116,764 행, 서로 다른 해시 29,644) |
| 수집기 지표 `onbid-item-detail.prom`(00:22Z run) | `foundation_onbid_photo_original_attempted 0`, `rule_mismatched 0`, 사진 있는 완료 회차 44,896 |
| item-detail Bronze 객체 175 개(10-25 이후 전부) | `potoUrlListOriginal` 줄 0; 아직 URL 로 남은 `potoUrlList[].urlAdr` 는 전부 `www.onbid.co.kr/op/cm/syc/filemng/filemngprcs/FileMngPrcsController/dnldFile.do?atchFileLstNo=…&atchSn=…&hashCrpsNo=…&downloadImageKind=THNL_NM` |
| 계약 `media.fields.originals.potoUrlList` | `path_suffix` **`/dnldImgFile.do`**, `query` `thnImgDownloadFlag=false`·`downloadImageKind=CLG_FILE_NM` |
| 물건상세 실호출(그 물건, 한 번) 후 같은 URL 을 질의 값만 바꿔 공개 GET(0.6초 간격) | `THNL_NM` 178×134 9,897 B(저장된 b7ba8489… 와 sha256 일치) · **`CLG_FILE_NM` 747×643 489,437 B**(ADR-0424 가 잰 원본과 같은 바이트) · `ELGM_FILE_NM` 640×480 · 값 없음 = 원본; 같은 물건의 1024×576 두 장도 같다 |

원인은 하나다. ADR-0424 는 원본 URL 을 **온비드 웹 화면**의 `dnldImgFile.do` 로 정했는데, 물건상세 **API** 가 주는 URL 은
`dnldFile.do` 다. 규칙의 경로 끝이 한 번도 맞지 않아 `original_url` 이 늘 없음을 돌려줬고, 원본은 한 번도 물어지지
않았다. 그 아래(Silver `<field>Original` 적재, Gold `photos_by_item` 의 `original`, 판 4 문서, 웹의 `photo.original ?? photo`
사다리)는 모두 맞게 이어져 있었다 — 받은 원본이 없었을 뿐이다.

왜 아무것도 말하지 않았나:

- 수집기 시험의 썸네일 URL 상수가 실물이 아니라 웹 화면의 꼴(`dnldImgFile.do?…thnImgDownloadFlag=true`)이었다. 규칙과
  시험이 같은 착오를 공유해 통과했다.
- 원본 규칙 감시(`FoundationOnbidPhotoOriginalRuleMismatch`, ADR-0427 §5)는 "물었는데 썸네일이 왔다"만 본다. 한 run 에
  10 번 이상 **시도**해야 울리므로 시도 0 은 영원히 조용하다.
- 결과 원장(ADR-0480)은 작업 단위다. 수집 작업은 매 run `changed` 였다(썸네일·감정평가서·상세는 정말 바뀌었다).
  "화면에 원본이 보이는가"는 어느 기대에도 없었다.

사다리는 돌고 있었지만 썸네일에서 만들어졌다: R2 `onbid/media/renditions/r1/` 사다리 표시 7,359 개, 공매 사진 해시
29,629 중 **5,177(17.5 %)** 이 사다리를 가졌고 모두 178px 원천이다(원천보다 크게 늘리지 않는다).

## Decision

1. **원본 규칙은 API 가 주는 URL 로 정한다.** `data-go-kr-onbid.contract.json` `media.fields.originals.potoUrlList`:
   `path_suffix` `/dnldFile.do`, `query` `{downloadImageKind: CLG_FILE_NM}`. 다른 질의 값(`atchFileLstNo`·`atchSn`·
   `hashCrpsNo`)은 그대로 둔다. `thnImgDownloadFlag` 는 `dnldFile.do` 가 읽지 않는 값이라 뺀다. 규칙의 자리와 모양
   (ADR-0424 §1)은 바꾸지 않는다 — 값만 실물로 고친다. 시험의 URL 상수는 실물 꼴(합성 번호 99999)이고, 그 주석이 이
   사고를 적는다(ADR-0001).
2. **이미 완료된 열린 물건은 기존 판 올림으로 원본을 받는다.** `coverage::MEDIA_FIELDS` 3 → 4(ADR-0424 §4·ADR-0427 §4 와
   같은 길): 판 3 으로 완료된 물건은 상세를 한 번 더 받고, 저장된 썸네일은 `state.media` 로 다시 받지 않으며 원본만 새로
   받고, 화면에 보일 원천(원본)의 사다리를 만든다. 닫힌 물건은 다시 받지 않는다(서빙되지 않는다).
3. **화면의 결과를 지표와 경보로 지킨다.** 수집기가 회차 줄마다 원본이 저장된 사진 수(`LineOutcome.originals`)를 세어
   물건 표시(`ItemMark.originals`, 옛 상태 파일은 0)에 남기고, 커버리지 파일에 `foundation_onbid_item_detail_with_originals_conditions`
   (사진이 있는 완료 회차 중 원본이 하나라도 있는 것)를 더한다. 새 경보 `FoundationOnbidPhotoOriginalsNotShown` 은 그것이
   사진 있는 회차의 **절반 미만으로 이틀** 머물면(사진 있는 회차 100 이상일 때) 알린다. 시도 수가 아니라 화면에 보이는
   결과를 본다 — 규칙이 아무 URL 에도 맞지 않든, 수집기가 아예 묻지 않든, 원본이 4xx 로 영구 제외되든 같은 하나의 경보다.
   두 원본 경보는 새 규칙 묶음 `foundation-onbid-photos` 에 모으고 promtool 시험(`foundation-onbid-photos.rules.test.yml`)이
   이 사고의 시계열(사진 44,896, 원본 0, 시도 0)로 새 경보는 울리고 옛 경보는 울리지 않음을, 건강한 백필에는 울리지 않음을
   보인다.

ADR-0480 의 작업 단위 결과 기대에 넣지 않은 이유: ADR-0480 은 "실행이 무엇을 썼나"를 작업마다 같은 어휘로 보고, 원천의
행 수 같은 생산자별 결과는 일부러 뺐다(그 기각한 대안). 이 결과는 수집기만 셀 수 있으므로(어느 URL 의 원본이 저장됐나)
수집기가 이미 쓰는 커버리지 파일(ADR-0330 §4)이 자리다. 같은 꼴: Monte Carlo·Soda 의 "볼륨·분포" 검사(실행 성공과
별개로 산출의 모양을 기대값과 비교), Google SRE Workbook *Data Processing Pipelines* 의 "산출의 정확성 SLO" — 실행이
성공해도 산출이 기대와 다르면 위반이다.

## Consequences

- **백필(계산)**: 대상은 사진 있는 열린 회차 44,896 의 물건(물건마다 한 번, ADR-0339)이고 원본은 공매 사진 해시
  29,629 이하다(BMP 등 형식 불명은 원본도 같은 BMP 라 받지 않는다). 상세 재호출은 run 당 600(하루 한도 20,000 안).
  원본 크기는 이번 실측 3 장 평균 486 KB, ADR-0424 표본 16 장 평균 0.87 MB → **~14~26 GB**. 내려받기를 ADR-0424 가 잰
  0.8 MB/s 로 잡으면 run 당 600 초 예산으로 최대 ~54 run(**~2.3 일**, 위 상한). 절반은 약 하루라 경보의 이틀 안이다.
- **돈(계산)**: R2 저장 ~26 GB × $0.015 = **~$0.39/월**, 쓰기 ~29,600 Class A ≈ **$0.13 한 번**; 사다리(ADR-0427 표의
  원천당 413 KB × 29,629 ≈ 12 GB → $0.18/월, 쓰기 ~20.7 만 Class A ≈ $0.93 한 번). 합 ~$1.06 한 번, ~$0.57/월. 소액
  재량 안.
- **사다리 지연(계산)**: 원본의 사다리는 rav1e 추정 원천당 ~4.5 초, run 당 300 초 × 동시 4 ≈ 267 원천 → 29,629 원천에
  ~111 run(~4.6 일). 그동안 사다리가 없는 칸은 웹이 이미 하는 대로 저장된 원본 파일로 바꿔 보인다(ADR-0427 §3) —
  화면은 원본 크기를 바로 보이고, 바이트만 사다리 뒤보다 크다(칸당 ~0.5~0.9 MB).
- **굽기**: Gold 일일 재빌드(`gold_public_auction_rebuild`)가 원본 행을 붙이면 그 PNU 만 다시 굽는다(ADR-0312 증분).
  스키마는 바뀌지 않는다(판 4 의 `original` 은 이미 있다).
- **남은 것(이 ADR 밖)**: 물건상세 사진의 일부는 제공자가 BMP 로 올린 파일이다(같은 물건 5 장 중 2 장, 2.98 MB
  810×920; 최신 Bronze 의 남은 URL 438 개). `MediaFormat` 이 BMP 를 모르므로 형식 불명으로 영구 제외되어 화면에 아예
  없다. 사다리가 BMP 를 디코딩하면 보일 수 있으나 공용 커널·공짱 거울·엣지 형식 목록이 함께 바뀌는 별도 결정이다.
- 제공자가 질의 값의 뜻을 바꾸면 `FoundationOnbidPhotoOriginalRuleMismatch`(썸네일이 옴)가, URL 꼴을 바꾸면 이 ADR 의
  `FoundationOnbidPhotoOriginalsNotShown`(원본이 안 옴)이 알린다.
