# ADR 0603: 산업용지 분양·처분·공급 공고는 ILIS·한국산업단지공단·LH 셋에서 첨부까지 내려가기 전에 모으고, 읽은 기록과 첨부 색인만 Silver 에 둔다

- Status: Accepted
- Date: 2026-11-22
- Builds on: [ADR-0484](./0484-industrial-complex-notice-attachments-are-collected-whole-by-foundation-and-read-later-by-intelligence-on-ai-server.md)(고시 첨부를 전부 모으고 읽기는 나중에),
  [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md)(내용주소 저장소),
  [ADR-0152](./0152-bronze-keys-of-reused-provider-file-numbers-name-their-bytes.md)(내용 주소 Bronze 키),
  [ADR-0352](./0352-court-auction-recrawl-is-computed-each-run-from-the-lakehouse-not-a-stored-queue.md)(다음에 물을 것은 레이크하우스에서 계산),
  [ADR-0387](./0387-the-media-in-r2-and-the-silver-index-that-names-it-are-reconciled-daily.md)(미디어 대조),
  [ADR-0032](./0032-provider-identity-is-derived-from-domain-label.md)(공급자 도메인)

## Context

사장 승인(2026-11-22 "맞아 산업단지", 2026-10-30 미수집 원천 전부 수집): 산업단지의 산업용지 처분공고·분양공고·공급공고를 모은다.
원천 조사(2026-11-22, 읽기만, 사이트마다 요청 10 개 안팎):

| 원천 | 받는 길 | 범위 | 잰 것 |
| --- | --- | --- | --- |
| ILIS 산업입지정보시스템 분양공고(`il/parcel`) | 목록 POST 하나(576 KB)에 전부, 상세 GET, 첨부는 고시와 같은 `cmm/file/download` | 17 개 시도, 2011-04 부터 741 건(일반 507·농공 163·국가 45·도시첨단 26) | 상세에 단지 코드(`danji_cd` = `official_complex_code`)·면적·가격·신청기간·업종·시행자, 첨부 평균 1.65 개(목록 `sk_cnt`). **par_seq 1..846 중 105 건(12 %)이 목록에서 사라졌고 그 상세는 HTTP 400** — 826~832 같은 최근 것도 |
| 한국산업단지공단 임대/분양 게시판(`boardList/1003`) | HTML, 한 쪽 10 건(쪽 크기 매개변수 없음), 글 쪽에 첨부 버튼 `fnFileDownload('<번호>', '<id>')`, 받기 `api/fnFile/commonFileDownload/<번호>/<id>` | 공단 자체 단지, 962 건, 2008-07 부터 | 계양 4차·경산4·울산테크노·오송·장성나노 = ILIS 분양공고에 **0 건**. robots.txt 전부 허용 |
| LH 토지 공고(data.go.kr B552555) | 공고문 조회 `lhLeaseNoticeInfo1`(유형 01 토지), 상세 `lhLeaseNoticeDtlInfo1`(`dsAhflInfo[].AHFL_URL`), 공급정보 `lhLeaseNoticeSplInfo1`(필지마다 용도·소재지·지번·면적·예정가격) | 전국 | LH 산업시설용지 공고는 ILIS 에 일부만: 남동 8차는 있고 밀양나노·포항블루밸리 2단계·사천지구·남청주현도(11-13)는 없다. 서비스키는 있지만(`DATA_GO_KR_SERVICE_KEY`) 세 서비스의 **활용신청이 사장 결정**이다 |

넣지 않은 것: 시도 개발공사·시군 게시판(사이트 17 개 넘게, ILIS 가 일반·농공 공고 대부분을 싣는다), 전남 분양단지 API(한 지역, 공고 아님),
국토부 분양현황(면적 통계), 공장설립온라인지원시스템(robots.txt 가 `/bbs/`·`/srch/` 를 막는다; 그 표본 20 건 중 6 건을 이 세 원천이 덮는다 —
나머지 게시처의 허용 여부는 이 ADR 의 남은 일).

## Decision

1. **세 원천, 한 꼴.** 수집기 `collect-industrial-land-sale-notices`(원천마다 작업 하나:
   `industrial_land_sale_notice_{ilis,kicox,lh}_collect`, 유닛 `foundation-industrial-land-sale-notice-collect@<원천>.service`)가
   계약 `industrial-land-sale-notice.contract.json` 하나를 읽는다. 원천의 목록을 새 것부터 쪽마다 읽고, 공고마다 상세를 받은 그대로
   두고, 첨부는 형식에 상관없이 공용 내용주소 저장소(`data-go-kr-onbid.contract.json#/media/object_prefix`)에
   `industrial_complex_notice_collect::store_attachment` 로 한 번만 담는다(ADR-0298, ADR-0484 §2 와 같은 저장소·같은 함수). 글을 꺼내지 않는다(ADR-0484 §1).
2. **Bronze.** 읽은 줄 하나 = `{notice_source, notice_id, list_fingerprint, list_row, requested_at_utc, http_status, read_outcome, error, body, attachments}`:
   `body` 는 제공자의 답 그대로(KICOX 는 `{"html": 쪽}`, LH 는 상세와 공급정보 답 둘), 수집기가 한 일은 `attachments` 에만 적는다.
   `commit_every`(50) 건이 gzip JSON Lines 객체 하나이고, 키는 내용 주소다(`…-<run>-<batch>--sha256-<hex>.gz`, ADR-0152).
   ILIS·LH 의 목록 답도 쪽마다 내용 주소 키로 둔다(`list.keep_in_bronze`): 같은 답은 같은 객체이고, ILIS 처럼 공고를 내리는 원천에서
   목록이 언제 무엇을 실었는지가 남는 유일한 기록이다. KICOX 목록은 쪽마다 조회수가 바뀌어 두지 않는다(글 쪽이 목록의 값을 모두 싣는다).
3. **부분·새 것만.** 다음에 물을 공고는 매 실행 레이크하우스에서 계산한다(ADR-0352 의 꼴): 목록의 공고 중
   `silver.industrial_land_sale_notice_read` 의 마지막 읽음이 **같은 목록 지문**의 `read` 가 아닌 것. 지문은 계약이 원천마다 정한 목록
   필드(ILIS 제목·공고일·구분·승인·단지·첨부 수, KICOX 제목·등록일·첨부 유무, LH 공고명·상태·게시일·마감일) 값의 sha256 이다:
   제공자가 목록에서 바꾼 공고는 다시 읽고, 번호·조회수처럼 늘 바뀌는 값은 넣지 않는다. 상한 `per_run_limit`(400)·`run_time_budget_seconds`
   (1,800 초), 요청 사이 2 초, 상세 다섯 번 연달아 실패하면 멈춘다. 목록 걷기는 아무것도 빚지지 않은 쪽에서 멈추되, 읽은 공고 수가
   제공자가 밝힌 총 건수에 닿은 뒤에만 멈춘다(첫 채우기 동안은 깊은 쪽까지 간다). 매시 한 번 — 첫 채우기가 몇 시간에 끝나고, 내려가는
   공고를 한 시간 안에 잡는다; 첫 채우기 뒤의 실행은 목록 쪽 하나다.
4. **Silver 는 수집의 장부 둘만.** `load-industrial-land-sale-notice-silver`(작업 `industrial_land_sale_notice_silver`, 발행기 안의 DuckDB 와
   Iceberg 추가 커밋, ADR-0559·0560)가 세 원천의 상세 객체를 객체마다 붙인다: 먼저 저장소에 담긴 첨부를
   `silver.industrial_land_sale_notice_attachment`((원천, 공고, 해시)마다 한 줄; 새것·바뀐 것만), 그다음 읽음을
   `silver.industrial_land_sale_notice_read`(읽기마다 한 줄, 실패 포함). 두 표는 첫 커밋이 만든다. 공고의 뜻(면적·가격·기간·필지)을 푸는
   Silver 는 이 ADR 이 아니다 — Bronze 가 답을 그대로 갖고 있어 나중에 다시 만든다.
5. **첨부 색인은 대조기에.** 첨부 색인은 미디어 대조기의 `media.reconcile.index_tables` 다섯째다(`media_index_delta` 의 읽는 이도).
   막는 사고: 색인 없이 같은 `sha256/` 에 첨부만 쌓이면 48 시간 뒤부터 그 수천 개가 매일 고아로 세어져 `FoundationMediaObjectOrphaned` 가
   울린다(ADR-0429 의 경매 사진, ADR-0484 §5 와 같은 일). 표 집합이 바뀌므로 대조기는 다음 실행에 색인을 한 번 통째로 다시 읽는다(설계된 동작).
6. **LH 는 꺼진 채로 들어온다.** 서비스키가 없거나 게이트웨이가 세 서비스를 거부하면(`refusal_codes`) 수집기는 아무것도 쓰기 전에
   `industrial-land-sale-notice-refused` 를 찍고, 스크립트가 78 로 끝낸다. 작업은 `enabled: false` 이고 `disabled_reason` 에 사장이 할
   활용신청 셋(15058530·15057999·15056765)을 적는다. 첨부 URL 은 `apply.lh.or.kr` 의 https 만 묻는다: 응답이 다른 호스트를 가리켜도 적기만 한다.
7. **공급자.** `kicox.or.kr` 을 승인 도메인에 더한다(ADR-0032, `kicoxorkr__…`). LH 는 data.go.kr 공급자이고 세 연산의 dataset 은
   `lh_land_notice_dataset_slug` 지도가 정한다(`source_slug_catalog_parity`). HTML 은 `scraper`(Servo 의 html5ever·selectors) 로 찾는다 —
   글 번호와 첨부 번호는 쪽이 스스로 부르는 `onclick` 호출에서만 읽는다.

### 대안

| 대안 | 왜 아닌가 |
| --- | --- |
| ILIS 분양공고 하나만 | 표본으로 KICOX 자체 단지 5 개·LH 산업용지 4 건이 빠진다 |
| 공장설립온라인 분양공고 목록을 받기 | 그 robots.txt 가 `/bbs/` 를 막는다; 원 게시처에서 받는다 |
| LH 를 청약센터 웹 목록으로(열쇠 없이) | 첨부 받기 경로(`/lhapply/lhFile.do`)를 robots.txt 가 막고, 목록은 최근 창만 보인다; 공식 API 가 같은 URL 을 준다 |
| 다음에 물을 것을 Bronze 원장(Postgres)으로 | 같은 일을 고시 수집기가 Silver 읽은 기록으로 한다(ADR-0484 §3); 두 번째 방식은 결함이다 |
| 첨부를 Bronze 객체(원장 행)로 | ADR-0298 이 미디어 blob 에 원장 행을 기각했다; 저장소 하나 + Silver 색인이 정본이다 |
| 첨부 색인 없이 수집 먼저 | §5 의 고아 경보가 매일 운다 |
| 하루 한 번 | 첫 채우기가 2~3 일, 내려가는 공고를 하루 동안 놓칠 수 있다; 매시 실행은 목록 쪽 하나(ILIS 576 KB — 고시 수집기의 13.5 MB 목록이 이미 매시)다 |
| KICOX 목록 쪽도 Bronze 에 | 쪽마다 조회수가 바뀌어 매시 새 객체(하루 3.5 MB)가 되고, 글 쪽이 같은 값을 싣는다 |

같은 꼴: 공고·공시 아카이브(미국 SEC EDGAR 의 전문 원본 보존과 색인 분리, Common Crawl 의 WARC 원본과 파생 분리)는 원본을 내용 해시로
한 번 두고, 무엇을 언제 받았는지의 장부를 따로 둔다; 뜻은 원본에서 나중에 다시 만든다.

## Consequences

- **양(계산)**: 첨부 ≈ ILIS 741 × 1.65 ≈ 1,220 개 + KICOX 962 × ~1.5 ≈ 1,450 개, 합 ≈ 3~4 GB(고시 첨부 평균 1.25 MB 기준). Bronze 상세 ≈ 35 객체
  (KICOX 쪽 HTML ~170 KB 를 gzip 으로). R2 저장 ≈ 월 $0.06, PUT ≈ 2,700 + 35 ≈ $0.01 한 번.
- **첫 바퀴(계산)**: 공고 하나 ≈ (상세 1 + 첨부 1.5~1.65) × 2 초 ≈ 5 초 → 실행 하나에 ~340 건 → ILIS ~3 실행, KICOX ~3 실행(매시: 몇 시간).
- **검증하지 못한 것**: 운영 실행 시간·메모리(첫 실행의 저널), KICOX 의 옛 쪽(2008~)의 HTML 모양이 같은지, LH 의 답(활용신청 전이라
  한 번도 못 봤다 — 가이드의 예제로만 짰다), 게시일 전 기간을 한 번에 묻는 LH 목록이 받아들여지는지.
- **남은 일**: (1) 첫 실행의 저널로 `measured_run_seconds` 를 적고 `first_run` 을 지우는 PR; (2) 사장의 LH 활용신청 뒤 LH 작업을 켜는 PR(첫
  답을 인용); (3) 공장설립온라인 표본 20 건 가운데 이 세 원천 밖의 게시처(지자체·공사·K-water·한국농어촌공사·KICOX 지사 처분신청) — robots 와 약관이
  허용하는 곳만, 2026 년 공고로 빠진 몫을 잰 뒤 더한다; (4) 공고의 뜻을 푸는 Silver(필지·면적·가격·기간)와 intelligence 의 첨부 읽기(ADR-0484 남은 일과 같은 길).
