# ADR 0603: 산업용지 분양·처분·공급 공고는 공장설립온라인·ILIS·한국산업단지공단·LH 넷에서 첨부까지 내려가기 전에 모으고, 읽은 기록과 첨부 색인만 Silver 에 둔다

- Status: Accepted
- Date: 2026-11-22
- Builds on: [ADR-0484](./0484-industrial-complex-notice-attachments-are-collected-whole-by-foundation-and-read-later-by-intelligence-on-ai-server.md)(고시 첨부를 전부 모으고 읽기는 나중에),
  [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md)(내용주소 저장소),
  [ADR-0152](./0152-bronze-keys-of-reused-provider-file-numbers-name-their-bytes.md)(내용 주소 Bronze 키),
  [ADR-0352](./0352-court-auction-recrawl-is-computed-each-run-from-the-lakehouse-not-a-stored-queue.md)(다음에 물을 것은 레이크하우스에서 계산),
  [ADR-0387](./0387-the-media-in-r2-and-the-silver-index-that-names-it-are-reconciled-daily.md)(미디어 대조),
  [ADR-0032](./0032-provider-identity-is-derived-from-domain-label.md)(공급자 도메인),
  [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)·[ADR-0560](./0560-the-publisher-commits-duckdb-staged-parquet-with-iceberg-rust-fast-append-and-only-tables-whose-commit-is-an-append-move-off-spark.md)(발행기 안의 DuckDB 와 Iceberg 추가),
  [ADR-0429](./0429-a-photo-whose-collector-run-left-its-ladder-gets-it-from-the-silver-photo-index-hourly.md)(같은 저장소의 색인)

## Context

사장 승인(2026-11-22 "맞아 산업단지", 2026-10-30 미수집 원천 전부 수집): 산업단지의 산업용지 처분공고·분양공고·공급공고를 모은다.
원천 조사(2026-11-22, 읽기만, 사이트마다 요청 10 개 안팎):

| 원천 | 받는 길 | 범위 | 잰 것 |
| --- | --- | --- | --- |
| 공장설립온라인지원시스템(팩토리온, 한국산업단지공단) 분양 및 처분신청 공고(`bbs/lttotNdDspsPblancList.do`) | HTML, 한 쪽 10 건, 상세 `lttotNdDspsPblancDetail.do?selectBbsSn=`(단지명·분양주체·소재지·분양면적·가격·접수기간·사업시행자·용도), 첨부 `atchFileDownload.do` | 전국, 1,567 건(157 쪽), 가장 오래된 번호 1 | 분양주체가 가장 넓다: 공단 본사·지사(처분신청), 시도·시군, 공사, K-water, 한국농어촌공사, 민간. 코디네이터 표본(11-06~11-20) 20 건이 모두 여기 있다. **robots.txt `Disallow: /bbs/`** (§8) |
| ILIS 산업입지정보시스템 분양공고(`il/parcel`) | 목록 POST 하나(576 KB)에 전부, 상세 GET, 첨부는 고시와 같은 `cmm/file/download` | 17 개 시도, 2011-04 부터 741 건(일반 507·농공 163·국가 45·도시첨단 26) | 상세에 단지 코드(`danji_cd` = `official_complex_code`)·면적·가격·신청기간·업종·시행자, 첨부 평균 1.65 개(목록 `sk_cnt`). **par_seq 1..846 중 105 건(12 %)이 목록에서 사라졌고 그 상세는 HTTP 200 에 `code` 400·`result` null** — 826~832 같은 최근 것도 |
| 한국산업단지공단(KICOX) 임대/분양 게시판(`boardList/1003`) | HTML, 한 쪽 10 건(쪽 크기 매개변수 없음), 글 쪽에 첨부 버튼 `fnFileDownload('<번호>', '<id>')`, 받기 `api/fnFile/commonFileDownload/<번호>/<id>` | 공단 자체 단지, 962 건, 2008-07 부터 | 계양 4차·경산4·울산테크노·오송·장성나노 = ILIS 분양공고에 **0 건**. robots.txt 전부 허용 |
| 한국토지주택공사(LH) 토지 공고(data.go.kr B552555) | 공고문 조회 `lhLeaseNoticeInfo1`(유형 01 토지), 상세 `lhLeaseNoticeDtlInfo1`(`dsAhflInfo[].AHFL_URL`), 공급정보 `lhLeaseNoticeSplInfo1`(필지마다 용도·소재지·지번·면적·예정가격) | 전국 | LH 산업시설용지 공고는 ILIS 에 일부만: 남동 8차는 있고 밀양나노·포항블루밸리 2단계·사천지구·남청주현도(2026-11-13 공고)는 없다. 서비스키는 있지만(`DATA_GO_KR_SERVICE_KEY`) 세 서비스의 **활용신청이 사장 결정**이다 |

공장설립온라인을 빼면 표본 20 건 중 ILIS·KICOX·LH 가 덮는 것은 6 건(30 %)이다: 빠진 14 건은 KICOX 지사 처분신청 4·세종시·한국농어촌공사·
K-water·민간 2·남동구청·기장군·LH 입주기준·고양도시관리공사·보령시/충남개발공사다. 그 원 게시처의 robots.txt(2026-11-22): 세종시·한국농어촌공사·
고양도시관리공사는 모두 막고 보령시는 자동 요청을 거부한다; K-water·남동구·기장군·새만금개발청은 허용한다. 공장설립온라인이 있어도 셋을
둔다: ILIS 는 공장설립온라인에 없는 공고를 싣고(2026-11-11 평동3차 처분신청 매각 재공고·인천남동도시첨단 8차 공급공고 — 공장설립온라인 1~3 쪽, 11-03~11-20 에 없다), KICOX 게시판은 공장설립온라인의
분양·처분 게시판에 없는 **임대 공고**(국유토지(공장용지) 임대·스마트혁신지원센터 임대·청사 입주자 모집)를 싣고 그 분양 글은 대부분 겹친다(Silver 에서 하나로), LH 는 필지별 예정가격을
주는 유일한 구조화 원천이다. 넣지 않은 것: 시도 개발공사·시군 게시판(원 게시처; §8 의 대안), 전남 분양단지 API(한 지역, 공고 아님),
국토부 분양현황(면적 통계).

## Decision

1. **네 원천, 한 꼴.** 수집기 `collect-industrial-land-sale-notices`(원천마다 작업 하나:
   `industrial_land_sale_notice_{factoryon,ilis,kicox,lh}_collect`, 유닛 `foundation-industrial-land-sale-notice-collect@<원천>.service`; 템플릿은
   가장 긴 공장설립온라인의 4 시간을, 매시 원천은 40 분 드롭인을 갖는다, ADR-0574 §8)가
   계약 `industrial-land-sale-notice.contract.json` 하나를 읽는다. 원천의 목록을 새 것부터 쪽마다 읽고, 공고마다 상세를 받은 그대로
   두고, 첨부는 형식에 상관없이 공용 내용주소 저장소(`data-go-kr-onbid.contract.json#/media/object_prefix`)에
   `industrial_complex_notice_collect::store_attachment` 로 한 번만 담는다(ADR-0298, ADR-0484 §2 와 같은 저장소·같은 함수). 글을 꺼내지 않는다(ADR-0484 §1).
2. **Bronze.** 읽은 줄 하나 = `{notice_source, notice_id, list_fingerprint, list_row, requested_at_utc, http_status, read_outcome, error, body, attachments}`.
   제공자의 답은 `body` 에 손대지 않고 둔다: ILIS 는 상세 JSON 그대로, KICOX 는 글 쪽 HTML 을 `{"html": …}` 로 감싸고, LH 는 상세 답과
   공급정보 답 둘을 `{"detail": …, "supply": {"http_status", "answer"}}` 로 묶는다. 그 밖의 칸은 수집기가 적는다: `list_row`(ILIS·LH 는 목록
   답의 행 그대로, KICOX 는 목록 쪽에서 읽은 글 번호·제목·등록일·첨부 유무·조회수), `http_status`(ILIS·KICOX 는 답을 받으면 200 으로 적는다),
   `attachments`(첨부마다 해시·형식·크기·출처 URL·결과). `commit_every`(50) 건이 gzip JSON Lines 객체 하나이고, 키는 내용 주소다
   (`…-<run>-<batch>--sha256-<hex>.gz`, ADR-0152). 목록 답은 `list.keep_in_bronze` 가 참인 원천(ILIS·LH)만 쪽마다 내용 주소 키로 둔다:
   같은 답은 같은 객체이고, ILIS 처럼 공고를 내리는 원천에서 목록이 언제 무엇을 실었는지가 남는 유일한 기록이다(뒷날 "언제 내려갔나"를
   세는 Silver 가 읽는다; 지금 읽는 것은 없다). KICOX 목록은 쪽마다 조회수가 바뀌어 두지 않는다(글 쪽이 목록의 값을 모두 싣는다).
3. **부분·새 것만.** 다음에 물을 공고는 매 실행 레이크하우스에서 계산한다(ADR-0352 의 꼴): 목록의 공고 중
   `silver.industrial_land_sale_notice_read` 의 마지막 읽음(키마다 가장 새 실행·줄)이
   - 없거나(처음 보는 공고),
   - 다른 목록 지문이거나(제공자가 목록에서 바꾼 공고; 계약의 `fingerprint_fields` 를 바꾸면 모든 공고가 한 번 다시 읽힌다),
   - `failed`·`incomplete` 로 끝난 지 `retry_after_hours`(24) 가 지난 것.

   `read`(상세와 모든 첨부가 끝남)와 `gone`(제공자가 없다고 함: 4xx, ILIS 는 HTTP 200 의 `code` 4xx·`result` 없음)은 끝난 것이다. 늘
   실패하는 공고는 하루 한 번만 다시 묻는다: 매 실행 밀린 공고보다 먼저 물어져 연속 실패 상한(다섯 번, `gone` 은 세지 않는다)에 걸리지
   않는다. 지문은 계약이 원천마다 정한 목록 필드(ILIS 제목·공고일·구분·승인·단지·첨부 수, KICOX 제목·등록일·첨부 유무, LH 공고명·상태·게시일·
   마감일) 값을 그 순서의 JSON 배열(없는 필드는 null)로 쓴 글의 sha256 이다; 번호·조회수처럼 늘 바뀌는 값은 넣지 않는다.
   상한 `per_run_limit`(400)·`run_time_budget_seconds`(1,800 초), 요청 사이 2 초.

   **목록 걷기.** 매시 실행은 최근 쪽(`list.recent_pages`: ILIS 1 = 목록 전체, KICOX 3 = 30 건, LH 1 = 100 건)을 걷고, 그 뒤로는 빚진 것이
   없는 쪽을 만나되 **한 번이라도 물어본 공고 수**가 제공자가 밝힌 총 건수에 닿았을 때 멈춘다: 첫 채우기 동안은 깊은 쪽까지 가고, 실패를
   거듭하는 공고 하나가 걷기를 열어 두지 않는다. 하루 한 번(`full_walk_hour_utc` 17 시 UTC 에 시작한 실행) 목록 전체를 걸어 깊은 쪽에서
   바뀐 공고(LH 상태, KICOX 정정)와, 내려간 공고가 수를 부풀려 첫 채우기가 일찍 멈춘 경우의 나머지를 잡는다(KICOX 97 쪽 × 2 초 ≈ 3 분).
   매시 — 첫 채우기가 몇 시간에 끝나고, 내려가는 공고를 한 시간 안에 잡는다; 첫 채우기 뒤의 실행은 목록 쪽 몇 장이다.

   **실행 순서.** 수집은 매시 ILIS :35, KICOX :45, LH :55(꺼짐)에, 공장설립온라인은 하루 한 번 18:10 UTC 에 시작하고, Silver 적재는 수집이 Bronze 를 바꾸면(그리고 :15 마다) 돈다
   (ADR-0484 §6·§7 의 첫 실행·기대 결과 규칙이 그대로다: `per_run_cap.first_run` 은 계산값이고, 첫 실행의 저널로 `measured_run_seconds` 를
   적는 PR 이 그것을 지운다). 다음 수집은 이미 실린 읽음만 본다: 적재가 앞 실행의 마지막 묶음을 싣기 전에 수집이 시작하면 그 묶음의
   공고를 한 번 더 묻는다(같은 답은 같은 첨부 해시라 저장소에는 아무것도 더해지지 않는다).
4. **Silver 는 수집의 장부 둘만.** `load-industrial-land-sale-notice-silver`(작업 `industrial_land_sale_notice_silver`, 발행기 안의 DuckDB 와
   Iceberg 추가 커밋, ADR-0559·0560)가 네 원천의 상세 객체를 객체마다 붙인다: 먼저 저장소에 담긴 첨부를
   `silver.industrial_land_sale_notice_attachment`((원천, 공고, 해시)마다 한 줄; 새것·바뀐 것만), 그다음 읽음을
   `silver.industrial_land_sale_notice_read`(읽기마다 한 줄, 실패 포함). 두 표는 첫 커밋이 만든다. 같은 공고가 둘 이상의 원천에 있으면
   (공장설립온라인과 KICOX·ILIS·LH) 줄은 원천마다 따로 있고 같은 첨부 바이트는 저장소에 한 번이다; 공고를 하나로 묶는 것은 뜻을 푸는 Silver 의 일이다. 공고의 뜻(면적·가격·기간·필지)을 푸는
   Silver 는 이 ADR 이 아니다 — Bronze 가 답을 그대로 갖고 있어 나중에 다시 만든다.
5. **첨부 색인은 대조기에.** 첨부 색인은 미디어 대조기의 `media.reconcile.index_tables` 다섯째다(`media_index_delta` 의 읽는 이도).
   막는 사고: 색인 없이 같은 `sha256/` 에 첨부만 쌓이면 48 시간 뒤부터 그 수천 개가 매일 고아로 세어져 `FoundationMediaObjectOrphaned` 가
   울린다(ADR-0429 의 경매 사진, ADR-0484 §5 와 같은 일). 표 집합이 바뀌므로 대조기는 다음 실행에 색인을 한 번 통째로 다시 읽는다(설계된 동작).
6. **LH 는 꺼진 채로 들어온다.** 서비스키(`DATA_GO_KR_SERVICE_KEY`)가 없으면 스크립트가 아무것도 하기 전에 78(EX_CONFIG)로 끝낸다.
   게이트웨이가 서비스를 거부하면(`refusal_codes`: 활용신청 전) 목록이든 상세·공급정보든 수집기는 그 읽음을 쓰지 않고
   `industrial-land-sale-notice-refused` 를 찍어 끝나고, 스크립트가 그 줄을 보고 78 로 끝낸다(목록만 승인되고 상세가 거부되는 반쯤의 승인도
   같다; 이미 받은 목록 답은 Bronze 에 남는다). 작업은 `enabled: false` 이고 `disabled_reason` 에 사장이 할 활용신청 셋(15058530·15057999·
   15056765)을 적는다. 첨부 URL 은 `apply.lh.or.kr` 의 https 만 묻는다: 응답이 다른 호스트를 가리켜도 적기만 한다. 그 URL(`/lhapply/lhFile.do`)은
   청약센터 robots.txt 가 크롤러에게 막는 경로다 — 공식 OpenAPI 가 공고의 첨부로 내주는 주소이고 그 이용허락범위가 "제한 없음"이라 받는 것으로
   정했지만, LH 를 켜는 결정(활용신청)과 함께 사장이 확인한다.
7. **공급자.** `kicox.or.kr` 을 승인 도메인에 더한다(ADR-0032, `kicoxorkr__…`). LH 는 data.go.kr 공급자이고 세 연산의 dataset 은
   `lh_land_notice_dataset_slug` 지도가 정한다(`source_slug_catalog_parity`). HTML 은 `scraper`(Servo 의 html5ever·selectors) 로 찾는다 —
   글 번호와 첨부 번호는 쪽이 스스로 부르는 `onclick` 호출에서만 읽는다.
8. **공장설립온라인은 사장이 알고 결정했다.** 사장은 2026-11-22 공장설립온라인의 robots.txt 가 `Disallow: /bbs/`(`/srch/`·`/common/popup/` 도)라는
   것을 듣고 두 번 수집을 결정했다: 가장 넓고 새로운 원천이고 처분신청 공고까지 싣는다. 부담은 가장 작게 한다(계약 `sources.factoryon`):
   일반 User-Agent(`Mozilla/5.0 (compatible; data-collector/1.0)`, 사람의 정보 없음), 요청 사이 7 초(`request_spacing_ms`, 5~10 초 안),
   전체 이력은 한 번만 여러 실행에 나눠(하루 한 번 18:10 UTC, 실행 상한 `run_time_budget_seconds` 12,600 초·`per_run_limit` 800, 유닛 240 분:
   1,567 건이면 두세 실행), 그 뒤 매일 목록 1 쪽과 새 공고의 쪽·첨부만(`full_walk: false` — 하루 한 번 전체 걷기를 하지 않는다). 4xx·429, 또는
   목록·공고가 아닌 쪽(차단 쪽)을 받으면 그 자리에서 멈춘다(`Blocked`): 읽은 묶음을 커밋하고 실행은 실패로 끝나 알리며(`OnFailure`), 그 실행
   안에서 다시 묻지 않는다. 대안은 원 게시처의 게시판이다(위 robots 조사; 허용하는 곳만, 빠진 몫을 잰 뒤).

### 대안

| 대안 | 왜 아닌가 |
| --- | --- |
| ILIS 분양공고 하나만 | 표본으로 KICOX 자체 단지 5 개·LH 산업용지 4 건이 빠진다 |
| ILIS 분양공고를 고시 수집기(ADR-0484)의 둘째 목록으로 | 고시는 한 번 내면 고치지 않아 끝난 고시를 다시 묻지 않지만, 분양공고는 목록에서 바뀐다(승인·첨부 수); 원천 셋이 같은 꼴을 써야 한다 |
| 매시 최근 쪽만, 깊은 쪽은 영영 | 깊은 쪽의 LH 상태·KICOX 정정을 놓친다; 하루 한 번 전체 걷기(KICOX ~3 분)로 잡는다 |
| 실패한 공고를 매 실행 다시 | 늘 실패하는 공고가 밀린 공고보다 먼저 물어져 연속 실패 상한에 걸린다 |
| 공장설립온라인 없이 원 게시처만 | 게시처가 수십 곳이고 그중 넷은 robots.txt 가 막거나 자동 요청을 거부한다; 사장 결정(§8)으로 공장설립온라인을 받고 원 게시처는 대안으로 둔다 |
| KICOX 게시판을 빼기(분양 글은 공장설립온라인에 있다) | 공장설립온라인의 분양·처분 게시판에 없는 임대 공고(국유토지·지원센터)를 싣는다; 매시 목록 쪽 셋이라 부담이 작다 |
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
  (KICOX 쪽 HTML ~170 KB 를 gzip 으로). R2 저장 ≈ 월 $0.06, PUT ≈ 2,700 + 35 ≈ $0.01 한 번. LH 는 켜질 때까지 0(양을 모른다: 답을 못 봤다).
- **제공자 부담(계산)**: 매시 ILIS 목록 1 요청(576 KB), KICOX 목록 3 요청(쪽마다 ~146 KB), 하루 한 번 KICOX 전체 97 요청(~14 MB); 새 공고마다
  상세 1 + 첨부 몇 개. 공장설립온라인: 첫 채우기 ≈ 157 목록 + 1,567 상세 + ~2,300 첨부 ≈ 4,000 요청을 7 초 간격으로 두세 날에, 그 뒤 하루 목록 1
  요청과 새 공고(하루 몇 건)의 상세·첨부.
- **새 의존성**: HTML 은 `scraper` 0.29.0(ISC; Servo 의 `html5ever`·`selectors`, 명령줄 기능은 끔)으로 찾는다 — 저장소에 HTML 해석기가 없었다.
- **첫 바퀴(계산)**: 공고 하나 ≈ (상세 1 + 첨부 1.5~1.65) × 2 초 ≈ 5 초 → 실행 하나에 ~340 건 → ILIS ~3 실행, KICOX ~3 실행(매시: 몇 시간).
- **검증하지 못한 것**: 운영 실행 시간·메모리(첫 실행의 저널), KICOX 의 옛 쪽(2008~)의 HTML 모양이 같은지, LH 의 답(활용신청 전이라
  한 번도 못 봤다 — 가이드의 예제로만 짰다), 게시일 전 기간을 한 번에 묻는 LH 목록이 받아들여지는지.
- **남은 일**: (1) 첫 실행의 저널로 `measured_run_seconds` 를 적고 `first_run` 을 지우는 PR; (2) 사장의 LH 활용신청 뒤 LH 작업을 켜는 PR(첫
  답을 인용); (3) 대안: 공장설립온라인이 막을 때를 위해 원 게시처(K-water·남동구·기장군·새만금개발청 등 robots 가 허용하는 곳, KICOX 지사 처분신청
  게시처는 아직 못 찾음)를 계속 조사해, 빠지는 몫을 잰 뒤 더한다; (4) 공고의 뜻을 푸는 Silver(필지·면적·가격·기간)와 intelligence 의 첨부 읽기(ADR-0484 남은 일과 같은 길).

개정(2026-11-23, W99): **원천은 셋이고, 수집은 끝까지 간다.** 사장이 첫 채우기(2026-11-22 18:52Z 부터 ai-server)를 보고 정한 둘을
적는다. 본문의 §3 상한, §6 의 "사장이 할 활용신청", §8 의 요청 간격·실행 상한, Consequences 의 "제공자 부담"·"첫 바퀴"·"남은 일 (2)" 을
이것이 대신한다.

1. **원천.** 공장설립온라인·ILIS·KICOX 게시판 셋이 돈다. LH 는 지금 하지 않는다("팩토리온만 해 일단"): 작업은 꺼진 채이고 활용신청을
   요청하지 않으며 LH 의 겹침도 재지 않는다. 같은 공고가 여러 원천에 있으면 Bronze·읽은 기록은 원천마다 그대로 두고(같은 첨부 바이트는
   내용 주소라 저장소에 한 번), 전환 뒤의 Silver 가 (단지, 제목, 공고일, 첨부 해시들)마다 공고 하나로 묶는다: 공장설립온라인 것이 먼저이고
   다른 원천이 빈 필드를 채우며, 출처 목록을 남긴다.
2. **간격과 상한.** 요청 사이 간격을 뺀다 — 공장설립온라인의 7 초도, ILIS·KICOX 의 2 초도(사장의 말: "7 초 같은 간격 없이",
   계약의 `request_spacing_ms` 0): 앞 답을 받으면 다음 요청을 보내고, 한 번에 하나만 보낸다(동시 요청은 없다). 4xx·429·차단 쪽이면 그 자리에서
   멈추는 §8 의 규칙은 그대로다. 세 수집기의 실행마다 공고 수 상한(`per_run_limit`)과 시간 예산(`run_time_budget_seconds`)을 없앤다: 한 실행이 새 것을 다 모을
   때까지 간다. 그 대신 멈춤 지킴이 하나(`stall_minutes` 15): 그 분 동안 목록 쪽 답도, 끝까지 읽힌(`failed` 아닌) 공고도 없으면 읽은 묶음을
   커밋하고 실패로 끝나 알린다(`Stalled`, `OnFailure`); 공고 하나의 읽기도 남은 창 안에서 끝나야 한다(요청 하나는 클라이언트가 120 초·ILIS
   3 분으로 끊는다). 유닛의 시간 한계는 일의 상한이 아니라 잰 전체 이력에서 나온다(작업 정의 `per_run_cap` 은 `unit: run`, `first_run` 이 계산):

   | 원천 | 잰 첫 채우기(2026-11-22) | 전체 이력(한 실행) | 한계 |
   | --- | --- | --- | --- |
   | 공장설립온라인 | 508 건·첨부 980 개(1.54 GB), 1,539 요청 12,629 초 중 10,766 초가 7 초 기다림 → 요청당 약 1.21 초 | 1,067 건 ≈ 3,233 요청 ≈ 3,900 초 | 120 분 |
   | ILIS | 741 건 중 290 건·첨부 413 개(1.32 GB), 704 요청 1,819 초 중 1,406 초가 2 초 기다림 → 요청당 약 0.59 초 | ≈ 1,797 요청 ≈ 1,100 초 | 40 분 |
   | KICOX | 962 건 전부·첨부 2,088 개(4.53 GB), 다섯 실행 8,794 초·3,335 요청 중 6,668 초가 2 초 기다림 | ≈ 2,150 초 | 60 분 |

   첫 채우기 뒤의 실행은 목록 쪽 몇 장과 새 공고(몇 초~몇 분)다. 매시 작업이 한 시간보다 오래 돌면 다음 시작은 그 실행이 끝날 때까지
   기다리고, 그동안 밀린 시작은 하나로 접힌다(Airflow `max_active_runs` 1, Dagster `decide.py`).
3. **첨부는 통째로 메모리에 두지 않는다.** 잰 최대(공장설립온라인 977.6 MB·스왑 383.9 MB, 1 GB 상한)는 실행 길이가 아니라 가장 큰 첨부
   하나다: 받은 파일 전체와 그 사본 둘을 함께 들었다. 첨부는 받는 대로 디스크(유닛의 `/tmp`)에 쓰며 조각마다 해시하고, 그 파일을
   스트리밍 put 으로 보낸다(큰 파일은 조각으로): 메모리는 파일 크기와 상관없이 조각 하나다. 내용 주소 키(`sha256/<hex>`)는 끝에야 아는
   해시라 디스크를 거친다 — 그래서 이미 있는 키는 HEAD 로 알고 보내지 않는다(객체에 `x-amz-meta-sha256` 을 찍는다). HWP 5 의 서명은
   파일 어디에나 있을 수 있어 받는 동안 지켜본다. `max_file_bytes` 는 이제 그 임시 파일의 디스크 상한(2 GiB)이다. ILIS 고시 수집기
   (ADR-0484)도 같은 저장 함수를 쓴다. 상한이 커지면 그 전에 파일을 담지 못한 읽기(읽은 기록의 `file_count` > `files_stored`)는 끝난
   것이 아니다: 계약의 `max_file_bytes_since` 보다 앞선 그런 읽기는 다음 실행이 한 번 다시 묻는다 — 2026-11-22 공장설립온라인이 옛 256 MiB
   를 넘어 담지 못한 두 파일이 그렇게 다시 받아진다(그날 ILIS·KICOX 는 모든 파일을 담았다).
