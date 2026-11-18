# ADR 0575: 법원경매는 매각물건명세서 PDF 를 판마다 한 번, 문건·송달을 바뀐 사건마다 받는다 — 사이트의 남은 자료 화면도 모두 받는다

- Status: Accepted
- Date: 2026-11-19
- Supersedes: [ADR-0540](./0540-court-auction-loads-every-document-of-a-case-and-keeps-personal-names-in-bronze.md)
  §1 의 "매각물건명세서 PDF 와 문건·송달은 받지 않는다" 와 Rejected 의 "매각물건명세서 PDF 자동 수집" 만
- Builds on: ADR-0540(문서마다 Silver 표·이름은 Bronze 에만), ADR-0290(바뀐 것과 기일로 다시 받기·내용 해시),
  ADR-0298(내용 주소 저장은 한 번 쓰기), ADR-0352(받을 목록은 매 실행 계산), ADR-0286(행 버전), ADR-0314(부분 처리가 기본),
  ADR-0566(수집 실행은 실행 예산이 끝낸다)

## Context

사장 지시(2026-11-18): "모든 걸 수집해라 — 매각물건명세서 PDF 와 문건·송달까지. 명세서는 물건마다 한 번, 명세서가 바뀐 때만
다시. 문건·송달은 사건이 바뀐 때만 다시. 이름은 Bronze 에만, 화면·API 에는 절대 없다. 사이트 전체를 훑어 남은 자료 화면을
모두 적고, 물건·매각 자료를 담은 것은 모두 받아라."

ADR-0540 §1 은 두 문서를 받지 않았다. 명세서 PDF 는 부를 때마다 법원 쪽에 열람 기록이 남고 개인정보 워터마크가 붙으며, 그 내용
(인수되는 권리·지상권·비고·최선순위 설정)은 물건상세가 칸으로 주기 때문이었다. 문건·송달은 당사자 서술이 대부분이라는 이유였다.
두 이유 모두 "받지 않는다" 가 아니라 "한 번만 받는다"·"이름을 뺀 칸만 싣는다" 로 풀린다.

### 실호출(2026-11-19, 진행 물건 하나·사건 하나, 각 한 번)

| 단계 | 경로 | 답 |
|---|---|---|
| 물건상세 | `/pgj/pgj15B/selectAuctnCsSrchRslt.on` | `dspslGdsDxdyInfo` 에 `dspslGdsSpcfcEcdocId`(명세서 전자문서 ID), `gdsSpcfcWrtYmd`(명세서 작성일), `orvParam`. Silver `court_auction_item_spec.spec_document_id`·`spec_written_date` 로 이미 싣는다 |
| 열람 기록 | `/pgj/pgj15B/insertDspslGdsSpecArtcWdrwInf.on`(`dma_dspslGdsSpecLog`: 법원·사건·물건·`orvParam`·문서 ID·`cortAuctnMbrsId` NONUSER·`docFlag` 1) | HTTP 200, `dma_dspslSpcfcInfo`: 뷰어 주소(`ecfs.scourt.go.kr/sgvo/…/SGVO201M01.xml`)와 `encParam`. 이 호출이 법원 쪽 열람 기록이다 |
| 문서 정보 | `ecfs.scourt.go.kr/sgvo/sgvomain/selectDocVwrInf.on`(`dma_parm`: `encParam` 받은 그대로·`sidParam` NA) | HTTP 200, 2,046 바이트. `dlt_rcrd`(문서 ID·순번·문서명 "매각물건명세서(재작성,1)"·파일명·작성 일시·제출자 이름), `dma_csInf`(사건·법원·담당계·대표 당사자 이름) |
| PDF | `ecfs.scourt.go.kr/sgvo/sgvomain/downloadOtpt.on`(`dma_downloadOtpt`: 사건번호·법원명·문서명·`ecdocLst`[법원·문서 ID·순번]·`wmYn` Y·`wmTxt`·`purposeCd`·`extnlUserYn` Y) | HTTP 200 `application/x-octetstream`, `%PDF-1.4`, 191,460 바이트, 2 쪽, 1.06 초. **캡차 없음**, 로그인 없음. 브라우저 흉내 없는 클라이언트·우리 이름(UA)으로도 같은 답(쿠키는 `WMONID` 하나를 서버가 준다) |
| 문건·송달 | `/pgj/pgj15A/selectDlvrOfdocDtsDtl.on`(`dma_srchDlvrOfdocDts`: 법원·사건) | `dlt_ofdocDtsLst` 17 행(접수일 `ofdocRcptYmd`·서술 `rcptDts`·결과 `rcptRslt`), `dlt_dlvrDtsLst` 26 행(등록일 `dlvrbkRegYmd`·도달 `lastDlvrblRchYmd` "2026.01.30 도달"·서술 `dlvrDts`) |

- 명세서 버튼은 사이트 화면이 정한 때에만 보인다(`PGJ15BM01.xml`): 실물(`realMulKind` 1)이고 `orvParam` 이 있으며, 기일입찰
  (`bidDvsCd` 000331)은 매각기일 7 일 전부터, 기간입찰(000332)은 입찰 시작 7 일 전부터. 그 전에는 명세서가 공개되지 않는다.
- 워터마크 글(`wmTxt`)은 부르는 쪽이 보낸다. 사이트는 "개인정보유출주의 제출자:<이름>, 제출일시:…, 다운로드일시:…" 를 보내고
  전자소송 사건은 제출자 없이 "개인정보유출주의 제출일시:…" 를 보낸다. 우리는 이름도 받는 시각도 넣지 않은 뒤쪽 꼴을 보낸다.
- PDF 본문에는 이름이 있다(담임법관·사법보좌관, 점유자·임차인). 문건·송달 서술의 이름은 사이트가 이미 가린다("김OO",
  "국OOOOOOO OOOOOO") — 가렸어도 성·글자 수가 남으므로 이름 자리로 다룬다.

## Decision

1. **매각물건명세서 PDF 는 판마다 한 번.** 판의 키는 (법원, 사건, 물건, `dspslGdsSpcfcEcdocId`)다. 받을 목록은 매 실행 계산한다
   (ADR-0352): 이번 실행에 읽은 물건상세 가운데 사이트 화면이 명세서를 여는 조건(위 Context)을 만족하고, 그 판이
   `silver.court_auction_spec_pdf_read` 에 없는 것. 문서 ID 가 그대로면 다시 묻지 않는다 — 명세서가 고쳐지면(재작성) 문서 ID 나
   작성일이 바뀌고 그것이 바뀜 신호다. 그래서 법원 쪽 열람 기록은 판마다 한 번만 생긴다.
2. **세 호출, 이름 없는 워터마크.** 열람 기록 → 문서 정보 → PDF 를 같은 세션으로 차례로 묻는다. 워터마크 글은 "개인정보유출주의
   제출일시: <작성 시각>" 만 보낸다(이름·다운로드 시각 없음). 판은 문서 ID 가 가르고, 내용 해시는 같은 바이트를 두 번 쓰지 않게 할 뿐이다(같은 판을 같은 워터마크로 두 번 받으면 바이트가 다르다: 191,460 과 191,461 바이트, 해시 다름 — 실측 2026-11-19. 그래서 판가름은 해시가 아니라 문서 ID 다). 캡차·로그인 요구가 오면 그 판은
   건너뛰고 읽음 행에 그 결과를 적으며, 다음 실행이 다시 묻는다. 캡차는 풀거나 우회하지 않는다.
3. **PDF 는 비공개 Bronze 에 내용 주소로.** PDF 에 이름이 있으므로 필지 화면이 내주는 공용 미디어 저장소(ADR-0298 의 `onbid/media`)가
   아니라 수집기의 비공개 Bronze 버킷에 `sha256/<hex>.pdf` 로 한 번 쓴다(같은 해시는 다시 쓰지 않는다). Silver 읽음 표는 키·문서 ID·
   문서명·작성 일시·해시·바이트 수·결과만 싣고 이름 칸(제출자·대표 당사자)은 없다. 명세서의 내용 칸은 지금처럼 물건상세에서 온다.
4. **문건·송달은 바뀐 사건마다.** 사건 문서(`case_documents`)의 네 번째 문서로, 다른 사건 문서와 같은 frontier 를 탄다(ADR-0352,
   ADR-0290): 이번 실행에 물건 목록에 있는 사건 가운데 한 번도 읽지 않았거나, 목록 감시 칸(기일·상태·최저가 등)이 마지막 읽음 뒤로
   바뀌었거나(새 기일·결과), 기일 전후라 때가 된 사건. 사건마다 한 번 묻는다. 그 밖에는 다시 묻지 않는다.
5. **문건·송달 Silver 는 이름 없는 칸만, 사건마다 판.**

   | 표 | 키 | 행 버전 | 칸 |
   |---|---|---|---|
   | `silver.court_auction_case_filing` | 법원·사건·순번 | 사건마다 판 | 접수일, 제출자 자격(이름 앞 낱말: 채권자·채무자겸소유자·감정인·등기소·이해관계인·기타 …), 문건 종류(이름 뒤 낱말: 보정서·감정평가서 …), 처리(제출 등 끝 낱말), 결과 |
   | `silver.court_auction_case_delivery` | 법원·사건·순번 | 사건마다 판 | 송달 등록일, 도달일, 송달 결과(도달·폐문부재·송달간주 …), 받는 사람 자격, 송달 문서 종류, 처리(발송 등) |
   
   읽은 기록은 다른 사건 문서처럼 `silver.court_auction_case_document_read` 에 한 줄씩 쌓는다. 서술을 나누는 규칙은 하나다: 가림 글자(O)가 든 낱말들이 이름 자리이고, 그 앞이 자격, 그 뒤가 종류다. 이름 자리는 Silver 에
   싣지 않는다. 가림이 없는 서술은 처리 낱말만 싣는다: 가린 낱말이 없으면 이름과 종류를 가를 수 없다. 원문은 Bronze 에만 있다. Gold·API·화면에는 이름도 가린 이름도 없다.
6. **남은 자료 화면은 모두 적고, 물건·매각 자료는 모두 받는다.** 아래 조사표가 정본이다. "받음" 이 아닌 줄 가운데 물건·매각 자료를
   담은 화면은 이어지는 PR 이 수집기·Silver 표를 더한다. 담지 않는 화면(용어·양식·FAQ·마이페이지)은 받지 않고 그 이유를 표에 적는다.
7. **감정평가서 캡차는 센다.** 감정평가서 PDF(ADR-0540 사슬 ⑥)가 캡차로 건너뛴 수는 읽음 표의 결과로 남으므로 Trino 로 센다.
   캡차 없는 길은 위 명세서처럼 사이트가 화면에서 쓰는 호출 그대로다(⑥ 이 이미 캡차 없는 뷰어 → `display?key=` 길이다).
8. **SSOT.** 경로·요청 모양·화면 조건·워터마크 꼴·버킷 접두는 `courtauction-go-kr.contract.json` 하나, 표 모양은 Rust 표 계약
   (`lakehouse-domain::court_auction`)이 정본이다.

### 사이트 조사표(2026-11-19)

2026-11-19 에 사이트 화면 120 개에서 자료 화면 45 개를 각 한 번씩 불렀다(148 요청, 2 초 간격, 읽기 경로만, 캡차 0). "받음" 은 수집기가
매 실행 묻는 것, "다음 PR" 은 §6 에 따라 이어지는 PR 이 더하는 것이다. 화면 열은 필지 경매 칸에 보이는지다.

| 화면 | 경로 | 키 | 담긴 것(실측) | 받음 | Silver 표 | 화면 |
|---|---|---|---|---|---|---|
| 물건 검색 | `pgjsearch/searchControllerMain` | 법원·사건·물건 | 목록 칸 | 받음 | `court_auction_item` | 보임 |
| 매각결과 검색 | `pgjsearch/selectDspslSchdRsltSrch` | 법원·사건·물건 | 매각가. 서버가 기간 칸을 받지만 1~2 달 뒤 빠진다(B000210: 11 월 456·10 월 159·8 월 17·2025 년 1 행) | 받음(매일 쌓여 과거가 남는다) | `court_auction_sale_result` | 보임 |
| 물건상세 | `pgj15B/selectAuctnCsSrchRslt` | 법원·사건·물건 | 사건 기본·명세서 요약·기일내역·목록내역·제시외건물·감정요항표·사진 | 받음 | `court_auction_item_detail` 외 6 표·사진 | #1415 배포 뒤 |
| 공통코드 | `scframe/lib/sccd/list.on` | 묶음·코드 | 코드 이름 | 받음 | `court_auction_code` | 이름으로 |
| 사건상세 | `pgj15A/selectAuctnCsSrchRslt` | 법원·사건 | 관련사건·목록 종국결과·당사자 구분 | 받음(#1340) | `court_auction_case`·`_case_related`·`_case_object`·`_case_party` | #1340 배포 뒤 |
| 현황조사(목록) | `pgj15B/selectCurstExmnRletIndctLst` | 법원·사건·회차 | 목록별 점유관계·임차인 수 | 받음(#1340) | `court_auction_survey`·`_survey_object` | #1340 배포 뒤 |
| 감정평가 | `pgj15B/selectAeeWevlInfo` | 법원·사건·회차 | 평가번호·가격시점·작성일 | 받음(#1340) | `court_auction_appraisal` | #1340 배포 뒤 |
| 감정평가서 PDF | `ca.kapanet.or.kr/view/…` | 사건·회차·평가번호·작성일 | PDF | 받음(#1341) | `court_auction_appraisal_report(_read)` | 링크(#1341) |
| 매각물건명세서 PDF | `insertDspslGdsSpecArtcWdrwInf` → `ecfs` | 물건·문서 ID | PDF(이름 있음) | 받음(#1454) | `court_auction_sale_specification_read` | 없음(이름) |
| 문건·송달 | `pgj15A/selectDlvrOfdocDtsDtl` | 법원·사건 | 접수 문건·송달(이름은 사이트가 가림) | 다음 PR(§4·§5) | `court_auction_case_filing`·`_case_delivery` | 자격·종류·날짜만 |
| 사건 기일내역 | `pgj15A/selectCsDtlDxdyDts` | 법원·사건 | 물건별 기일·장소·결과·최저가 | 받지 않음: 물건상세 `gdsDspslDxdyLst` 와 같은 칸 | (`court_auction_item_schedule`) | 보임 |
| 현황조사서 | `pgj15B/selectCurstExmndc` | 법원·사건·회차·목록 | 점유·임차인(전입일·보증금)·비고. 점유자 이름 `intrpsNm` | 다음 PR | `court_auction_survey_tenant`(이름 칸 없음) | 이름 빼고 |
| 매각공고 목록·상세 | `pgj143/selectRletDspslPbanc(Dtl)` | 법원·담당계·매각기일 | 기일마다 물건·최저가·감정가·비고, **정정·취소 목록**. 오늘 이후만 답한다 | 다음 PR(법원마다 매일) | `court_auction_sale_notice`·`_sale_notice_item` | 정정·취소 |
| 배당요구종기 목록·물건 | `pgj142/searchDstrtDemnLstprdPbanc`·`selectDstrtDemnLst` | 법원·담당계·사건 | 개시일·공고일·종기일·대표지번, 물건 주소·**등기 고유번호(14 자리)**. 채권자·채무자·소유자 **이름이 가려지지 않은 채** 온다 | 다음 PR(이름은 Bronze 만) | `court_auction_dividend_notice`·`_dividend_notice_object` | 날짜·등기번호 |
| 최고공고 목록·부동산표시·공고문 | `pgj145/selectHistPbanc*` | 법원·사건·공고순번 | 구조만 확인(10 조합에서 행 0) | 다음 PR(행이 있을 때) | `court_auction_hist_notice` | 미정 |
| 기일별 검색(부동산) | `pgj153/selectDxdyRletSrchRslt` | 법원·담당계·날짜 | 매각기일·법정·시각 | 다음 PR(법원마다, 값쌈) | `court_auction_sale_day` | 없음 |
| 동산 공고·상세·물건·기일 | `pgj144/*`·`pgj15B/selecctMvprpGdsDtlInf`·`pgj153/selectDxdyMvprpSrchRslt` | 집행관사무소(`O…`)·사건 | 동산 매각 공고·물건·감정가·최저가. 업체 이름 | 다음 PR | `court_auction_movable_*` | 없음(필지 아님) |
| 매각통계(연도·법원·지역·용도·그래프·메인) | `pgj161`~`164`·`pgj111/selectRletYrDspslStats` | 법원·지역·용도·기간 | 접수·매각·건수·금액·매각률. 2000 년부터, 매달 갱신 | 다음 PR(매달) | `court_auction_sale_stats` | 미정 |
| 인근매각통계 | `pgj15B/selectAuctnTongSrchRslt` | 물건 | 3·6·12 달 인근 매각 통계 | 다음 PR(물건상세와 같은 때) | `court_auction_nearby_stats` | 미정 |
| 지도 반경 검색 | `pgj152/selectGdsInfAroundGdsLst` | 물건·반경 | 1~3 km 안 진행 28·**9 달 매각 110(매각가 포함)** | 다음 PR(매각결과 과거 9 달 채우기) | (`court_auction_sale_result`) | 보임 |
| 인근 매각·진행·지도 검색·마이핀 | `pgjsearch/selectAround*`·`pgj152/*` | 물건·지역 | 같은 사이트 물건의 다른 묶음 | 받지 않음: 물건 검색·매각결과의 부분집합 | — | — |
| 사진 팝업 | `pgj15B/selectPicInf` | 사건·사진 | 물건상세와 같은 사진 + 설명·출처 | 받지 않음: 물건상세가 같은 사진·제목·구분을 준다 | (`court_auction_item_photo`) | 보임 |
| 물건 비교·목록 팝업 | `selectGdsCmprsInf`·`selectPopUpGdsLstDts` | 물건 | 물건상세 요약과 같음 | 받지 않음: 중복 | — | — |
| 공지·팝업 공지·집행기관·용어·양식·FAQ·경매비용·조회 코드 | `pgj141`·`pgj111`·`pgj182`·`pgj172`·`pgj173`·`pgj184`·`pgj176`·`pgjComm` | — | 안내·서식·주소·계산기·법원·행정동 코드 | 받지 않음: 물건·매각 자료가 아니다 | — | — |
| 마이페이지(관심·일정·핀) | `pgj19x` | 회원 | 로그인 회원 자료 | 받지 않음: 회원 전용·쓰기 | — | — |

## Consequences

- 법원 쪽 열람 기록: 명세서 판마다 한 번. 명세서가 공개되는 물건(매각기일 7 일 안)만 묻는다.
- 저장(계산): 명세서 한 판 약 0.19 MB(실측 1 건). 진행 물건 3.7 만 건이 한 해에 판 두 개씩이어도 약 14 GB, R2 월 약 0.2 달러.
- 문건·송달은 사건당 요청 하나(실측 수십 행, 수 KB). 기일이 바뀐 사건만 다시 묻는다.
- 이어지는 PR 사슬: ① 이 ADR → ② 명세서 PDF 수집기·읽음 Silver(#1454) → ③ 문건·송달 → ④ 현황조사서·배당요구종기(이름은
  Bronze 만) → ⑤ 매각공고·기일별·최고공고 → ⑥ 매각통계·인근매각통계 → ⑦ 지도 반경으로 매각결과 과거 9 달 → ⑧ 동산.
- 배당요구종기 목록은 사람 이름을 가리지 않고 준다. 그 칸은 Silver 칸이 없고 Bronze(비공개)에만 있다.

## Rejected

- **명세서 PDF 를 공용 미디어 저장소에:** 필지 화면이 해시만 알면 내주는 곳이라 이름이 든 문서를 둘 수 없다.
- **명세서를 매 실행 다시 받기:** 실행마다 법원 쪽 열람 기록이 쌓인다. 문서 ID 가 판을 가른다.
- **사이트가 보내는 워터마크(제출자 이름·다운로드 시각) 그대로:** 이름이 우리 쪽 글로 다시 들어가고, 받는 시각이 바이트마다 박힌다.
- **문건·송달 서술을 통째로 Silver 에(가린 채):** 가린 이름도 성·글자 수가 남는다. 자격·종류만으로 화면이 쓸 칸이 된다.
