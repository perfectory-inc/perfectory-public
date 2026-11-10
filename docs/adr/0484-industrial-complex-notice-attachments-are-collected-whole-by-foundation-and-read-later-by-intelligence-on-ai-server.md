# ADR 0484: 산업단지 고시의 첨부는 foundation 이 형식에 상관없이 전부 모으고, 읽기·추출은 intelligence platform 이 ai-server 에서 나중에 한다

- Status: Accepted
- Date: 2026-11-09
- Builds on: [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md)(내용주소 저장소),
  [ADR-0429](./0429-a-photo-whose-collector-run-left-its-ladder-gets-it-from-the-silver-photo-index-hourly.md) §1(저장 하나),
  [ADR-0352](./0352-court-auction-recrawl-is-computed-each-run-from-the-lakehouse-not-a-stored-queue.md)(다음에 물을 것은 레이크하우스에서 계산),
  [ADR-0457](./0457-a-job-never-run-takes-its-first-run-unattended-bounded-by-its-unit-and-the-run-is-the-measurement.md)(첫 run 은 사람 없이, 유닛의 상한 안에서),
  [ADR-0480](./0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md)(정의가 기대하는 결과),
  ITP-ADR-0002(RAG 는 Foundation 이 낸 것을 읽는다)

## Context

사장 지시(2026-11-09): 산업입지정보시스템(ILIS) 산업단지 고시의 첨부(PDF·HWP)를 AI 가 읽어 사실을 뽑아 보이게 하되,
"고시문 읽어서 뽑는 건 인텔리전스 플랫폼이 되어야 하는 거 아니야? 일단 수집만 전부 하는 거, pdf hwp 든 모든 걸.
클로드로 뽑는 게 아니라 ai-server 에서 하는 거잖아."

실측(2026-11-09, 읽기만):

| 무엇 | 결과 |
| --- | --- |
| 운영 원장(`catalog.bronze_object`)의 ILIS 객체 | **0** — 목록·고시 수집 명령(`collect-industrial-complex-address-source`)은 있지만 작업 등록부에 없고 운영에서 돈 적이 없다 |
| `catalog.complex_notice`·`notice_attachment` | 0 행(표만 있다, `serving-complex-notice` = schema_defined) |
| 고시 목록 `POST il/danref/list.do` | **18,793 건**, 13.5 MB, 요청 하나; 목록 행의 `files`·`saveFiles` 는 null |
| 고시 상세 `GET il/danref/det.do/<ref_seq>` | `result.saveFiles[]` 에 첨부(`file_path`·`real_file_name`·`file_extension`·`file_size`·`use_yn`) |
| 첨부 `GET cmm/file/download?real_file_name=..&file_path=..` | 바이트 그대로(`application/octet-stream`), 바이트 수 = 제공자 `file_size` |
| 무작위 표본 60 건(2006–2026 고시) | 첨부 1.05 개/고시, **PDF 94 %·HWP 6 %**, 평균 1.25 MB/고시; HWP 는 모두 OLE 복합 파일 + `HWP Document File` 머리(HWP 5) |
| 저장소에 이미 있는 것 | 공용 내용주소 저장소 `onbid/media/sha256/`(공매·경매 사진, 사진 형식만 판정), 미디어 대조기(색인 표마다) |
| intelligence platform | knowledge·normalization(한국어 형태소)·RAG ADR(ITP-0002~0006) — 문서(PDF·HWP) 읽기는 아직 없다 |

## Decision

1. **경계.** foundation 은 **모으기만** 한다: 고시 상세와 그 첨부 전부를 Bronze 와 공용 저장소에 쌓고 Silver 색인을
   붙인다. 글 꺼내기(PDF 텍스트·OCR·HWP), 구조화 추출, 단지 패널 사실은 **intelligence platform** 의 일이고,
   ai-server 의 **로컬 모델**로 한다. foundation 에 Claude API 를 부르는 코드, 텍스트·OCR 코드를 두지 않는다. 문서
   파이프라인은 하나다: intelligence 는 이 색인(§4)으로 저장소의 바이트를 찾아 읽고(ITP-ADR-0002 처럼 Foundation 이
   낸 것을 읽는 쪽), 문서 sha256 이 추출 캐시의 열쇠다(같은 바이트는 한 번만 읽힌다).
2. **형식에 상관없이 전부, 한 저장소에 한 번.** 첨부 바이트는 `media_store::store_bytes_as` 로 공용 내용주소 저장소
   (`data-go-kr-onbid.contract.json#/media/object_prefix` = `onbid/media`) 의 `sha256/<해시>.<확장자>` 에 CreateOnly 로
   한 번 쓴다(ADR-0298, ADR-0429 §1). 형식은 커널 `MediaFormat::sniff_document` 가 바이트로 정한다: 사진 형식·PDF,
   **HWP**(HWP 5 = OLE 복합 파일 안의 `HWP Document File` 머리, HWP 3 = 그 머리로 시작), **HWPX**(OCF: 첫 항목
   `mimetype` = `application/hwp+zip`), 그 밖의 ZIP, 이름 없는 바이트는 **bin**. 사진 판정 `MediaFormat::sniff` 는
   그대로다 — 공매·경매 화면 스키마는 jpg/png/gif/pdf 만 알고, 엣지는 그 확장자만 낸다.
   제공자가 내린 첨부(`use_yn` ≠ `Y`)는 받지 않는다. 4xx 는 사라진 파일(끝남), 상한(256 MiB) 넘는 파일은 해시만 적고
   센다, 그 밖의 실패는 다음 run 이 다시 받는다.
3. **부분·새 것만.** 수집기 `collect-industrial-complex-notice-attachments`(작업 `industrial_complex_notice_collect`,
   매시 :20)는 목록을 한 번 받고, 다음에 물을 고시를 매 run **레이크하우스에서 계산한다**(ADR-0352 의 꼴, 저장된
   대기열 없음): 목록의 고시 중 `silver.industrial_complex_notice_detail` 의 마지막 읽음이 `read`(모든 첨부가 끝남)가
   아닌 것, **ref_seq 큰 것(새 것)부터**, 상한 `notice_detail.per_run_limit`(400)·`run_time_budget_seconds`(1,800 초).
   끝난 고시는 다시 묻지 않는다 — 고시는 관보처럼 한 번 내고 고치지 않으며, 바꿀 때는 새 고시(새 ref_seq)를 낸다.
   요청 사이 2 초(주소 수집기의 하한), 상세 다섯 번 연달아 실패하면 그 run 은 멈춘다.
4. **Bronze·Silver.** 상세는 받은 그대로에 첨부마다 `file_sequence`·`store_outcome`·`sha256`·`media_format`·
   `size_bytes`·`source_url`·`fetched_at_utc` 를 붙여(경매 사진과 같은 주석 방식) `notice_detail.commit_every`(50) 건마다
   gzip JSON Lines Bronze 객체 하나로 커밋한다 — 멈춘 run 은 그 묶음만 잃는다(`commits: as_it_goes`). Silver 적재
   `industryland_or_kr.py`(작업 `industrial_complex_notice_silver`, 수집이 Bronze 를 바꾸면)가 객체마다 먼저
   **`silver.industrial_complex_notice_attachment`**(고시·해시마다 한 줄: 단지 코드·파일 이름·제공자 크기·경로·id·
   출처 URL·받은 시각·형식·바이트 수; 저장소에 있는 것만)를, 그다음 **`silver.industrial_complex_notice_detail`**
   (읽기마다 한 줄, 실패 포함: 고시 번호·단지 코드·제목·번호·고시일·종류·첨부 수)을 쌓는다. 둘 다 append-only, 키마다
   최신이 현재이고 사라지지 않는다.
5. **대조기.** 첨부 색인은 미디어 대조기(ADR-0387)의 `media.reconcile.index_tables` 에 들어간다. 막는 사고: 첨부가
   같은 `sha256/` 에 쌓이는데 대조기가 사진 색인만 보면 첨부 ~2만 개가 전부 고아로 세어져 매일 경보한다(ADR-0429 의
   경매 사진과 같은 일).
6. **켜진 채로 들어온다**(ADR-0457): 두 작업 모두 `per_run_cap.first_run` 을 적고 사람 없이 첫 run 을 돈다. 수집기는
   1,900 초(목록 한 번 + `run_time_budget_seconds` 1,800 초 + 마지막 고시), Silver 적재는 같은 꼴의 경매 Silver 적재(잰 278 초)에서
   600 초로 계산했다 — 둘 다 40 분 제한의 90 %(2,160 초) 안이다. Silver 적재는 매시 Silver 이웃(공매·실거래 Silver)과 같이
   Spark 풀 1 칸·우선순위 10 이다: 3 칸·우선순위 1 이면 매시 입력에 대해 풀 굶주림 한계(20 주기)를 넘는다(`job_specs.pool_starvation`). 둘 다 `commits: as_it_goes`·`retries: 0` 이다. 첫 run 의 저널이
   곧 측정이고, 그 값을 적는 PR 이 `first_run` 을 지운다.
7. **기대 결과**(ADR-0480): 고시는 정해진 공개일이 없어(엔드포인트 카탈로그 `expected_cadence.release = irregular`) 두 작업 모두
   `changes_within_hours` 가 null 이고 이유를 적는다. 살아 있음은 모든 켜진 작업의 `done` 기대(일정 한 주기 + 제한 안에 `changed`
   또는 `unchanged`)가 본다. 과거 고시 채우기 동안은 매 run 이 `changed` 다.

### 대안

| 대안 | 왜 아닌가 |
| --- | --- |
| foundation 에서 Claude API 로 추출(첫 지시) | 사장 정정(11-09): 추출은 intelligence, ai-server 로컬 모델 |
| 첨부를 고시 하나당 Bronze 객체로 | 18,793 객체 = Iceberg 커밋 18,793 번; 50 건 묶음이면 ~380 |
| 다음에 물을 고시를 상태 파일·대기열로 | 레이크하우스와 어긋날 수 있다(ADR-0352 가 같은 이유로 기각) |
| PDF·HWP 만 저장, 나머지 버림 | 사장 "모든 걸"; 형식 판정이 틀려도 바이트는 남아야 다시 읽을 수 있다(bin) |
| 사진 판정 `sniff` 를 넓히기 | 공매 감정평가서·경매 사진 경로가 zip·hwp 를 화면 스키마 밖 형식으로 내게 된다 |

같은 꼴: 문서 수집과 해석을 나누는 것은 큰 검색·법률 데이터 회사의 표준이다 — 원본은 내용 해시로 한 번 보관하는
불변 저장소(Git·IPFS·S3 Object Lock 의 내용주소), 해석은 그 해시를 열쇠로 하는 별도 단계(Common Crawl 의 WARC 원본과
WET 텍스트 파생, Apache Tika·Unstructured 를 원본 저장 뒤 파생 단계로 두는 구성).

## Consequences

- **양(계산, 표본 60 건 기준)**: 첨부 ≈ 18,793 × 1.05 ≈ **19,700 개, ≈ 23.6 GB**(PDF ~22.2 GB, HWP ~1.4 GB).
- **돈(계산, R2 2026-11 가격)**: 저장 23.6 GB × $0.015 ≈ **월 $0.35**; 쓰기 ~19,700 PUT + ~380 Bronze ≈ 2만 Class A ≈
  **$0.09 한 번**; HEAD ~2만 Class B ≈ $0.01. ILIS 요청 ≈ 18,793 상세 + 19,700 받기 + 47 목록 ≈ **38,500**, run 당 ≤ ~810.
  사장 승인 범위(소액) 안이다.
- **첫 바퀴(계산)**: 고시 하나 ≈ (상세 1 + 첨부 1.05) × 2 초 + 받기 ≈ 4.5 초 → run 당 ~400 → **~47 run(켜진 뒤 매시
  ~2 일)**. 그 뒤 run 은 새 고시만(지금 하루 몇 건).
- 엣지(`parcel_media.path_pattern`)는 jpg·png·gif·webp·avif·pdf 만 내므로 PDF 첨부는 엣지로도 보이고, HWP·HWPX·ZIP·
  bin 은 R2 로만 읽힌다(추출은 ai-server 에서 R2 를 직접 읽는다). 화면에 보이는 일은 intelligence 의 추출이 생긴 뒤다.
- 단지 코드(`danji_cd`)와 고시 번호만 싣고, 고시의 법정동 코드(`noti_addr`)는 Silver 에 옮기지 않는다(Bronze 에 그대로
  있다): 코드 변경 따라가기(ADR-0329 §3)가 필요한 값을 새로 만들지 않는다. 단지 연결은 `official_complex_code` 이다.
- **검증하지 못한 것**: 운영 run 시간·메모리(첫 run 의 저널), 표본 밖의 형식·최대 크기(첫 바퀴의
  `industrial-complex-notice-summary-json` 의 `attachments.by_format`·`oversize`), 오래된 고시 파일이 사라진 비율(`gone`).
- 남은 일: (1) 첫 run 의 저널로 `measured_run_seconds` 를 적고 `first_run` 을 지우는 PR; (2) intelligence platform 의 문서 읽기(PDF 텍스트·OCR,
  HWP 5/HWPX 텍스트 — 성숙한 OSS: pdfplumber/docling, pyhwp/LibreOffice)와 ai-server 로컬 모델 추출, 그 결과의 단지 패널
  절과 단지-필지 소속(`silver.complex_parcel_memberships`) 공급 — 별도 ADR.
