# ADR 0477: foundation 은 intelligence 에 문서와 place_id 를 넘기고, 인용 달린 사실을 intelligence 만 쓰는 표로 돌려받는다 — 첫 조각은 고시 첨부의 쪽·위치 텍스트

- Status: Accepted
- Date: 2026-11-11
- Builds on: ADR-0456(고시 첨부 수집과 그 Silver 색인 — W40 의 PR 로 들어온다),
  [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md)(내용주소 저장소),
  [ADR-0288](./0288-every-parcel-source-converges-on-one-place-identity.md) §5(place_id 가 유일한 조인 열쇠),
  ADR-0329 §3(법정동 코드 변경 따라가기), [ADR-0455](./0455-one-job-cannot-take-the-host-every-unit-declares-its-memory-and-batch-is-killed-first.md)(유닛 메모리 상한),
  ADR-0386 §3(새 작업은 꺼진 채), ADR-0352(다음 일은 레이크하우스에서 계산),
  ITP-ADR-0002(RAG 는 Foundation 이 낸 것을 읽는다)·ITP-ADR-0006(기본 모델은 측정으로)

## Context

사장 지시(2026-11-10): 인텔리전스 플랫폼도 되어야 하고, 그것은 foundation 과 얽혀 있다. 11-09 에는 "고시문 읽어서 뽑는 건
인텔리전스 플랫폼이 되어야 하는 거 아니야? … 클로드로 뽑는 게 아니라 ai-server 에서 하는 거잖아"(ADR-0456 §Context).

**현황(2026-11-11, 읽기만 — 정본 표는 [intelligence 현황](../../platforms/intelligence-platform/docs/current-state.md))**:

| 무엇 | 사실 |
| --- | --- |
| intelligence 코드 | 정규화 제안(C0–C1)·지식 색인(형태소·RRF·결과 조립, ITP-ADR-0003~0005)이 있으나 **배포된 적 없다**: ai-server 에 intelligence 유닛·컨테이너가 없고 `:8010` 이 응답하지 않는다. main 의 최근 intelligence 변경은 문서·가드 정리뿐이다 |
| ai-server 에서 도는 것 | `ollama`(Qwen3.6-35B-A3B 기본, `qwen2.5vl:7b` 등)·`litellm`·`open-webui`. RTX 3060 8 GiB 는 측정 때 유휴 |
| foundation 데이터를 읽는 intelligence 코드 | **없다**(`git grep`: `trino`·`place_id`·`by_pnu` 0건). C2 이벤트(`knowledge.source-upserted`)는 생산자가 없다 |
| 문서 | ADR-0456 이 고시 첨부(≈19,700 개, PDF 94 %·HWP 6 %)를 공용 저장소에 모으고 `silver.industrial_complex_notice_attachment` 를 붙이기로 했다. 그 읽기·추출은 "별도 ADR"로 남겼다 |

두 플랫폼 사이에 문서·필지·사실이 오가는 계약이 없다. 없으면 생기는 일은 정해져 있다: 한쪽이 다른 쪽의 표를 고치거나(누가
틀렸는지 모른다), intelligence 가 PNU 를 스스로 해석해 법정동 코드 변경을 놓치거나(ADR-0288 의 화성 분구 사고), 같은 문서를
판마다 다시 읽는다.

**대기업·오픈소스는 이렇게 한다.** Google Document AI 는 뽑은 값마다 `textAnchor.textSegments[startIndex,endIndex]` 와
`pageAnchor` 를 돌려 값이 원문의 어디인지 가리키게 한다. AWS Textract 도 블록마다 쪽과 위치를 준다. Common Crawl 은 원본(WARC)과
텍스트 파생(WET)을 나눠, 파생을 원본의 해시·판으로 다시 만들 수 있게 둔다. Apache Tika 는 파서를 별도 프로세스(tika-server)로
돌려 파일 하나의 실패가 작업을 죽이지 않게 한다. 데이터 메시(Dehghani)는 표마다 쓰는 팀을 하나로 두고 다른 팀은 읽기만 한다.

## Decision

1. **계약 파일 하나.** [`platforms/intelligence-platform/config/document-extraction.contract.json`](../../platforms/intelligence-platform/config/document-extraction.contract.json)
   이 넘기는 것(`inputs`)과 돌려받는 것(`outputs`)을 적는다. 문서 텍스트 작업이 이 파일을 읽고, 시험
   (`test_inputs_and_outputs_share_no_table`)이 두 목록에 같은 표가 없고 출력이 모두 `intelligence.` 이름공간임을 확인한다.
   - **넘기는 것(foundation 이 쓰고 intelligence 는 읽기만):** 첨부 색인 `silver.industrial_complex_notice_attachment`
     (`sha256`·`media_format`·`notice_sequence`), 그 바이트(공용 저장소 `…/sha256/<해시>.<형식>`, 접두사는 foundation 계약
     포인터로만), 필지 `gold.place_id_registry`·`gold.place_id_bridge`.
   - **돌려받는 것(intelligence 가 쓰고 foundation 은 읽기만):** `intelligence.document_text_outcome`(문서·판마다 결과),
     `intelligence.document_text_page`(쪽마다 텍스트와 위치), `intelligence.document_fact`(값 하나·인용 하나). 모두 append-only.
     foundation 은 이 표를 읽어 자기 Gold·패널로 싣고, 무엇을 보일지는 foundation 이 정한다.
   - **두 쪽이 함께 쓰는 표는 없다.** intelligence 는 foundation 표·원장·저장소에 쓰지 않는다(정규화 제안이 Foundation API
     로만 가는 것과 같다, ITP 아키텍처 §플랫폼 간 계약).
2. **필지는 place_id 로만, 해석은 foundation 이.** intelligence 는 PNU 를 만들지 않는다. 문서가 말하는 지번은 원문 그대로
   (`value_json` 에 법정동 이름·지번 문자열·고시일) 사실로 돌려주고, place_id 로 바꾸는 것은 foundation 의 유일한
   해석기(ADR-0288 §1)가 하며 법정동 코드 변경은 `views.changed_codes`(ADR-0329 §3)로 따라간다. 적재 때 한 번 바꾸고
   멈추지 않는다.
3. **같은 바이트는 한 판으로 한 번.** 열쇠는 `(sha256, extractor_version)`. run 마다 색인에서 이 판의 결과가 없는 해시만
   고른다(ADR-0352 의 꼴, 저장된 대기열 없음), 새 고시부터, `per_run_limit` 까지. 실패도 결과이므로 같은 판으로 다시 읽지
   않는다. **판을 올리는 것은 전량 재처리이고 선언된 예외다** — 그 PR 이 이유와 측정 비용(아래 처리량 × 문서 수)을 적는다.
4. **첫 조각: 쪽·위치 텍스트(성숙한 OSS 만, 별도 프로세스).**
   - PDF: 텍스트 층을 pdfplumber(MIT)로 쪽마다 읽는다. 보이는 글자가 `ocr_below_chars`(20) 보다 적은 쪽만 ocrmypdf(MPL-2.0)
     + Tesseract(Apache-2.0, `kor+eng`)로 OCR 하고(`--force-ocr --pages <그 쪽들>`), 그 쪽의 글만 바꾼다. 쪽마다 `method`
     (`text_layer`·`ocr`)가 남는다.
   - HWP 5·HWPX: LibreOffice(MPL-2.0) + H2Orestart(GPL-3.0, LibreOffice 확장, 해시로 고정)로 PDF 로 인쇄한 뒤 PDF 길.
     쪽 번호는 인쇄된 PDF 의 것이다. **pyhwp 는 쓰지 않는다**: AGPL 이고 이 독점 라이선스 트리에 import 된다. 도구는 모두
     자식 프로세스이고 각자 시한이 있어, 한 파일의 실패는 그 파일의 `failed` 결과다.
   - 작업은 Python 이다: 성숙한 도구가 Python 라이브러리(pdfplumber)와 CLI(ocrmypdf·soffice)이기 때문이다. ITP-ADR-0001 이
     폐기한 것은 Python **서비스**(API 경계)이고, 플랫폼 경계·제출의 정본은 그대로 Rust 다. 이 작업은 HTTP 를 열지 않고 계약의
     표만 낸다.
   - 그 밖의 형식(zip·그림·bin)은 `unsupported_format` 으로 세고 남긴다 — 바이트는 저장소에 있어 다음 판이 읽는다.
   - 위치: 쪽을 `\f` 로 이은 문서 전체 텍스트에서 쪽마다 `char_start`·`char_end`. **인용은
     `(sha256, page_number, char_start, char_end)` 이고 그 구간의 글자가 인용문과 같을 때만 유효하다**(`citation_holds`).
     문서가 말하지 않은 값은 인용될 수 없다.
   - docling(IBM, MIT)은 표 구조·배치 모델이 있어 더 많이 보지만 torch·모델 가중치를 끌고 오고 쪽당 수 초다. 텍스트 층이
     있는 PDF(아래 측정의 대부분)에는 과하다; 표 규칙이 못 읽는 표의 2차 후보로 측정한 뒤에만 들인다.
5. **사실은 규칙 먼저, 로컬 모델은 나머지만, 정확도 문 뒤에.** 다음 조각의 순서를 지금 정한다.
   (1) 정형 표(토지이용계획의 용지별 면적·비율, 유치업종)는 쪽 텍스트와 표 규칙으로 뽑고 합계 검산을 한다(`method = rule`).
   (2) 규칙이 못 읽은 것만 ai-server 의 로컬 모델(ITP-ADR-0006 의 기본 모델, LiteLLM/Ollama 경유 — **Claude API 아님**)에
   JSON 스키마로 묻고, 모든 값에 인용을 요구하며, `citation_holds` 가 거짓인 값은 버린다(`method = local_llm`, `model` 기록).
   (3) `document_fact` 의 사실 종류마다 손으로 확인한 표본(`accuracy_gate.hand_checked_sample_min` = 30)에서 정밀도
   `min_precision`(0.98) 이상이 측정되기 전에는 foundation 이 화면에 싣지 않는다. 정밀도는 `method`(text_layer·ocr 쪽)별로
   따로 잰다: 아래 손 확인에서 OCR 쪽 숫자는 85 % 만 정확했으므로, OCR 쪽의 숫자는 표 합계 검산이 맞거나 쪽 그림을 읽은
   로컬 비전 모델(`qwen2.5vl:7b`, GPU 8 GiB 안)과 일치할 때만 사실이 된다.
6. **작업으로 돈다, 상한을 말하고.** 문서 텍스트는 자기 이미지(`jobs/document_text/Dockerfile`, 고정 base digest·해시 잠근
   wheel·해시 확인한 H2Orestart)로 돈다. CPU 만 쓰므로 Spark 풀 슬롯도 GPU 도 잡지 않는다. 메모리 상한은 아래 측정의 1.5 배
   이상(ADR-0455 §3) — 측정 최고점은 run 프로세스 486 MiB + 가장 큰 자식 777 MiB 라 **2G** 이고 `ocr_jobs` 를 올리면 다시 잰다 —,
   등급 `batch`(OOM 때 먼저 죽는다). run 은 `per_run_limit` 건 또는 `run_time_budget_seconds`(3,300 초) 중 먼저 닿는 데서 새
   문서를 멈추고, 문서마다 결과를 바로 쓴다(멈춘 run 은 끝낸 문서를 잃지 않는다). 모델 단계가 생기면 GPU 하나를 나누는 `gpu` 풀(슬롯 1)을
   `orchestration/jobs.v1.json` 에 두고 그 단계만 잡는다. 작업은 **꺼진 채로** 들어오고(ADR-0386 §3), 입력 수집
   (`industrial_complex_notice_collect`, ADR-0456 §6)이 켜지고 첫 attended run 을 잰 PR 이 켠다.

### 대안

| 대안 | 왜 아닌가 |
| --- | --- |
| foundation 이 텍스트·사실을 뽑는다 | 사장 정정(11-09); 문서 파이프라인이 둘이 된다(ADR-0456 §1) |
| intelligence 가 foundation 의 Silver 에 사실을 덧붙인다 | 한 표를 두 쪽이 쓴다 — 틀렸을 때 누가 고칠지 모르고, foundation 적재가 intelligence 의 판을 덮는다 |
| intelligence 가 지번을 PNU 로 바꿔 돌려준다 | 해석기가 둘이 된다(ADR-0288 이 없앤 것); 법정동 코드 변경을 놓친다 |
| Claude API 로 추출 | 사장: ai-server 로컬 모델 |
| pyhwp(hwp5txt) | AGPL, beta(0.1b15) — 독점 트리에 import 불가 |
| 문서 전체를 OCR | 텍스트 층 있는 쪽까지 래스터화해 느리고(아래 측정), 정확한 텍스트 층을 OCR 오류로 바꾼다 |
| 처리 완료를 상태 파일로 | 레이크하우스와 어긋날 수 있다(ADR-0352) |

## Consequences

### 측정 (2026-11-11, 실측)

ILIS 고시 목록(18,793 건)에서 무작위 고시 280 건(시드 고정)의 첨부를 받아(요청 간 2 초, 표본 파일은 저장소에 넣지 않는다) 이
이미지 그대로 노트북 Docker 에서 돌렸다: PDF 는 2 CPU·2 GiB, HWP·HWPX 는 1 CPU·2 GiB, Tesseract 1 job. **ai-server 에서 잰
것이 아니다** — 그 측정은 첫 attended run 이다.

| 형식 | 문서 | 쪽 | 결과 | OCR 쪽 | 초/쪽 | 실패 |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| PDF | 79 | 1,669 | text 54 · ocr 21 · partial_ocr 4 | 444 | 3.33 | **0** |
| HWP 5 | 16 | 528 | text 7 · partial_ocr 9(그림 쪽) | 129 | 1.38 | **0** |
| HWPX | 2 | 36 | text 2 | 0 | 1.21 | **0** |
| 합 | 97 | 2,233 | 빈 결과 0 · 형식 밖 0 | 573(26 %) | — | **0** |

- 텍스트 층만 있는 문서: **0.30 초/쪽**, 문서 중앙값 2.3 초. 전부 스캔인 문서: **11.8 초/쪽**(Tesseract 1 job). 가장 긴 문서
  727 초(60 쪽 스캔), 가장 많은 쪽 260(HWP). PDF 의 27 % 가 전부 스캔이다.
- 메모리 최고점: run 프로세스 486 MiB, 가장 큰 자식(LibreOffice·ocrmypdf) 777 MiB.
- 표본 첨부 278 개의 형식(내용으로 판정): PDF 93 %·HWP 6 %·HWPX 0.7 % — ADR-0456 의 표본(PDF 94 %·HWP 6 %)과 같다.

**손 확인(쪽 그림과 대조).** 텍스트 층 쪽의 토지이용계획·유치업종 표(숫자 칸 45 개)는 **45/45 정확**하지만, 병합 셀의 행
구조("합 계"와 그 값, "공공시설 › 소계")는 줄 텍스트에서 흩어진다 — 표 규칙은 줄 텍스트가 아니라 표 추출(pdfplumber
`extract_tables`)에서 읽어야 한다. OCR 쪽의 같은 표(숫자 칸 27 개)는 **23/27(85 %)** 만 정확했다(`4?,353.6`·`42,.353.6`·
`2.866.6` 두 번; 구성비 9 개는 모두 정확). 시보 머리의 호수는 자리가 바뀌었다(`제2084호` → `제208호4`). 지도·도면 쪽의
OCR 은 의미 없는 글자다. §5 의 방법별 정밀도와 OCR 숫자 이중 확인이 이 측정에서 나왔다.

**첫 바퀴(계산).** 첨부 ≈ 19,700 개 × 평균 23 쪽 ≈ 45 만 쪽, 그 26 % ≈ 11.8 만 쪽이 OCR → 11.8 만 × 11.8 CPU 초 ≈ 390 CPU 시간,
텍스트 층 33 만 쪽 × 0.3 초 ≈ 28 시간. `ocr_jobs` 4 면 ≈ 4–5 일(노트북 CPU 기준; 서버 측정으로 고친다). 그 뒤는 새 고시의
첨부만이다(하루 몇 개). 돈: 로컬 CPU 뿐이라 추가 비용 0, R2 읽기 ≈ 2 만 Class B ≈ $0.01.

### 그 밖
- intelligence 의 Iceberg 쓰기(`intelligence.` 이름공간, 그 접두사로 범위를 묶은 쓰기 자격)는 다음 조각이다. 이 조각의 run 은
  계약의 열 그대로 JSON Lines 를 낸다.
- 남은 일: (1) R2 읽기·`intelligence.` 표 쓰기와 작업 등록(꺼진 채); (2) 표 규칙(용지별 면적·유치업종)과 손 확인 표본;
  (3) 로컬 모델 단계와 `gpu` 풀; (4) foundation 이 `document_fact` 를 단지 패널·`complex_parcel_memberships` 로 싣는 것(W38·W39).
