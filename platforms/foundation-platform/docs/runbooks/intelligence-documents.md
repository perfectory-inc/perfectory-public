---
status: current
owner: intelligence-platform
doc_type: runbook
last_reviewed: 2026-11-11
---

# 고시 첨부 텍스트·사실 작업 런북

루트 ADR-0477(문서 경계: foundation 이 문서와 place_id 를 넘기고, 인용 달린 사실을 intelligence 만 쓰는 표로 돌려받는다)과
ADR-0478(로컬 모델 런타임)의 운영 절차다. 값(추출기 판·쪽 기준·상한·컨테이너 메모리)은
`platforms/intelligence-platform/config/document-extraction.contract.json` 하나에 있고, 이 문서는 값을 다시 적지 않는다.
작업 정의는 `definitions/intelligence-documents.definition.json` 이다.

## 무엇이 어디서 도나

| 작업 | 유닛 | 풀 | 읽는 것 | 쓰는 것 |
|---|---|---|---|---|
| `intelligence_document_text` | `foundation-intelligence-document-text.service` | `documents` 1칸 | `silver.industrial_complex_notice_attachment`, 공용 저장소의 바이트 | `intelligence.document_text_page` → `intelligence.document_text_outcome` |
| `intelligence_document_fact` | `foundation-intelligence-document-fact.service` | `documents` 1칸 + 모델 유닛의 메모리 claim, 텍스트가 쪽을 쌓으면·매시 | 위 두 표, OCR 쪽이 있는 PDF 의 바이트 | `intelligence.document_fact` → `intelligence.document_fact_outcome` → `gold.complex_document_facts` |
| `industrial_complex_edge_export` | `foundation-industrial-complex-edge-export.service` | 풀 없음(Spark 안 씀), Gold 나 문서 사실이 바뀌면·매일 | `gold.complex_catalog`, `gold.complex_document_facts`, 엣지 목록 | 바뀐 단지의 프로필, 목록의 새 세대 |

문서 작업 둘의 한 run 은 순서가 같다: Spark 로 계획(`infra/lakehouse/spark/jobs/intelligence_documents.py plan-*`) → 읽기 키로 바이트 받기
→ intelligence 이미지(`intelligence-document-text:local`)가 로컬 파일만 읽고 씀 → Spark 로 쌓기(`append-*`). 이미지는 레이크하우스에
닿지 않는다. 표는 이 Spark 단계만 쓴다.

- **왜 텍스트 작업은 spark 풀이 아닌가.** OCR 은 CPU 만 쓰고 한 run 이 한 시간 가까이 걸린다(실측 문서당 평균 65 초, 2026-11-11).
  spark 풀에 1칸으로 두면 그 풀의 굶주림 한도(STARVATION_CYCLES × 주기)를 매시 run 으로는 넘을 수 없다(최악 대기 9,305 분 > 1,200).
  그래서 자기 풀 `documents`(1칸)에서 돌고, 계획·쌓기만 작은 Spark(`spark-small`)다. 메모리 예산은 두 풀이 동시에 찬 경우를 센다
  (`every-container-has-a-memory-cap.sh`: 61.34g / 62g).
- **왜 사실 작업은 spark 풀이 아닌가(루트 ADR-0563).** 모델 런타임을 `Requires=` 하므로 그 run 동안 런타임이 뜨고, 첫 질문에서
  모델이 메모리에 오른다. 그 메모리는 풀의 칸이 아니라 작업의 `memory_claim`(모델 유닛의 `MemoryMax`)이다: 시작할 때
  `job_units.py pool-full` 이 호스트의 `MemAvailable` 에서 그만큼을 빼고도 `memory_claims.keep_available` 이 남는지 본다. 안 남으면
  `foundation-job-outcome waiting the host has no memory for …` 로 끝나고 다음 주기(매시)가 다시 묻는다. spark 3칸을 다 쥐던
  때는 풀이 빌 때만 시작할 수 있어 2026-11-13 에 24,600 초를 기다렸다. `documents` 1칸이라 텍스트 작업과는 번갈아 돈다(GPU 는 한 작업).
- **단지 패널까지.** 사실 작업이 접기에서 `gold.complex_document_facts` 를 바꾸면 `changed` 로 끝나고, 그것이
  `industrial_complex_edge_export` 를 시작시킨다(계보 순환이 단지의 코드를 바꿨을 때도 같다). 내보내기는 내용 해시가 바뀐 단지의
  프로필만 쓰고 목록 세대를 올린다. 저널: `/var/lib/foundation-platform/industrial-complex-edge/journal.log` 의
  `industrial-complex edge generation=… written=… unchanged=… document_facts_complexes=…`.
- **이미지는 서버에서 짓지 않는다.** CI 의 릴리스 빌드가 `platforms/intelligence-platform/compose.document-jobs.yml` 로 짓고 서명한
  릴리스에 담는다. 릴리스 승인이 compose 이름으로 태그한다(루트 ADR-0276 §13). 그 이미지가 없는 옛 릴리스로 되돌려도 승인된다.
- **판은 이미지의 것이다.** 계획 단계는 이미지가 담은 계약(`/opt/intelligence/config/document-extraction.contract.json`)을 꺼내 읽는다.
  그래서 한 run 이 한 판으로 계획하고 다른 판으로 읽는 일이 없다. 판을 올리는 것은 전량 재처리이고 선언된 예외다(ADR-0477 §3).

## 켜기

두 작업은 입력 작업(`industrial_complex_notice_collect`·`industrial_complex_notice_silver`)과 함께 **첫 run 으로 켜진 채** 들어온다
(루트 ADR-0457 `per_run_cap.first_run`: 추정 초와 그 근거, `commits as_it_goes`, `retries 0`). 사람이 손으로 돌리지 않는다: 스케줄러가
유닛의 시간·메모리 상한 안에서 시작하고, 색인 표가 아직 없으면 계획이 `owed 0` 으로 끝난다(`unchanged`). 꺼진 독자는
`test_no_reader_of_a_first_run_is_left_off` 가 막는다. 첫 run 을 잰 PR 이 `first_run` 을 `measured_run_seconds` 로 바꾼다. 잴 것:

1. 입력: `silver.industrial_complex_notice_attachment` 의 줄 수와 형식별 수(Trino, 읽기만).
2. 저널의 마지막 줄 `intelligence document text ok run=… owed=… planned=… {read, run_seconds, peak_memory_mib, by_format}`
   과 `/data/foundation-platform/intelligence-documents/lakehouse/runs/text-<run>/run.log` 의 문서별 줄(형식·결과·쪽·초).
3. 표: `intelligence.document_text_outcome` 의 형식·결과별 수, `document_text_page` 의 method 별 쪽 수.
4. 사실: `intelligence.document_fact` 의 종류·방법별 수, `document_fact_outcome` 의 `model_calls`·`dropped`, 모델 런타임 유닛이
   함께 뜨고 멈추는지(`systemctl status intelligence-model-runtime-ollama.service`, 읽기만).

## 멈췄을 때

- 결과가 없는 문서는 다음 run 이 다시 고른다: 쪽이 쌓이고 결과가 쌓이기 전에 멈추면 그 문서는 쪽이 두 번 쌓일 수 있다. 읽는 쪽은
  `current_rows`(열쇠마다 가장 늦은 `run_id`) 로만 읽는다.
- 같은 run 의 쌓기를 다시 돌리면 `append_batch_once` 가 그 `run_id` 를 이미 가진 표에는 쓰지 않는다.
- 받은 바이트(`…/runs/<run>/media`)는 run 이 끝나면 성공하든 실패하든 지워진다. run 폴더의 나머지(계획·산출 JSONL·run.log)는 남는다.
