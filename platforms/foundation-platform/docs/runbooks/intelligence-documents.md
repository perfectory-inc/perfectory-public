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
| `intelligence_document_fact` | `foundation-intelligence-document-fact.service` | `spark` 3칸 전부, 하루 한 번 | 위 두 표, OCR 쪽이 있는 PDF 의 바이트 | `intelligence.document_fact` → `intelligence.document_fact_outcome` |

한 run 의 순서는 둘 다 같다: Spark 로 계획(`infra/lakehouse/spark/jobs/intelligence_documents.py plan-*`) → 읽기 키로 바이트 받기
→ intelligence 이미지(`intelligence-document-text:local`)가 로컬 파일만 읽고 씀 → Spark 로 쌓기(`append-*`). 이미지는 레이크하우스에
닿지 않는다. 표는 이 Spark 단계만 쓴다.

- **왜 텍스트 작업은 spark 풀이 아닌가.** OCR 은 CPU 만 쓰고 한 run 이 한 시간 가까이 걸린다(실측 문서당 평균 65 초, 2026-11-11).
  spark 풀에 1칸으로 두면 그 풀의 굶주림 한도(STARVATION_CYCLES × 주기)를 매시 run 으로는 넘을 수 없다(최악 대기 9,305 분 > 1,200).
  그래서 자기 풀 `documents`(1칸)에서 돌고, 계획·쌓기만 작은 Spark(`spark-small`)다. 메모리 예산은 두 풀이 동시에 찬 경우를 센다
  (`every-container-has-a-memory-cap.sh`: 61.34g / 62g).
- **왜 사실 작업은 spark 풀 전부인가.** 모델 런타임을 `Requires=` 하므로 그 run 동안 모델이 뜬다. 풀을 다 쥐어 모델이 Spark 옆에서
  메모리를 쥐지 않는다(ADR-0478 §2). 하루 한 번(02:50 UTC)이고 입력이 바뀌었다는 신호로는 시작하지 않는다: 매시 시작하면 같은 굶주림
  한도를 넘는다.
- **이미지는 서버에서 짓지 않는다.** CI 의 릴리스 빌드가 `platforms/intelligence-platform/compose.document-jobs.yml` 로 짓고 서명한
  릴리스에 담는다. 릴리스 승인이 compose 이름으로 태그한다(루트 ADR-0276 §13). 그 이미지가 없는 옛 릴리스로 되돌려도 승인된다.
- **판은 이미지의 것이다.** 계획 단계는 이미지가 담은 계약(`/opt/intelligence/config/document-extraction.contract.json`)을 꺼내 읽는다.
  그래서 한 run 이 한 판으로 계획하고 다른 판으로 읽는 일이 없다. 판을 올리는 것은 전량 재처리이고 선언된 예외다(ADR-0477 §3).

## 켜기

두 작업은 꺼진 채로 들어온다(ADR-0386 §3). 사람이 손으로 돌리지 않는다: 입력 작업(`industrial_complex_notice_collect`·
`industrial_complex_notice_silver`)이 켜지면, 텍스트 작업을 켜는 PR 이 `enabled` 와 함께 `per_run_cap.first_run`(추정 초와 그 근거,
루트 ADR-0457)을 둔다. 첫 run 은 그 유닛의 시간·메모리 상한 안에서 스케줄러가 시작하고, 문서마다 바로 쓰므로 멈춰도 끝낸 문서를
잃지 않는다. 그 run 을 잰 PR 이 `first_run` 을 `measured_run_seconds` 로 바꾼다. 잴 것:

1. 입력: `silver.industrial_complex_notice_attachment` 의 줄 수와 형식별 수(Trino, 읽기만).
2. 저널의 마지막 줄 `intelligence document text ok run=… owed=… planned=… {read, run_seconds, peak_memory_mib, by_format}`
   과 `/data/foundation-platform/intelligence-documents/lakehouse/runs/text-<run>/run.log` 의 문서별 줄(형식·결과·쪽·초).
3. 표: `intelligence.document_text_outcome` 의 형식·결과별 수, `document_text_page` 의 method 별 쪽 수.

사실 작업은 텍스트가 쌓인 뒤 같은 방식으로 켠다. 모델 런타임 유닛이 함께 뜨고 멈추는지(`systemctl status
intelligence-model-runtime-ollama.service`, 읽기만)와 `model_calls`·`dropped` 를 함께 본다.

## 멈췄을 때

- 결과가 없는 문서는 다음 run 이 다시 고른다: 쪽이 쌓이고 결과가 쌓이기 전에 멈추면 그 문서는 쪽이 두 번 쌓일 수 있다. 읽는 쪽은
  `current_rows`(열쇠마다 가장 늦은 `run_id`) 로만 읽는다.
- 같은 run 의 쌓기를 다시 돌리면 `append_batch_once` 가 그 `run_id` 를 이미 가진 표에는 쓰지 않는다.
- 받은 바이트(`…/runs/<run>/media`)는 run 이 끝나면 성공하든 실패하든 지워진다. run 폴더의 나머지(계획·산출 JSONL·run.log)는 남는다.
