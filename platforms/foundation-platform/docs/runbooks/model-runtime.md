---
status: current
owner: intelligence-platform
doc_type: runbook
last_reviewed: 2026-11-11
---

# 로컬 모델 런타임 런북

루트 ADR-0478 의 운영 절차다. 런타임마다의 엔진·가중치·포트·평가 기준은
`platforms/intelligence-platform/config/model-runtime.contract.json` 하나에 있고, 메모리는
`definitions/platform.definition.json` 의 `systemd_units`(생성된 `tools/host-memory-budget.contract.json`)에 있다.
앱은 엔진 이름을 모른다: `MODEL_RUNTIME_BASE_URL`·`MODEL_RUNTIME_CHAT_PATH`·`MODEL_RUNTIME_DEFAULT_MODEL` 만 읽는다.

## 지금 도는 것

| 런타임 | 유닛 | 포트 | 상한 | 언제 도나 |
|---|---|---|---|---|
| Ollama + Qwen3.6-35B-A3B Q4 | `intelligence-model-runtime-ollama.service` | 127.0.0.1:11434 | 24G, batch(OOM 먼저) | 그것을 `Requires=` 하고 `memory_claim` 으로 이름 대는 작업이 도는 동안만(`StopWhenUnneeded=yes`, `[Install]` 없음) |
| Strata + Qwen3.8-Flash-Next Q2_0 | (하드웨어 증설 뒤 첫날 PR) | 127.0.0.1:11437 | 증설 뒤 계산 | 같은 방식 |

손으로 깔았던 `ollama.service`(0.0.0.0:11434, 상한 없음)는 배포의 `timers` 단계가 모델을 쥐고 있지 않을 때 끈다.
쥐고 있으면 그 배포는 두고 다음 배포가 끈다. 유닛 파일은 그대로 두고 disabled 로만 만든다.

## 부르는 작업이 지켜야 할 것

- 작업 유닛에 `Requires=intelligence-model-runtime-ollama.service` 와 `After=` 같은 이름.
- 작업은 `jobs.v1.json` 에서 `memory_claim.units` 에 런타임 유닛을 적고, 칸이 하나인 풀(`documents`)에서 돈다(루트 ADR-0563).
  시작할 때 호스트의 남는 메모리(`MemAvailable`)가 유닛의 `MemoryMax` 와 `memory_claims.keep_available` 을 담을 때만 승인된다.
  spark 풀의 칸은 잡지 않는다. claim 의 실측 근거는 `tools/host-memory-budget.contract.json` 의 `scheduled_jobs.memory_claims` 다.
- 문서 하나가 끝날 때마다 `job_units.py preempted` 를 물어 더 높은 claim 이 기다리면 멈춘다(문서 단위가 step 경계).
- 텍스트의 NUL 등 제어 문자는 모델에 보내기 전에 지운다: llama.cpp 계열은 첫 NUL 에서 프롬프트를 끊는다
  (2026-11-11 ILIS 고시 20117: 9,060 토큰 중 255 토큰만 읽혔다).

## 평가(엔진을 바꾸기 전)

```sh
python3 platforms/intelligence-platform/jobs/model_runtime/eval_extraction.py \
  --sample <표본 디렉터리> --base-url http://127.0.0.1:<포트> --model <모델> --out report.json
```

표본은 `<id>.txt`(쪽을 `\f` 로 이은 텍스트)와 `<id>.gold.json`(`{"facts": {...}}`)의 쌍이다. 질문의 조건은 계약의 `evaluation`, 통과 기준은 `document-extraction.contract.json` 의 `accuracy_gate`(ADR-0477).
자동 점수가 틀렸다고 한 칸은 문서를 열어 손으로 확인한다: ILIS 의 단지 이름은 짧고 고시는 여러 블록을 함께 쓴다.

## 하드웨어 증설 첫날(Strata)

1. NVMe 를 `/srv/intelligence-models` 에 마운트한다(계약 `model_root`).
2. `python3 platforms/intelligence-platform/jobs/model_runtime/fetch_weights.py strata_qwen38_flash_next`
   — 고정 리비전에서 받고 크기·SHA-256 을 맞춘다. NVMe 가 아니면 거절한다.
3. 메모리 정의의 `physical_memory` 를 `free -b` 의 MemTotal 로 고치고 Strata 유닛 PR 을 올린다.
4. 위 평가를 같은 표본으로 Ollama 와 Strata 에 한 번씩 돌리고, 정확도·문서/시간이 나으면 계약의 `serving` 과
   `MODEL_RUNTIME_*` 값을 바꾼다.
