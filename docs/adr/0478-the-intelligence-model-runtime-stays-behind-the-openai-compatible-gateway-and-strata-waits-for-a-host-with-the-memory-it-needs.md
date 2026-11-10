# ADR 0478: 인텔리전스의 로컬 모델은 OpenAI 호환 관문 뒤에서 바꿔 끼우고, Strata(큐넥플)는 그것이 요구하는 메모리를 가진 호스트를 기다린다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ITP-ADR-0006](../../platforms/intelligence-platform/docs/adr/0006-default-model-follows-measured-accuracy.md)
  (기본 모델은 실측 정확성을 따른다), [ADR-0455](./0455-one-job-cannot-take-the-host-every-unit-declares-its-memory-and-batch-is-killed-first.md)
  (모든 유닛이 메모리를 선언한다), [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)
  (예약 작업 풀), [ADR-0457](./0457-a-job-never-run-takes-its-first-run-unattended-bounded-by-its-unit-and-the-run-is-the-measurement.md)
  (첫 run 이 측정이다), [ADR-0097](./0097-dependencies-track-latest-and-version-pins-live-in-one-contract.md) (버전 핀 정본)
- Related: ADR-0477(문서 추출 경계 — 모델을 부르는 쪽, W54)

## Context

사장이 인텔리전스 플랫폼의 로컬 모델로 **Strata + Qwen3.8-Flash-Next(큐넥플)**를 제안했다(2026-11-10). 문서 추출은
ai-server 의 로컬 모델로 한다(사장 11-09, Claude API 아님). 1차 원천을 2026-11-11 에 직접 확인했다.

**Strata 와 모델(1차 원천).**

| 항목 | 값 | 출처 |
|---|---|---|
| 저장소 | `github.com/Niko1221/Strata`, MIT, 2026-09-24 생성, 별 42.8k, 열린 이슈 1,270 | GitHub API |
| 성숙도 | 커밋 대부분이 한 사람(5,117 건, 다음 기여자 35 건). 릴리스 v0.1.37(10-02) → v0.2.13(11-09), **이틀에 한 번꼴** | GitHub API |
| 범위 | 이제 큐넥플 전용이 아니다: Qwen3.8-Flash-Next·Qwen3.6-35B-A3B·DeepSeek V4.1-Flash 를 같은 서버가 낸다 | README |
| 공급망 | 엔진·Control 갱신은 Ed25519 서명 피드(`strata-feed.json.sig`), 릴리스마다 `SHA256SUMS.txt`, 모델 파일마다 고정 SHA-256·크기. 엔진에 네트워크 코드·텔레메트리 없음. 소스 빌드(CMake, CUDA 12/13) 가능 | docs/SAFETY.md, docs/SOURCE_INSTALL.md |
| 평가한 판 | v0.2.13, `strata-engine-0.1.59-linux-x86_64-cuda-cuda12.tar.gz` sha256 `4bf321cb…babbc`, `SHA256SUMS.txt` sha256 `d25f46a5…bb116` | GitHub release asset digest |
| 서버 | `127.0.0.1:8080`, OpenAI·Anthropic 호환. 기본은 **한 번에 요청 하나**(`"parallel": N` 로 2–8 슬롯) | docs/SAFETY.md, docs/BATCHING.md |
| 모델 | Qwen3.8-Flash-Next, 125B MoE(활성 6B), 2026-08-26 공개, **Qwen Community License 1.0** — 내부 사용·우리 서비스의 사실 추출은 허용, "Model as a Service"로 제3자에게 추론을 내주면 별도 라이선스 | huggingface.co/Qwen/Qwen3.8-Flash-Next LICENSE |
| 가중치(권장 Q2_0) | ISTA-DASLab `Qwen3.8-Flash-Next-GSQ-RCO-GGUF` rev `ed59f920`, 조각 1 37.6 GB(sha256 `69820c02…9b720`) + 조각 2 28.8 GB n-gram 표(sha256 `316b46f3…1e113`) = **디스크 66.4 GB** | HF API(LFS oid) |
| 메모리 | Q2_0 은 **RAM + VRAM 37.6 GB**(IQ2_XS 39.2, IQ3_XXS 47.0, IQ3_S 54.8). "48 GB 면 Q2_0·IQ2_XS". 8 GB 카드도 돈다(RTX 2060 SUPER 8 GB, RTX PRO 2000 8 GB, RX 5700 XT 8 GB 보고) — **VRAM 12 GB 는 하한이 아니라 빠름의 문턱**이고 제약은 RAM 이다 | docs/MODELS.md, docs/DETAILS.md, docs/OLDER_GPUS.md |
| SSD | 메모리에 못 든 전문가와 n-gram 표는 SSD 에서 읽는다. 문서는 **NVMe** 를 전제한다. SATA(450 MB/s 흉내) 측정에서 긴 프롬프트는 디스크 읽기가 지배한다 | docs/MODELS.md, docs/DETAILS.md |

**ai-server(2026-11-11 실측, 읽기 전용).**

| 항목 | 값 |
|---|---|
| GPU | RTX 3060 **8 GB**(GA106, 8192 MiB, sm_86, PCIe 4.0 x16), 측정 시 9 MiB 사용(유휴) |
| RAM | 62 GiB, swap 4 GiB. **선언된 상한 합계 58.84 / 62.00 g**(`every-container-has-a-memory-cap.sh`: 상시 31.84 + 예약 작업 22.00 + host_reserved 5.00) → **남는 몫 3.16 g** |
| 디스크 | 루트 = **SATA SSD** WD Green 240 GB(`sdb`, ROTA 0), 여유 134 GB. `/data` = HDD. 보드 TUF GAMING B660M-PLUS WIFI 에 NVMe 슬롯은 있으나 비어 있다(VMD 컨트롤러만 보임) |
| 이미 도는 모델 런타임 | `ollama.service` 0.30.10(:11434, `MemoryMax=infinity`, 예산 밖 — host-memory-budget 의 `uncapped`), 사용자 공간 ollama(:11435, ITP-ADR-0006). 기본 모델 Qwen3.6-35B-A3B Q4(23 GB) |
| 포트 | **:8080 은 Open WebUI 가 0.0.0.0 으로 이미 쓴다** — Strata 기본 포트와 겹친다 |

**큰 곳의 방식.** 모델 서버는 바꿔 끼우는 부품이고, 앱은 하나의 관문 계약만 안다: OpenAI 호환 `/v1/chat/completions`
(vLLM·SGLang·llama.cpp server·Ollama·TGI 가 모두 같은 표면을 낸다), LiteLLM·Envoy AI Gateway·Kong AI Gateway 가 그 앞에서
엔진을 고른다. 엔진 채택은 같은 평가 세트의 정확도·처리량·자원 상한으로 판정한다(Hugging Face TGI→vLLM 이행, Databricks
Model Serving 의 엔진 교체가 같은 모양이다).

우리 트리에는 이미 이 관문이 있다: 인텔리전스 API 의 `MODEL_RUNTIME_BASE_URL`·`MODEL_RUNTIME_CHAT_PATH`·
`MODEL_RUNTIME_DEFAULT_MODEL`(정본 `platforms/intelligence-platform/config/local-ollama.env.example`), 어댑터
`intelligence-normalization-infrastructure/src/openai_compatible.rs`. 새 스위치는 필요 없다.

## Decision

1. **모델 런타임은 OpenAI 호환 관문 하나 뒤에 있다.** 추출 파이프라인(ADR-0477)은 `MODEL_RUNTIME_*` 로만 모델을 부르고, 엔진
   이름(Ollama·Strata·vLLM)을 코드에 적지 않는다. 엔진 교체 = 그 값과 런타임 유닛의 교체이며 추출 코드는 바뀌지 않는다.
   사실 한 줄에 남기는 `model` 은 `MODEL_RUNTIME_DEFAULT_MODEL` 값 그대로다(같은 바이트·같은 모델 판이면 같은 열쇠).

2. **지금 ai-server 에는 Strata 를 깔지 않는다.** 이유는 VRAM 이 아니라 RAM 과 디스크다(위 표, 계산):
   - Q2_0 을 상주시키려면 RAM 이 약 30–34 GB 필요하다(37.6 GB − 8 GB 카드에 드는 몫). 호스트에 남은 몫은 3.16 g 이다.
     예약 작업 풀의 가장 큰 자리(22 g)를 통째로 잡아도 25.16 g 이라 모자란다.
   - 모자란 만큼을 `--mmap-experts` 로 SSD 에서 읽으면, 그 SSD 는 SATA(약 0.5 GB/s)이고 n-gram 표 28.8 GB 도 같은 디스크다.
     Strata 자신의 SATA·16 GB 측정이 보여주듯 긴 고시문 프롬프트에서 디스크가 병목이 된다.
   - 66.4 GB 가중치는 루트 여유 134 GB 의 절반이다. HDD(`/data`)에는 두지 않는다(사장 조건).
   - `127.0.0.1:8080` 은 Open WebUI 와 겹친다.
   그래서 "8 GB 에서 도는가"는 **돈다(문서상)**, "다른 작업을 굶기지 않고 안전하게 도는가"는 **아니다(계산)** 다. 측정을 위해
   깔았다가 지우는 단계도 두지 않는다: 이 판정은 측정이 아니라 선언된 예산의 산수에서 나왔고, 산수가 바뀔 때(4) 다시 본다.

3. **지금의 로컬 모델은 ITP-ADR-0006 그대로다**(Ollama 의 Qwen3.6-35B-A3B Q4, 실측 문서당 9.8 s, 날짜 뒤바뀜 없음). 8 GB 에
   통째로 드는 작은 모델(qwen2.5:7b 4.7 GB·gemma2:9b 5.4 GB 가 이미 서버에 있다)은 ITP-ADR-0006 에서 gemma2:9b 가 한국어
   법령 날짜를 뒤바꿔 판정 역할에서 빠졌으므로, ADR-0477 의 손 확인 표본으로 측정하기 전에는 기본값이 되지 않는다.

4. **재평가는 측정이 당긴다.** 다음 중 하나가 생기면 Strata 를 같은 표본·같은 관문으로 측정하는 작업을 ADR-0457 의 방식(선언된
   유닛, `MemoryMax`, 풀 인지, 첫 run = 측정)으로 연다:
   - 호스트 RAM 이 Q2_0 상주분을 예산 안에 담는다(128 GB 로 증설하면 상시+예약+여유 뒤에 약 69 g 가 남는다, 계산: 128 − 58.84).
   - NVMe SSD 가 생겨 가중치 66.4 GB 를 그 위에 둔다.
   - ADR-0477 의 손 확인 표본(최소 30 건)이 생겨 현재 모델의 정확도가 측정된다 — 비교의 기준선이다.
   측정 항목: tokens/s(프롬프트·생성), RAM 최고치(cgroup `memory.peak`), SSD 읽기량, 표본 정밀도(인용 일치 포함), 문서당 시간.
   그때의 Strata 는 판(태그)·엔진 묶음 sha256·가중치 rev 와 sha256 을 `tools/technology-versions.contract.json` 에 핀하고,
   바이너리는 서명 피드가 아닌 핀한 sha256 으로 받는다(자동 갱신 `STRATA_UPDATE_CHECK=0`).

5. **하드웨어는 사장의 결정이다.** 선택지(가격은 2026-11 국내 대략값, 견적 전; 속도는 Strata 문서의 같은 카드 측정이며
   우리 호스트의 측정이 아니다):

   | 선택지 | 대략 비용(KRW) | 큐넥플 Q2_0/IQ2_XS 생성 속도(문서) | 필요한 RAM |
   |---|---:|---|---|
   | RAM 64 → 128 GB(32 GB×2 추가) | 약 30–60 만 | 카드 그대로면 8 GB 카드 수치 없음(추정 10–25 tok/s) | 이것이 첫 조건 |
   | NVMe 1–2 TB | 약 10–25 만 | 디스크 병목 제거 | — |
   | RTX 3060 12 GB(중고) | 약 25–35 만 | 3060 12 GB·32 GB RAM: IQ2_XS 43–51, IQ3_XXS 59–67 tok/s | 32–48 GB |
   | RTX 4070 12 GB(중고 66 만 확인, 신품 약 80–90 만) | 약 66–90 만 | 4070 12 GB·32 GB: IQ2_XS 48 tok/s(커뮤니티), 5070 12 GB: Q2_0 94 | 32–48 GB |
   | RTX 4090 24 GB(중고) | 약 250–350 만 | 3090 24 GB: Q2_0 128–140 tok/s(4090 은 그 이상) | 32 GB 로도 Q2_0 |

   어느 카드든 RAM 증설이 먼저다: 카드만 바꾸면 RAM 예산 문제(2)는 그대로다.

## Consequences

- 엔진은 이미 바꿔 끼울 수 있다. 추출 쪽(W54, ADR-0477)은 관문 값만 읽으면 된다 — Strata 든 vLLM 이든 같은 코드다.
- 지금 ai-server 에는 아무것도 깔리지 않았고, 지울 것도 없다.
- 위험으로 남는 것: `ollama.service` 는 여전히 상한 없이 23 GB 모델을 올린다(host-memory-budget `uncapped`, 2026-10-01 결정).
  모델 런타임을 ADR-0455 의 유닛 상한 안으로 넣는 일은 이 ADR 의 범위 밖이며, 추출 작업이 그 런타임을 상시로 부르기 시작할
  때 같이 다룬다.
- Strata 는 7 주 된, 한 사람이 거의 모든 커밋을 쓰는 프로젝트다. 재평가 때 판을 핀하고, 그 판의 이슈 목록을 먼저 읽는다.
