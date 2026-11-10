# ADR 0478: 인텔리전스의 모델은 증설한 호스트의 Strata + Qwen3.8-Flash-Next 이고, 그때까지는 Qwen3.6-35B-A3B 가 spark 풀을 시간으로 나눠 쓴다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ITP-ADR-0006](../../platforms/intelligence-platform/docs/adr/0006-default-model-follows-measured-accuracy.md)
  (기본 모델은 실측 정확성을 따른다), [ADR-0455](./0455-one-job-cannot-take-the-host-every-unit-declares-its-memory-and-batch-is-killed-first.md)
  (모든 유닛이 메모리를 선언한다), [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)
  (예약 작업 풀), [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)
  (용량 claim), [ADR-0097](./0097-dependencies-track-latest-and-version-pins-live-in-one-contract.md) (버전 핀은 한 계약)
- Related: ADR-0477(문서 추출 경계, W54: 모델을 부르는 쪽)

## Context

사장이 인텔리전스 플랫폼의 로컬 모델로 **Strata + Qwen3.8-Flash-Next(큐넥플)**를 제안했고(2026-11-10), 11-11 에 그것으로
가기로 정했다. 하드웨어는 ai-server 에 더한다. 그리고 지금의 호스트에서 바로 써 보라고 했다. 1차 원천과 실측은 다음과 같다.

**Strata 와 모델(1차 원천, 2026-11-11).**

| 항목 | 값 | 출처 |
|---|---|---|
| 저장소 | `github.com/Niko1221/Strata`, MIT, 2026-09-24 생성, 별 42.8k, 열린 이슈 1,270 | GitHub API |
| 성숙도 | 커밋 대부분이 한 사람(5,117 건, 다음 기여자 35 건). 릴리스 v0.1.37(10-02)에서 v0.2.13(11-09)까지 이틀에 한 번꼴 | GitHub API |
| 공급망 | 엔진·Control 갱신 피드는 Ed25519 로 서명(`strata-feed.json.sig`). 릴리스마다 `SHA256SUMS.txt`, 모델 파일마다 고정 SHA-256. 엔진에 네트워크 코드·텔레메트리 없음. 소스 빌드 가능 | docs/SAFETY.md, SOURCE_INSTALL.md |
| 서버 | OpenAI·Anthropic 호환, 기본은 한 번에 요청 하나. 기본 포트 8080 은 ai-server 에서 Open WebUI 가 쓴다 | docs/BATCHING.md, 실측 |
| 모델 | 125B MoE(활성 6B), Qwen Community License 1.0: 내부 사용은 허용, 제3자에게 추론을 파는 "Model as a Service"는 별도 라이선스 | HF `Qwen/Qwen3.8-Flash-Next` LICENSE |
| 가중치 Q2_0 | ISTA-DASLab rev `ed59f920…`, 37.6 GB + n-gram 표 28.8 GB = 66.4 GB. Strata 가 그 위에 전문가 묶음 31.6 GiB 를 한 번 만든다 | HF API, 실측 |
| 메모리 | Q2_0 는 RAM + VRAM 37.6 GB. 8 GB 카드도 돈다(VRAM 12 GB 는 하한이 아니라 속도의 문턱) | docs/MODELS.md |

**ai-server(실측).** RTX 3060 **8 GB**, RAM 62 GiB(선언된 상한 합계 58.84 g, 남는 몫 3.16 g), 루트 = **SATA SSD**(WD Green 240 GB),
`/data` = HDD, 보드 TUF GAMING B660M-PLUS WIFI(빈 M.2 2개, DIMM 4개 최대 128 GB). 상시 컨테이너: 상한 33.3 G, 최고치 합 27.3 G,
지금 익명 메모리 10.9 G(최고치는 페이지 캐시가 상한까지 차서 상한과 같아 보인다; Trino 16 G 상한에 15.1 G).

**지금 호스트의 시험(2026-11-11, 같은 16 건, 같은 질문).** ILIS 고시 27 건을 받아(최신 순, PDF), 텍스트 층이 없는 스캔 5 건과
바이트가 같은 중복을 빼고 16 건을 남겼다. 정답은 ILIS 가 같은 고시에 붙인 구조화 필드(고시 번호·단지 이름)이고, 자동 점수가 틀렸다고
한 칸은 문서를 열어 손으로 확인했다. 두 런타임 모두 OpenAI 호환 `/v1/chat/completions` 로 불렀고, 앞 12,000 자, 온도 0.1.
spark 풀이 비어 있을 때만 돌렸고, Strata 는 사용자 cgroup `MemoryMax=22G`·`OOMScoreAdjust=1000` 안에서, spark 풀 작업이나 claim
이 나타나면 15 초 안에 멈추는 감시와 함께 돌렸다. 감시는 19:12:58Z 에 by-PNU 레인 cut-over 가 풀을 잡자 Strata 를 멈췄다: 16 건 중
10 건을 쟀고, 두 런타임은 그 같은 10 건으로 비교한다. 자동 점수가 틀렸다고 한 세 칸(20120·20128·20130)은 두 모델이 같았고,
고시가 여러 블록·단지를 함께 고시해 모델의 답이 문서와 맞다(ILIS 는 그중 하나만 적는다).

| 측정 | Ollama + Qwen3.6-35B-A3B Q4 | Strata + Qwen3.8-Flash-Next Q2_0 |
|---|---:|---:|
| 8 GB 에서 뜨는가 | 뜬다(모델 5.5 GB 가 GPU) | **뜬다**(전문가 2,224 개 2.86 GiB 가 GPU, 700 MiB 남음) |
| 문서당 시간(같은 10 건, 웜) | 13.3 s(16 건 14.2 s) | **274.3 s**(20.6 배) |
| 문서/시간 | 270 | **13.1** |
| 프롬프트 읽기 | ~383 tok/s | 33.1 tok/s(평균, 10 요청) |
| 생성 | ~29 tok/s | 4.18 tok/s(평균) |
| RAM 최고치 | 21.4 GB(상한 없는 서비스) | 20.0 GiB(22G cgroup; 익명 최고 3.4 GiB, 나머지는 매핑한 전문가의 페이지 캐시) |
| VRAM | 5.7 GB | 7.3 GB |
| SSD 읽기 | 첫 적재 23 GB | **요청 하나에 평균 66 GB, 최대 81 GB**(10 요청 630 GB) |
| 정답(자동, 같은 10 건) | 17/20 | 17/20(같은 세 칸) |
| 정답(손 확인) | 20/20(16 건 32/32) | 20/20 |
| 인용이 원문 그대로 | 55/56(16 건 88/91) | 58/58 |

같은 시험에서 찾은 것: PDF 텍스트 층에 NUL 이 섞인 고시(20117)는 llama.cpp 계열이 첫 NUL 에서 프롬프트를 끊어 9,060 토큰 중
255 토큰만 읽었다. 평가 도구는 제어 문자를 지우고 보낸다. 추출 작업(ADR-0477)도 같아야 한다.

**큰 곳의 방식.** 모델 서버는 바꿔 끼우는 부품이고 앱은 OpenAI 호환 관문 하나만 안다(vLLM·SGLang·llama.cpp·Ollama·TGI 가 같은
표면, LiteLLM·Envoy AI Gateway 가 그 앞에서 고른다). 엔진 채택은 같은 평가 세트의 정확도·처리량·자원 상한으로 판정한다. GPU 를
쓰는 일괄 작업은 큐가 시간을 나눠 준다(Slurm 의 GRES, Kubernetes 의 Kueue): 상시 예약이 아니라 작업이 도는 동안만 잡는다.

## Decision

1. **앱은 엔진을 모른다.** 추출 작업은 `MODEL_RUNTIME_BASE_URL`·`MODEL_RUNTIME_CHAT_PATH`·`MODEL_RUNTIME_DEFAULT_MODEL` 로만
   부른다(기존 `openai_compatible.rs`). 런타임마다의 엔진·가중치·포트·평가 기준은
   `platforms/intelligence-platform/config/model-runtime.contract.json` 하나에 있고, `serving` 이 지금 관문이 가리키는 런타임이다.
   Strata 는 그 계약에 릴리스 묶음의 SHA-256 으로 핀한다(호스트에 NVIDIA 컨테이너 런타임이 없어 이미지가 아니라 묶음이다).
   가중치는 고정 리비전·크기·SHA-256 이고 `jobs/model_runtime/fetch_weights.py` 가 받으면서 맞춘다. 갱신 확인은 끈다.

2. **지금은 Qwen3.6-35B-A3B 가 spark 풀을 시간으로 나눠 쓴다.** 런타임은
   `intelligence-model-runtime-ollama.service`: 상한 24G·batch(OOM 이 먼저 고른다)·루프백 전용·한 번에 모델 하나,
   `StopWhenUnneeded=yes` 에 `[Install]` 이 없다. 그것을 `Requires=` 하는 작업이 spark 풀의 슬롯을 전부 잡고 도는 동안만 뜨고,
   작업이 끝나면 멈춘다. 손으로 깐 `ollama.service`(0.0.0.0:11434, 상한 없음, cgroup 최고치 29.7 GiB)는 배포의 `timers` 단계가
   모델을 쥐고 있지 않을 때 끈다. 사용자 세션에서 띄운 두 번째 Ollama(*:11435, 모델 99 GB 가 HDD)는 운영자가 정리한다.

3. **증설한 호스트에서 Strata + Qwen3.8-Flash-Next Q2_0 가 `serving` 이 된다.** 지금 호스트의 시험은 "뜨고 맞힌다, 그러나 문서당
   274 초(문서/시간 13.1, 같은 문서의 Ollama 의 1/20.6)"였다. 느린 까닭은 GPU 보다 RAM 과 디스크다: 22G 안에서 전문가 34 GB 를 다 담지 못해 요청마다 평균 66 GB 를
   SATA 에서 다시 읽었다. 그래서 순서는 RAM, NVMe, GPU 다:
   - **RAM +64 GB(DDR5 32 GB×2)**: Q2_0 상주분(37.6 GB)이 예산 안에 든다(128 GB − 58.84 ≈ 69 g).
   - **NVMe 2 TB(PCIe 4.0)**: 가중치(`/srv/intelligence-models`)와 Spark 임시 쓰기(`/srv/scratch`, 따로 PR)를 둔다.
   - **GPU 12–16 GB**: 같은 Q2_0 에서 생성이 8 GB 의 몇 배로 오른다(아래 표, Strata 문서의 같은 카드 측정).
   첫날 순서는 런북 `platforms/foundation-platform/docs/runbooks/model-runtime.md` 다. 증설 뒤 같은 16 건을 같은 도구
   (`jobs/model_runtime/eval_extraction.py`)로 두 런타임에 돌려 정확도와 문서/시간이 낫게 측정되면 `serving` 을 바꾼다.

4. **사장 결정용 하드웨어 표.** 가격은 2026-11 국내 대략값(견적 전), 속도는 Strata 문서의 같은 카드 측정이며 우리 호스트의 측정이
   아니다. 우리 호스트에서 잰 것은 8 GB·22G·SATA 행뿐이다.

   | 구성 | 대략 비용(KRW) | Qwen3.8-Flash-Next 생성 | 문서/시간(계산) |
   |---|---:|---|---:|
   | 지금(8 GB, 22G cgroup, SATA) | 0 | 4.18 tok/s(**실측**) | 13.1(**실측**) |
   | + RAM 64 GB + NVMe 2 TB, 카드 그대로 | 약 45–80 만 | 8 GB NVIDIA 수치 없음; 8 GB AMD 보고 생성 15–23, 프롬프트 115–149 tok/s | 약 30–45 |
   | + RTX 3060 12 GB(중고) | 위 + 약 25–35 만 | 3060 12 GB·32 GB: 생성 IQ2_XS 43–51, 프롬프트 ~140 tok/s | 약 50–60 |
   | + RTX 4070 Ti Super 16 GB | 위 + 약 110–130 만 | 16 GB 급(5060 Ti 16 GB) Q2_0 생성 80–87, 프롬프트 ~470 tok/s | 약 170–200 |
   | + RTX 4090 24 GB(중고) | 위 + 약 250–350 만 | 3090 24 GB Q2_0 생성 128–140, 프롬프트 ~490 tok/s(4090 은 그 이상) | 약 200–300 |

   문서/시간(계산)은 문서 하나 = 프롬프트 약 8,000 토큰 + 답 약 220 토큰으로, 프롬프트 속도는 같은 출처의 4K–32K 값으로 셌다.
   지금의 Ollama + 35B-A3B 는 같은 호스트에서 이미 270 문서/시간(실측)이고, 이 표본의 정답은 두 모델이 같았다(20/20). 그래서
   하드웨어를 사는 이유는 속도가 아니라, 더 어려운 항목(사업시행자·면적·변경 전후)에서 큐넥플이 낫다는 측정이어야 한다: 손 확인
   표본(ADR-0477, 30 건 이상)으로 두 모델을 그 항목까지 먼저 잰다. 16 GB 카드가 있어야 큐넥플이 지금의 35B-A3B 와 비슷한 속도에 닿는다.
   어느 카드든 RAM 이 먼저다: 카드만 바꾸면 22G 안의 디스크 재읽기(3)는 그대로다. 전원(PSU)은 확인되지 않았다; 4070 Ti Super 는
   권장 750 W, 4090 은 850 W 다.

## Consequences

- 추출(W54)은 관문 값만 읽으면 되고 엔진이 바뀌어도 코드는 그대로다. 런타임 작업은 spark 풀을 다 잡으므로 Spark 와 메모리를 함께
  쥐지 않는다. 그 작업의 `scheduled_jobs.memory` 에 런타임 상한을 넣어 풀 합계를 맞추는 일은 그 작업의 PR 이 한다.
- Open WebUI·LiteLLM 은 `ollama.service` 를 끈 뒤 상시 모델을 잃는다(10-01 이후 채팅 50 건, 모두 127.0.0.1). 사람의 채팅도 같은
  런타임을 풀 작업으로 띄워야 한다.
- 시험에 쓴 것(엔진·가중치·묶음 약 100 GB, Ollama 별칭 하나, 파이썬 이미지, 사용자 유닛)은 측정 직후 지웠다(루트 여유 133 GB 로 복귀).
  그동안 루트 디스크가 85 % 까지 찼다. 증설 첫날의 유닛·메모리 정의는 #1248(draft), Spark 임시 쓰기의 NVMe 이동은 #1246(draft)이다.
- Strata 는 7 주 된, 한 사람이 거의 모든 커밋을 쓰는 프로젝트다. 판을 올릴 때는 그 판의 이슈를 먼저 읽고 같은 16 건으로 다시 잰다.
