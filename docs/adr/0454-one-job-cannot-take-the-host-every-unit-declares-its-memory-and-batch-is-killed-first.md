# ADR 0454: 한 작업이 호스트를 쓰러뜨리지 못한다 — 모든 유닛이 메모리를 선언하고, OOM 은 배치를 먼저 죽인다

- Status: Accepted
- Date: 2026-11-09
- Amends: ADR-0118 §6(모든 컨테이너는 메모리 상한을 말한다) — 상한을 systemd 유닛까지 넓힌다. ADR-0138(공유 풀의 슬롯 조합 합)의
  계산은 그대로다.
- Builds on: ADR-0270 §1(유닛이 되풀이하는 사실은 한 곳에서 렌더한다), ADR-0385(호스트가 메모리·OOM 을 스스로 경보한다),
  ADR-0415(정의에서 생성한다)

## Context

**사고 (W31·W36).** 2026-11-08 03:43Z `foundation-source-sweep`(유닛 안에서 `foundation-outbox-publisher` 바이너리가 돈다)이
62 GB 호스트에서 익명 메모리 33,062,452 kB 를 잡았다. 커널 OOM 킬러가 그것을 끝냈고, 같은 시각 돌던 PostgreSQL 백업
(pgBackRest)은 연결을 잃고 실패했다(ADR-0445 §5, #1188). 11-04 에도 같은 일이 있었다. 원인은 고른 파일마다 작업 전체를 복사한
n² 복사였고 #1183 이 고쳤다. 그러나 그 유닛에는 상한이 없었다. **유닛 58 개 가운데 46 개가 `MemoryMax` 없이** 돌았고, 어느
유닛도 `MemoryHigh`·`OOMScoreAdjust` 를 말하지 않았다. 버그 하나가 데이터베이스와 백업을 함께 쓰러뜨릴 수 있었다.

**측정 (ai-server, 읽기만).** systemd 는 메모리 계정이 켜진 유닛이 끝날 때 `Consumed …, <n> memory peak` 을 저널에 남긴다
(cgroup v2 `memory.peak`, 페이지 캐시 포함). 저널이 남은 2026-10-18..11-08 의 6,793 줄을 유닛 파일(템플릿은 인스턴스를
합쳐)별 최고점으로 모았다.

| 유닛 | 지금 상한 | 측정 최고점 | 실행 수 |
|---|---:|---:|---:|
| foundation-source-sweep | 4G(#1183) | 1,536M(10-21) — 10-31..11-08 는 39.6–42.2G(버그) | 26 |
| foundation-silver-refresh@ | 16G | 15,668M(building-register-unit-areas) | 84 |
| foundation-by-pnu-serving-bake | 14G | 13,517M | 25 |
| foundation-zoning-tile-bake | 없음 | 3,175M | 7 |
| foundation-autodeploy | 없음 | 2,253M | 236 |
| foundation-onbid-collect | 없음 | 1,844M | 486 |
| foundation-outbox-publish | 없음 | 770M | 341 |
| 그 밖의 스크립트형 유닛 25 개 | 대부분 없음 | 306–547M | — |
| 저널에 최고점이 없는 유닛 19 개 | 대부분 없음 | (상한이 없어 계정이 꺼져 있었다) | — |

컨테이너를 띄우는 유닛(Gold 재구축·타일 굽기·Spark)은 스크립트만 갖는다. 컨테이너는 Docker 데몬의 cgroup 이고 compose 나
계약이 이미 상한을 둔다(ADR-0118 §6). 상주 서비스 합 30.75g + host_reserved 5g 이므로, **유닛 하나가 26.25g 를 넘게 가지면
그것 하나로 호스트가 넘친다.**

**대기업·오픈소스는 이렇게 한다.** Kubernetes 는 컨테이너마다 `resources.limits.memory`(cgroup `memory.max`)를 두고, QoS 등급에
따라 kubelet 이 `oom_score_adj` 를 Guaranteed −997, BestEffort 1000 으로 매겨 OOM 킬러가 덜 중요한 것을 먼저 고르게 한다.
MemoryQoS 는 그 아래 `memory.high` 를 둬 죽이기 전에 조이고 회수한다. Google Borg 는 production 과 batch 를 우선순위 대역으로
나누고 자원이 모자라면 batch 를 먼저 쫓아낸다. Meta 의 oomd·Senpai 는 `memory.high` 압력으로 OOM 전에 되돌린다. systemd 는 같은
cgroup v2 손잡이를 `MemoryHigh=`·`MemoryMax=`·`OOMScoreAdjust=` 로 준다.

## Decision

1. **유닛마다 한 곳에서 선언한다.** `tools/host-memory-budget.contract.json`(정본은 `definitions/platform.definition.json`
   의 그 산출물, ADR-0415) `systemd_units.unit_files` 가 `infra/systemd` 의 모든 `*.service` 파일마다 `class`·`memory_max`
   ·`measured_peak`·`measured`(어디서 잰 값인가, 없으면 왜)를 갖는다. 템플릿(`a@.service`)은 한 줄이다: 인스턴스는 한 유닛
   파일에서 돌고 상한도 그 파일의 것이다. 새 유닛 파일은 이 예산 결정 없이 호스트에 못 간다 — compose 파일이 `projects` 에
   놓여야 하는 것(ADR-0118 §6)과 같은 규칙이다.
2. **렌더한다, 손으로 쓰지 않는다.** `scripts/ci/unit_memory.py --write` 가 각 유닛에
   `MemoryHigh=`(상한의 `memory_high_of_max` = 0.9, MiB 내림)·`MemoryMax=`·`OOMScoreAdjust=`(등급의 값) 세 줄을 렌더한다.
   유닛이 상한에 대해 쓴 설명 주석은 그대로 둔다. 실행 중 자기 일꾼 수를 남은 메모리로 정하는 명령
   (`process_memory.rs`)은 `memory.high` 도 한계로 읽는다: 넘으면 커널이 조이기 때문이다.
3. **크기는 측정에서 나온다.** `memory_max ≥ 1.5 × measured_peak`(`memory_max_over_peak`), 바닥 128M. 이미 결정된 경계가
   그보다 가까우면 `headroom_why` 가 이유를 말한다: by-PNU 굽기 14G 는 행 상한으로 묶인 측정의 1.3 배(ADR-0138), Silver
   레인 16G 는 내보내기가 남은 메모리의 3/4 로 일꾼을 늘려 상한 가까이 차는 것이 설계다. 최고점이 아직 없는 유닛은
   `measured` 에 왜 그 크기인지(같은 스크립트의 측정, 컨테이너가 따로 잡는 것)를 쓰고, 상한 아래 첫 실행부터 systemd 가
   최고점을 남긴다. 새 측정이 1.5 배를 넘으면 이 가드가 실패하므로 측정이 상한을 끌어올린다.
4. **OOM 은 배치를 먼저 죽인다.** 등급은 셋이다. `batch` +500: 예약 작업·굽기·수집·이행·리허설 — 멈춘 run 은 이어 받거나
   다시 돈다. `orchestration/jobs.v1.json` 의 작업이 도는 유닛은 모두 batch 여야 한다. `control` 0: 배포·경보·탐침·점검.
   `recovery` −500: 데이터베이스 백업 — 11-08 에 끊긴 그것이다. 데이터베이스와 인증(Zitadel)·관문 컨테이너는 compose
   `oom_score_adj` 로 그보다 더 보호한다(§6).
5. **가드.** `scripts/guard/every-container-has-a-memory-cap.sh`(항상 돈다)가 `unit_memory.problems` 를 부른다:
   항목 없는 유닛 파일, 렌더와 다른 유닛, 없는 유닛을 가리키는 항목, 모르는 등급, 배치가 아닌 작업 유닛, 1.5 배 미만이면서
   이유 없는 상한, 그리고 **혼자서 상주 서비스 합 + host_reserved 옆에 들어가지 못하는 상한**을 거부한다. 자체 시험
   (`every-container-has-a-memory-cap-self-test.sh`)이 각 거부를 증명한다.
6. **compose 서비스도 같은 두 겹을 갖는다(후속 PR).** 상한(`mem_limit`)은 이미 모든 서비스에 있다. 데이터베이스·Zitadel·
   관문(foundation-api, identity-api, 루프백·터널)은 `oom_score_adj` 음수, 일회성 Spark 는 양수를 계약에 선언하고 같은 가드가
   compose 와 대조한다. Docker 에는 `memory.high` 손잡이가 없고 `mem_reservation` 은 cgroup v2 에서 `memory.low`(보호)다:
   보호가 필요한 상주 서비스에만 측정 최고점으로 둔다.
7. **입력과 함께 자라는 작업은 흘려 처리한다.** 훑기(VWorld 파일 수집)는 목록을 파일에서 읽으며 해석하고(`from_reader`,
   목록 글을 따로 들지 않는다), 고른 파일이 나눠 갖는 작업은 파일 목록 없는 머리(`VWorldDatasetFileJob::header`)다: 한
   run 은 목록의 파일 하나를 한 번만 든다. 내려받기는 이미 스풀로 흐른다(ADR-0168). 측정 최고점이 입력 크기와 함께 자라는
   다른 유닛 — Silver 레인(내보내기가 전국 색인을 든다)·by-PNU 굽기(샤드를 통째 해석한다) — 은 상한이 결정된 경계이고
   행 상한이 묶는다; 그 흘려 처리는 그 ADR 들의 몫으로 남긴다.

## Consequences

- 유닛 46 개가 처음으로 상한을 갖고, 58 개 모두 `MemoryHigh` 와 OOM 등급을 갖는다. 같은 종류의 버그는 그 유닛 하나에서 끝난다
  (`oom-kill` 결과, `OnFailure` 경보, ADR-0445).
- `MemoryMax` 가 메모리 계정을 켜므로 지금 최고점이 없는 19 개 유닛도 다음 실행부터 저널에 최고점을 남긴다.
- Silver 레인은 `memory.high`(14,745M)를 넘지 않게 일꾼을 조금 덜 띄운다. 전국 색인처럼 일꾼 수와 상관없는 부분이 그 선을
  넘으면 죽지 않고 조여져 느려진다.
- 상한·등급을 바꾸는 것은 계약 한 줄과 `--write` 다; 손으로 고친 유닛은 CI 가 거부한다.
