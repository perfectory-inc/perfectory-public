# ADR 0301: 배포 드레인은 중단 가능한 긴 작업을 몇 시간 기다리지 않고 멈춘다, 그리고 긴 DAG pause 는 경보한다

- Status: Accepted
- Date: 2026-10-26
- Builds on: [ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md)(위생 — E 배포안전·C 무음실패·J 성능), [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md)(autodeploy)
- 관련 사건: 2026-10-26 #867(공매 사진 수정) 배포가 `foundation-by-pnu-serving-bake` 에 1.5h+ 막혀 수집이 2h 정지했다(운영자가 손으로 `systemctl stop` 해 복구).

## Context

배포(`scripts/deploy/foundation-deploy.sh`) 순서: pre-flight → **step 0(전 DAG pause + "등록된 작업이 하나도 안 돌 때까지 드레인", 최대 5h: `:138`,`:151-161`)** → step 1 checkout → step 2 build → step 2b `deploy_unpause_unheld`(이 릴리스가 건드리지 않는 작업 재개) → … → step 7 resume. **드레인(step 0)이 끝나야 빌드로 넘어간다.** 즉 step 0 에서 막히면 그동안 전 DAG(수집 포함)이 pause 다.

2026-10-26: `foundation-by-pnu-serving-bake`(asset-triggered 09:33Z, full gen-6 섹션팩, 샤드마다 느려짐)가 step 0 드레인을 1.5h 막았다 → 그동안 전 DAG pause → **수집 10:07 이후 2h 정지.** 그대로면 5h 타임아웃(~15:44Z)에 배포가 실패(`:159 exit 1`)하고 DAG 은 계속 pause 로 남는다. 운영자가 bake 유닛을 멈추자 드레인이 즉시 0 이 되어 배포가 진행·복구됐다.

두 가지가 겹쳤다:

1. **드레인이 중단해도 안전한 긴 작업을 5h 기다린다.** bake 는 append-only·create-only(샤드별 커밋, 새 세대 gen-6 은 미발행이라 서빙=gen-5 무영향)라 멈춰도 손실·불일치가 없다. 드레인이 그걸 5h 기다릴 이유가 없는데, 등록된 "작업이 돈다"는 사실만으로 전부를 똑같이 기다린다.
2. **그 pause 가 무음이다.** paused DAG 은 run 자체가 없어 unit `OnFailure` 경보가 울리지 않는다(경보는 run 실패에만). 수집 2h 정지를 아무도 못 봤고, 배포 상태를 확인하다 우연히 발견했다.

bake 가 왜 느려지는지는 이 결정과 **무관하다**. 이 bake 의 degrade 는 2026-10-24 #852 가 한 O(n²) 경로를 O(1)로 고쳤으나 2026-10-26 재현했고(초반 1~2s/샤드 → 후반 270s/샤드), 코드상 full-bake 경로는 O(1)(plan fast-path + `first_pack_key`)로 보여 두 번째 경로를 코드로 확정하지 못했다(단일 샤드 재실행 측정이 필요하고 그건 서버 attended). **이 ADR 은 bake 가 빠르든 느리든 배포를 인질로 잡지 못하게** 배포안전을 세운다 — 느려짐의 근본 수정은 별개 과제로 둔다.

## Decision

**1. 등록 작업은 `orchestration/jobs.v1.json`(스케줄·서비스·계보의 단일 정의)에 `interruptible` 을 선언한다.** append-only·멱등으로 **부분 산출이 안전하고 다음 run 에 다시 도는** 긴 작업(by-PNU serving bake, gold rebuild, tile bake 류)이 `interruptible: true`. **수집·outbox·마이그레이션 인접 작업은 `interruptible: false`**(짧게 끝나 드레인이 금방 비고, 중간에 끊으면 일관이 깨질 수 있다).

**2. step 0 드레인은 `interruptible` 작업을 기다리지 않고 멈춘다.** 드레인은 (a) 비-interruptible 작업이 끝나길 기다리고(수초~수분), (b) `interruptible` 작업은 짧은 grace 뒤 멈춘다(그 유닛을 stop → SIGTERM). 멈춘 작업의 부분 세대는 미발행이라 안전하고, 배포 후 스케줄/asset 로 재실행된다. **운영자가 손으로 `systemctl stop` 할 필요가 없어진다 — 배포가 스스로 한다.**

**3. 긴 pause / 멈춘 배포는 경보한다(C 무음실패).** 배포 시작·종료를 메트릭/하트비트로 드러내고, step 0(또는 배포 전체)이 임계(예: 20분)를 넘기면 경보한다. paused-DAG halt 가 다시는 무음이 되지 않는다.

**4. `interruptible` 은 jobs.v1.json 한 곳에만 선언하고 가드가 검사한다(SSOT·일률).** 모든 등록 작업이 이 값을 갖도록(미선언 거부), 그리고 **수집 계열이 `interruptible: true` 로 선언되지 못하도록**(실수로 수집을 중단 가능으로 두면 신선도 손실) 가드가 막는다.

## Consequences

- 긴 bake 가 배포를 막지 못한다 — 2026-10-26 같은 "수집 2h 정지 + 배포 5h 대기 후 실패" 가 재발 불가가 된다.
- 멈춘 bake 는 배포 뒤 재실행된다(지금은 이미 구운 샤드의 compute 를 다시 함 — 진짜 resume[done 샤드 skip]은 ADR-0300 H 이어하기 후속).
- 긴 pause 가 경보로 드러난다(무음 halt 종료).
- jobs.v1.json 이 "무엇이 중단 가능한가"의 단일 출처가 되고, 가드가 빠짐없이 강제한다.

## Alternatives rejected

- **드레인 타임아웃만 짧게** — 비-interruptible 작업(수집 커밋 등)을 중간에 끊으면 일관이 깨진다. `interruptible` 구분이 전제라서 단순 단축은 기각.
- **빌드(step 2)를 pause(step 0) 앞으로 재배치** — build-alone 과 드레인 순서에 이유가 있어(릴리스 전환 중 옛 릴리스로 작업이 도는 것 방지) 리스크가 크다. 이 ADR 범위 밖(별도 검토).
- **경보만 추가** — 경보는 알려줄 뿐 여전히 사람이 손으로 멈춰야 한다. **멈춤 자동화가 핵심**이고 경보는 보조.
- **bake 내부 속도부터 고치기** — 두 번째 느려짐의 근본을 코드로 확정 못 했고(측정 필요), 설령 고쳐도 "배포가 긴 작업에 인질"이라는 구조 결함은 남는다. 배포안전을 먼저 세운다.

## Open

- `interruptible` 판정 기준(어떤 작업이 안전히 중단 가능한지)과 grace 시간은 구현 PR 에서 jobs.v1.json 실물 대조로 확정.
- bake 의 두 번째 느려짐(단일 샤드 재실행으로 누적 vs 샤드 크기 변이를 가림)과 진짜 resume(이미 구운 샤드 skip)은 후속.
- 긴-pause 경보의 임계·전달 경로는 기존 Alertmanager/Prometheus 위에서 확정.
