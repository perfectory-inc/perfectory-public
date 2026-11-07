# ADR 0441: 배포는 돌고 있는 작업도 운영자의 실행도 기다리지 않는다 — 각 실행은 시작한 릴리스를 지킨다

- Status: Accepted
- Date: 2026-11-07
- Supersedes: [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md) 의 "운영자의 유닛이 돌면 배포 틱은 기다린다", 이후 그 규칙을 이어 쓴 결정의 같은 부분
- Builds on: [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)(승인된 릴리스만 쓴다), [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md)·[ADR-0308](./0308-the-deploy-stops-only-the-jobs-its-release-holds-and-skips-a-step-whose-inputs-did-not-change.md)(배포가 잡는 것은 그 릴리스가 바꾸는 저장소의 작업뿐), [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(자동 승격)
- 관련 사건: W20(SEV1), 2026-11-07 — 07:46Z..10:05Z 배포 틱이 "waiting for the operator's …"로 70회 넘게 아무것도 배포하지 않았다. 자동 승격이 lane cut-over 를 연이어(07:42Z parcel-regulation-zones, 09:12Z parcel-ownership g2) `systemd-run` 으로 띄웠고, 틱은 임시(transient) `foundation-*` 유닛이 하나라도 돌면 건너뛰었다. 그동안 필지 레인 SEV1 수정(#1130)은 병합되고도 배포되지 못했다.

## Context

릴리스는 이미 불변이다: `/opt/foundation-platform/releases/<sha>` 와 그것을 가리키는 `current` 심볼릭 링크. 등록 작업은
시작할 때 자기 물리 릴리스에 묶이고(`admitted-writer-runtime.sh`, `pwd -P`), 배포는 릴리스가 바꾸는 저장소(마이그레이션이
건드리는 표, Postgres 마이그레이션)의 작업만 잡는다(ADR-0308, 2026-11-07 의 #1131 이 더 좁힘). 그 밖의 등록 작업은
"릴리스 전환을 지나 계속 돈다".

운영자의 실행(lane cut-over 와 그것이 띄우는 Gold 재생성·굽기·관문 유닛)만 예외였다. 그 단계들은 매번
`/opt/foundation-platform/current` 를 다시 읽었고(`by-pnu-pack-operator.sh` 의 `RELEASE`, 재생성 사본의 `ExecStart` 경로),
작성자 승인 `--current` 는 지금의 `current` 만 받았다. 그래서 배포가 cut-over 도중 `current` 를 옮기면 다음 단계가 거절되거나
다른 릴리스로 돌았다. 배포 틱은 그 위험을 "운영자 유닛이 돌면 기다린다"로 막았는데, 자동 승격이 cut-over 를 사람 없이
연이어 띄우면서 그 기다림이 배포 정지가 되었다.

대기업의 방식은 기다림이 아니라 버전 고정이다: Capistrano·Kubernetes 롤링 배포는 새 릴리스로 링크·레플리카를 바꾸고
옛 프로세스는 자기 릴리스로 끝까지 돈다. Temporal·Argo Workflows 는 워크플로를 시작할 때의 정의 판에 고정한다.

## Decision

1. **실행은 시작한 릴리스를 지킨다.** 운영자의 실행(`by-pnu-pack-operator.sh`)은 시작할 때 `current` 를 한 번 해석해
   고정하고, 그 릴리스의 id 를 `FOUNDATION_RUN_RELEASE` 로 자기가 띄우는 모든 유닛(`systemd-run -E`)에 넘긴다. 실행의
   단계(cut-over 유닛의 자식)는 그 이름의 릴리스를 쓴다. 재생성 사본은 단위 파일이 `current` 아래로 적은 스크립트를 고정
   릴리스에서 돌린다.
2. **승인은 실행의 릴리스를 받는다.** `admitted-writer-runtime.sh --current` 는 `current` 이거나, 그 파일이 물리적으로 있는
   릴리스가 `FOUNDATION_RUN_RELEASE` 와 같을 때 받는다. 이름은 다른 릴리스를 가리킬 수 없고(경로는 여전히 파일의 물리
   위치), 빌드 산출물(`build.json`) 검증도 그대로다. 옛 릴리스는 정리(prune)가 돌고 있는 프로세스가 쥔 것을 지키므로 실행
   중에 사라지지 않는다.
3. **배포는 운영자의 실행을 기다리지 않는다.** `foundation-autodeploy.sh` 의 "운영자 유닛이 돌면 건너뜀"을 지운다.
   **참으로 위험한 것만 남긴다:** 릴리스가 작업을 잡을 때(마이그레이션이 그 작업이 쓰는 표나 Postgres 스키마를 바꿀 때)만
   `foundation-deploy.sh` 가 운영자 유닛을 기다린다(종료 75, 다음 틱이 다시 묻는다; 실패로 적지 않는다). 운영자 유닛은
   작업 목록 밖에서 쓰므로 어떤 잡힌 저장소를 쓰는지 알 수 없어서다. 잡는 목록 자체는 ADR-0308·#1131 그대로다.
4. 가드가 막는 실제 사고(루트 ADR-0001): **2026-11-07 07:46Z..10:05Z 자동 승격의 cut-over 두 개 동안 배포가 멈춰 SEV1
   수정이 배포되지 못했다**(`test_an_operators_unit_does_not_hold_the_tick`,
   `test_a_release_that_holds_nothing_deploys_beside_an_operators_unit`,
   `test_an_operators_unit_waits_the_deploy_only_when_the_release_holds_jobs`, `test_admitted_writer_runtime.py`).

## Consequences

- 표를 바꾸지 않는 배포(대부분 — 11-05..06 의 13개 중 12개, #1131 재생 측정)는 cut-over 가 돌아도 바로 배포된다. cut-over 는
  자기 릴리스로 끝난다.
- 마이그레이션을 싣는 배포는 지금처럼 운영자의 실행이 끝나기를 기다린다. 그 기다림의 상한·경보는 ADR-0301 의 긴 정지
  경보가 그대로 잡는다.
- 남은 일: 등록 작업 가운데 실행 도중 형제 스크립트를 다시 부르는 것(`gold-panel-rebuild.sh all` 이 표마다 자기를 다시
  부름)은 지금도 그 순간의 `current` 를 따른다. 각 표 실행은 그 자체로 승인되고 핀을 기록하므로 안전하지만, 같은 방식으로
  시작 릴리스에 고정하는 것은 후속이다.
