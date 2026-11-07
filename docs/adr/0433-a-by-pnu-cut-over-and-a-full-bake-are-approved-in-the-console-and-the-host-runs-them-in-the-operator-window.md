# ADR 0433: by-PNU cut-over 와 전량 굽기는 더니어에서 승인하고, 호스트가 운영자 창 안에서 한 번 돌린다

- Status: Accepted
- Date: 2026-11-07
- Builds on: [ADR-0161](./0161-pack-cutover-root-steps-run-through-one-granted-script.md)(cut-over 의 root 단계는 허가된 스크립트 하나로),
  [ADR-0190](./0190-unit-prices-are-their-own-gold-panel-and-by-pnu-lane.md)·ADR-0193 §6(묶음 전용 레인의 첫 cut-over),
  [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md) §2b(운영자 시작은 한 번의 요청, 재시도 루프 없음),
  [ADR-0282](./0282-operator-switches-are-issued-by-foundation-api-and-land-as-files-the-runner-reads.md)(운영자 스위치는 직원 신원으로 foundation-api 가 내고 파일로 남는다),
  [ADR-0141](./0141-by-pnu-serving-publishes-changed-documents-as-patch-generations.md)·[ADR-0310](./0310-a-long-bake-finishes-at-its-pin-then-patches-forward.md)(전량 굽기와 핀 이어 하기),
  [ADR-0425](./0425-clock-started-batch-never-runs-into-the-by-pnu-operator-window-and-a-late-start-waits-for-its-next-schedule.md)(운영자 창은 `job_units.py scheduled_window()` 하나)

## Context

2026-11-06 의 parcel-ownership 첫 cut-over(호스트 journal `sudo`·`foundation-operator-start`, 증거 `by-pnu-bake/cutover/*.jsonl`, 잰 값).

| 시각(UTC) | 무슨 일 |
|---|---|
| 00:24:21 | 사장의 `sudo` 비밀번호 실패(`pam_unix(sudo:auth): authentication failure`) |
| 00:24:27 | 사장이 손으로 `/run/systemd/system/foundation-by-pnu-lane-bake@parcel-public-auction.service.d/90-force-full.conf` 를 쓰고 `daemon-reload`, 굽기 시작 |
| 00:37:30 | 같은 drop-in 을 손으로 지움(지우는 것을 잊으면 다음 굽기마다 전량) |
| 00:37:35 | `ssh -tt ... sudo by-pnu-pack-operator.sh parcel-ownership cutover 1 --requested-at ... --reason ...` |
| 01:22 | gold 단계가 Spark 를 **45분** 기다리다 실패(ADR-0425 의 사고, 그 ADR 이 고침) |
| 03:46:38 | 다시 시작 → gold 단계 거부: "would run into the scheduled window 04:30-21:50 UTC (budget 60 min)" — **창을 놓침** |

cut-over 자체는 이미 자동이다. `by-pnu-lane-cutover.sh` 가 history·preview·gold·parity·bake·equality·latency·publish·
monitor-sample 을 차례로 돌리고 첫 실패에서 멈추며, 단계마다 창을 확인한다. 사람이 하는 일은 셋뿐이었다: (1) 서버 셸로
들어가 sudo 비밀번호를 친다, (2) 창이 열린 시각에 깨어 있다, (3) 전량 굽기가 필요하면 drop-in 을 쓰고 지운다. 사고는 셋
모두에서 났다. 작업·Silver 레인의 시작은 ADR-0282 로 이미 더니어 단추가 되었고(운영자 원장 `catalog.operator_action`,
`switches/` 파일, root 로 도는 `foundation-run-now.path` → `run-now-dispatch.sh`), cut-over 와 전량 굽기만 그 밖에 남았다.

## 업계 비교

| 방식 | 승인 | 창 밖에서 승인하면 | 분석 단계 |
|---|---|---|---|
| Argo Rollouts | `pause` 단계 + `kubectl argo rollouts promote`(RBAC) | 없음(사람이 시각을 맞춘다) | AnalysisRun 이 지표로 통과·중단 |
| **Spinnaker** | Manual Judgment 단계(인증·기록) | **Restricted Execution Window: 창이 열릴 때까지 기다렸다 돈다** | 단계별 check, Kayenta canary |
| GitHub Environments | required reviewers(SSO 신원·감사 로그) | wait timer, 거부하지 않음 | 작업이 곧 검사 |

공통점: 승인은 **신원**이 하고(비밀번호를 서버에 치지 않는다), 기록이 남고, 시각은 시스템이 지킨다. 창 밖 승인은
거부가 아니라 **대기**다.

## Decision

1. **승인은 운영자 원장의 두 동작이다.** ADR-0282 의 경로 `POST /data-catalog/v1/scheduled-jobs/{job}/operator-actions`
   (Zitadel 직원 신원, 범위 `operations.jobs.operate`, `reason` 필수)에 둘을 더한다. 새 표·새 비밀·새 범위는 없다.
   - `approve_cutover`: 대상은 cut-over 러너 `by_pnu_lane_cutover`(작업 목록의 작업이 아니다). `lanes`(`lanes`, 레인 하나,
     또는 `lane,lane,...` 순서대로)와 `generation`(1–9999)을 적는다. 48시간 뒤 만료된다(창은 하루 한 번 열린다).
     러너는 `approve_cutover` 와 `stop` 만 받는다. `stop` 은 아직 시작 안 한 승인을 취소하고, 도는 run 은 지금 단계 뒤에
     아무것도 시작하지 않게 끝낸다.
   - `force_full`: by-PNU 굽기 작업(`by_pnu_<lane>_bake`)의 레인 **하나**를 다음 run 에서 전량 굽기로 재기준한다
     (`FOUNDATION_BY_PNU_BAKE_FORCE_FULL` + 이유). 7일 뒤 만료된다.
   원장에는 누가·언제·무엇을(레인·세대)·왜가 덧붙이기만으로 남고(트리거가 `lanes`·`generation` 고쳐 쓰기도 거부),
   `switches/operator-actions.log` 에도 한 줄 남는다. 마이그레이션 `20261107090000_...`.
2. **API 는 파일만 쓴다(ADR-0282 §3 그대로).** `switches/cutover.d/<id>`·`switches/full.d/<id>` 에 요청 id·누가·이유·
   만료·레인·세대를 통째로 쓴다. API 는 셸도 sudo 도 갖지 않는다.
3. **호스트가 창에 맞춰 한 번 시작한다.** 이미 root 로 도는 `run-now-dispatch.sh`(`foundation-run-now.path` 가
   `cutover.d` 를 본다)가 승인마다 한 번:
   - 시작 시각은 `job_units.py operator-start <jobs> <지금> <분>` 하나가 정한다. `<분>` 은 러너의 첫 단계 예산
     (`by-pnu-lane-cutover.sh <lanes> <generation> --start-budget`: 첫 레인 gold 단계의 예산, 시간 안 잰 레인은 단위 한도)
     이다. 운영자 창(ADR-0425 의 `scheduled_window()` 밖) 안이고 남은 시간이 그만큼이면 **지금**, 아니면 **다음 창이 열리는
     시각**이다. 거부하지 않는다. 그 길이가 창보다 길면 언제도 못 들어가므로 그때만 `refused` 로 남긴다.
   - 지금이면 바로, 나중이면 systemd 일회성 타이머(`systemd-run --on-calendar=<그 시각>`, 단위
     `operator-cutover-<id>`)로 `by-pnu-pack-operator.sh <lanes> cutover <generation> --approved <id>` 를 부른다.
4. **승인이 곧 '사람이 지금 요청함'이다(ADR-0270 §2b 대체 증명).** 운영자 스크립트는 `--approved <id>` 를
   `--requested-at`·`--reason`·터미널 대신 받는다. 단, sudo 로 불리지 않았을 때(`SUDO_USER` 없음: 사장의 허가로는 못 씀)이고,
   `cutover.d/<id>` 가 같은 레인·세대를 적고, 만료 전이며, 처음 쓰일 때(자기 상태 폴더에 배타적 표시)만이다. journal
   `foundation-operator-start` 에 누가·왜·요청 id 가 남는다. 재시도는 없다: 단계가 실패하면 run 이 멈추고 승인은 쓰였다.
   다시 하려면 다시 승인한다. 10-16 의 nohup 대기 스크립트와 다른 점: 대기가 원장에 선언되고, 화면에 보이고, 만료되고,
   취소되고, 한 번만 시작한다.
5. **전량 굽기는 승인으로만.** `by-pnu-serving-bake.sh <lane>` 은 시작할 때 `full.d/` 에서 자기 레인의 만료 전·안 쓴
   요청 가운데 가장 새것을 읽어 `mode=full`, 이유 "`<reason>` (console request `<id>` by `<actor>`)" 로 굽고, 그 고름을 자기
   상태 폴더에 표시해 다시 쓰지 않는다. 덜 끝난 전량은 ADR-0310 의 핀으로 이어 가므로 요청이 다시 필요 없다. drop-in 과
   환경 파일의 `FORCE_FULL` 줄은 runbook 에서 은퇴한다(시험용 환경 변수는 남는다).
6. **진행과 결과는 승인 id 하나로 읽는다.** 디스패처와 러너가 `/var/lib/foundation-jobs/releases/<id>.jsonl` 에 줄을
   덧붙인다(예약됨·시작·단계마다 러너의 기록 그대로·끝). API 는 그 폴더를 읽기로만 마운트해 승인 목록과 상태를 돌려주고,
   더니어 **데이터 흐름 → 운영** 에 승인 단추(레인·세대·이유)와 진행이 보인다.
7. **관문은 그대로다.** history·preview·gold·parity·bake·equality·latency·publish·monitor-sample 이 분석 단계이고(Argo 의
   AnalysisRun 자리), 러너의 창 확인·Spark 대기도 그대로다. 이 ADR 은 누가·언제 시작하는가만 바꾼다. ADR-0282 결과에 "남는 일: 묶음 전환 같은 운영자 스크립트의 나머지 동작" 으로 적힌 것이 이것이다.

## 버린 대안

- **GitHub Actions environment 의 required reviewers 로 승인하고 self-hosted runner 가 실행.** 승인 UI 는 거저지만 운영 호스트에
  CI 실행을 들이는 것이 ADR-0420 이 막은 길이다. 신원도 GitHub 계정이 되어 Zitadel 직원 범위와 두 갈래가 된다.
- **cut-over 를 jobs 목록의 작업으로(run-now 로 시작).** 세대·레인 인자가 없고, run-now 는 10분 만료라 "다음 창" 을 못 담는다.
  DAG·가드·파이프라인 그래프 간선이 붙는데 cut-over 는 데이터를 만드는 작업이 아니라 출시 행위다.
- **창 밖 승인은 거부.** 11-06 의 03:46 과 같다: 사람이 다시 깨어 있어야 한다. Spinnaker 도 기다린다.
- **디스패처가 창이 열릴 때까지 루프로 기다림.** ADR-0270 §2b 가 금한 대기 루프다. 일회성 타이머는 systemd 가 들고 있고
  `systemctl list-timers` 에 보인다.
- **API 에 sudo 경로.** 비밀번호·키를 API 나 노트북에 두게 된다. 파일과 이미 root 인 path 단위로 충분하다.

## Consequences

- 사장 손: `ssh`·`sudo` 비밀번호·drop-in 0. 단추 한 번(+ 이유). 11-06 재현(계산값): 00:37 승인 → 그 자리에서 시작(창 안,
  남은 233분 ≥ 60분), 03:46 승인 → 거부 대신 21:50 UTC 에 시작, 00:24 drop-in → `force_full` 승인 한 줄.
- 일회성 타이머는 호스트 재부팅에 사라진다. 디스패처는 승인마다 "예약됨인데 타이머가 없다" 를 보고 다시 예약하므로, 다음
  스위치 변경 때 복구된다. 재부팅 직후 자동 복구는 남은 일이다.
- 단계: (a) 원장·API (b) 호스트: 디스패처·`operator-start`·`--approved`·`--start-budget`·굽기의 `full.d`·진행 파일과
  읽기 API (c) 더니어 화면. 앞 단계에 기대는 PR 은 draft.
- 비용: 돈 0. 시작마다 파이썬 한 번(jobs 목록 읽기).

---

개정 각주(2026-11-07): §3 의 "운영자 창에 맞춘 시작"(`job_units.py operator-start`, 일회성 타이머)은
[ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)
가 대체했다: 승인된 cut-over 는 바로 시작하고 spark 풀을 claim 해 용량을 기다린다. 레인의 첫 발행은 승인 없이 자동 승격이
시작하므로, 승인은 이미 cut-over 된 레인의 재전환·전량 굽기(`force_full`)·중지 같은 예외 동작에만 남는다.
