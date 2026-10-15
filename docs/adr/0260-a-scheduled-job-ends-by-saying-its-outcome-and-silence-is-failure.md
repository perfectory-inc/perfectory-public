# ADR 0259: 예약 작업은 결과를 말하고 끝나며, 말하지 않은 실행은 실패다

- Status: Proposed
- Date: 2026-10-16
- Builds on: [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(작업 = systemd 유닛, 호스트의 한 진입점),
  [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)(`foundation-job-outcome changed|unchanged` 줄),
  [ADR-0195](./0195-dawneer-shows-every-jobs-health-on-the-data-flow-map.md)(지도의 신호),
  [ADR-0208](./0208-the-data-flow-maps-problems-reach-slack-through-the-existing-alertmanager.md)(지도 → Alertmanager)
- Amends: ADR-0171 §3("말하지 않은 실행은 바뀜으로 센다")

## Context

매일 도는 작업의 성공·실패는 지금 **systemd 의 종료 결과 하나**로 정해진다. `start-scheduled-job.sh` 가
`Result=success` 면 0, 아니면 1 로 끝나고, Airflow 의 `run` 작업이 그 값을 받아 성공·실패가 되며, 더니어 지도와
Alertmanager 는 그 Airflow 결과를 읽는다. ADR-0171 의 결과 줄은 하류를 깨울지만 정하고, 줄이 없으면 "바뀜"으로 읽는다.

그래서 **스크립트가 0 으로 끝나기만 하면 무엇을 했든 성공**이다. 하루(2026-10-15~16)에 다섯 번 드러났다.

| 일 | 무엇이 거짓이었나 |
|---|---|
| #589 | 관문이 죽었는데 굽기가 계속됐다 |
| 운영 드라이버 | 여섯 중 하나가 멈췄는데 "여섯 모두 전환" 을 찍었다 |
| #591 | "새 판이 계약을 기다림" 이 실패(빨강)로 보였다 — 기다림을 말할 자리가 없어 종료 코드 3 을 빌렸다 |
| #597 | 함수 안의 실패를 ERR trap 이 못 잡아 실패가 남지 않았다 |
| 유닛 11개 | 실패한 유닛이 몇 시간 아무도 모른 채 있었다 |

공통점: **작업이 "끝까지 해냈다" 를 스스로 말하지 않아도 성공으로 셌다.** 중간에 빠져나온 0, 조용히 건너뛴 단계,
기다림과 실패의 구분이 모두 같은 0 이었다. 31개 작업 중 7개는 결과 줄을 아예 찍지 않는다.

## Decision

1. **결과는 한 줄, 한 어휘다.** 모든 예약 작업은 실행의 마지막 동작으로 다음 줄 하나를 찍는다.

   ```text
   foundation-job-outcome changed
   foundation-job-outcome unchanged
   foundation-job-outcome waiting <이유>
   foundation-job-outcome failed <이유>
   ```

   - `changed`·`unchanged`: 성공. 하류를 깨우는지는 ADR-0171 그대로다.
   - `waiting <이유>`: 실패가 아니다. 이 실행은 할 일을 끝냈고, 다음 단계는 사람이나 다른 작업을 기다린다
     (예: 새 판이 계약 항목을 기다림). 바꾼 것이 없으므로 하류를 깨우지 않는다(`unchanged` 와 같다).
   - `failed <이유>`: 실패. 종료 코드가 0 이어도 실패다.
   - `<이유>` 는 한 줄 값이다. 화면 문구는 읽는 쪽이 만든다.

   새 저장소·새 전송은 없다. 이 줄은 이미 유닛 저널 → `start-scheduled-job.sh` → Airflow 작업 로그 → 지도의 원인 한 줄
   (`facts.rs`) 로 흐르는 바로 그 줄이다.

2. **러너가 판정한다.** `start-scheduled-job.sh` 는 유닛이 끝나면 **그 실행(InvocationID)의 저널**에서 마지막 결과 줄을
   읽어 다음과 같이 끝난다.

   | systemd | 결과 줄 | 러너 |
   |---|---|---|
   | success | `changed`·`unchanged`·`waiting` | 0 |
   | success | `failed` | 1 |
   | success | **없음** | **1** — `foundation-job-outcome failed no outcome reported` 를 찍는다 |
   | 그 밖 | 무엇이든 | 1 |

   러너는 판정한 줄을 자기 마지막 줄로 다시 찍는다. 저널을 따라가던 출력이 끝 줄을 놓쳐도 DAG 는 러너의 줄을 읽는다.
   그래서 ADR-0171 §3 의 "말하지 않으면 바뀜" 은 러너가 성공으로 끝낸 실행에서는 생기지 않는다.

3. **실패는 이미 있는 길로 알린다.** 러너가 1 이면 Airflow `run` 이 실패하고, 지도가 실패와 원인 한 줄을 보이며,
   ADR-0208 의 `FoundationScheduledJobProblem` 이 Slack 에 간다. 새 알림 경로는 만들지 않는다. `waiting` 은 실패가
   아니므로 빨강이 되지 않는다. 지도가 기다림과 이유를 보이는 일은 이 결정의 후속 단계다(지도는 지금 성공한 실행의
   로그를 읽지 않는다).

4. **시험이 새 작업을 거부한다.** `orchestration/tests/test_job_specs.py` 가 `jobs.v1.json` 의 모든 작업에 대해 그 유닛이
   실행하는 진입 스크립트가 결과 줄을 찍는지 본다. 찍지 않는 작업을 넣는 PR 은 CI 에서 실패한다. 진짜 강제는 2항의
   러너다. 이 시험은 운영에서 실패하기 전에 PR 에서 잡는 그물이다.

5. **순서.** ① 결과 줄 어휘를 받는 쪽(`job_specs.job_outcome`, DAG)과 결과 줄이 없는 작업 7개가 줄을 찍게 한다.
   ② 그 뒤에 러너의 판정(2항)과 시험(4항)을 켠다. 거꾸로 하면 첫날 7개 작업이 빨강이 된다.

## Rejected

- **종료 코드에 뜻을 더 싣기**(#591 의 `SuccessExitStatus=3` 처럼): 작업마다 다른 번호가 생기고, 0 으로 빠져나온 중간
  종료는 여전히 성공이다. "끝까지 했다" 는 적극적인 말이 필요하다.
- **결과 파일(상태 디렉터리의 JSON)**: 운영자가 읽을 수 없는 디렉터리이고(ADR-0174), 지도·Airflow 가 읽는 길이
  새로 필요하다. 저널 줄은 이미 모두가 읽는다.
- **지도 쪽에서 판정**: 지도는 사후에 읽는다. Airflow 가 성공으로 끝낸 실행을 지도만 빨강으로 보이면 하류는 이미
  깨어난 뒤다. 판정은 실행이 끝나는 자리, 러너에서 한다.
- **결과 줄이 없으면 경고만**: 오늘의 문제가 바로 "조용한 성공" 이다. 말하지 않은 실행은 실패다.

## Consequences

- 작업 스크립트는 중간에 `exit 0` 으로 빠져나오는 모든 자리에서 결과 줄을 찍어야 한다. 빠뜨리면 그 실행은 실패로
  보인다 — 그것이 의도다. 거짓 성공 대신 시끄러운 실패를 고른다.
- `waiting` 이 생겨 기다림을 실패로 빌려 쓰지 않는다. vworld 판 수집의 종료 코드 3 은 `waiting` 줄로 바꿀 수 있다.
- 손으로 시작한 유닛(`systemctl start`)은 러너를 거치지 않으므로 이 판정 밖이다. 유닛 실패 알림(`OnFailure`)은 그대로다.
