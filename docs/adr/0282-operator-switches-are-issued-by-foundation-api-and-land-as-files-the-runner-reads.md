# ADR 0282: 운영자 스위치는 foundation-api 가 직원 신원으로 내고, 러너가 읽는 파일로 남는다

- Status: Proposed
- Date: 2026-10-19
- Relates to: [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md) §2 운영자 스위치(이 ADR 이 "누가 파일을 쓰나"를 바꾼다), [ADR-0260](./0260-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md)(결과 어휘)

## Context

ADR-0270 §2 는 작업마다 `pause`·`pause --drop-pending`·`resume`·`run-now`·`stop` 스위치를 두고, 스위치는
`/var/lib/foundation-jobs/switches/<job>` 파일, 쓰는 쪽은 운영자 그룹, 모든 조작은 `switches.log` 에 덧붙이기만 한다고 정했다.
그 뒤 두 사고가 "누가 쓰나"를 다시 묻게 했다.

- 2026-10-16: 에이전트가 두고 간 nohup 대기 스크립트가 10시간 뒤 운영 Silver 쓰기를 시작했다. #736·#757 이 운영자 스크립트에
  요청 시각(10분)·이유·터미널·저널 기록을 붙였지만, 사람의 터미널에서 `&` 로 띄운 루프가 실행 때 시각을 새로 찍으면 통과한다.
  셸을 가진 쪽이 쓰는 파일은 같은 틈을 갖는다: 운영자 그룹에 든 계정의 스크립트는 언제든 파일을 쓸 수 있다.
- 2026-10-18: 운영자가 띄운 일회성 작업을 Airflow 가 몰라 같은 대기열 자리에 FLOOR 를 띄웠다(#732).

우리에게 이미 있는 것:

- foundation-api 는 직원 쓰기 경로를 `protected_route(post(..), 자원, 범위)` 로 신원 확인 뒤에만 연다(계보 검토의 claim·decide,
  [lineage_review.rs](../../platforms/foundation-platform/services/foundation-api/src/routes/lineage_review.rs)). 로그인은 사람이
  브라우저에서 하는 일이다.
- foundation-api 는 같은 호스트에서 돌고, 운영 지도 때문에 Airflow API 계정을 이미 가진다(`operations-map` 그룹).
- 시작 문(`start-scheduled-job.sh`)은 Airflow 가 SSH 로 부르는 유일한 입구다.

## Decision

1. **발급자는 foundation-api 하나다.** `POST /v1/operations/jobs/{job}/{pause|resume|run-now|stop}`(본문: `reason` 필수, `pause` 에
   `drop_pending`)은 직원 범위 `operations.jobs.operate` 를 가진 신원만 연다. 서버 셸·sudo·운영자 그룹 쓰기는 없다(ADR-0270 §2 의
   "운영자 그룹이 쓴다"를 대신한다). 운영자 스크립트(`by-pnu-pack-operator.sh job|silver start`)는 이 경로가 생기면 은퇴한다.
2. **기록은 덧붙이기만 한다.** 모든 요청은 Postgres 표 `operator_action`(id·job·action·who·reason·requested_at·expires_at·
   consumed_at·outcome)에 한 줄로 들어가고, 고치는 것은 `consumed_at`·`outcome` 을 한 번 채우는 것뿐이다(트리거로 그 밖의 UPDATE·
   DELETE 거부). 같은 내용을 호스트의 `/var/lib/foundation-jobs/switches.log` 에도 한 줄 덧붙인다.
3. **스위치는 파일로 남는다(ADR-0270 그대로).** API 컨테이너는 `/var/lib/foundation-jobs/switches` 만 쓰기로 마운트하고
   `switches/<job>` 에 `paused`(+이유) 를 쓰거나 지운다. 시작 문과 러너는 파일만 읽는다: API 가 죽어도 작업은 지금 상태대로 돈다.
   `pause` 된 작업의 회차는 시작하지 않고 `skipped paused` 로 남는다(결과 어휘에 `skipped` 추가, ADR-0260).
4. **`run-now` 는 한 번짜리, 10분 만료다.** API 는 `switches/requests/<id>`(job·who·reason·expires_at)를 쓰고 Airflow 에 그 작업의
   DAG 실행을 conf `{"operator_request": "<id>"}` 로 연다. 시작 문은 `<job> requested <id>` 로 불려 그 파일을 원자적으로
   `requests/consumed/<id>` 로 옮긴 뒤에만 시작한다. 이미 옮겨졌거나, 만료됐거나, 다른 작업의 것이면 시작하지 않고 `skipped` 로
   남긴다. 대기열·빌드 기다림(`wait_limit_minutes`)은 일반 시작과 같고, 기다리는 동안 만료되면 시작하지 않는다.
5. **`stop` 은 도는 회차만 멈춘다.** API 는 `switches/<job>` 에 `stop <invocation>` 을 쓰고, 러너(ADR-0270 §3)가 그 회차를 끝내고
   `failed stopped by <who>` 로 보고한다. 다음 회차는 막지 않는다.

## Rejected

| 대안 | 기각 이유 |
|---|---|
| 운영자 그룹이 셸로 파일을 쓴다(ADR-0270 §2 원안) | 셸을 가진 스크립트는 언제든 쓴다: 10-16 사고와 같은 틈 |
| 시작 문이 시작마다 API 에 묻는다 | API 가 죽으면 모든 작업이 멈춘다. 파일은 API 없이 읽힌다 |
| Airflow DAG 정지로 pause | Airflow 밖에서 시작한 회차를 못 막는다(ADR-0270 §2), 배포가 DAG 를 멈추고 푼다(충돌) |

## Consequences

- 사람이 브라우저 로그인으로만 작업을 멈추고·켜고·한 번 돌린다. 나중에 실행되는 스크립트가 쓸 수 있는 통로가 없다.
- 단계: (a) `operator_action` 표·마이그레이션(운영 표 모양 원장 시험) (b) API 경로와 범위 (c) 시작 문의 `paused`·`requested <id>`
  (d) 러너의 `stop` (e) 더니어 화면의 단추 (f) 운영자 스크립트의 `job`·`silver start` 은퇴. 작은 PR 로, 앞 단계에 기대는 것은 draft.
- 범위 `operations.jobs.operate` 를 누가 갖는지는 사장 결정이다(신원 플랫폼의 직원 역할).
- 남는 일: 묶음 전환 같은 운영자 스크립트의 나머지 동작은 이 ADR 밖이다(#757 규칙이 계속 지킨다).
