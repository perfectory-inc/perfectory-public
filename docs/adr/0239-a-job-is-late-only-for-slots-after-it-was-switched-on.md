# ADR 0239: 예약 작업은 켜진 뒤의 회차에 대해서만 늦는다 — 작업 목록이 켜진 시각을 적는다

- Status: Accepted
- Date: 2026-10-14
- Amends: [ADR-0195](./0195-dawneer-shows-every-jobs-health-on-the-data-flow-map.md)(늦음 신호)

## Context

#539 가 Silver 레인 다섯을 켜고 배포된 직후(2026-10-14), 운영 지도가 다섯 모두를 `late`("10-13 16:00 KST 에 돌았어야 하는데
아직 성공 못 했어")로 올렸고 슬랙이 울릴 참이었다. 거짓 경보다:

- 늦음 규칙(`health.rs` `late`)은 "시간 상한이 지난 가장 최근 예약 시각 뒤에 성공이 없다"이다. 켜진 시각을 모른다. 그래서 하루
  한 번 도는 작업을 켜면 첫 예약 시각까지 거의 하루를 "늦음"으로 보인다.
- Airflow 의 `CronTriggerTimetable`(catchup 없음)은 켤 때 지난 회차를 돌리지 않는다 — 놓친 회차가 아니라 맡은 적 없는 회차다.
- 켜기 전 감독 실행은 운영자 통로(`systemctl start`)로 돌아 Airflow 실행 기록이 없다.
- 배포는 매번 모든 DAG 를 멈췄다 켠다(ADR-0159). Airflow 의 unpause 시각은 "켜진 시각"이 아니다.

## Decision

1. 작업 목록 `orchestration/jobs.v1.json` 의 켜진 작업은 `enabled_since`(UTC, `YYYY-MM-DDTHH:MM:SSZ`)를 적는다. 꺼진 작업은 적지
   않는다. 켜는 PR 이 병합 시각을 적는다. `job_specs.py` 가 어긋나면 거부한다.
2. 늦음은 예약 시각이 `enabled_since` 이상인 회차만 센다(`late`, foundation-api). 첫 회차는 그대로 빚이다.
3. 지금 켜진 작업의 값은 git 이력에서 그 작업이 처음 켜진(그 뒤로 꺼지지 않은) 커밋 시각으로 채웠다.
4. 감독 실행을 실행 기록으로 남기는 길은 택하지 않았다: 실행 기록의 정본은 Airflow·DataHub 이고, 운영자 실행을 거기 섞으면 "예약이
   돌았다"는 뜻이 흐려진다.

## Consequences

- 막 켠 작업은 첫 예약 시각과 그 시간 상한이 지날 때까지 늦음이 아니다. 그 뒤에는 이전과 같다.
- 병합과 배포 사이에 예약 시각이 끼면 그 회차는 늦음으로 보일 수 있다(자동배포는 보통 수십 분).
- 시험: `health.rs` `a_job_is_not_late_for_a_slot_from_before_it_was_switched_on`(옛 규칙에서 실패),
  `test_job_specs.py` `test_an_enabled_job_says_since_when_and_a_disabled_one_does_not`.
