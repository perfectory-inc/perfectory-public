# ADR 0500: 공급자가 목록에 올리고 내놓지 않은 파일은 정한 기한까지 다음 스윕을 기다린다

- Status: Accepted
- Date: 2026-11-11
- Amends: [ADR-0363](./0363-a-deploy-judges-the-release-not-the-provider-or-the-archive-bytes.md) Consequences("공급자 응답 없음(75)은
  수집 작업에서 지금처럼 실패다" — 일일 소스 스윕의 VWorld 레인에서는 기한 안이면 실패가 아니다)
- Builds on: [ADR-0168](./0168-the-daily-sweep-collects-the-vworld-land-datasets-within-a-byte-budget.md)(스윕의 VWorld 레인),
  [ADR-0172](./0172-the-daily-vworld-sweep-fetches-everything-it-lacks.md)(파일마다 커밋, 멈춘 실행은 이어 받는다)

## Context

2026-11-11 03:40Z 스윕은 그날 새로 올라온 VWorld 판 243 파일(6.0 GB)을 받았다. 그런데 공인중개사 판 두 파일
(`20171128DS00161`·`00162`, file_no 20)이 두 실행(04:31Z, 04:36Z 재시도) 모두 빈 본문으로 답했다. 수집기는 빈 본문을 우리 쪽
`Infrastructure` 오류로 분류해 `failed` 로 셌고, 레인이 실패하면서 10,979 파일 스윕 전체가 실패로 남았다. 한 번은 같은 실행의
다른 실패(ADR-0441 자식 단계 거부, #1298)와 겹쳤다.

목록에 있는 파일에 200 빈 본문으로 답한 것은 공급자가 그 파일을 내놓지 않은 것이다. 로그인 점검 페이지도 이미 이렇게 분류한다
(`login_server_error_and_maintenance_page_are_the_provider_unavailable`). ADR-0363 은 공급자 응답 없음(75)을 수집 작업에서 실패로 남겼다.
"다음 실행이 다시 받는다"는 이유였는데, 그 다음 실행도 실패로 끝나니 매일 빨간 알림이 울리고, 같은 날 받은 다른 파일의 알림과 섞인다.

큰 배치 수집은 항목 하나의 실패로 배치를 버리지 않는다. 실패한 항목은 따로 두고 다시 시도하며, 정한 횟수나 기한을 넘기면 알린다
(AWS SQS 의 `maxReceiveCount` 와 데드레터 큐, Kafka Connect 의 `errors.tolerance` 와 DLQ).

## Decision

1. **빈 본문은 공급자 측이다.** VWorld 파일 클라이언트는 목록에 있는 파일이 빈 본문으로 오고 RAON 아카이브도 아니면
   `ProviderUnavailable` 로 돌려준다. 수집 실행은 그 파일을 `provider_unavailable` 로 적고, 다른 실패가 없으면 75 로 끝난다(ADR-0363 그대로).
2. **스윕은 그런 파일 때문에 실패하지 않는다.** VWorld 레인의 rc 가 0 또는 75 이고, 받지 못한 파일이 모두 `provider_unavailable` 이면
   레인은 실패가 아니다. 저널 줄에 `unserved=<n>` 을 적고, 슬랙에 안내 한 줄(ℹ️)과 파일 이름을 보낸다. 받은 파일이 있으면 결과는 `changed`,
   없으면 `waiting` 이다. Bronze 에 없는 파일이니 다음 실행이 다시 묻는다.
3. **기한을 넘기면 실패다.** 스윕은 파일마다 처음 못 받은 시각을 상태 디렉터리의 `vworld-unserved.json` 에 둔다. 받으면 지우고, 계속 못
   받으면 처음 시각을 그대로 둔다. 카탈로그의 `daily_collections.source_sweep.unserved_file_fail_after_hours`(72, 사흘의 일일 실행)를
   넘긴 파일이 하나라도 있으면 레인은 실패이고 빨간 알림이 간다. 우리 쪽 실패(`failed`·`provider_acquisition_blocked`)가 하나라도 있으면
   지금처럼 실패다.

## Consequences

- 11-11 재생(시험): 두 파일만 못 받은 실행은 초록이고, 결과는 `changed`(243 파일 받음)다. 다음 실행이 두 파일을 다시 묻는다.
- 계속 오지 않는 파일은 늦어도 72 시간 뒤 실패로 알려진다. 그동안 그 파일의 데이터는 없다(앞의 판은 Bronze 에 그대로 있다).
- 상태 파일 하나가 늘었다(파일 id → 처음 못 받은 시각, 못 받은 파일 수만큼). 비용은 0 이다.
- 시험: `vworld_dataset_file_client.rs::an_empty_answer_to_a_listed_file_that_is_no_raon_archive_is_the_provider_not_serving`,
  `test_daily_source_sweep_command.py` 의 2026-11-11 재생·기한·우리 쪽 실패 셋.
