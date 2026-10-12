# ADR 0195: 더니어 데이터 흐름 지도는 작업마다 다섯 신호로 건강을 보이고, 맨 위 한 줄이 문제를 센다

- Status: Accepted
- Date: 2026-10-12
- Amends: [ADR-0188](./0188-dawneer-shows-the-data-flow-as-one-graph-with-live-job-state.md)
  (칸의 상태와 그 순위, 옆 패널), [ADR-0165](./0165-dawneer-shows-operations-status-from-the-data-catalog-and-the-job-list.md)
  결정 2(`GET /data-catalog/v1/scheduled-jobs` 의 칸)

## Context

ADR-0188 의 데이터 흐름 화면은 작업이 "지금 도는지·끝났는지·실패했는지"만 보인다. 사장의 요구는 "모든 작업이 정말
잘 돌고 있는지 한눈에, 문제는 바로 찾기"다. 지금 화면으로는 다음을 알 수 없다:

- 실패했다면 **왜**(메모리 부족인지, 원천 파일이 없는지). 이유는 Airflow 의 작업 로그에만 있다.
- 돌았어야 할 시각이 지났는데 **안 돈** 것(늦음). 실패가 아니라 "아무 일도 안 일어남"이라 빨간색이 안 된다.
- 평소보다 **오래 걸린** 것(느림), 평소의 절반만 **쓴** 것(양 이상).
- 앞 단계가 실패해서 **입력이 오지 않을** 작업(막힘).

확인한 것(2026-10-12 운영 데이터, 터널로 읽음):

- 무엇을 언제 돌리나: `orchestration/jobs.v1.json` 의 `schedule`·`timeout_minutes`. 누가 누구를 먹이나:
  `job-dependencies.v1.json` 의 `producers`(ADR-0171). 실행 기록: DataHub 의 실행(ADR-0165, 작업마다 최근 10개).
- 실행이 쓴 행 수: 각 Iceberg 표의 `main` 스냅숏 요약 `added-records`. Gold 재생성은 읽은 Silver 스냅숏을
  `foundation.source-iceberg-snapshots` 로, Silver 적재는 넣은 원천 파일을 `foundation.ingest-batch-objects` 로
  커밋에 적는다(ADR-0130, ADR-0084). 즉 "어느 스냅숏이 어느 스냅숏을 읽었나"는 이미 기록돼 있다.
- 실패 이유: Airflow 의 `run` 태스크 로그. 호스트의 forced command 가 작업 출력 앞뒤에
  `start-scheduled-job: … invocation=`·`… result=<systemd Result>` 를 찍는다. 실제 로그 셋에서 읽은 이유:
  `FAILED: building_panel_silver_to_gold refused or crashed`, `refused: another lane holds …refresh.lock`,
  `failed lanes: vworld`.
- Airflow 는 직원 로그인이 Zitadel 뿐이지만, `POST /auth/token` 은 FAB DB 사용자의 비밀번호를 받는다
  (`auth_user_db`). Viewer 역할 계정 하나면 실행과 로그를 읽고 아무것도 못 바꾼다.
- 운영 foundation-api 컨테이너에는 레이크하우스 카탈로그 설정도 Airflow 주소도 없다.
- R2 카탈로그는 한 번도 안 쓴 표의 메타데이터에서 `current-snapshot-id` 와 `snapshots` 를 뺀다. 공용 파서가 이를
  거부해 `silver.map_edit_ledger` 를 "응답 없음"으로 보고했다.

### 벤치마크

| 도구 | 본 것 | 가져온 것 |
|---|---|---|
| Marquez(OpenLineage 참조 구현) | 데이터셋을 누르면 버전(스냅숏) 목록, 각 버전을 만든 실행, 실행 목록(시작·걸린 시간·결과) | 옆 패널의 실행 표와 스냅숏 목록, "이걸 읽은 것/읽은 것" |
| Dagster asset health | 자산마다 신선도(freshness policy)·검사·실행 상태를 합친 한 색 | 다섯 신호를 한 칸 색으로, 맨 위 "문제 N건" |
| Monte Carlo / 데이터 관측 도구 | 볼륨 이상(평소 대비 행 수), 신선도 이상 | 양 이상 = 최근 실행 중앙값 대비 ½ 이하 또는 2배 이상 |
| Airflow SLA miss | 예약 시각 + 허용 시간 뒤 성공이 없으면 늦음 | 늦음 = 마지막 예약 시작 + `timeout_minutes` 가 지났는데 그 뒤 성공 없음, 단 Airflow 에 대기·실행 중인 실행이 없을 때 |

## Decision

1. **다섯 신호는 Foundation 이 계산하고 `ScheduledJobStatus.problems` 로 내보낸다.** 나쁜 것부터:
   - `failed` — 마지막 실행 실패(또는 재시도 대기). Airflow 로그에서 원인을 `out_of_memory`·`timed_out`·
     `source_missing`·`other` 로 가르고, 그 줄을 비밀값을 가린 채 240자로 싣는다. 가리는 규칙은
     `scripts/ops/job-journal.sh` 의 것에 더해 띄어 쓰거나 따옴표로 감싼 값(`"password": "x"`,
     `aws_secret_access_key = x`), 명령줄 옵션(`--password x`), `Authorization: Basic x` 도 잡는다.
     systemd `result=` 가 `oom-kill`·`timeout` 이면 그것이 원인이다. 아니면 작업 자신의 `FAILED`/`refused` 줄,
     없으면 마지막 오류 줄. Spark 셔플 파일의 "No such file" 은 원천 없음이 아니다.
   - `blocked` — `upstream_job_ids` 를 따라 올라가 켜진 작업 중 실패했거나 막힌 것이 있으면. 막힌 작업은 늦음으로
     또 세지 않는다.
   - `late` — 켜진 작업의 마지막 예약 시작이 `timeout_minutes` 전보다 앞서는데 그 뒤 성공이 없고, Airflow 가 그
     DAG 의 실행을 대기(`queued`, 또는 `running` 인데 태스크가 풀 자리를 기다림)·실행 중으로 들고 있지도 않으면.
     Spark 풀은 한 자리라 접기 작업은 설계상 최대 1,200분까지 기다린다(`job_specs.py` 의 굶주림 상한); 그 기다림은
     늦음이 아니다. 실행 기록을 못 읽었으면 아무것도 늦었다고 하지 않는다. Airflow 를 못 읽었으면 직전에 읽은 대기 목록을
     쓰고, 그것도 없으면 예약 시각으로 판정하며 `scheduler_error` 가 그 사실을 말한다.
   - `slow` — 마지막 실행(도는 중이면 지금까지)이 중앙값의 1.5배를 넘고 중앙값 + 10분 이상. 성공 3회 이상일 때만.
   - `volume` — 쓰는 Iceberg 표마다, 행을 쓴 최근 성공 실행의 행 수가 그 전 실행들 중앙값의 ½ 이하이거나 2배 이상.
     비교 대상이 3회 이상일 때만. 실행의 행 수 = 그 실행의 시작~끝 사이에 커밋된 `main` 스냅숏의 `added-records` 합
     (`ScheduledJobRun.rows_written`). **같은 종류의 실행끼리만 비교한다**: 표 전체를 다시 쓴 실행(커밋 하나라도
     `added-records` ≥ `total-records`)은 전체 재작성끼리, 나머지는 증분끼리. 운영 Gold 패널(2026-10-07~12)은 전체
     재작성 5,945,767행 넷 뒤에 증분 병합 47,153행이 왔고 Iceberg 는 둘 다 `operation=overwrite` 라 `operation` 으로는
     못 가른다; 종류를 안 가르면 이런 전환마다 양 이상이 울린다.
   새 작업은 작업 목록에 넣는 것만으로 다섯 신호를 다 받는다. 신호용 목록은 따로 없다.
2. **세 저장소를 한 번 읽어 30초 동안 쓰고, 그 뒤 요청은 들고 있던 것을 바로 주며 뒤에서 다시 읽는다**
   (stale-while-revalidate, `routes/data_catalog/facts.rs`). 표는 그 표를 쓰는 작업이 끝난 뒤나 30분 뒤에만 다시 읽고,
   실패 로그는 실행마다 한 번 읽는다. Airflow 가 답하지 않은 것(못 준 로그, 거절된 로그인)은 10분 동안 다시 묻지 않고
   그 이유를 계속 보인다. Airflow 의 대기·실행 중 실행 목록은 예약 시각으로 늦은 작업이 있을 때만, 모든 DAG 를 GET
   한 번(`/api/v2/dags/~/dagRuns?state=queued&state=running`)으로 읽는다. 로그인은 읽기마다 한 번이다.
3. **`GET /data-catalog/v1/dataset-snapshots?node_id=` 가 데이터셋의 스냅숏을 최신부터 준다**: 언제, 무슨 커밋,
   몇 행, 만든 실행(그 시각에 돌던 쓰는 작업의 실행), 넣은 원천 파일, 읽은 스냅숏(`read_from`), 그 스냅숏을 읽은 다른
   표의 스냅숏(`read_by`). 더니어 릴레이는 이 경로만 더 연다(GET 하나).
4. **화면**: 맨 위 한 줄은 "✓ 전체 정상" 또는 "⚠ 문제 N건"과 문제마다 한 줄(누르면 그 데이터셋 패널이 열리고 초점이
   옮겨 간다). 칸 색은 실패 빨강, 막힘 회색 점선, 늦음 주황, 느림 노랑, 양 이상 보라; 아이콘과 낱말이 함께 있어 색만이
   신호가 아니다. 상태 순위는 ADR-0188 을 바꿔 문제가 "도는 중"보다 앞선다. 옆 패널은 실행 표(시작·걸린 시간·쓴 행·결과),
   스냅숏 목록, Airflow 실패 로그·DataHub 계보 바로가기를 보인다.
5. **설정**: `compose.orchestration-bridge.yml` 이 foundation-api 에 Airflow API 주소를 주고, 커밋하지 않는
   `/etc/foundation-platform/operations-map.env`(`config/runtime-secrets.contract.json` 의 `operations-map`, 선택)가
   Viewer 계정·레이크하우스 카탈로그·두 도구의 웹 주소를 준다. `airflow-runtime.sh api-reader` 가 Viewer 계정을 만든다
   (다시 돌리면 비밀번호를 바꾼다). 파일이 없으면 지도는 그대로 답하며 어느 저장소를 못 읽었는지 말한다.
6. 공용 Iceberg 파서는 `current-snapshot-id` 가 없는 표를 "스냅숏 없음"으로 읽는다.

## Consequences

- 측정(2026-10-12, 운영 저장소를 터널로, 디버그 빌드, 노트북): `scheduled-jobs` 재시작 후 첫 응답 1.9초, 이후
  200회 p50 7ms·p95 8.6ms(30초마다 뒤에서 다시 읽는 동안 포함). `dataset-snapshots` 1,040회 p95 8ms. 실패 로그 한
  건 읽기 약 100ms. 첫 화면 열기만 1.9초이고 그 뒤는 300ms 목표보다 한참 아래다.
- 같은 측정에서 실제 문제 9건이 나왔다: 오늘 07:00 Silver 갱신 7개가 평소 1분 안팎에서 11~12분, 층별 작업이 41초에서
  19분. 모두 같은 시각에 시작해 같은 잠금을 기다린 것으로 보인다 — 지금까지 화면에 안 보이던 사실이다.
- 운영 반영에는 사람 손이 한 번 필요하다: 서버에서 `operations-map.env` 를 만들고(Viewer 계정은 이 결정을 쓰며 이미
  만들어 두었다, `~/airflow-state/operations-map-reader.env`), 카탈로그 토큰을 넣는다. 지금 넣을 카탈로그 토큰은
  쓰기 권한이 있는 것뿐이다. 읽기 전용 R2 카탈로그 토큰으로 바꾸는 것이 남은 일이다.
- 느림의 기준(1.5배 + 10분)과 양 이상의 기준(½·2배)은 `health.rs` 의 상수다. 운영에서 헛경보가 잦으면 그 상수를
  바꾸는 ADR 을 쓴다.
- 문제 신호는 아직 화면에만 있다. 슬랙 경보로 같은 `problems` 를 보내는 것은 다음 일이다.
