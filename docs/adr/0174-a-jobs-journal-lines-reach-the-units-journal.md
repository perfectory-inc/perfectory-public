# ADR 0174: 작업의 기록 줄과 실패한 실행 로그의 끝은 유닛 저널에 간다

- Status: Accepted
- Date: 2026-10-10
- Builds on: [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(작업 = systemd
  유닛, Airflow 는 그 실행의 저널을 받아 본다), [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)
  (마지막 `foundation-job-outcome` 줄)

## Context

여러 등록 작업은 자기 상태 디렉터리 `/var/lib/foundation-platform/<작업>/` 에 `journal.log`(실행마다 한 줄)와 실행
로그(`last-run.log` 또는 `runs/<시각>/run.log`)를 쓰고, 그 내용은 표준 출력에 내지 않았다. 그 디렉터리는 서비스 계정
`foundation-platform` 의 것이라 운영자 계정이 읽지 못한다. 2026-10-09 매일 훑기가 실패했을 때 `journalctl -u
foundation-source-sweep.service` 에는 "exit 1" 밖에 없었고, 이유(밀린 파일이 하루 예산을 넘음)를 알려고 서버 주인에게
`sudo tail` 을 부탁해야 했다.

전수 조사(작업 목록 `orchestration/jobs.v1.json` 의 유닛 전부와 그 스크립트가 부르는 것):

| 스크립트 | 이전 |
|---|---|
| `daily-source-sweep.sh`, `map-edit-fold.sh`, `lineage-stewardship-cycle.sh`(+ `legal-dong-code-load.sh`), `legal-dong-code-collect.sh`, `parcel-number-change-collect.sh`, `vworld-parcel-edition-collect.sh` | 기록 줄과 실패 이유가 파일에만 |
| `raon-large-files.sh` | 이미 줄을 출력함(훑기가 그 출력을 실행 로그로 받음) |
| `silver-refresh.sh`, `gold-panel-rebuild.sh`, `by-pnu-serving-bake.sh`, `building-register-floor-cycle.sh`, `publish-outbox.sh`, `data-quality-check.py` | 이미 출력하거나 실패 시 로그 끝을 stderr 로 냄 — 바꾸지 않음 |

## Decision

1. **유닛 저널이 기록의 정본이다.** 작업이 자기 기록 파일에 쓰는 줄은 같은 내용을 표준 출력에도 낸다. 파일은 그대로
   둔다 — 런북과 뒤 단계가 읽는다.
2. **실패한 실행은 실행 로그의 끝을 stderr 로 낸다.** 마지막 40 줄, 줄마다 `"  | "` 를 앞에 붙이고(그래서 넘겨준 줄이
   ADR-0171 의 `foundation-job-outcome` 줄로 읽히지 않는다) 400 자에서 자르며, URL 안의 자격증명·Bearer 토큰·
   `*SECRET*`/`*PASSWORD*`/`*TOKEN*`/`*ACCESS_KEY*` 대입은 가린다. 성공한 실행은 요약 줄만 낸다: 실행 로그에는 받은
   파일마다 한 줄이 있고, Airflow 의 SSH 는 그 실행의 저널을 그대로 받아 본다.
3. **한 가지 방법만 있다.** `scripts/ops/job-journal.sh` 의 `job_journal <파일> <줄>` 과 `job_run_log_tail <실행
   로그>` 이다. 둘 다 실패하지 않는다(ERR trap 안에서 돈다). 매일 훑기는 실패한 레인과 그 이유(슬랙에 보내는 것과 같은
   글)도 stderr 로 낸다.
4. **시험이 지킨다.** `orchestration/tests/test_job_journal.py` 가 도우미를 돌려 보고(파일+표준 출력, 가림, 접두,
   40 줄), `${journal}` 을 쓰는 모든 `scripts/ops/*.sh` 가 도우미를 source 하고 기록 파일에 직접 쓰지 않으며 실행
   로그가 있으면 실패 시 그 끝을 내는지 본다. 작업별 명령 시험(훑기·필지 번호 변동·법정동 수집/적재)은 실패 이유와 기록
   줄이 출력에 있는지 본다. 도우미가 출력을 안 내거나, 가리지 않거나, 훑기가 로그 끝을 안 내거나, 접기가 파일에 직접
   쓰게 바꾸면 시험이 깨진다(확인함).
5. 권한은 넓히지 않는다. `/etc/foundation-platform` 의 환경 파일도, 상태 디렉터리도 그대로다. 운영자가 저널을 읽는
   것은 `adm` 또는 `systemd-journal` 그룹이어서다(Ubuntu 의 첫 계정은 `adm` 이다). 그 그룹이 아니면 `usermod -aG
   systemd-journal <계정>` 이 길이고, 이것은 저장소가 아니라 서버의 사실이다 — 서버에서 `id <계정>` 으로 확인한다.

## Consequences

- `journalctl -u <유닛>` 하나로 실패 이유를 본다. 런북의 `sudo tail /var/lib/...` 은 `journalctl` 로 바뀌었다.
- 실행 로그의 끝이 저널로 가므로, 거기 찍히는 것은 저널을 읽는 계정에게도 보인다. 가림은 알려진 모양만 막는다 —
  publisher 와 Spark 가 비밀값을 찍지 않는다는 기존 규칙이 여전히 1차 방어다.
- `set -E` 인 스크립트는 함수 안 실패에서 ERR trap 이 여러 번 돌 수 있고, 그만큼 같은 줄이 여러 번 나온다(이전에도
  파일에는 그랬다).

## 개정 기록

- 2026-10-10: 도우미에 셋째 함수 `job_failed_files` 가 생겼다. 2026-10-10 매일 훑기가 VWorld `failed=3` 을 냈지만
  어느 파일이 왜 실패했는지는 root 만 읽는 증거 JSON 에만 있었다. 이제 실패한 레인이 증거에 실패 파일을 적었으면 훑기가
  레인마다 20 줄까지 `failed <원천>:<파일> <이유>` 를 stderr 로 낸다(넘으면 `failed <레인>:+<n> …` 한 줄). 이유는
  2 항과 같은 가림 표(도우미 안 한 곳)로 가린 **뒤** 200 자로 자른다 — 먼저 자르면 반쪽 비밀이 가림을 빠져나간다.
  `failed ` 로 시작하므로 `foundation-job-outcome` 줄로 읽히지 않는다. 시험은 `test_job_journal.py` 와
  `test_daily_source_sweep_command.py` 이고, 가림·출력·20 줄 상한을 빼면 깨진다(확인함). 위 결정 본문은 고치지 않았다.
