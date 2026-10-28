# ADR 0321: Gold 커밋은 자기를 만든 코드를 함께 적는다, 그리고 긴 배포 멈춤은 경보한다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0191](./0191-a-release-becomes-a-changelog-once-and-every-layer-reads-only-the-changes.md) §5(빌드
  코드 변경 → `foundation.build-source-sha256`), [ADR-0139](./0139-panel-gold-is-rebuilt-when-a-silver-input-changes.md)(Silver
  핀), [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md)(증분), [ADR-0274](./0274-the-by-pnu-bake-does-only-the-work-a-change-needs.md) §1(내용 지문)
- Closes: ADR-0312 §4(초안 PR #902 의 후속), [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md) §3

## Context

**1. 계획은 Silver 핀만 보고 생산자 코드는 보지 않는다.** `gold_rebuild.plan` 은 Gold 머리의 Silver 핀과 지금
Silver 를 비교한다. 핀이 같으면 `nothing_to_do`, 다르면 증분이다. 생산자 코드가 바뀌어도 핀이 같으면 아무 일도
없고, 핀이 바뀌면 증분이 바뀐 PNU 만 새 코드로 다시 만든다. 나머지 행은 옛 코드의 결과로 남는다. 이것을 잡는 것은
패리티 표본(ADR-0180 §5)뿐이고, 표본 20,000 PNU 가 n 개 PNU 에만 닿는 코드 변경을 잡을 확률은
`1 − (1 − 20000/N)^n` 이다(ADR-0312 Context 4, 계산값): 필지 39.86M 에서 n = 1,000 이면 39%, n = 4,000 이면 87%.

**2. 생산자 코드는 자주 바뀐다.** 2026-09-28…10-28 30일, 필지 생산자의 코드 묶음(아래 §1)을 건드린 커밋은 103개다
(측정, `git log`). 그중 두 공유 등록부가 절반이다: `industrial_complex_lakehouse_contracts.json` 39개(모든 표의 계약),
`r2-connections.contract.json` 38개(섹션 팩 16, 건물 관문 12 — Gold 생산자가 읽지 않는 부분). 파일 통째로 해시하면
코드 판이 30일 중 25일 바뀐다(측정). 그러면 매일 전량이다.

**3. 전량과 증분의 운영 비용(측정, ai-server 저널 10-15…10-26).** 필지 패널 전량 5,555 · 5,648 · 7,040 · 7,503 ·
8,831 초, 증분 3,096 · 8,206 초. 건물 패널 전량 6,427 초, 증분 3,793 초.

**4. 긴 배포 멈춤이 무음이다(ADR-0301 §3 미구현).** 2026-10-26 배포 0단계가 by-PNU bake 를 1.5h 기다리는 동안 모든
DAG 이 멈췄고 수집이 2h 섰는데 아무 경보도 없었다. paused DAG 은 run 이 없어서 unit `OnFailure` 도, 운영 지도의
`FoundationScheduledJobProblem` 도 울지 않는다. 배포 상태를 Prometheus 가 볼 길이 없다(스크레이프 대상은
foundation-api 하나).

## Decision

### 생산자 코드 판 (Bazel action key · Dagster code version · dbt `state:modified`)

1. **코드 판은 생산자의 코드 묶음 해시다.** `infra/lakehouse/spark/jobs/gold_code_version.py`:
   - 생산자 스크립트 + 그것이 import 하는 이 디렉터리의 모듈, 깊이와 위치를 가리지 않고(함수 안 지연 import 포함,
     AST 로 찾는다) + 그 모듈들이 문자열로 이름 대는 JSON 계약(`infra/lakehouse/contracts/`, `config/`).
   - Python 은 docstring 을 뺀 구문 트리(`ast.dump`)로, JSON 은 정규형으로 해시한다. 주석·docstring·서식·키 순서만
     바뀐 것은 코드 변경이 아니다.
   - 값: `{"sha256": 전체, "files": {경로: 해시 앞 16자}}`. 계획이 바뀐 파일을 이름으로 댄다.
   - Bazel 의 action key 가 입력 파일 digest 와 명령을 함께 담는 것, Dagster 가 자산에 `code_version` 을 붙여 바뀌면
     stale 로 보는 것, dbt 가 노드 본문 checksum 으로 `state:modified` 를 고르는 것과 같은 자리다. Dagster 처럼 사람이
     판을 올리게 하지 않는다(잊는다). Bazel 처럼 입력에서 자동으로 계산한다.
2. **좁히기는 계약에 이유와 함께 적는다**(`contracts/gold-panel-rebuild.contract.json` `code_version`):
   - `plumbing_modules`: 행을 바꿀 수 없는 모듈(계획·커밋 작성기, 지문, 구역 검사, Spark 세션 설정, 핀 파일 읽기)과
     이유. 그 모듈과 그것만 import 하는 것은 빠진다. 없는 모듈 이름은 거부한다.
   - `scoped_contracts`: 여러 표가 함께 쓰는 등록부는 이 표가 읽는 항목만 센다. dbt 가 프로젝트 전체가 아니라 노드의
     config 만 비교하는 것과 같다. `tables` 는 Gold 표와 그 입력 표의 계약, `lanes` 는 Gold 나 호스트 패널이 이 표인
     by-PNU 레인. `keep` 은 통째로 세는 부분(값 영역, 건물 관문의 PNU 패턴). 모르는 선택자와 이유 없는 항목은 거부한다.
3. **모든 생산자 커밋이 코드 판을 핀 옆에 적는다**(`foundation.build-source-sha256`, ADR-0191 §5 의 이름). 쓰는 곳은
   `gold_rebuild.pins_properties` 한 곳이고, `write_gold_snapshot`(전량·빈 append)과 증분 MERGE(빈 합치기 포함)가 모두
   거기를 지난다. 생산자 프로세스가 자기 진입 스크립트(`spark <producer>.py`)로 계산하므로, 계획과 커밋이 같은 트리의
   같은 함수로 같은 값을 낸다.
4. **계획: 머리의 핀 옆 코드 판 ≠ 지금 코드 판 → 재구축, 모드는 전량.** 이유에 바뀐 파일을 적는다
   (`code_changed_files`). 핀이 같아도 재구축하고, 입력이 바뀌어 증분이었을 것도 전량으로 바꾼다.
   - **영향 키를 코드에서 유도하지 않는다(선언된 전량 예외).** Python/Spark 생산자에서 "이 변경이 어떤 열·필터·행에
     닿는가" 를 정적으로 알 길은 건전하지 않다. dbt 도 수정된 노드를 통째로 다시 만들고, Dagster 도 코드 판이 바뀐
     자산(파티션)을 통째로 다시 만든다. 대신 **아래 층은 부분으로 남는다**: 전량 결과의 내용 지문이 같으면 한 행도
     다시 쓰지 않고(ADR-0274 §1), 다르면 by-PNU delta 가 `row_digest` 가 바뀐 PNU 만 굽는다. 즉 영향 키는 결과의 차이로
     얻는다(출력 diff, Datafold 류).
   - **기록이 없는 머리(이 ADR 이전 Gold)는 지금 코드로 만든 것으로 받아들인다.** 다음 커밋이 판을 적는다. 배포 직후
     모든 Gold 를 한 번씩 전량으로 만들지 않기 위해서다(필지만 1.5–2.5h). 대가: 이 ADR 과 같은 배포에 실린 코드
     변경은 지금처럼 표본만 잡는다. `code_version_recorded: false` 가 계획 요약에 남는다.
5. 코드 판은 패리티 표본을 대체하지 않는다. 표본은 증분이 전량과 같은지(합치기 자체의 버그)를 보고, 코드 판은 지난
   빌드와 지금 코드가 같은지를 본다.

### 긴 배포·멈춤 경보 (ADR-0301 §3)

6. **배포가 자기 상태를 Prometheus 텍스트 파일로 쓴다.** `foundation-deploy.sh` 가 단계마다
   `/var/lib/perfectory/metrics/deploy.prom` 을 통째로(rename) 바꾼다(`deploy-start-once.sh deploy_metric_*`):
   `foundation_deploy_started_timestamp_seconds{sha}`, `foundation_deploy_phase_started_timestamp_seconds{phase}`
   (drain·build·switch·start-once·datahub), `foundation_deploy_dags_paused_since_timestamp_seconds{scope}`(all: 0단계부터
   2b 까지 전부, held: 2b 부터 7단계까지 릴리스가 잡은 것). 값은 시작 시각이라 라벨이 멈춤 하나 동안 그대로다
   (ADR-0208 의 "같은 문제는 같은 라벨"). 배포가 끝나면 사라지고, EXIT trap 이 DAG 을 멈춘 채 두면 멈춤 줄은 남는다.
   쓰기 실패는 배포를 멈추지 않는다.
7. **`node-exporter` 의 textfile 수집기만 켜서 그 파일을 내준다**(`compose.observability.yml`). Prometheus 문서가
   배치 작업 상태에 권하는 길이다. 호스트 자원 수집기(ADR-0299 C-host)는 같은 서비스에 더한다.
8. **규칙**(`foundation-api.rules.yml`, `service: deploy`, Slack `#alerts`): 드레인 20분, 배포 1시간, 전부 멈춤 30분,
   held 멈춤 1시간, 지표를 못 읽음 30분. 기준은 ADR-0301 1단계 뒤 9회 실측(드레인 1.1–2.6분, 전부 멈춤 2.9–4.5분,
   held 11.3–14.5분, 전체 11.7–14.9분)의 몇 배다. 런북: `docs/runbooks/deploy-alerts.md`.

## Consequences

- 생산자 코드 변경이 다음 재구축에서 반드시 드러난다(표본 확률 39–87% → 주석·서식 외의 모든 변경). 바뀐 파일이
  계획 로그에 남는다.
- 코드 판 변경은 그 표의 전량 재구축 한 번이다(측정 비용 §3). 결과가 같으면 쓰기와 굽기는 0, 다르면 바뀐 PNU 만
  굽는다. 30일 빈도는 PR 본문의 측정 표에 있다. 두 공유 등록부를 표의 항목으로 좁힌 것이 그 빈도를 줄인다.
- 긴 배포 0단계·긴 배포·DAG 멈춤이 Slack 으로 온다. 배포가 실패해 DAG 이 멈춘 채로 남아도, unpause 될 때까지 경보가
  남는다.
- 백필 커밋(초안 PR #902 의 `lakehouse_schema_migrate.carried_properties`)이 핀만 옮기고 코드 판을 옮기지 않으면, 그
  표는 다음 커밋까지 "기록 없음"으로 받아들여진다. #902 와 합칠 때 `gold_code_version.PROPERTY` 도 옮길 속성에 더한다.

## Alternatives rejected

- **커밋 SHA 를 코드 판으로**: 관계없는 커밋마다 모든 Gold 가 전량이 된다.
- **생산자 파일 하나만 해시**(ADR-0191 §5 의 첫 문장): 공유 모듈(`gold_incremental`, `parcel_land`)이 바뀌어도 못 잡는다.
- **등록부 파일을 통째로**: 30일 중 25일 판이 바뀐다(측정). 표의 항목만 센다.
- **코드 변경에서 영향 키 정적 유도**: 건전하지 않다. 틀리면 조용히 옛 행이 남는다. 결과 diff 로 대신한다.
- **기록 없는 머리를 전량으로**: 배포 직후 모든 Gold 를 전량(필지 1.5–2.5h ×표 수)으로 만든다.
- **배포 경보를 foundation-api 가 Airflow 에서 읽어 내기**: API 가 DAG pause 시각을 모르고(Airflow 는 `is_paused` 만 준다),
  배포 자신이 아는 사실을 다른 서비스가 추측하게 된다. 배포가 쓴다.
- **systemd 타이머 검사 + `OnFailure` Slack**(#882·#893 방식): 멈춘 동안 매 실행마다 알림이 쌓이거나 한 번만 온다.
  Alertmanager 는 시작·해소 한 번씩과 묶음·억제를 이미 한다.
