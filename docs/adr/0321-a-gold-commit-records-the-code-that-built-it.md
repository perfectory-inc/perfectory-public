# ADR 0321: Gold 커밋은 자기를 만든 코드를 함께 적고, 코드가 바뀌면 바뀐 키만 게시한다, 그리고 긴 배포 멈춤은 경보한다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0191](./0191-a-release-becomes-a-changelog-once-and-every-layer-reads-only-the-changes.md) §5(빌드
  코드 변경 → `foundation.build-source-sha256`), [ADR-0139](./0139-panel-gold-is-rebuilt-when-a-silver-input-changes.md)(Silver
  핀), [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md)(증분), [ADR-0274](./0274-the-by-pnu-bake-does-only-the-work-a-change-needs.md) §1(내용 지문), [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md) §1(바뀐 PNU 기록),
  [ADR-0183](./0183-a-release-loaded-in-batches-is-staged-on-a-branch-and-published-at-once.md)(브랜치에 쌓고 한 번에 게시)
- Closes: ADR-0312 §4(PR #902 의 후속; 바뀐 키 기록 `__changes` 를 코드 변경 게시가 쓴다), [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md) §3

## Context

**1. 계획은 Silver 핀만 보고 생산자 코드는 보지 않는다.** `gold_rebuild.plan` 은 Gold 머리의 Silver 핀과 지금
Silver 를 비교한다. 핀이 같으면 `nothing_to_do`, 다르면 증분이다. 생산자 코드가 바뀌어도 핀이 같으면 아무 일도
없고, 핀이 바뀌면 증분이 바뀐 PNU 만 새 코드로 다시 만든다. 나머지 행은 옛 코드의 결과로 남는다. 이것을 잡는 것은
패리티 표본(ADR-0180 §5)뿐이고, 표본 20,000 PNU 가 n 개 PNU 에만 닿는 코드 변경을 잡을 확률은
`1 − (1 − 20000/N)^n` 이다(ADR-0312 Context 4, 계산값): 필지 39.86M 에서 n = 1,000 이면 39%, n = 4,000 이면 87%.

**2. 생산자 코드는 자주 바뀐다.** 2026-09-28…10-28 30일, 필지 생산자의 코드 묶음을 건드린 커밋은 103개다
(측정, `git log`). 그중 두 공유 등록부가 절반이다: `industrial_complex_lakehouse_contracts.json` 39개(모든 표의 계약),
`r2-connections.contract.json` 38개(섹션 팩 16, 건물 관문 12 — Gold 생산자가 읽지 않는 부분). 파일 통째로 해시하면
코드 판이 30일 중 25일 바뀐다(측정). 모듈 단위로 좁혀도(이 ADR 의 첫 판) 필지는 22일, 건물은 20일 바뀐다(아래 표).
코드 판이 바뀔 때마다 전량을 덮어쓰면 `gold_panel_rebuild` 가 거의 매일 전량(필지 1.5–2.5h)이고, 아래 굽기도 두
스냅숏을 통째로 비교한다(40M 레인 410–1,536 초, ADR-0312 측정). "모든 처리는 부분, 전량은 선언된 예외" 에 어긋난다.

**3. 전량과 증분의 운영 비용(측정, ai-server 저널 10-15…10-26).** 필지 패널 전량 5,555 · 5,648 · 7,040 · 7,503 ·
8,831 초, 증분 3,096 · 8,206 초. 건물 패널 전량 6,427 초, 증분 3,793 초.

**4. 긴 배포 멈춤이 무음이다(ADR-0301 §3 미구현).** 2026-10-26 배포 0단계가 by-PNU bake 를 1.5h 기다리는 동안 모든
DAG 이 멈췄고 수집이 2h 섰는데 아무 경보도 없었다. paused DAG 은 run 이 없어서 unit `OnFailure` 도, 운영 지도의
`FoundationScheduledJobProblem` 도 울지 않는다. 배포 상태를 Prometheus 가 볼 길이 없다(스크레이프 대상은
foundation-api 하나).

## Decision

### 생산자 코드 판 — 호출 그래프의 심볼 (dbt `state:modified.body` · Bazel action key)

1. **코드 판은 생산자 진입점에서 닿는 최상위 심볼들의 해시다.** `infra/lakehouse/spark/jobs/gold_code_version.py`:
   - 시작은 `spark <producer>.py` 가 실행하는 것, 곧 생산자 스크립트의 최상위 문장(`if __name__ == "__main__":` 이
     `main` 을 부른다)이다.
   - 심볼은 이 디렉터리 모듈의 최상위 함수·클래스·대입이다. 닿은 심볼이 쓰는 이름을 따라간다: 같은 모듈의 심볼,
     `import x` 뒤 `x.f`, `from x import f`(함수 안 지연 import 포함), 깊이 무관. 모듈을 값으로 쓰면
     (`getattr(x, name)`) 그 모듈 전체다. 닿은 클래스는 메서드 전부다(인스턴스를 통한 호출은 정적으로 못 풀기 때문).
     심볼이 하나라도 닿은 모듈은 import 때 실행되는 나머지 최상위 문장도 센다. 다른 모듈의 `__main__` 블록은 세지 않는다.
   - 바깥 모듈에 묶인 이름(`F` = `pyspark.sql.functions`)은 그 묶음 자체를 센다. 다른 곳을 가리키게 바꾸면 변경이다.
   - 닿은 심볼이 문자열로 이름 대는 JSON 계약(`infra/lakehouse/contracts/`, `config/`)을 센다.
   - 심볼은 docstring 과 함수 인자·반환 주석(annotation)을 뺀 구문 트리로, JSON 은 정규형으로 해시한다. 주석·
     docstring·서식·타입 주석·키 순서, **그리고 생산자가 닿지 않는 함수**(다른 생산자의 것, CLI 도우미, 새로 더한 함수)
     는 코드 변경이 아니다.
   - 값: `{"sha256": 전체, "parts": {"경로::심볼" 또는 계약 경로: 해시 앞 16자}}`. 계획이 바뀐 심볼을 이름으로 댄다.
   - dbt 가 모델마다 본문 checksum(`state:modified.body`)과 그 모델이 부르는 매크로만 비교하는 것, Bazel 이 action 마다
     그 action 이 실제로 읽는 입력의 digest 로 key 를 만드는 것과 같은 자리다. 파일이나 모듈이 아니라 출력이 기댈 수
     있는 코드만 센다. Dagster 처럼 사람이 판을 올리게 하지 않는다(잊는다).
2. **좁히기는 계약에 이유와 함께 적는다**(`contracts/gold-panel-rebuild.contract.json` `code_version`):
   - `plumbing_modules`: 행을 바꿀 수 없는 모듈(계획·커밋 작성기, 지문, 구역 검사, Spark 세션 설정, 핀 파일 읽기)과
     이유. 그 모듈의 어떤 심볼도 따라가지 않는다. 없는 모듈 이름은 거부한다.
   - `plumbing_symbols`: 전량 빌드가 실행하지 않거나 빌드한 행을 바꾸지 않고 내려놓기만 하는 `모듈.심볼`과 이유.
     증분 합치기 기계(`gold_incremental.Outcome`·`plan_merge`·`merge_into_gold`…)와 생산자마다의 증분 의존 지도
     (`affected_pnus`·`restrict_inputs`)다. **코드 판은 전량 빌드, 곧 핀에서 행으로 가는 함수의 정체다.** 증분이 전량과
     같은지는 패리티 표본과 증분 = 전량 Iceberg 시험이 지킨다(ADR-0180 §4, 아래 7). 없는 심볼 이름은 거부한다.
   - `scoped_contracts`: 여러 표가 함께 쓰는 등록부는 이 표가 읽는 항목만 센다. `tables` 는 Gold 표와 그 입력 표의
     계약, `lanes` 는 Gold 나 호스트 패널이 이 표인 by-PNU 레인. `keep` 은 통째로 세는 부분(값 영역, 건물 관문의 PNU
     패턴). 모르는 선택자와 이유 없는 항목은 거부한다.
3. **모든 생산자 커밋이 코드 판을 핀 옆에 적는다**(`foundation.build-source-sha256`, ADR-0191 §5 의 이름). 쓰는 곳은
   `gold_rebuild.pins_properties` 한 곳이고, 전량 쓰기·빈 append·증분 MERGE·아래 5의 게시가 모두 거기를 지난다. 생산자
   프로세스가 자기 진입 스크립트로 계산하므로, 계획과 커밋이 같은 트리의 같은 함수로 같은 값을 낸다. 스키마 백필
   (`lakehouse_schema_migrate.carried_properties`)은 핀과 함께 코드 판도 옮긴다.
4. **계획: 머리의 핀 옆 코드 판 ≠ 지금 코드 판 → 재구축, 계산은 전량, 게시는 바뀐 키만**(`publish: changed_keys`).
   이유에 바뀐 심볼을 적는다(`code_changed_parts`). 생산자에 `--publish-changed-keys <표의 max_changed_key_fraction,
   없으면 1.0>` 을 넘긴다.
   - **계산 전량은 선언된 예외다.** 어떤 행이 코드 변경에 닿는지는 Python/Spark 코드에서 건전하게 유도할 수 없다
     (열·필터·조인이 데이터에 따라 갈린다). 틀리면 옛 행이 조용히 남는다. dbt 도 수정된 모델을 통째로 다시 만들고
     (`--select state:modified+`), Dagster 도 코드 판이 바뀐 자산을 통째로 다시 만든다. 비용은 필지 5,555–8,831 초
     (측정, 10-15…10-26 전량 실행)이고, 아래 빈도 표가 그 횟수다.
   - **쓰기와 아래 층은 부분이다**(5). 결과가 같으면 데이터 파일 0개, 다르면 바뀐 키만.
   - **기록이 없는 머리(이 ADR 이전 Gold)는 지금 코드로 만든 것으로 받아들인다.** 다음 커밋이 판을 적는다. 배포 직후
     모든 Gold 를 한 번씩 전량 계산하지 않기 위해서다. 대가: 이 ADR 과 같은 배포에 실린 코드 변경은 지금처럼 표본만
     잡는다. `code_version_recorded: false` 가 계획 요약에 남는다.
5. **게시는 Write-Audit-Publish 다**(Netflix 의 Iceberg WAP, `gold_incremental.Outcome.publish_by_key`):
   1. 먼저 내용 지문(ADR-0274 §1)이 머리와 같으면 빈 append 하나로 새 코드 판·핀만 적는다(브랜치도 안 만든다).
   2. **Write**: main 머리에서 그림자 브랜치 `foundation_code_change_audit` 를 만들고(`CREATE OR REPLACE BRANCH … AS OF
      VERSION`), 전량 결과를 거기에 덮어쓴다. 브랜치 쓰기는 `lakehouse_ingest` 의 적재 브랜치(ADR-0183, ADR-0311 의
      비교 브랜치)와 같은 `branch_<이름>` 식별자·`checked_ref` 규칙이다. main 과 그 독자는 아무것도 못 본다.
   3. **Audit**: 생산자의 품질 관문·행 하한·구역 범위(ADR-0272)는 쓰기 전에 이미 통과했다. 브랜치의 행 수가 빌드와
      다르면 거부하고 main 은 그대로다.
   4. **Diff**: 브랜치와 main 을 `row_key` 로 잇고 `row_digest` 를 비교한다(`gold_incremental.merge_source`, 증분이
      쓰는 그 함수): 새 키·digest 가 다른 키는 upsert, main 에만 있는 키는 delete.
   5. **Publish**: 바뀐 키가 0이면 빈 append(데이터 파일 0개)로 코드 판을 받아들인다. 바뀐 PNU 가 표의
      `max_changed_key_fraction` 이하이면 그 키만 MERGE(copy-on-write: 그 키가 든 파일만 다시 쓴다)하고, 커밋에
      `foundation.keys-changed` 를, `gold.<t>__changes` 에 바뀐 PNU 를 적는다(ADR-0312 §1, #902 의 장치). 넘으면
      main 을 브랜치로 fast-forward 한다(`CALL system.fast_forward`; 그 커밋이 이미 만든 전량 쓰기다). 그때도 바뀐 키는
      `__changes` 에 적힌다.
   6. 브랜치를 지운다. 실패하면 main 은 그대로이고 브랜치는 다음 실행의 `CREATE OR REPLACE` 가 바꾼다.
   - 그래서 by-PNU 굽기의 delta 는 두 스냅숏을 통째로 잇지 않고 기록된 PNU 만 본다(ADR-0312 §1). 바뀐 키가 0이면 굽기는
     할 일이 없다.
6. **빈도(측정, `origin/main` first-parent 2026-09-28…10-28, jobs·contracts·config 를 건드린 216 커밋을 커밋마다
   `git archive` 해 같은 계약 설정으로 판을 계산)**:

   | 표 | 모듈 단위(첫 판) 변경 / 일 | 심볼 단위 변경 / 일 |
   |---|---|---|
   | `gold.parcel_panel` | 53 / 22 | **33 / 19** |
   | `gold.building_panel` | 56 / 20 | **21 / 10** |
   | `gold.unit_price` | 31 / 11 | **12 / 7** |
   | `gold.parcel_*` 6개 kind | 26 / 9 | **15–16 / 8** |
   | `gold.parcel_public_auction` | 2 / 1 | **2 / 1** |

   필지에 남은 19일은 실제로 행에 닿을 수 있는 변경이다: 단독 원인 상위가 `r2-connections` 의 필지 레인 항목 5회(패널이
   만드는 구역이 바뀜), 표 계약 2회, `read_source`·`build_gold_panel_frame`·`read_inputs` 등. 더 좁히려면 행을 바꾸는
   코드를 덜 고치는 수밖에 없다. 그래서 빈도가 아니라 **바뀐 날의 비용을 부분으로** 만든다(5).
7. 코드 판은 패리티 표본을 대체하지 않는다. 표본은 증분이 전량과 같은지(합치기와 의존 지도의 버그)를 보고, 코드 판은
   지난 빌드와 지금 전량 빌드가 같은 함수인지를 본다. 증분 기계를 `plumbing_symbols` 로 뺀 것이 기대는 곳이 이것이다.

### 긴 배포·멈춤 경보 (ADR-0301 §3)

8. **배포가 자기 상태를 Prometheus 텍스트 파일로 쓴다.** `foundation-deploy.sh` 가 단계마다
   `/var/lib/perfectory/metrics/deploy.prom` 을 통째로(rename) 바꾼다(`deploy-start-once.sh deploy_metric_*`):
   `foundation_deploy_started_timestamp_seconds{sha}`, `foundation_deploy_phase_started_timestamp_seconds{phase}`
   (drain·build·switch·start-once·datahub), `foundation_deploy_dags_paused_since_timestamp_seconds{scope}`(all: 0단계부터
   2b 까지 전부, held: 2b 부터 7단계까지 릴리스가 잡은 것). 값은 시작 시각이라 라벨이 멈춤 하나 동안 그대로다
   (ADR-0208 의 "같은 문제는 같은 라벨"). 배포가 끝나면 사라지고, EXIT trap 이 DAG 을 멈춘 채 두면 멈춤 줄은 남는다.
   쓰기 실패는 배포를 멈추지 않는다.
9. **`node-exporter` 의 textfile 수집기만 켜서 그 파일을 내준다**(`compose.observability.yml`). Prometheus 문서가
   배치 작업 상태에 권하는 길이다. 호스트 자원 수집기(ADR-0299 C-host)는 같은 서비스에 더한다.
10. **규칙**(`foundation-api.rules.yml`, `service: deploy`, Slack `#alerts`): 드레인 20분, 배포 1시간, 전부 멈춤 30분,
   held 멈춤 1시간, 지표를 못 읽음 30분. 기준은 ADR-0301 1단계 뒤 9회 실측(드레인 1.1–2.6분, 전부 멈춤 2.9–4.5분,
   held 11.3–14.5분, 전체 11.7–14.9분)의 몇 배다. 런북: `docs/runbooks/deploy-alerts.md`.

## Consequences

- 생산자 코드 변경이 다음 재구축에서 반드시 드러난다(표본 확률 39–87% → 전량 빌드에 닿는 모든 변경). 바뀐 심볼이
  계획 로그에 남는다.
- 코드 판이 바뀌는 날(필지 30일 중 19일, 건물 10일, 측정 §6)의 비용, 필지 기준:

  | | 첫 판(전량 덮어쓰기) | 이 결정(WAP, 바뀐 키만 게시) |
  |---|---|---|
  | 계산 | 전량 5,555–8,831 초(측정) | 같음(선언된 예외 §4) |
  | 쓰기 | main 전량 덮어쓰기 | 그림자 브랜치 전량 쓰기(같은 양, main 밖) + main 에는 바뀐 키가 든 파일만; 같으면 0 |
  | 비교 | 굽기 delta 가 두 스냅숏 79.7M 행 통째 join 410–1,536 초(측정, ADR-0312) | 생산자가 브랜치↔main 을 `row_key`·`row_digest` 로 한 번 비교(같은 크기의 join, 추정 같은 시간); 굽기 delta 는 기록된 PNU 만(ADR-0312: 데이터 파일 0개~수천 행) |
  | 굽기 | `row_digest` 가 바뀐 PNU | 같음, 그 목록을 비교 없이 읽는다 |
  | 바뀐 키 기록 | 없음(전량 덮어쓰기는 단계를 기록하지 않는다) | `__changes`·`keys-changed` |

  즉 전량은 계산 한 곳에만 남고 그 이유가 이 ADR 에 적혀 있다. 쓰기와 아래 층은 바뀐 것만이다.
- 그림자 브랜치 쓰기는 R2 에 전량만큼의 파일을 한 번 더 쓴다(필지 Gold 한 벌). 브랜치를 지우면 그 스냅숏은 아무 참조도
  없어 다음 스냅숏 만료가 파일을 지운다([ADR-0237](./0237-lakehouse-snapshots-are-kept-30-days-12-month-ends-and-every-year-end.md) 의 30일 만료).
- 증분 기계를 고친 것은 재구축을 부르지 않는다(`plumbing_symbols`). 그 정확성은 패리티 표본과 증분 = 전량 시험이 진다.
- 긴 배포 0단계·긴 배포·DAG 멈춤이 Slack 으로 온다. 배포가 실패해 DAG 이 멈춘 채로 남아도, unpause 될 때까지 경보가
  남는다.
- 바뀐 키 기록은 #902(ADR-0312)의 `__changes`·`keys-changed` 를 그대로 쓴다. 새 기록 장치는 없다.

## Alternatives rejected

- **커밋 SHA 를 코드 판으로**: 관계없는 커밋마다 모든 Gold 가 전량이 된다.
- **생산자 파일 하나만 해시**(ADR-0191 §5 의 첫 문장): 공유 모듈(`gold_incremental`, `parcel_land`)이 바뀌어도 못 잡는다.
- **등록부 파일을 통째로**: 30일 중 25일 판이 바뀐다(측정). 표의 항목만 센다.
- **코드 변경에서 영향 키 정적 유도**: 건전하지 않다. 틀리면 조용히 옛 행이 남는다. 결과 diff 로 대신한다.
- **코드 판이 바뀌면 전량 덮어쓰기**(이 ADR 의 첫 판): 계산은 같은데 main 전량을 다시 쓰고, 굽기가 두 스냅숏을 통째로
  비교하며, 바뀐 키가 기록되지 않는다. 쓰기와 아래 층까지 전량이 된다.
- **브랜치 없이 메모리의 빌드와 main 을 바로 비교**: 쓰기는 줄지만, 많이 바뀐 날 게시할 커밋(fast-forward 할 것)이 없고,
  비교 전 결과를 Trino 로 볼 수도, 중간에 죽은 실행을 살펴볼 수도 없다. WAP 의 Write 가 그 자리다.
- **모듈 단위 해시**(첫 판): 같은 모듈의 닿지 않는 함수(다른 생산자의 경로, 증분 기계)가 바뀌어도 판이 바뀐다. 필지
  22일 → 19일, 건물 20일 → 10일(측정 §6).
- **기록 없는 머리를 전량으로**: 배포 직후 모든 Gold 를 전량(필지 1.5–2.5h ×표 수)으로 만든다.
- **배포 경보를 foundation-api 가 Airflow 에서 읽어 내기**: API 가 DAG pause 시각을 모르고(Airflow 는 `is_paused` 만 준다),
  배포 자신이 아는 사실을 다른 서비스가 추측하게 된다. 배포가 쓴다.
- **systemd 타이머 검사 + `OnFailure` Slack**(#882·#893 방식): 멈춘 동안 매 실행마다 알림이 쌓이거나 한 번만 온다.
  Alertmanager 는 시작·해소 한 번씩과 묶음·억제를 이미 한다.
