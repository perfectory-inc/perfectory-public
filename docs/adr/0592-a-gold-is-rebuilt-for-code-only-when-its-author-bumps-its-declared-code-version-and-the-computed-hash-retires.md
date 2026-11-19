# ADR 0592: Gold 는 작성자가 선언한 code_version 을 올릴 때만 코드 때문에 다시 지어진다 — 계산한 해시와 손 목록은 없앤다

- Status: Accepted
- Date: 2026-11-19
- Supersedes: [ADR-0321](./0321-a-gold-commit-records-the-code-that-built-it.md) §1(코드 버전 = 호출 그래프의 해시, 그리고 "사람이
  판을 올리게 하지 않는다"는 판단을 뒤집는다), §2(`plumbing_*`·`scoped_contracts`·`row_neutral_fields`), §4 의 `code_changed_parts`,
  §7 의 `plumbing_symbols` 근거
- Keeps: ADR-0321 §5(코드 때문에 짓는 빌드는 그림자 브랜치에서 하고, `row_digest` 가 다른 키만 main 에 게시한다),
  [ADR-0499](./0499-a-failed-job-runs-again-once-when-a-release-changes-the-code-it-reaches.md)(실패한 작업을 다시 돌리는 작업 단위 버전)
- Builds on: [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)(바뀐 행만 쓰는 공통 부품),
  ADR-0583(작업을 묶음 넷으로 Dagster 로 옮기고 Gold 가 먼저; PR #1476)

이 글에서 **판**은 생산자 코드의 버전 문자열이다(잠금 파일의 패키지 판과 다르다). **통째 재빌드**는 모든 행을 다시 계산하는
빌드다. **계획기**는 `gold_rebuild.plan` 이다. 시각은 모두 UTC 다.

## Context

**잰 것.** ai-server 의 `journalctl -u foundation-gold-panel-rebuild.service --since 2026-11-05 --until 2026-11-20`, 줄
`full rebuild: the producer code changed` 와 `gold-kind gold.<표> mode=`:

| 시각 | 표 | 바뀐 것(로그의 목록) | 초 | 바뀐 키 |
|---|---|---|---|---|
| 11-09 10:44 | parcel_panel | r2-connections 계약 | 7,094 | 0 |
| 11-09 20:26 | parcel_panel | r2-connections 계약 | 5,764 | 0 |
| 11-11 15:23 | parcel_panel | r2-connections·vworld 원본 계약 + `by_pnu_kinds.py`·`vworld_parcel_editions.py` 코드 | 9,031 | **1,389** |
| 11-13 10:44 | parcel_panel | vworld 원본 계약 + `vworld_parcel_editions.py` 코드 | 7,087 | 0 |
| 11-14 02:31 | parcel_panel | industrial_complex 계약 | 6,170 | 0 |
| 11-14 17:36 | building_panel | industrial_complex 계약 | 4,985 | 0 |
| 11-19 14:52 | parcel_panel | r2-connections·industrial_complex 계약 | 4,796 | 0 |
| 11-19 14:54 (끝나지 못함) | building_panel | industrial_complex 계약 + `unit_official_price.py` 코드 | — | — |

이 기간에 이 줄로 시작한 통째 재빌드는 17번이다. 끝까지 가 결과를 남긴 것은 위의 7번이고, 그중 행이 바뀐 것은 11-11 한 번이다.
11-11 은 계약 둘과 코드 둘이 한 배포에 함께 바뀌었다. 그래서 1,389키가 어느 쪽 때문인지는 기록으로 가를 수 없다. 나머지 10번은
시간 한도(`TimeoutStartSec=160m`)나 배포 멈춤에 끊겨 아무것도 남기지 못했다. 그러면 다음 run 이 같은 통째를 다시 시작한다.
2026-11-19 에는 08:39 run 이 160분 한도에 죽었다(systemd 가 162분째에 끝냄). 13:30 run 은 parcel 을 마친 뒤 building 을 짓다가
15:00 배포에 멈췄다. 그동안 spark 풀 3칸을 다 쥐어, 풀을 기다리던 작업 19개(Gold 재빌드와 화면을 내는 served Gold·필지 타일)가
8시간 넘게 밀렸다. 39,861,511 은 `gold.parcel_panel` 의 행 수다(같은 로그의 `rows=`).

**왜 계속 넓은가.** ADR-0321 의 버전은 생산자 스크립트의 호출 그래프가 닿는 심볼과, 그 심볼이 이름으로 부르는 JSON 계약의
해시다. 계약은 여러 표가 함께 쓰는 등록부라 이 표가 읽지 않는 항목이나 필드가 바뀌어도 버전이 바뀐다. 그때마다 손 목록을
늘려 왔다(`plumbing_modules` 5개, `plumbing_symbols` 46개, `scoped_contracts` 2개, `row_neutral_fields`). 목록을 좁히는 고침도
해시를 바꾼다. 15a4f570(2026-11-18 18:42)이 `current_rows_sql` 을 해시에서 빼자, 그 직전 1d0ded6c 와 직후 2c3fce08 에서 다시 계산한
`gold.parcel_panel` 의 차이는 그 항목의 `"current_rows_sql": null` 한 줄뿐이었다. #1518 이 이 경우만 막는 잠정 고침이다. 값에
`scheme` 과 `blobs` 필드를 더한다.

**기록된 값을 읽는 곳.** `foundation.build-source-sha256` 은 이것들이 쓴다.
- 계획기: 통째로 지을지 정한다.
- 증분 run 의 재개 키: `gold_incremental.py` 의 `plan_key`.
- 백필이 옮겨 싣는 값: `lakehouse_schema_migrate.carried_properties`.
- by-PNU 굽기의 설명: `by_pnu_panel_delta.code_sha`, ADR-0346.
- 판을 쌓는 표의 `run_id`: `gold_versions_commit.rs`.

**배포 보류도 같은 분석을 쓴다.** `scripts/deploy/release_changes.py` 는 `gold_code_version.parts()` 를 손 목록 없이 불러, 이주
프로그램의 호출 그래프가 바뀌었는지 본다(ADR-0437). 이것은 Gold 를 다시 짓는 결정이 아니다.

**두 번째 해시.** `job_code_version.py`(ADR-0499)는 작업이 닿는 파일의 해시다. 지금 `observe_assets.code_versions` 가 이 값을
Gold 를 포함한 모든 자산의 `AssetSpec.code_version` 으로 낸다. ADR-0499 의 재시도는 `asset_reconcile.py` 가 이 값을 직접 읽어 한다.

**Dagster 1.13.27(잠금 파일 `orchestration/dagster/requirements.txt`).** 문서
[Asset versioning and caching](https://docs.dagster.io/guides/build/assets/asset-versioning-and-caching)(2026-11-19 읽음)과
소스 `dagster/_core/definitions/declarative_automation/operands/operands.py` 를 보았다.
- `code_version` 은 작성자가 `@asset`/`AssetSpec` 에 주는 문자열이다. 주지 않으면 Dagster 는 run id 를 쓰므로 run 마다 다르다.
  문서는 코드를 바꾸면 작성자가 문자열을 바꾸는 예를 든다.
- 현재 문자열이 마지막 materialization 에 붙은 값과 다르면 UI 에 "Unsynced" 가 표시된다.
- `AutomationCondition.code_version_changed()` 는 지난 평가의 커서와 현재 문자열을 비교한다. 첫 평가는 거짓이다.
- 지금 `orchestration/` 은 `AutomationCondition` 을 쓰지 않는다. 빚진 run 은 `decide.py` 센서가 `OWED_TAG` 로 연다.

## Decision

1. **판은 작성자가 선언한다.** Gold 생산자의 판은 그 생산자를 선언한 곳의 `code_version` 문자열이다.
   - `gold-panel-rebuild.contract.json` 의 `tables.<표>.code_version`.
   - kind 표: `kind_tables.code_version`(모든 kind 가 함께 쓰는 생산자)과 `r2-connections.contract.json` 의
     `by_pnu_lanes.<lane>.code_version`(그 kind 의 줄)을 `+` 로 이은 값.
   - 판이 바뀔 때만 코드 때문에 다시 짓는다. 그 빌드는 ADR-0321 §5 그대로 그림자 브랜치에서 짓고, 다른 키만 게시한다.
   - 계약 편집은 그 자체로 다시 짓게 하지 않는다. 행을 바꾸는 편집(생산자가 읽는 kind 줄, 값 영역)이면 작성자가 같은 PR 에서 판을
     올린다.
   - 판의 꼴은 `YYYY-MM-DD.#<PR 번호>` 다. 두 PR 이 같은 판을 고를 수 없으므로, 큐에서 둘이 같은 값을 싣지 않는다.
2. **막는 검사는 생산자 자기 파일에만 둔다.**
   - PR 이 생산자 스크립트(`jobs/<producer>.py`) 자체를 바꾸었는데 그 생산자의 판이 그대로면 CI 가 실패한다. 공유 모듈은 막지
     않는다. 공유 모듈까지 막으면 `gold_rebuild`·`gold_incremental` 변경마다 모든 생산자의 올림을 요구하게 된다.
   - 행이 바뀌지 않는 변경이면 PR 본문에 `code-version-unchanged: <producer> — <이유>` 줄을 쓴다. 검사는 그 줄을 읽고 통과한다.
     기록은 PR 에 남고 저장소에 목록이 생기지 않는다.
   - 공유 모듈이나 계약이 바뀌면, 영향받는 Gold 생산자를 적은 PR 댓글을 단다(막지 않음). 대상은 `job_code_version.py --files`
     목록에 그 파일이 든 생산자다. 올릴지는 작성자와 검토자가 정한다.
   - 검사가 보지 못하는 것: 이름으로 읽는 파일, `producer_arguments`, 이미지·pyspark 판 올림. 이들은 작성자 몫이다.
3. **누가 무엇을 정하나.**
   - Gold 커밋은 지은 판을 `foundation.build-source-sha256` 에 문자열로 적는다. 이름은 그대로 두므로 지금의 독자들이 계속 읽는다.
   - Dagster 는 언제 돌릴지 정한다. Gold 묶음이 옮긴 뒤 `decide.py` 센서가 Iceberg 머리에 기록된 판과 선언된 판을 비교한다. 다르면
     빚진 run 을 연다(입력 변경과 같은 `OWED_TAG` 경로, 그래서 한 번 올려도 run 은 하나다).
   - 계획기는 그 run 이 통째일지 증분일지 정한다.
   - `code_version_changed()` 는 쓰지 않는다. 지난 평가와만 비교하므로, 올린 판의 빌드가 실패하면 다시 켜지지 않는다. 기록된 판과
     비교하면 성공할 때까지 빚이 남는다.
   - 같은 문자열을 Gold 노드의 `AssetSpec.code_version` 으로도 낸다. Dagster UI 의 "Unsynced" 가 같은 사실을 보인다.
4. **넘어가기: 옛 JSON 기록은 판 `"1"` 로 읽는다.**
   - 모든 생산자의 첫 선언은 `"1"` 이다. 그래서 넘어가는 배포에서 다시 짓는 Gold 는 없다. 끝까지 간 통째 7번 중 6번이 0키였다.
   - 진짜 코드 변경이 아직 지어지지 않은 생산자는 넘어가는 PR 이 `"2"` 로 선언한다. 2026-11-19 기준으로는
     `building_panel_silver_to_gold`(`unit_official_price.py` 변경) 하나다. 그 PR 을 쓰는 날 다시 확인한다.
   - 같은 PR 에서 지우는 것: `gold_code_version.value`·`same`·`changed_parts`·`settings`·`scheme`·`remeasured`, 계약의
     `code_version` 절, #1518 의 `scheme`·`blobs` 필드와 그 시험.
   - 손 목록 없는 호출 그래프 분석(`parts`)은 `release_changes.py` 의 배포 보류가 쓰므로 `call_graph.py` 로 이름을 바꿔 남긴다.
   - `PROPERTY` 는 `gold_rebuild.py` 로 옮긴다.
   - 바꿔 붙일 호출자: `gold_rebuild.py`(계획·커밋 기록), `gold_incremental.py`(재개 키는 판 문자열을 쓴다),
     `lakehouse_schema_migrate.py`(옮겨 싣기는 그대로), `release_changes.py`(`call_graph` 로).
   - `gold_versions_commit.rs` 의 `run_id` 와 `by_pnu_panel_delta` 의 설명은 넘어가는 날 한 번 "코드가 움직였다" 로 바뀐다. 그 뒤로는
     판이 오를 때만 바뀐다.
5. **ADR-0499 는 그대로다.**
   - `observe_assets.code_versions` 는 §4 가 덮는 Gold 노드만 선언된 판으로 덮는다. 다른 자산은 작업 단위 값을 그대로 쓴다.
   - `gold_panel_rebuild` 가 함께 쓰는 `silver.unit_official_price` 는 Gold 가 아니라 작업 단위 값을 쓴다.
   - ADR-0499 의 재시도는 `asset_reconcile.py` 가 `job_code_version` 을 직접 읽으므로 바뀌지 않는다.
   - `job_code_version.py` 에는 Gold 를 위한 예외를 두지 않는다.
6. **이 ADR 이 덮는 Gold** 는 `gold_rebuild.load_contract()` 가 내는 표다. 자기 해시를 가진 다른 Gold 는 이 ADR 밖이다.
   `parcel_nearby_versions` 는 발행기의 `parcel_nearby_versions_build.rs` 가 자기 `code_version()`(SQL 글에서 계산)과 기록을
   비교한다. 각자 자기 PR 에서 같은 선언으로 옮긴다.

## Consequences

- 코드 때문에 생기는 통째 재빌드는 작성자가 판을 올린 PR 에서만 생긴다. 11-05..19 기록에 대 보면, 끝까지 간 7번 중 6번(0키)은
  생기지 않는다. 11-11 은 코드가 바뀌었으므로 작성자가 올렸어야 하고, 올렸다면 같은 1,389키를 게시한다. 끝나지 못한 10번은 같은
  변경을 다시 시작한 것이어서, 판을 올린 경우만 남는다.
- 대가: 행을 바꾸는 계약 편집에서 판 올림을 잊으면, 그 Gold 는 다음 올림이나 입력 변경 전까지 옛 계약대로 남는다. 증분 run 의
  parity 표본(ADR-0180)과 바뀐 행만 쓰는 비교(ADR-0564)가 차이를 본다. 공유 모듈 PR 의 댓글(§2)이 잊음을 줄인다.
- 판 올림을 되돌리는 PR 은 판을 다시 바꾸므로 다시 짓는다. 그것이 맞다.
- 배포가 긴 run 을 멈추는 문제는 이 ADR 로 드물어진다(2026-11-19 15:00 에 `interruptible` 이라 멈춰 90분이 사라짐). 배포가
  기다릴지 체크포인트할지는 지금 정하지 않는다. 기준은 [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md)
  그대로다.
- 이 ADR 을 구현하는 PR 은 Gold 묶음 스위치(ADR-0583) 앞에 들어간다. 그래서 스위치는 선언된 판을 싣는다.

## 기각한 대안

- **계약만 바뀌면 표본 대조로 받아들이기.** 바뀐 부분이 계약뿐이면 통째 대신 `parity_sample_keys`(패널 20,000키)를 새 코드로
  다시 계산하고, 같으면 새 버전만 기록한다. 해시와 손 목록이 그대로 남아 주인의 결정("손 목록은 없앤다")에 맞지 않는다. 표본도
  약하다. 11-11 크기의 변경(1,389 / 39,861,511)을 20,000키 표본이 하나도 못 볼 확률은 약 50%(e^-0.70)다. 같은 표본은 선택한 안에서도
  뒷받침일 뿐 결정 근거가 아니다. 이 감사를 표본으로 하는 공개 도구(Datafold `data-diff`, dbt `audit_helper`)가 있지만, 둘 다
  해시를 없애 주지는 않는다.
- **계산 해시를 필드 단위로.** 생산자가 읽는 필드만 센다. Bazel 의 액션 키와 dbt 의
  [`state:modified`](https://docs.getdbt.com/reference/node-selection/methods#state) 는 파일이나 노드 본문을 단위로 계산한다. 필드
  단위로 계산하는 공개 도구는 이번에 찾지 못했다(위 두 문서와 Dagster 문서를 봄). 손 목록이 필드 목록이 되어 더 커지고 주인의 결정에
  맞지 않는다. 11-05..19 에 몇 번을 피했을지는 필드 단위로 다시 재야 알 수 있다. 기각 근거는 그 수가 아니라 유지 부담이다.
- **공유 모듈까지 막는 검사, 또는 저장소 안의 "안 올림" 목록.** 모듈 단위로 막으면 공유 모듈 PR 마다 모든 생산자의 올림을 요구한다.
  ADR-0321 의 측정으로, 모듈 단위로 세면 필지 판이 30일 중 22일, 건물 판이 20일 바뀐다. 저장소 안의 예외 목록은 없앤 손 목록이 이름만 바뀐 것이다.
