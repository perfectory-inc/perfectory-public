# ADR 0314: 부분 처리가 기본, 전량은 계약 예외

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0099](./0099-daily-serving-updates-bake-only-changed-parcels.md)·[ADR-0274](./0274-the-by-pnu-bake-does-only-the-work-a-change-needs.md)(바뀐 필지만 굽는다), [ADR-0164](./0164-panel-gold-is-written-in-pnu-order-and-a-shard-reads-only-its-files.md)(샤드는 자기 파일만 읽는다), [ADR-0189](./0189-an-appended-silver-input-is-its-own-change-set.md), [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md), [ADR-0259](./0259-ci-migrates-the-production-table-shape.md)(모양 원장), [ADR-0273](./0273-panel-gold-maps-code-zone-and-lineage-changes-to-pnus.md), [ADR-0277](./0277-a-job-checks-for-work-before-it-waits-for-its-pool.md)(일감 확인 먼저)
- 관련 사건: 2026-10-26 `gold.parcel_panel` 0.12% 변경(PNU 48,477개)이 39,861,511행 4.27 GB 전부를 다시 썼다. 2026-10-28 감사에서 by-PNU bake 샤드마다 레인 Gold 4 GB 전부를 읽었다(`skipped_data_files: 0`, 전국 1회 약 1 TB).

## Context

사장 원칙은 반복해서 같다: **모든 처리는 바뀐 것만 다시 하고, 각 단위는 자기 몫만 읽는다.** 그런데 이 원칙이 영역별로만 기록되어(ADR-0099 는 서빙, ADR-0164 는 샤드 읽기, ADR-0189 는 Silver 입력…) 새 작업은 매번 빈 자리에서 시작했고, 어디에도 "이 작업은 무엇을 처리하는가"를 말할 칸이 없었다.

2026-10-28 감사에서 잰 것(모두 운영 Iceberg `$snapshots`·`$files` 메타데이터, 읽기 전용):

| 위치 | 잰 값 |
| --- | --- |
| `gold.parcel_panel` 2026-10-26 04:44 MERGE | 0.12% 변경에 추가 4,265,500,804 B = 전체의 100% |
| 같은 표 05:01 compaction(`replace`) | 4,061,289,766 B 다시 씀 = 전체 |
| `gold.parcel_panel` 데이터 파일 9개 | 9개 모두 PNU 11..52 (전국) 범위 → 시도 하나 읽기가 파일 100% 를 연다 |
| `gold.parcel_forest_ledger`, `gold.parcel_transfer_history` | 11/11, 20/20 파일이 전국 범위 |
| Gold 9개 표의 마지막 데이터 커밋 | 9개 모두 바이트 100% 추가, 바뀐 키 수 선언 0 |
| 건축물대장 Silver 3 레인(표제부·전유부·전유공용면적) | 마지막 커밋이 표 100% overwrite (8.38 GB 포함) |
| 법정동 코드 수집 | 55 MB 를 하루 약 10번, 거의 항상 그대로 |
| 웹만 바꾼 커밋 | 배포 12분 |

같은 일을 이미 옳게 하는 패턴도 저장소에 있다 — 새로 만들 것이 아니라 이것들을 기본으로 삼는다:

- Silver 적재 `plan-silver-load-objects` / `append_source_object`: 어느 커밋도 기록하지 않은 원천 객체만 적재한다(`new-objects`).
- ADR-0277 check-before-slot: 일감이 없으면 풀 슬롯도 잡지 않는다(`skip-if-unchanged`).
- ADR-0273·ADR-0189: 바뀐 코드·구역·계보와 추가된 Silver 입력을 PNU 변경 집합으로 바꾼다(`changed-keys`).
- ADR-0099/0274: by-PNU 서빙은 row_digest 변경 집합만 patch 로 굽는다(`changed-keys`).
- ADR-0193: 자료 종류마다 자기 Gold 표·레인 — 바뀐 종류만 다시 한다.
- ADR-0164: Gold 를 PNU 순서로 쓰고 샤드는 경계가 겹치는 파일만 연다.
- `scripts/deploy/worker_autodeploy.py` 의 inputs 지문: 입력이 바뀐 Worker 만 배포한다.
- 등록공장·법정동 수집의 `accepted.json`: 원천이 같으면 내려받지 않고 확인 시각만 남긴다.

## Decision

**1. 모든 작업 단위는 자기 처리 범위(scope)를 기존 SSOT 에 선언한다.** 값은 넷 중 하나다:

| scope | 뜻 |
| --- | --- |
| `changed-keys` | 지난 실행 뒤 내용이 바뀐 키만 |
| `new-objects` | 어느 실행도 기록하지 않은 원천 객체만 |
| `skip-if-unchanged` | 나눌 수 없는 입력 하나(파일·릴리스·회차·표 스냅숏); 그대로면 아무것도 안 한다 |
| `full` | 매번 전부 — **계약 예외**: `full_reason`(왜), `full_cost`(잰 비용, 날짜), `full_tracked_by`(부분으로 바꿀 변경, 또는 전량이 옳은 단위인 근거)를 함께 적는다 |

선언하는 SSOT 는 새로 만들지 않고 이미 있는 것이다: `orchestration/jobs.v1.json` 의 작업(`scope`, 이 ADR), Silver 레인 계약(`content_columns`/`full_reason`), Gold 표 계약의 load(`incremental`/`full_reason`), CI `tools/ci/change-scope.json` 의 always 항목(이미 이유 문장을 요구한다), 배포 단계. 어휘(`full_reason`)는 모두 같다.

**2. `scripts/guard/every-job-declares-its-scope.sh` 가 jobs.v1.json 을 검사한다.** scope 가 없거나 넷 밖이면, `full` 인데 세 칸 중 하나라도 비면, 부분 작업에 full 칸이 남아 있으면 실패한다. `full` 작업은 `tools/ci/full-scope-jobs.baseline.json` 에 있어야 하고, 그 목록은 줄기만 한다(`--shrink`, [`scripts/catalog/documentation_ratchets.py`](../../scripts/catalog/documentation_ratchets.py) 와 같은 래칫). 2026-10-28 기준 38개 중 24개가 `full` 이고, 각자 잰 비용과 고칠 곳을 적었다.

**3. 키로 가지치기되는 표는 그 키로 모여 있어야 하고, 매일 잰다.** 클러스터 키는 새 칸이 아니라 표 계약에 이미 있는 것이다: `write_distribution: range` 인 표의 `sort_order` 첫 칸(서빙 Gold 는 `pnu`). 매일 도는 모양 원장 점검(`scripts/ops/shape-ledger-check.py`, ADR-0259)이 그 표들의 `$files` 하한·상한을 읽어 **시도(PNU 앞 2자리) 하나를 읽을 때 여는 파일 비율의 평균**을 구하고, 50% 를 넘으면 실패한다. "시도 둘 이상에 걸친 파일 비율"은 쓰지 않는다: 정렬이 옳은 표도 파일 9~14개로 시도 17개를 덮으면 대부분 파일이 경계 둘에 걸친다(2026-10-28 정렬된 표 7개가 6~10개 파일에서 그랬다). 잰 값: 정렬된 표 0.16~0.33, 전국 범위 표 3개 1.00.

**4. 실행은 무엇을 읽고 쓰고 바꿨는지 기록하고, 증폭은 경보한다.**

- `spark_run_summary.v1` 에 `bytes_read`·`bytes_written`·`keys_changed`(선택 칸; 아직 재지 않는 쓰기는 비워 둔다).
- by-PNU bake 의 샤드 요약에 `read`(데이터 파일 수·건너뛴 수·읽은 바이트), 실행 요약 `run-summary.json` 에 `data_files`·`bytes_read`·`shards_reading_every_data_file`·`bytes_written`·`keys_changed`.
- 커밋 자신이 바뀐 키 수를 스냅숏 summary 속성 `foundation.keys-changed` 로 남긴다(이미 쓰는 `foundation.source-iceberg-snapshots` 와 같은 자리). 점검은 실행 기록이 아니라 **지금 살아 있는 커밋**을 판정해야 하므로 커밋에 둔다.
- 모양 원장 점검이 각 클러스터 표의 마지막 데이터 커밋(`replace` 제외)을 읽어, `added-files-size / total-files-size` 가 `10 × max(keys-changed / total-records, 1%)` 를 넘으면 실패한다. 2026-10-26 사건(0.12% 변경, 100% 기록)은 이 식으로 실패한다. 바뀐 키 수를 말하지 않는 50% 넘는 재기록은 이름을 남긴다(NOTE) — 쓰기 쪽이 속성을 남기기 시작하면 실패로 판정된다.
- 경보 경로는 새로 만들지 않는다: 점검 실패는 작업 실패이고, 작업 실패는 운영 지도 문제(`foundation_scheduled_job_problem`, Prometheus `FoundationScheduledJobProblem`)로 이미 Alertmanager 에 간다. 샤드가 레인의 모든 파일을 읽는 상황(`skipped_data_files == 0`, `data_files > 1`)은 3의 판정(시도 읽기 비율 100%)이 표 쪽에서 같은 것을 잡고, bake 실행 요약은 `shards_reading_every_data_file` 로 실제 횟수를 남긴다.

## Consequences

- 새 작업은 scope 없이는 CI 를 통과하지 못하고, 새 `full` 은 기준선을 늘려야만 들어온다 — 리뷰에서 보인다.
- 오늘 운영 배치는 3의 판정에 걸린다(`gold.parcel_panel`·`gold.parcel_forest_ledger`·`gold.parcel_transfer_history`). 점검은 PR #896(MERGE·compaction 뒤에도 PNU 범위 유지)이 배포되고 그 표들이 다시 쓰일 때까지 매일 실패하고 경보한다. 이것이 의도다.
- 기준선 24개는 각자 고칠 곳을 가리킨다: Silver 레인 부분 적재, Gold 부분 쓰기와 #896, 법정동 수집의 변경 확인, ADR-0304(#840). 이 ADR 은 그 변경들을 하지 않는다 — 규칙과 가드만 둔다.
- `jobs.v1.json` 의 다른 독자(`job_specs.py`, foundation-jobs, foundation-api)는 모르는 칸을 무시한다; 새 칸이 그들을 바꾸지 않는다.

## Alternatives rejected

- **영역마다 따로 규칙(지금까지)** — 새 영역마다 새는 것이 사건으로 증명됐다.
- **새 `cluster_key` 칸** — `write_distribution: range` + `sort_order` 가 이미 같은 것을 말한다. 같은 사실을 두 곳에 두면 어긋난다.
- **전용 Prometheus 지표와 규칙** — foundation-api 가 새 지표를 내려면 Postgres 집계·구조체·시험을 넓게 바꿔야 한다. 매일 점검의 실패가 이미 작업 문제 경보로 가므로 같은 경보를 두 번 만들지 않는다.
- **"시도 둘 이상에 걸친 파일 비율" 판정** — 정렬된 표도 걸린다(3 참조).
