# ADR 0602: 서빙 필지 경계와 fold 는 발행기 안의 DuckDB 가 시도마다 한 번에 짓고, 판을 쌓는 표에 바뀐 필지만 남긴다

- Status: Accepted
- Date: 2026-11-22
- Builds on: [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)(변환은 발행기 안 DuckDB 에서, 엔진 동등 판정이 있어야 옮긴다),
  [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)(판을 쌓는 Gold 는 옛 표 곁의 자기 표로 태어나고, §6 그림자와 넘겨받기 판정 `takeover_equal`),
  [ADR-0599](./0599-a-silver-release-is-compared-by-duckdb-from-a-local-copy-and-appended-in-one-commit-so-the-lanes-need-no-branch-and-no-spark.md)(§8 스테이지의 디스크 예약),
  [ADR-0600](./0600-the-parcel-matching-gate-runs-in-duckdb-inside-the-publisher-one-sido-at-a-time-and-writes-the-verdict-the-spark-job-wrote.md)(대조 관문은 시도마다),
  [ADR-0375](./0375-regulation-zones-reach-the-panel-by-pnu-and-the-tile-handoff-reads-only-changed-tiles.md) §2(인계는 바뀐 타일만, fold 는 바뀐 PNU 와 이웃만)

## Context

필지 타일 굽기의 Gold 쪽(`scripts/ops/parcel-tile-bake.sh gold`) 3단계는 서빙 필지 경계를 Spark 컨테이너로 짓는다.
판 전체는 `parcel_boundary_served_gold.py` 가, 지도 편집의 부분 반영은 `parcel_boundary_fold.py` 가 맡는다. 둘 다
`gold.parcel_boundary_served` 를 통째로 다시 쓰고(fold 는 MERGE), 굽기 v2 인계(`foundation-platform.polygon_served_gold.v2`)를
남긴다. 사장 우선순위(2026-11-20)는 DuckDB·Dagster 가 Spark·Airflow 를 대신하는 전환이다. 2단계 대조 관문은
ADR-0600 으로 먼저 옮겼다.

이 표는 사용자가 보는 지도 타일의 입력이다(서빙 Gold). 그래서 새 생산자는 그림자로 먼저 태어나고, 운영 unit 이
지은 그림자를 Dagster 검사 `takeover_equal` 이 시도 하나에서 판정한 뒤에만 굽기가 그것을 읽는다(rules.txt 2026-11-22
"SHADOW FIRST, VERDICT IN PRODUCTION", "VERDICT SCOPE IS ONE 시도").

운영 상태(2026-11-22, 카탈로그 메타데이터만 읽음):

| 표 | 머리 | 내용 |
| --- | --- | --- |
| gold.parcel_boundary_served | 1137192785416147188, overwrite, 2026-11-20 18:21Z | 39,876,546행(202609 연습 빌드), 58개 파일 6,053,964,102 bytes; 태그는 `monthly-2026-10` 하나, `foundation-gold-build` 없음 |
| silver.parcel_boundaries | 5192991326950639971, replace, 2026-10-30 | 두 판 79,738,057행, 29개 파일 14,756,034,046 bytes, 판마다 파티션·PNU 순 정렬 |
| silver.map_edit_ledger | 스냅숏 0개 | 편집 없음 |

측정(202609 판, ai-server, 굽기 unit 과 같은 2 스레드·2 GiB, 읽기 전용 자격, 커밋 없음, W86):

| 실행 | 시간 | 정점 메모리 | 결과 |
| --- | --- | --- | --- |
| Spark, 운영 연습 2026-11-20 | Spark 컨테이너 | Spark 컨테이너 | 64개 part, 39,876,546행 |
| DuckDB, 시도마다 diff 뒤 전국 표를 모아 복사하고 인계를 위해 R2 를 다시 읽음 | 7,000초 상한에서 멈춤(diff 4,277초) | 2.149 GB | 인계 없음 |
| DuckDB 그림자 첫 빌드(이 결정, 운영 unit) | 전환 PR 이 기록 | 전환 PR 이 기록 | 전환 PR 이 기록 |

첫 DuckDB 시도는 시도마다 diff 했지만, 그 결과를 기하까지 담은 전국 표 하나로 모아 다시 복사했고(4천만 행), 인계를
쓰려고 R2 의 판을 전국으로 한 번 더 읽었다.

Spark job 은 빌드 태그도 입력 핀(`foundation.source-iceberg-snapshots`)도 남기지 않는다. ADR-0564 §6 의 그림자(레인
1–3)는 옛 표의 `foundation-gold-build` 태그 커밋이 적은 핀으로 짓는데, 이 표에는 그 태그가 없다. 또 Spark job 은 계약의
열 목록을 그대로 쓰므로, 옛 표의 계약에 열을 더하면 그림자 동안 운영 굽기가 깨진다.

## Decision

1. **명령.** `foundation-outbox-publisher build-parcel-boundary-served` 가 굽기 3단계의 새 생산자다. 편집 인계를
   원장에 한 번씩 더하고, 판과 원장을 머리에 고정해 짓는다. fold 는 `FOLD=1` 이고, 부분이 될 수 없으면 아무것도
   쓰지 않고 exit 3 과 `parcel-boundary-fold-not-partial` 줄을 남긴다(스크립트는 전과 같이 판 전체로 넘어간다).
   `NO_COMMIT=1` 은 짓고 인계를 쓰되 두 표 어디에도 붙이지 않는다. 굽기가 이 명령을 부르는 것은 전환(8)부터다.
2. **표.** 서빙본은 `gold.parcel_boundary_served_versions` 에 쌓는다(ADR-0564). 계약은 옛 표의 계약에서
   `gold_versions_table()` 이 파생한다(`VERSIONED_OUTSIDE_THE_LANES`). 옛 표에 `row_digest` 가 없으면 파생이 옛 열
   뒤에 더하고 현재 행 관문 "row_digest contains content only" 를 단다: 옛 표의 계약·Spark 생산자·독자는 그대로다.
   판마다 바뀐 필지만 덧붙이고, 사라진 필지는 `row_state = 'removed'` 표지를 받는다.
3. **내용 지문.** `row_digest` 는 내용만 담는다: `pnu`·`geometry_checksum_sha256`·`geometry_srid`·`origin`. 판 번호,
   실버 스냅숏, 원장 위치, 발행 시각은 넣지 않는다. 그래서 새 판은 기하나 편집 출처가 바뀐 필지만 쌓고, 그 밖의
   행은 자기를 마지막으로 바꾼 빌드의 도장(`source_snapshot_id`·`silver_iceberg_snapshot_id`·`edits_through_change_seq`)을
   지닌다.
4. **시도마다 한 번.** 판 전체는 시도마다 다음을 한 번에 끝낸다: 시도의 키 범위(`pnu >= '41' AND pnu < '42'`)로 판을
   읽어 `built` 짓기(편집된 PNU 를 빼고 최종 upsert 를 더함) → 그 시도의 diff 와 검사 → 지문 합 → stage 파일 → 굽기
   run 이면 `built` 에서 인계 part. 판의 시도와 행 수는 한 번의 집계로 읽는다. 기하를 담은 전국 표는 만들지 않고 R2 의
   판은 한 번만 읽는다(Silver 는 판마다 PNU 순이라 범위 밖 파일을 건너뛴다). 커밋은 모든 시도 파일을 덧붙이는 append
   하나이고, 지문은 시도별 합을 `after_digests` 가 더한다. 원장에 스냅숏이 없으면 핀에 넣지 않는다.
5. **스테이지는 SSD 에.** DuckDB 의 데이터베이스·spill·stage 파일은 unit 상태 폴더(SSD)에 두고, 열 때와 시도마다
   stage 파일을 쓴 뒤 `require_room` 으로 host-layout 의 `duckdb_stage_reserve_bytes` 를 확인한다(ADR-0599 §8).
   요약의 `stage_bytes_peak` 가 예약과 견줄 실측이다.
6. **그림자 먼저.** 굽기가 옛 표를 읽는 동안 `build-parcel-boundary-served-versions` 가 판 표를 옛 표의 그림자로 짓는다:
   작업 `gold_parcel_boundary_served_versions_build`, unit `foundation-gold-versions@parcel-boundary-served`
   (`gold-versions-build.sh parcel-boundary-served`, 레인 1–3 의 unit·키·풀), 매일 11:30(옛 표는 07:30 의 Spark
   job 만 바꾼다; 원장이 매시간 움직여도 그림자는 옛 표의 빌드만 따른다).
   - 옛 표의 빌드는 `main` 에서 압축(`replace`)이 아닌 가장 새 커밋이다. 입력은 각 입력의 그 커밋 시각까지의 가장 새
     스냅숏이다(Iceberg 의 시각 기준 읽기를 카탈로그 스냅숏 기록에서; `gold_versions_commit::untagged_shadow`). 그때
     스냅숏이 없던 입력(빈 원장)은 핀에 없다. 판 이름은 그 빌드의 source 행이 가진 `source_snapshot_id` 하나다.
   - 굽기에 아무것도 넘기지 않고 원장에 편집을 더하지 않는다(굽기 run 이 이미 했다).
   - 핀·판·코드가 기준 커밋과 같으면 `unchanged`. 아니면 모든 시도를 짓고, 바뀐 키가 0이어도 커밋해 핀을 남긴다
     (판정이 핀으로 그림자 커밋을 찾는다). 커밋 뒤 입력 태그 `foundation-gold-input-gold-parcel_boundary_served_versions`
     가 그 핀을 지킨다.
7. **판정은 운영의 Dagster 검사.** 그 unit 이 그림자 커밋 뒤에(그 머리의 판정이 끝까지 돌 때까지 다음 실행마다)
   `takeover_equal` 을 시도 41(경기, 가장 큼)에서 돌린다(#1603, `gold-takeover-verdict.sh`): 그 시도의 행을 키로
   견주고, 표 전체는 집계로만 본다(행 수·고유 키·키 해시 합·열마다 null 아닌 수). 견주는 열은 내용
   (`pnu`·`geometry_wkb`·`geometry_srid`·`geometry_checksum_sha256`·`origin`)이고, 빌드 도장(3)은 뺀다
   (`gold_takeover_sides::BUILD_STAMPS`). 전국 바이트 대조, 인계 sha256 대조, ai-server 의 판정용 빌드는 하지 않는다
   (rules.txt "NO SPARK BYTE EMULATION", "VERDICT SCOPE IS ONE 시도"). 입력 핀이 Spark 가 실제로 읽은 것과 어긋나면
   (빌드 도중 입력이 커밋됨) 판정은 다름으로 끝날 뿐 같음으로 잘못 통과하지 않는다: 견주는 것은 옛 표의 그 빌드 자체다.
8. **전환은 다음 PR.** 통과한 run 을 인용해서만 연다: 굽기 3단계가 `build-parcel-boundary-served` 를 부르고(Spark
   컨테이너·스크래치 없음, 쓰기 쌍은 `source-sweep.env`), Spark job 둘과 그 시험, 그림자 명령과 레인, 한시 코드
   (`UNTAGGED_BUILDS`·`untagged_shadow`·`BUILD_STAMPS`)를 지우고, 그래프·계약·Dagster 간선을 판 표로 옮기고 옛 표는
   `contract_only` 가 된다. 전환 뒤 첫 굽기의 판 전체 빌드와 전량 굽기가 전국 검사다.

## Consequences

- 그림자 첫 빌드는 모든 필지를 쌓는다: SSD 에 Parquet 약 6 GB(옛 표의 58개 파일이 6.05 GB), 계산한 시간 약 1,800초
  (측정 아님; 작업의 `per_run_cap.first_run`). SSD 여유는 2026-11-22 85 GB, 예약 32 GiB.
- 같은 빌드를 다시 따르면 `unchanged` 이라 R2 에서 판 이름 한 열만 읽는다. Spark 가 같은 판을 다시 연습하면 핀이 같아
  덧붙일 것이 없다.
- 그림자는 시도 사이로 필지가 옮겨 가는 편집과, 머리가 있는 표 위의 판 전체 재구축(제거 표지의 `published_at_utc`)을
  운영에서 거치지 않을 수 있다. 둘 다 단위 시험이 diff 와 지문을 맡는다.
- 원장이 빈 지금 판정은 편집 적용을 견주지 않는다. 편집은 단위 시험(편집 최종 상태, fold 이웃 타일)과 전환 뒤 첫 굽기가
  맡는다.
- `parcel-boundary-equality.py` 는 아직 옛 표를 Trino 로 읽는다. 그 독자 이전은 따로 한다.
