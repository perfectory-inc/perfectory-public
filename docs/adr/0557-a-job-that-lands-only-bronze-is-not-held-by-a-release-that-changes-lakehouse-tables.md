# ADR 0557: Bronze 만 쓰는 작업은 레이크하우스 표를 바꾸는 릴리스가 붙잡지 않는다

- Status: Accepted
- Date: 2026-11-12
- Builds on: [ADR-0308](./0308-the-deploy-stops-only-the-jobs-its-release-holds-and-skips-a-step-whose-inputs-did-not-change.md)(배포는 릴리스가 붙잡는 작업만 멈춘다), [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md) §5(작업의 자산으로 붙잡기를 좁힘), [ADR-0514](./0514-the-raon-lane-takes-a-capped-count-of-the-newest-files-every-day-and-a-listed-file-not-in-bronze-pages.md)(RAON 레인 매일 8개)
- 관련 사건: 2026-11-12 03:50Z 에 시작한 매일 원천 훑기(source_sweep)를 04:19Z 배포(`76ec2f77`, `silver.real_estate_broker_member` 를 바꾸는 릴리스)가 29분 만에 멈췄다(`SIGTERM`, `stopped foundation-source-sweep.service: … holds it and it is interruptible`). ADR-0514 가 처음으로 켠 RAON 단계(경기·경북 용도지역지구 8개, 약 2.3시간)는 시작도 못 했다. 배포 로그: `jobs whose writes the pipeline graph does not name: … source_sweep`.

## Context

`scripts/deploy/release_changes.py writers()` 는 작업의 그래프 간선이 닿는 표(`table_name`)로 그 작업이 무엇을 쓰는지
정한다. 표를 하나도 못 찾으면 "그래프가 쓰기를 이름 짓지 않은 작업" 이고, 레이크하우스 표가 하나라도 바뀌는 배포는
그 작업을 붙잡는다(멈추고, 배포가 끝나면 다시 시작). 모르는 것을 막는 보수적 기본값으로는 맞다.

source_sweep 의 간선은 `daily-source-sweep-to-source-building-hub-bulk`·`daily-source-sweep-to-source-vworld-dataset` 둘이고,
둘 다 `source_group` 노드(원천 묶음 — 그 원천의 Bronze 객체와 원장)로 간다. 표 이름이 없으니 "모름" 이 되었다. 그러나
훑기가 쓰는 것은 R2 Bronze 객체와 Postgres 원장(`catalog.bronze_object_member` 등)이고, Iceberg 표는 쓰지 않는다
(`daily-source-sweep.sh`: HUB·VWorld·RAON·members 네 단계 모두 Bronze 착지). 레이크하우스 마이그레이션이 바꾸는 것은 Iceberg
표다. 둘은 겹치지 않는다. juso_electronic_map(간선이 `source-juso-electronic-map-bulk` 하나)도 같다.

배포는 하루에도 여러 번이다(main 병합마다 자동배포, ADR-0159). 8시간 상한의 훑기 안에서 2.3시간 걸리는 RAON 단계는,
아무 표나 바꾸는 배포 하나에 끊긴다. 끊긴 실행은 다시 시작되지만 다음 배포에 또 끊길 수 있다 — ADR-0514 의 "사람 없이
매일" 이 배포 빈도에 달린다.

## Decision

1. `writers()` 는 모든 간선이 `source_group` 노드로 가는 작업을 Bronze 착지 작업으로 본다. 레이크하우스 표 변경은 그 작업을
   붙잡지 않는다. Postgres 마이그레이션은 지금처럼 모든 작업을 붙잡는다(원장이 Postgres 에 있다).
2. 간선이 하나라도 다른 노드로 가면(표를 쓰거나 모르는 곳) 지금 규칙 그대로다. 간선이 없는 작업도 그대로 "모름" 이다.
3. 시험 `test_a_job_that_lands_bronze_only_is_not_held_by_a_table_change`(orchestration/tests/test_release_changes.py)가
   위 사건의 모양(표 하나를 바꾸는 릴리스, Bronze 만 쓰는 작업)으로 붙잡지 않음과, Postgres 마이그레이션이면 붙잡음을 확인한다.

## Consequences

- source_sweep·juso_electronic_map 은 레이크하우스 표만 바꾸는 배포에 끊기지 않는다. 다른 미배치 작업(data_quality·
  lakehouse_maintenance·media_*·outbox_publish·shape_ledger_check)은 그대로다 — 그들은 간선이 없거나 서빙 면으로 간다.
- 훑기 코드 자체가 바뀌는 릴리스는? 돌던 실행은 시작 때의 릴리스로 끝까지 간다([ADR-0441](./0441-a-deploy-never-waits-for-a-running-job-or-an-operators-run-each-keeps-the-release-it-started-with.md)).
  다음 실행이 새 코드를 쓴다.
- 비용 0.
