---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-29
---

# 자산 조정기 — 잃어버린 이벤트로 멈춘 자산 다시 시작하기

[루트 ADR-0333 §7](../../../../docs/adr/0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md).
`asset_reconcile` 작업(`orchestration/jobs.v1.json`, 5분마다, `default_pool`)은 `reconciles` 에 적힌 작업마다 자산 하나의
입력 판과, 그 자산의 마지막 실행이 읽은 판을 비교한다.

| 자산 | 작업 | 마지막에 읽은 판 | 지금 입력 판 |
|---|---|---|---|
| `iceberg://gold.unit_price` | `gold_unit_price_rebuild` | 단가 Gold 커밋이 기록한 핀 | 표제부·전유부: 건물 Gold 가 만들어진 판(`pins_from`), 단가 Silver: head |
| `perfectory://unit-price-by-pnu-section-packs` | `by_pnu_unit_price_bake` | `/data/foundation-platform/by-pnu-bake/unit-price/reads.jsonl` 마지막 줄 | `gold.unit_price` head (+ 건물 레인이 서빙하는 판) |
| `perfectory://parcel-public-auction-by-pnu-section-packs` | `by_pnu_public_auction_bake` | `.../parcel-public-auction/reads.jsonl` 마지막 줄 | `gold.parcel_public_auction` head |

입력이 읽은 판을 지나 행을 바꿨고(압축·빈 커밋은 아님), 실행 중이 아니고, 따르는 레인을 기다리는 중도 아니면
`foundation-asset-owed <작업> <키>` 를 찍는다. 조정기 DAG(`foundation_asset_reconcile`)의 `select` 가 대기·실행 중인
DAG 실행이 없는 작업만 고르고, `start` 가 실행 id `reconcile__<작업>__<키>` 로 한 번 시작한다. 같은 키는 두 번 시작하지
않는다. 시작된 실행은 다른 실행처럼 자기 풀 자리를 기다린다.

지표(`/var/lib/foundation-jobs/metrics/asset-reconcile.prom`, node-exporter) 와 규칙(`foundation-api.rules.yml`
`foundation-assets`):

| 경보 | 뜻 | 기준 |
|---|---|---|
| `FoundationAssetUnscheduled` | 입력이 바뀌었고 풀에 자리가 있는데 실행이 없음 (`asset_unscheduled_seconds`) | 15분(조정 주기 3번), 배포가 DAG 을 멈춘 동안은 제외 |
| `FoundationAssetReconcilerStopped` | 조정기가 돌지 않음 | 30분 |
| `FoundationAssetUnreadable` | 조정기가 자산의 판을 못 읽음(카탈로그 REST, reads.jsonl) | 30분 |

`asset_lag_seconds` 는 읽지 않은 가장 새 입력 변경 시각에서 읽은 판의 커밋 시각을 뺀 값이다(따라잡았으면 0).

## 울렸을 때

1. 조정기의 판단: `journalctl -u foundation-asset-reconcile.service -n 30`. 자산마다 `owed`/`current`, `running`, `pool full`,
   지연, 이유가 한 줄씩 나온다.
2. `FoundationAssetUnscheduled`: Airflow 에서 `foundation_asset_reconcile` 의 마지막 실행 `start` 과제를 본다.
   - `skipped`(이미 같은 키로 시작함)인데 자산이 계속 밀려 있으면, 그 키의 실행이 실패했거나 아무것도 바꾸지 않은 것이다.
     그 작업 DAG 의 `reconcile__...` 실행 로그를 읽고 원인을 고친 뒤 작업을 손으로 한 번 시작한다(run-now).
   - `select` 가 비었는데 `owed` 이면 Airflow 가 그 DAG 의 실행을 대기·실행 중이라고 답한 것이다: 그 실행이 왜 시작되지
     않는지(멈춘 DAG, 풀) 본다.
3. `FoundationAssetReconcilerStopped`: `foundation_asset_reconcile` DAG 이 멈춰 있는지(배포 뒤 unpause 안 됨), 유닛이
   실패하는지(`systemctl status foundation-asset-reconcile.service`) 본다.
4. 단가 레인이 `waiting` 이면 정상이다: 건물 레인이 단가 Gold 와 같은 표제부·전유부 판의 건물 Gold 를 서빙할 때까지 기다린다
   (`scripts/ops/lane_follow_gate.py`). 건물 레인이 그 판을 굽고 나면 조정기가 다음 주기에 시작한다.

## 확인

- `cat /var/lib/foundation-jobs/metrics/asset-reconcile.prom` 에 자산마다 `foundation_asset_lag_seconds` 와
  `foundation_asset_reconcile_timestamp_seconds` 한 줄.
- `curl -s localhost:19090/api/v1/query --data-urlencode 'query=asset_lag_seconds'` 가 자산 세 개를 답한다.
