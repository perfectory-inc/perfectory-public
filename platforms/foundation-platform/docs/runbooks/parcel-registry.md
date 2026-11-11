---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-14
---

# 필지 ID 원장 첫 등록 — 예약 작업 런북

[루트 ADR-0243](../../../../docs/adr/0243-the-parcel-registry-is-registered-region-by-region-by-a-scheduled-job.md) 의 운영 절차다.
`silver.parcel_registry`(ADR-0113)는 운영에 아직 없다(2026-10-14 카탈로그 확인). 예약 작업 `parcel_registry` 가 지도가 쓰는
필지 판(원천 계약의 `served_edition`)의 시도를 하나씩 bootstrap 한다.

| 무엇 | 정본 |
| --- | --- |
| 작업 | `orchestration/jobs.v1.json` 의 `parcel_registry`(spark 3 자리, 매일 10:10 UTC) |
| 실행 | `infra/systemd/foundation-parcel-registry.service`(150 분) → `scripts/ops/parcel-registry-cycle.sh` |
| 등록 | `infra/lakehouse/spark/jobs/parcel_registry_to_silver.py`(`--probe-missing`, `--bootstrap`) |
| journal | `/var/lib/foundation-platform/lineage-stewardship/parcel-registry/journal.log`(시도마다 `region= parcels= seconds=`) |

## 예상 (2026-10-14, 실측 전)

| 항목 | 값 |
| --- | --- |
| 대상 | 판 202606, 시도 17, 필지 약 3,986 만. 가장 큰 시도 약 593 만 |
| 실행 단위 | 시도 하나 = Spark 한 번. 스크립트가 PNU 와 행을 드라이버로 모으므로 전국 한 번은 메모리에 들어가지 않는다 |
| 메모리 | 가장 큰 시도에서 드라이버 8–10 GB 추정, `--driver-memory 16g`(compose `spark` 20g 상한) |
| 시간 | 큰 시도 5–15 분 추정, 전체 1.5–3 시간. 100 분 뒤에는 새 시도를 시작하지 않고 다음 실행이 잇는다 |
| R2 | 약 4 천만 행(zstd 1.5–2.5 GB). 저장 월 수십 원, 쓰기·읽기 요청 1 원 미만 |

## 감독 실행

시도가 작은 것부터 돈다(첫 실행에서 시간·메모리를 잰다). 감독 실행: 더니어 데이터 흐름 → 예약 작업 → 이 작업 줄의 운영 → "지금 한 번 돌리기", 이유를 적고 누른다(루트 ADR-0282; 꺼진 작업도 된다. 10분 안에 시작하고, 대기열·빌드는 예약 실행처럼 기다린다). 진행은:

```bash
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh job status parcel_registry
```

마지막 줄 `parcel-registry-outcome outcome=changed edition=… registered=<시도들> remaining=<n>`. journal 의 시도별
`seconds=` 와 Spark 컨테이너의 최대 메모리를 이 표에 실측으로 바꿔 적는다. 가장 큰 시도가 메모리로 죽으면(137) 켜지 않고
드라이버로 모으는 구조를 다시 연다. 모든 시도가 끝나면 다음 실행은 `unchanged reason=all_registered` 다. 그 뒤
`enabled: true` PR 을 낸다(ADR-0122 §4).

## 후보 판의 한 걸음 (root ADR-0490 §4a)

등록부는 판마다 쌓는다. 행마다 `to_snapshot_id`(그 행을 등록한 판)가 있고, 읽는 쪽은 모두 "판 X 까지"로 접는다
(`parcel_registry_frames.registry_through`; 표 계약의 `row_versions.canonical_reader` 라 다른 방식으로 접는 잡은 시험이 거부한다).

- 매 실행 2b 단계가 판정 보고서(`/var/lib/foundation-platform/parcel-editions/readiness.json`)에서 후보 판을 읽는다
  (`vworld_parcel_editions.py candidate`: 서빙판보다 새로, source·silver 를 통과하고 rehearsal 만 남은 가장 새 판).
- `--probe-advance` 가 후보의 행이 아직 없는 지역을 고른다. 지역마다 그곳에 등록된 가장 새 판에서 계보 쌍을 따라
  advance 한다. 계보가 잇는 시도(2026-07-01 29·46 → 12)는 한 지역이다. 그 쌍의 계보가 없으면 기다린다.
- advance 는 조인으로 돈다. 두 판이 같이 가진 필지는 행을 쓰지 않고 드라이버에도 오지 않는다.
- 서빙판을 읽는 place-id 발행은 서빙판까지 접으므로 승격 전에는 그대로다. 승격되면 그 판의 행이 승격 시각부터 보이는
  것으로 세어(`visible_at_utc`) 바뀐 지역만 다시 발행한다.
- 후보의 대조 관문(`parcel_matching_gate.py --snapshot-id <후보>`)은 후보까지 접어 (b) 를 본다.

확인: `journal.log` 의 `parcel registry advance edition=… region=… from=… parcels=… seconds=…` 줄, 그리고

```sql
SELECT to_snapshot_id, status, count(*) FROM silver.parcel_registry GROUP BY 1, 2;
```

## 이 작업이 하지 않는 것

- 계보(`silver.parcel_lineage`)는 `lineage_stewardship` 의 0b 단계가 판 쌍마다 만든다(#542). 이 작업은 읽기만 한다.
- 승격은 판정 작업이 한다(ADR-0490 §5). 이 작업은 후보의 등록부 한 걸음만 덧붙인다.
