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

자동배포가 돌지 않을 때, 시도가 작은 것부터 돈다(첫 실행에서 시간·메모리를 잰다):

```bash
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh job start parcel_registry --requested-at "$(date +%s)" --reason "<왜 지금 돌리는지>"
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh job status parcel_registry
```

마지막 줄 `parcel-registry-outcome outcome=changed edition=… registered=<시도들> remaining=<n>`. journal 의 시도별
`seconds=` 와 Spark 컨테이너의 최대 메모리를 이 표에 실측으로 바꿔 적는다. 가장 큰 시도가 메모리로 죽으면(137) 켜지 않고
드라이버로 모으는 구조를 다시 연다. 모든 시도가 끝나면 다음 실행은 `unchanged reason=all_registered` 다. 그 뒤
`enabled: true` PR 을 낸다(ADR-0122 §4).

## 이 작업이 하지 않는 것

- 판이 바뀐 뒤 id 를 잇는 advance 는 `silver.parcel_lineage` 를 읽는다. 그 표는 2026-10-14 부터 `lineage_stewardship` 의
  0b 단계가 판 쌍마다 만든다(#542). advance 를 이 작업에 붙이는 것은 그 표가 운영에 쌓인 뒤의 별도 변경이다.
- 필지 단위 발행 ID(`place_id_release_to_gold.py --unit parcel`)는 두 판의 날짜가 필요하다. advance 와 함께 온다.
