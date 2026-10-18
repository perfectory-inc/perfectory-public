---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-14
---

# 산업단지 경계 새 판 — 예약 작업 런북

[루트 ADR-0240](../../../../docs/adr/0240-the-complex-boundary-file-is-loaded-by-a-scheduled-job.md) 의 운영 절차다.
산단 경계 원천(`vworldkr__sandan_boundary`, 파일 하나)은 매일 수집이 받고, 예약 작업 `complex_boundary_release` 가
새 파일을 `silver.industrial_complex_boundaries` 에 덧붙이고 산단 타일을 다시 굽는다.

| 무엇 | 정본 |
| --- | --- |
| 작업 | `orchestration/jobs.v1.json` 의 `complex_boundary_release`(spark 3 자리, `started_by: inputs`, 대체 09:50 UTC) |
| 실행 | `infra/systemd/foundation-complex-boundary-release.service` → `scripts/ops/complex-boundary-release.sh` |
| 변환 | 발행기 `export-industrial-complex-boundary-silver-handoff` → `industrial_complex_boundaries_handoff_to_silver.py` |
| 지금 판 | 가장 새 `source_snapshot_id`(가장 늦은 `valid_from_utc`)의 행. 읽는 곳은 `current_official_boundaries` 하나 |
| 작업 폴더 | `/var/lib/foundation-platform/map-edit-fold/lakehouse/complex-boundary-release/<시각>`(받은 ZIP·넘김 파일은 지운다) |
| journal | `/var/lib/foundation-platform/map-edit-fold/complex-boundary-release/journal.log` |

## 한 번의 실행

1. Bronze 의 산단 경계 ZIP 중 **가장 늦게 올라온 것**을 고른다(키가 판을 말하지 않는다: 손 시절 `30137-1.zip`,
   수집이 쓰는 내용 주소 키).
2. 읽기 키로 받고, 발행기가 Silver 행으로 바꾼다. 판 id 는 `vworldkr__sandan_boundary-<객체 이름>` 이다.
3. Spark 가 적재한다. 표의 적재 기록(`foundation.ingest-batch-objects`)에 그 ZIP 이 이미 있으면 아무것도 쓰지 않는다.
4. 바뀌었으면 `map-edit-fold.sh complex` 를 FORCE 로 부른다.

마지막 줄:

```text
complex-boundary-release-outcome outcome=changed|unchanged reason=… object=<Bronze 키>
foundation-job-outcome changed|unchanged
```

| reason | 뜻 |
| --- | --- |
| `already_loaded object=…` | Silver 가 그 ZIP 을 이미 가졌다 |
| `no_object` | 산단 경계 ZIP 이 Bronze 에 없다 |
| (changed) `object=…` | 덧붙이고 타일을 다시 구웠다 |

## 켜기 — 감독 실행 (한 번)

작업은 꺼진 채 등록됐다. 자동배포가 돌지 않을 때:

```bash
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh job start complex_boundary_release --requested-at "$(date +%s)" --reason "<왜 지금 돌리는지>"
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh job status complex_boundary_release
```

지금 Silver 의 판은 2026-09-01 에 적재 기록과 함께 들어간 `30137-1.zip` 이다. 수집이 같은 파일을 다시 받지 않으면 첫
실행은 `unchanged reason=already_loaded` 다. 통과하면 `enabled: true` PR 을 낸다(ADR-0122 §4).

## 거부

| 문구 | 할 일 |
| --- | --- |
| `refusing an object key outside the Bronze layout` | 수집 장부·R2 를 조사한다(사고) |
| 발행기 `.prj` 투영 거부 | 제공자가 좌표계를 바꿨다. ADR-0042 와 계약의 `geometry_srid` 를 먼저 본다 |
| 서빙본·PostGIS 투영의 `two active official boundaries` | 같은 판 안에 한 단지 경계가 둘이다. 원천을 조사한다 |
