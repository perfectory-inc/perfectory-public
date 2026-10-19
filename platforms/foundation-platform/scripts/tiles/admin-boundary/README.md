---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-14
---

# 행정경계 원천 변환 레인

2026-09-05 첫 발행 때 ai-server 셸에서 손으로 돌던 단계의 저장소 사본이다. 2026-10-14 부터 아래 순서 전부를
예약 작업 `admin_boundary_release`(`scripts/ops/admin-boundary-release.sh`, 루트 ADR-0236)가 돈다: 매일 수집이
`boundary_emd` 를 받고, 작업이 새 회차를 알아보고 1–4 를 한다. 손으로 돌릴 일은 아래 "예약 작업"의 감독 실행뿐이다.

## 순서

루트 ADR-0112(레이크하우스 굽기)·ADR-0113(개편에도 id 유지) 이후의 레인이다.

1. `convert.sh download` 로 R2 의 읍면동 shapefile ZIP 을 전부 받고(읽기 키), `rounds.py` 가 ZIP 안 레이어 이름
   (`…_<시도>_<YYYYMM>`)에서 가장 새 회차를 고른다. `convert.sh convert <zip>…` 는 그 회차의 ZIP 만 GeoJSON 으로 바꾼다.
   함정 셋이 스크립트에 박제돼 있다: GEOS 없는 alpine GDAL 금지(-makevalid),
   busybox unzip 의 Zip64 실패(→ /vsizip), DBF 는 EUC-KR.
2. `merge.py --round YYYYMM --out-dir <작업 폴더>/out` — 그 회차 파일만 병합한다. 레이크하우스 Spark 컨테이너에서
   (`spark-submit /workspace/scripts/tiles/admin-boundary/merge.py ...`, 작업 폴더는 컨테이너가 보는 경로) 돈다.
   공식 코드목록은 `reference.legal_dong_code_snapshot` 의 최신 스냅숏이다(매일 code.go.kr 수집, 루트 ADR-0143) —
   손으로 받은 코드 파일은 없다. 시군구명은 거기서 가져온다(개편된 시도는 옛 지형도 시군구 파일에 이름이 없다).
   공식 목록의 시도 중 하나라도 회차 파일이 없거나 스냅숏이 비었으면 멈춘다. 결과는 `--out-dir` 옆에 쓴다. 제외한 행은 사유별로
   `merge-report.json` 에 남는다(코드 형식이 아닌 필지 파편 행이 매 회차 약 300개).
3. `infra/lakehouse/spark/jobs/administrative_boundaries_handoff_to_silver.py --legal-dong-change-table reference.legal_dong_code_change [--predecessor-parcel-snapshot-id <개편 전 필지 스냅숏>]`
   — `silver.administrative_boundaries` 에 회차 스냅숏을 덧붙인다. 코드가 바뀐 동은 옛 동의 id 를 잇는다.
   "새 법정동코드 ← 옛 법정동코드"는 코드 변경표의 view 로 읽는다(루트 ADR-0145, 리→읍면 올림 포함).
   손으로 만드는 선행 코드 파일은 없다. 동 둘이 하나로 합쳐졌으면 필지 스냅숏에서 필지를 더 많이 가진 쪽이 id 를 잇는다.
   그다음 `infra/lakehouse/spark/jobs/place_id_release_to_gold.py --unit admin --release-id admin-<UTC 시각> --previous-release-id <앞 release_id> --allow-non-smoke-write`
   — 그 회차의 발행 ID 목록·ID↔코드 연결표·ID 변경 내역을 Gold 에 낸다(루트 ADR-0113 8항). 앞 `release_id` 는
   `gold.place_id_registry` 의 `unit = 'admin'` 행에 있다(2026-09 회차: `admin-<YYYYMMDD>T<HHMMSS>Z` 꼴).
   예약 작업은 `--previous-release-id` 없이 돈다: 스크립트가 Silver 의 모든 회차를 차례로 읽어 변경 내역을 만든다.
4. `scripts/ops/map-edit-fold.sh admin` 을 `FOUNDATION_MAP_EDIT_FOLD_FORCE=1` 로 — Gold 서빙본과 타일을 새로 굽는다.

2026-09 회차 실측: 5,067개 동(7월과 같은 수), 16개 시도 파일(29·46 대신 12), 개편 동 703개 전부 옛 id 상속.

## 예약 작업

`orchestration/jobs.v1.json` 의 `admin_boundary_release`(spark 3 자리, `started_by: inputs` — 매일 수집이 새 파일을 받은
실행 뒤, 대체 09:30 UTC). 2026-10-14 감독 실행(40 초, `unchanged reason=already_loaded round=202609`) 뒤 켰다. 감독 실행은
그때는 운영자 스크립트의 `job start` 로 했다. 이제는 더니어 데이터 흐름 → 예약 작업 → 이 작업 줄의 운영 → "지금 한 번 돌리기", 이유를 적고 누른다(루트 ADR-0282; 꺼진 작업도 된다. 10분 안에 시작하고, 대기열·빌드는 예약 실행처럼 기다린다). 진행은:

```bash
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh job status admin_boundary_release
```

마지막 줄 `admin-boundary-release-outcome outcome=… reason=…` 과 `foundation-job-outcome …` 를 본다.

| reason | 뜻 |
| --- | --- |
| `already_loaded round=<YYYYMM>` | Silver 가 그 회차를 이미 가졌다. 받은 ZIP·변환본은 지운다 |
| `incomplete_round` | 공식 코드 목록의 시도 중 그 회차 파일이 없는 곳이 있다. 다음 날 다시 본다 |
| `no_round` | 경계 레이어를 가진 ZIP 이 없다(수집 전) |
| (changed) `round=… snapshot=… rows=…` | 덧붙이고, 발행 ID 를 내고, 타일을 다시 구웠다 |

통과하면 `enabled: true` PR 을 낸다(ADR-0122 §4). 작업 폴더는
`/var/lib/foundation-platform/map-edit-fold/lakehouse/admin-boundary-release/<시각>`, journal 은 그 위 `admin-boundary-release/journal.log`.

## 매개변수

- `ADMIN_BOUNDARY_WORKDIR` (기본 `/data/parcel-work/admin-src`)
- `ADMIN_BOUNDARY_GDAL_IMAGE` (기본 `ghcr.io/osgeo/gdal:ubuntu-small-3.10.2`)
