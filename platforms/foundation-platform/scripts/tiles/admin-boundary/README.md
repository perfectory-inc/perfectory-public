---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-09-06
---

# 행정경계 원천 변환 레인

2026-09-05 첫 발행 때 ai-server 셸에서 손으로 돌던 단계의 저장소 사본이다 — 2026-09-06
SSOT 정비가 명명한 "수동 구간"의 종결. 순서와 소유가 전부 여기 적혀 있으므로, 다음
빈티지 발행은 이 디렉터리만 따라가면 된다.

## 순서

루트 ADR-0112(레이크하우스 굽기)·ADR-0113(개편에도 id 유지) 이후의 레인이다.

1. `convert.sh` — R2 의 읍면동 shapefile ZIP 을 전부 GeoJSON 으로 변환(수집 회차가 섞여 있어도 된다).
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
   예약 작업이 없는 단계다: 회차가 올 때만 돈다(그래프 노드 `unscheduled: on_event`, 루트 ADR-0234).
4. `scripts/ops/map-edit-fold.sh admin` 을 `FOUNDATION_MAP_EDIT_FOLD_FORCE=1` 로 — Gold 서빙본과 타일을 새로 굽는다.

2026-09 회차 실측: 5,067개 동(7월과 같은 수), 16개 시도 파일(29·46 대신 12), 개편 동 703개 전부 옛 id 상속.

## 매개변수

- `ADMIN_BOUNDARY_WORKDIR` (기본 `/data/parcel-work/admin-src`)
- `ADMIN_BOUNDARY_GDAL_IMAGE` (기본 `ghcr.io/osgeo/gdal:ubuntu-small-3.10.2`)
