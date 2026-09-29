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
2. `merge.py --round YYYYMM --code-list <법정동코드 전체자료>` — 그 회차 파일만 병합한다.
   시군구명은 공식 코드목록에서 가져온다(개편된 시도는 옛 지형도 시군구 파일에 이름이 없다).
   공식 목록의 시도 중 하나라도 회차 파일이 없으면 멈춘다. 제외한 행은 사유별로
   `merge-report.json` 에 남는다(코드 형식이 아닌 필지 파편 행이 매 회차 약 300개).
3. `infra/lakehouse/spark/jobs/legal_dong_predecessor_map.py` — 두 지적 스냅숏의 PNU 와 공식
   코드목록으로 "새 법정동코드 ← 옛 법정동코드"를 만든다(필지 계보의 동 대응, 리→읍면 올림 포함).
4. `infra/lakehouse/spark/jobs/administrative_boundaries_handoff_to_silver.py --predecessor-map ...`
   — `silver.administrative_boundaries` 에 회차 스냅숏을 덧붙인다. 코드가 바뀐 동은 옛 동의 id 를 잇는다.
5. `scripts/ops/map-edit-fold.sh admin` 을 `FOUNDATION_MAP_EDIT_FOLD_FORCE=1` 로 — Gold 서빙본과 타일을 새로 굽는다.

2026-09 회차 실측: 5,067개 동(7월과 같은 수), 16개 시도 파일(29·46 대신 12), 개편 동 703개 전부 옛 id 상속.

## 매개변수

- `ADMIN_BOUNDARY_WORKDIR` (기본 `/data/parcel-work/admin-src`)
- `ADMIN_BOUNDARY_GDAL_IMAGE` (기본 `ghcr.io/osgeo/gdal:ubuntu-small-3.10.2`)
