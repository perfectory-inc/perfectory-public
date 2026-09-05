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

1. `convert.sh` — R2 의 읍면동 shapefile ZIP 17개를 시도별 GeoJSON 으로 변환.
   함정 셋이 스크립트에 박제돼 있다: GEOS 없는 alpine GDAL 금지(-makevalid),
   busybox unzip 의 Zip64 실패(→ /vsizip), DBF 는 EUC-KR.
2. `merge.py` — 17개를 병합해 발행 원천
   `official-administrative-boundary.geojson` 을 만들고 sha256 과
   canonical 10진값을 출력한다. 부모 시군구명이 없는 동은 세지 않고 제외 수로
   보고한다(첫 발행에서 법정동 5181033025 가 이 길로 명명 제외됐다 — 원천이 경계가
   아니라 필지 파편으로 배달된 사례).
3. `write-official-administrative-boundary-source-snapshot` (publisher 명령) — 스냅샷
   JSONL 생성.
4. `register-serving-source-lineage` (publisher 명령) — source_record/file_asset/
   revision(candidate) 를 실측값으로 등록. 손 SQL 은 이제 금지 가능하다.
5. `publish-administrative-boundary-postgis` → `promote-administrative-boundary-runtime`.

## 매개변수

- `ADMIN_BOUNDARY_WORKDIR` (기본 `/data/parcel-work/admin-src`)
- `ADMIN_BOUNDARY_GDAL_IMAGE` (기본 `ghcr.io/osgeo/gdal:ubuntu-small-3.10.2`)
