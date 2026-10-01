---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-09-29
---

# 지도 편집 접기 — 설치·운영·확인 런북

루트 [ADR-0112](../../../../docs/adr/0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md)
의 운영 절차다. 관리자 폴리곤 편집은 편집 저장소(D1, `foundation-map-edit-gateway`)에 쌓이고
손님 지도는 오버레이로 즉시 본다. 이 런북은 그 편집을 타일에 접는 매시간 작업을 다룬다.

## 흐름

유닛마다 Airflow 작업이 하나다(루트 ADR-0122, `orchestration/jobs.v1.json`). 둘 다 자원 묶음 `spark`(자리 하나)에 있어 한 호스트에서 Spark·굽기가 겹치지 않는다.

| Airflow 작업 | 유닛 | 시각(UTC) | Silver 좌표계 | Spark 작업 |
|---|---|---|---|---|
| `foundation_map_edit_fold_complex` | `complex` | 매시 05분 | EPSG:5186 | `industrial_complex_boundary_served_gold.py` |
| `foundation_map_edit_fold_admin` | `admin` | 매시 35분 | EPSG:4326 | `administrative_boundary_served_gold.py` |

둘 다 `foundation-map-edit-fold@<unit>.service` → `scripts/ops/map-edit-fold.sh <unit>`:

1. 손님 오버레이(`/overlay/{unit}`)가 200 인지 본다. 아니면 슬랙 `#alerts` 🔴.
2. 접히지 않은 편집 수와 가장 오래된 편집의 나이를 본다. 6시간 이상이면 🟠.
3. 편집이 없으면 끝(`pending=0 skipped` 한 줄). 같은 원천을 다시 구우면 타일 주소만 바뀌어 캐시가 버려진다.
   새 Silver 원천을 반영할 때는 `FOUNDATION_MAP_EDIT_FOLD_FORCE=1` 로 강제한다.
4. `export-map-edit-handoff`(유닛의 Silver 좌표계로) → 유닛의 Spark 작업(원장 덧붙이기 + Gold 서빙본)
   → `bake-lakehouse-tiles`(GDAL·tippecanoe 굽기, 관문, R2, 승격, 접기 기록).
5. 성공·실패 모두 `#alerts` 에 한 줄, journal 에 한 줄.

## 호스트에 필요한 것

| 경로 | 내용 | 권한 |
|---|---|---|
| `/etc/foundation-platform/map-edit.env` | `FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL`, `FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN`(Worker secret 과 같은 값) | root:foundation-platform 0640 |
| `/etc/foundation-platform/map-edit-fold.env` | 레이크하우스 카탈로그 3개(`..._CATALOG_URI`, `..._WAREHOUSE`, `..._CATALOG_TOKEN`)와 `..._CATALOG_PROVIDER`, R2 타일 파생물 9개(`r2-connections.contract.json` 의 `tile_derivatives.required_env`), `FOUNDATION_PLATFORM_RUNTIME_ENV=production`, `FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE`, `FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT`, `FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_{PUBLIC_TILES_BASE_URL,TOOL_TIMEOUT_SECONDS,OPERATOR_STAFF_ID}` | root:foundation-platform 0640 |
| `/var/lib/foundation-platform/bin/foundation-outbox-publisher` | 현재 릴리스로 빌드한 퍼블리셔 | root:foundation-platform 0755 |
| `/var/lib/foundation-platform/map-edit-fold/lakehouse` | 접기 전용 Spark 작업 폴더 (`foundation-release.sh timers` 가 만든다) | 0777 |
| `/var/lib/foundation-platform/lakehouse-ivy` | Spark 패키지 캐시 | 0777 |
| 도커 이미지 `foundation-tippecanoe:2.79.0-local` | `infra/tiles/tippecanoe/Dockerfile` 로 빌드 | — |

두 env 파일이 없으면 Airflow 가 시작시킨 서비스가 실패하고 그 실행이 Airflow 와 저널에 실패로 남는다.
운영 compose 는 `map-edit.env` 를 `FOUNDATION_PLATFORM_MAP_EDIT_ENV_FILE` 로 읽어 foundation-api 에
넘기므로, 이 파일을 바꾼 뒤에는 `foundation-release.sh migrate` 로 API 를 다시 올린다.

## 확인

```bash
# 운영 manifest 의 unit 이 lakehouse 굽기 release 이고 폴백이 없는가
docker exec foundation-platform-runtime-postgres-1 psql -U foundation_admin -d foundation -At -c \
  "SELECT unit_key, active_release_id, serving_generation, fallback_release_id IS NULL
   FROM catalog.vector_tile_publication_unit ORDER BY unit_key"
# Airflow 의 최근 실행과 마지막 접기 기록
bash /opt/foundation-platform/current/scripts/deploy/airflow-runtime.sh exec -T airflow-scheduler \n  airflow dags list-runs foundation_map_edit_fold_admin -o plain | head -5
sudo tail -3 /var/lib/foundation-platform/map-edit-fold/journal.log
```

## 함정 (2026-09-29 첫 설치에서 실제로 겪음)

- **공용 `/var/lib/foundation-platform/lakehouse` 의 소유를 바꾸지 말 것.** Spark(uid 185) 소유이고 다른
  적재가 쓴다. 접기는 전용 `map-edit-fold/lakehouse` 를 쓴다.
- **서비스 계정은 `recovery.env` 를 못 읽는다.** systemd 는 root 로 읽어 넘기므로 Airflow 가 시작시키는 서비스는 괜찮지만,
  손으로 돌릴 때는 root 가 env 를 읽고 `sudo --preserve-env -u foundation-platform` 으로 넘겨야 한다.
- **Wrangler 자동 생성 D1 은 `database_id` 를 설정에 쓰지 않는다.** `d1 migrations apply --remote` 는 id 가
  있어야 하므로 배포용 체크아웃에만 임시로 넣고 되돌린다(`wrangler d1 list --json` 으로 조회).
- **컨테이너 종료 코드를 따옴표 안에서 `$?` 로 찍으면 밖에서 먼저 풀려 늘 0 이다.** 결과는 journal 의
  `lakehouse-tile-bake-ok` 줄로 판정한다.
