---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-02
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

## 굽기 오븐 `bake-lakehouse-tiles` (루트 ADR-0133 §3)

오븐은 유닛을 가리지 않는다. 산업단지·행정경계도 전국 필지(약 4천만 건)도 같은 코드로 굽는다.

| 무엇 | 정본 | 내용 |
|---|---|---|
| 입력 요약 | 서빙본 요약의 `schema_version` | `polygon_served_gold.v1` 이면 `..._SERVED_HANDOFF` 는 JSONL 파일 하나다. `polygon_served_gold.v2` 이면 `..._SERVED_HANDOFF` 는 조각 폴더다. 요약의 `handoff_parts[{path, rows, sha256}]` 에 적힌 `path` 는 그 폴더 기준 상대 경로다 |
| 조각 검사 | `lakehouse_tile_bake.rs` 의 `prepare_handoff` | 조각을 한 번 흘려 읽는다. 읽으면서 행마다 검사하고, 조각마다 행 수와 SHA-256 을 요약과 대조하고, GDAL 입력을 쓴다. id 는 하나마다 16바이트 hash 만 남긴다 |
| 컨테이너 상한 | [`config/tile-bake-containers.contract.json`](../../config/tile-bake-containers.contract.json) 의 `images.*.memory_limit` | 컨테이너를 `--memory <상한> --memory-swap <상한>` 으로 띄운다. 호스트 합계에는 `tools/host-memory-budget.contract.json` 의 `one_shot_contracts` 를 거쳐 한 번만 도는 작업으로 들어간다 |
| 작업 디스크 | 같은 계약의 `work_disk` | 시작 전에 `..._WORK_ROOT` 의 여유 공간을 본다. `max(min_free_bytes, 입력 바이트 × min_free_bytes_per_handoff_byte)` 보다 작으면 거부한다. 작업 폴더 `<WORK_ROOT>/<unit>-<uuid>` 에는 GDAL CSV, GeoJSON 줄, tippecanoe 임시 폴더(`-t /w/tmp`), 결과 판이 생긴다. 굽기가 끝나면 폴더를 지운다 |
| maxzoom 관문 | `pmtiles_feature_ids.rs` | PMTiles 디렉터리를 타일 id 순서로 따라가며 maxzoom 타일만 읽는다. MVT 에서는 layer 이름, 속성표, feature 태그만 해독하고 도형은 읽지 않는다. 빠진 id 와 남는 id 를 세고, 다섯 개까지 이름을 남긴다 |

전국 필지의 작업 루트는 루트 디스크가 아니라 `/data` 아래(`work_disk.production_root`)에 둔다. 산업단지·행정경계는
지금처럼 접기 폴더를 쓴다.

메모리 상한(GDAL 4g, tippecanoe 8g)과 디스크 배수(8)는 서울 시험 굽기(898,741건, 최대 메모리 0.96GB)를 근거로
넉넉히 잡은 값이다. 전국을 처음 실제로 구운 뒤에는 최대 메모리, 작업 디스크, 걸린 시간을 여기 적고 계약 값을
고친다(ADR-0133 §7).

## 첫 lakehouse 판의 동등성 관문 (루트 ADR-0133 §4)

활성 release 가 `lakehouse_bake` 로 구운 판이 아니면(지금의 필지: PostGIS·`martin-cp` 판), 오븐은 승격 전에 새 판을
활성 판과 타일 단위로 비교한다. 활성 판이 이미 `lakehouse_bake` 면 비교를 건너뛰고, 증거에
`first_release_equivalence.skipped` 를 남긴다.

| 무엇 | 정본 | 내용 |
|---|---|---|
| 표본 | [`config/tile-equivalence.contract.json`](../../config/tile-equivalence.contract.json) | `zooms` 마다 비교한다. 지역마다 가운데와 안쪽으로 들인 네 모서리, 활성 판 디렉터리에서 가장 긴 maxzoom 타일 `densest_tiles` 개, 새 판 maxzoom 타일에서 `random_seed` 로 고른 `random_tiles` 개를 쓰고, 각 타일의 조상도 함께 본다. 지역은 `region_id_prefix_chars` 의 id 앞자리다(필지는 시도 코드 두 자리). 지역 범위는 maxzoom id 관문이 새 판을 읽을 때 같이 잰다 |
| 활성 판 읽기 | `tile_derivative` 읽기 설정 | R2 범위 읽기로 디렉터리와 표본 타일만 읽는다. 5GB 파일을 통째로 받지 않는다 |
| 통과 조건 | `tile_equivalence.rs` | (가) 활성 타일의 id 가 새 타일에 모두 있다. (나) 새 타일에만 있는 id 는 도형 범위가 타일의 `0..extent` 밖이다(버퍼 이웃). 이런 feature 만 도형을 해독한다. (다) 같은 id 의 속성이 글자로 같다. MVT 형식만 다르면(정수 → 문자열) `type_changed` 로 세지만 실패는 아니다 |
| 타일 안의 남는 id | 같은 파일의 `ExtraInside` | 모두 실패다. 원인을 보이도록 셋으로 나눠 zoom·지역별로 센다. `tiny`: 타일 안 부분이 `tiny_extent_units` 이하. `addition`: 그 위치의 활성 maxzoom 타일에도 없다(활성 판에 아예 없는 feature). `unexplained`: 그 밖 |
| 실패 | build 의 실패 사유 | 승격을 거부하고 build 를 실패로 남긴다. 사유에는 범주별 개수와 id 예시가 들어간다. 전체 증거(범주별 예시 20개, zoom·지역별 개수, 지역별 표본·통과 수)는 `<WORK_ROOT>/<unit>-first-release-equivalence-<build_job_id>.json` 에 남는다. 서빙되는 것은 바뀌지 않는다 |

### 2026-10-02 시험 비교 (서울 시범판 대 운영 필지판)

ai-server scratch 에서 서울만 담은 시범판(898,741건)을 운영 필지판과 비교했다. 활성 타일은 tiles.perfectory.io 에서
읽었다. 표본은 171장이다(z14 49, z15 57, z16 65). 게이트웨이에는 디렉터리가 없어서 densest 표본은 시범판 디렉터리에서
골랐다. 표본의 활성 feature 는 478,205개다.

| 범주 | 서울(앞자리 11) | 서울 밖 |
|---|---:|---:|
| 빠진 id | 0 | 50,597 (경기 41, 충남 44. 시범판에 없음) |
| 속성 변경 / 형식 변경 | 0 / 0 | — |
| 버퍼 안의 남는 id | 26,735 | — |
| 타일 안의 남는 id: `tiny` | 812 (z14 751, z15 58, z16 3) | — |
| 타일 안의 남는 id: `addition` | 0 | — |
| 타일 안의 남는 id: `unexplained` | 11 (z14 7, z15 4) | — |

`unexplained` 11개는 모두 가는 조각이다. 범위는 30–60 단위지만 넓이는 4–42 제곱 단위다. 그래서 범위로 재는
`tiny` 에 들지 않았다. 지금의 규칙 (나)는 타일 안의 남는 id 를 하나도 허용하지 않는다. 그래서 이 관문은 첫 전국
필지판을 거부한다. 규칙을 바꿀지는 ADR-0133 §4 를 잇는 결정(ADR-0134)이 정한다.

## 함정 (2026-09-29 첫 설치에서 실제로 겪음)

- **공용 `/var/lib/foundation-platform/lakehouse` 의 소유를 바꾸지 말 것.** Spark(uid 185) 소유이고 다른
  적재가 쓴다. 접기는 전용 `map-edit-fold/lakehouse` 를 쓴다.
- **서비스 계정은 `recovery.env` 를 못 읽는다.** systemd 는 root 로 읽어 넘기므로 Airflow 가 시작시키는 서비스는 괜찮지만,
  손으로 돌릴 때는 root 가 env 를 읽고 `sudo --preserve-env -u foundation-platform` 으로 넘겨야 한다.
- **Wrangler 자동 생성 D1 은 `database_id` 를 설정에 쓰지 않는다.** `d1 migrations apply --remote` 는 id 가
  있어야 하므로 배포용 체크아웃에만 임시로 넣고 되돌린다(`wrangler d1 list --json` 으로 조회).
- **컨테이너 종료 코드를 따옴표 안에서 `$?` 로 찍으면 밖에서 먼저 풀려 늘 0 이다.** 결과는 journal 의
  `lakehouse-tile-bake-ok` 줄로 판정한다.
