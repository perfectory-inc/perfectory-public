---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-13
---

# 필지 타일 전국 굽기 — 레이크하우스 오븐 런북

[루트 ADR-0133](../../../../docs/adr/0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md) §7과
[루트 ADR-0194](../../../../docs/adr/0194-retire-the-five-postgres-copies-that-r2-already-holds.md) 5번의 운영 절차다.
필지 지도 타일을 PostGIS 왕복 없이 레이크하우스(R2·Iceberg)에서 전국으로 다시 굽는다.

| 무엇 | 정본 |
| --- | --- |
| 굽는 판(edition) | `infra/lakehouse/contracts/vworld-parcel-source-objects.json` 의 `served_edition` (ADR-0148 §1) |
| 굽기 순서 | `scripts/ops/parcel-tile-bake.sh` 머리말 |
| 예약·시간 한계·풀 | `orchestration/jobs.v1.json` 의 `parcel_tile_bake`, `infra/systemd/foundation-parcel-tile-bake.service` |
| 컨테이너 이미지·메모리 상한·작업 디스크 | `config/tile-bake-containers.contract.json` (작업 루트 `work_disk.production_root`) |
| 동등성 표본 | `config/tile-equivalence.contract.json` (ADR-0133 §4, ADR-0135) |
| 환경 파일 | `config/runtime-secrets.contract.json` 의 유닛 `foundation-parcel-tile-bake.service` |
| 작업 폴더·증거 | `/data/foundation-platform/tile-bake/lakehouse/parcel-tile-bake/<run>/`(판정·서빙본 요약·조각·로그, 쌓기만) |

## 1. 한 번의 실행

1. 계약의 `served_edition` 이 이 작업이 마지막으로 구운 판과 같으면 "할 일 없음"으로 끝난다
   (`/var/lib/foundation-parcel-tile-bake/baked-edition`). `--force <이유>` 는 같은 판을 다시 굽는다.
2. 대조 관문 `parcel_matching_gate.py` 를 판의 모든 시도에 돌린다(ADR-0113 §7). 거부면 아무것도 쓰지 않는다.
3. `parcel_boundary_served_gold.py` 가 `gold.parcel_boundary_served` 를 커밋하고 v2 조각을 쓴다.
4. `bake-lakehouse-tiles` 가 GDAL·tippecanoe 로 굽고, 관문(최대 줌 feature id 전수)과 첫 판 동등성 관문을
   통과하면 R2 에 create-only 로 올리고 승격한다. 승격 전 실패는 서비스에 아무것도 바꾸지 않는다.

PostGIS 는 읽지도 쓰지도 않는다. 마지막 줄은 승격했으면 `foundation-job-outcome changed`, 아니면 `unchanged` 다.

## 2. 첫 감독 실행 (작업을 켜기 전)

작업은 `enabled: false` 로 등록된다. 켜기 전에 한 번 사람이 지켜보며 돌리고 아래를 적는다.

1. 동등성 증명(읽기만): `scripts/ops/parcel-boundary-equality.py` 로 Silver·서빙본·미러·발행본의 개수와 도형 해시를
   시도마다 맞춘다. 먼저 `--region 11`(표본), 그다음 전국. 차이·중복·해시 불일치가 하나라도 있으면 멈춘다.
2. 서버에서 `sudo systemctl start foundation-parcel-tile-bake.service` 를 쓰지 않는다(꺼진 작업은 호스트가 거부한다).
   서비스 계정으로 같은 스크립트를 돌린다. 시간·최대 메모리·`/data` 최대 사용량을 잰다.
3. 결과를 이 문서 4절에 적는다. 160분 안이면 `enabled: true` 로 바꾸는 PR 을 낸다. 넘으면 ADR-0138 의 풀 결정을
   다시 연다(이 줄만 올리지 않는다).

## 3. 되돌리기

승격 전 실패(관문 거부 포함)는 되돌릴 것이 없다. build 가 `failed` 로 남고 활성 판은 그대로다.

승격 뒤에는 **이전 판을 다시 승격하는 명령이 없다**(`readdress-static-release` 는 활성 판의 바이트만 새 주소로
옮긴다). 이전 판의 PMTiles 객체는 R2 에 create-only 로 남아 있다. 틀린 판은 앞으로 고친다. 원인(Silver·서빙본)을
고친 뒤 `--force <이유>` 로 다시 굽는다. 그래서 첫 승격은 1절의 두 관문과 2절의 동등성 증명을 모두 통과한 뒤에만 한다.

## 4. 실측

2026-10-13 운영 서버(release 1d7bcd4e)에서 판 202606 전국(39,861,511필지)을 단계별로 쟀다(ADR-0133 §7,
ADR-0194 5.2). 승격은 하지 않았다. 대조 관문이 판을 거부했고(아래), 오븐의 GDAL·tippecanoe 단계는 DB·R2 없이 같은
이미지·상한·인자로 따로 돌렸다. 같은 디스크(`/data`, HDD)에서 동등성 증명의 PostGIS 정렬 스캔이 함께 돌아, 시간은
경합 상태의 상한이다.

| 단계 | 시간 | 최대 메모리(상한) | `/data` 최대 |
| --- | --- | --- | --- |
| 대조 관문 `parcel_matching_gate.py` (`local[8]`, `--no-registry`) | 13.5분 | — | — |
| 서빙본 `parcel_boundary_served_gold.py` (`local[8]`, driver 16g) | 59분(Spark 41분 + 조각 해시 검증) | — | 조각 64개 25.0GB |
| GDAL `ogr2ogr -makevalid` | 60분 | 199MiB (4g) | 43.7GB (CSV 21.3 + GeoJSONSeq 22.3) |
| tippecanoe v2.79.0, z14–16 | 109분 | 5.42GiB (8g) | 33.7GB (GeoJSONSeq + 판) |
| 결과 판 | — | — | 5.55GB (PostGIS판 5.37GB) |

- 합계는 약 4시간이다. ADR-0133 §7의 예산 360분 안이지만 유닛 한계 160분(스파크 풀 굶김 한계)은 넘는다. 그래서 이
  작업을 켜기 전에 풀 결정(ADR-0138)을 다시 연다. 예: Spark 두 단계만 spark 풀에 두고, 굽기는 별도 작업으로 나눈다.
- 작업 디스크: 핸드오프 25.0GB에 대해 최대 43.7GB였다(1.75배, 핸드오프 포함 2.75배). 계약의 배수 8은 넉넉하다.
- 대조 관문은 202606을 거부했다. 공식 목록(2026-10-04)에 없는 법정동이 6,483,504필지, 읍면동 경계가 없는 필지가
  6,460,785필지다(2026-07 개편 지역). 202609는 22,740 + 2필지다. ADR-0133 §5에 따라 이 판으로는 굽지 않는다.
