---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-05
---

# VWorld 연속지적도 판 — 확인·수집·제안·Silver 적재 런북

[루트 ADR-0148](../../../../docs/adr/0148-cadastral-parcel-editions-are-held-side-by-side.md) 의 운영 절차다.
VWorld 연속지적도(MK/30563)는 판(provider edition, `YYYYMM`)마다 전국을 다시 낸다. 판은 원천 계약에 하나씩
적고, `silver.parcel_boundaries` 에 판마다 제 `source_snapshot_id` 로 나란히 쌓는다. 지도와 카탈로그는 계약의
`served_edition` 하나만 읽는다. 법정동 짝 맞추기의 지번 단계(ADR-0144 §3.2)는 폐지된 동마다 그 날짜를 감싸는 판
둘을 읽는다.

| 무엇 | 정본 |
| --- | --- |
| 판과 그 객체·추출일·핸드오프 자리, 지도가 쓰는 판 | `infra/lakehouse/contracts/vworld-parcel-source-objects.json` (`editions`, `served_edition`) |
| 판에서 파생되는 것(`source_snapshot_id`, `valid_from_utc`, 핸드오프 키, 짝 맞추기의 판 쌍) | `infra/lakehouse/spark/jobs/vworld_parcel_editions.py` (셸은 이 명령으로만 계약을 읽는다) |
| 매일 확인·수집 작업 | `orchestration/jobs.v1.json` 의 `vworld_parcel_edition` (default_pool, Spark 없음) → `infra/systemd/foundation-vworld-parcel-edition.service` → `scripts/ops/vworld-parcel-edition-collect.sh` |
| ZIP 측정 | `infra/lakehouse/spark/jobs/vworld_parcel_edition_members.py` (`tools/technology-versions.contract.json` 이 고정한 GDAL 이미지) |
| 작업 기록 | `/var/lib/foundation-platform/vworld-parcel-edition/journal.log`, 실행마다 `runs/<시각>/`, 제안 `proposed/<판>.json` |
| 변환·적재 | `scripts/load/vworld-parcel-handoff-export.sh`, `scripts/load/lakehouse-batch-load.sh` (둘 다 `VWORLD_PARCEL_EDITION` 하나만 받는다) |

## 1. 끝에서 끝까지

```text
vworld_parcel_edition (04:40, default_pool)
  1. 확인   plan → inventory (필지 데이터셋 하나) → provider-edition
            계약이 가진 판이면 journal 에 "provider=<판> held" 를 남기고 끝(0)
  2. 수집   새 판이면 그 판에서 바로 내려받는 파일 전부 → Bronze (ingest-vworld-dataset-files)
            제공자 갱신일이 같은 파일은 다시 받지 않는다(같은 파일 번호를 판마다 다시 쓰므로 번호로 가리지 않는다)
  3. 측정   ZIP 마다 중앙 디렉터리를 범위 요청으로 읽어 계약 항목을 만든다 → proposed/<판>.json
  4. 끝     3 으로 끝난다 = "계약에 넣을 판이 있다". 작업이 슬랙에 "new edition waiting for contract
            entry" 를 직접 보낸다. 재시도하지 않는다(jobs.v1.json retries 0)
사람
  5. PR     proposed/<판>.json 을 계약의 editions 에 넣는다(아래 3 절). 병합·배포
  6. 적재   그 릴리스에서 변환 → Silver 적재 (아래 4 절)
  7. 짝     다음 lineage_stewardship 이 그 판을 짝 맞추기의 지번 근거로 읽는다(판 쌍은 계약이 정한다)
```

Silver 적재가 작업 안에 없는 이유: 판은 저장소의 정본(계약)에 들어간 뒤에만 적재할 수 있고, spark 풀은
새 작업을 받을 자리가 없다(ADR-0138 의 굶주림 한도, 2026-10-04 실측 1,200/1,200 분).

## 2. 켜기

작업은 꺼진 채(`enabled: false`) 출시된다.

1. 릴리스가 `/var/lib/foundation-platform/vworld-parcel-edition` 을 만든다(`foundation-release.sh`). 없으면 단위가 시작하지 못한다.
2. 감독 실행 한 번:

   ```bash
   sudo systemctl start foundation-vworld-parcel-edition.service
   tail -3 /var/lib/foundation-platform/vworld-parcel-edition/journal.log
   ```

   계약이 제공자의 최신 판을 이미 가지면 `provider=<판> held` 한 줄이고 종료 코드는 0 이다. 2026-10-05 에 같은
   단계를 손으로 돌린 결과: 계획 `ready`, 목록 276 파일, `202609 held`.
3. 별도 PR 에서 `vworld_parcel_edition` 의 `enabled` 를 `true` 로 바꾸고 `disabled_reason` 을 지운다(ADR-0122 §4).

## 3. 새 판을 계약에 넣기

`proposed/<판>.json` 은 `{"<판>": {…}}` 하나다. 계약의 `editions` 에 그 항목을 그대로 넣는 PR 을 낸다. 손으로
고치지 않는다. 측정이 만든 항목은 계약과 합쳐 검사를 통과한 것이다(`vworld_parcel_editions.py propose`):

- 객체마다 그 판의 shapefile 이 정확히 하나 있다(`LSMD_CONT_LDREG_<코드>_<판>.shp`)
- 다른 판의 객체·파일 이름과 겹치지 않는다
- 시도 객체가 있는 곳에는 시군구 객체가 있다(시도 벌은 싣지 않고, 큰 시도 파일 여섯은 RAON 전용이라 받지 않는다)
- 판마다 핸드오프 자리가 다르다(`…/edition=<판>`)

`served_edition` 은 이 PR 에서 바꾸지 않는다. 지도를 새 판으로 옮기는 것은 굽기·검증을 거치는 별도 결정이다.

## 4. Silver 적재 (판 하나)

계약에 판이 들어간 릴리스가 `current` 인 ai-server 에서 돌린다. 두 단계 모두 같은 판 하나만 받는다. 계보 값
(`source_snapshot_id`, `valid_from_utc`)과 핸드오프 자리는 계약이 정하고, 손으로 넘기면 거부된다.

### 202609 (2026-10 첫 실행)

```bash
# 0) 자격증명: 값을 찍지 않고 환경으로만. parcel-publication.env 하나가 카탈로그 넷, R2 버킷·끝점,
#    읽기 키(적재)와 쓰기 키(변환)를 다 갖는다. 다른 수동 적재와 같은 방식으로 이름만 골라 읽는다.
docker run --rm -v /etc/foundation-platform:/t:ro alpine sh -c \
  'grep -E "^FOUNDATION_PLATFORM_(LAKEHOUSE_(CATALOG_URI|WAREHOUSE|CATALOG_TOKEN|CATALOG_PROVIDER)|R2_LAKEHOUSE_(BUCKET|ENDPOINT|READER_ACCESS_KEY_ID|READER_SECRET_ACCESS_KEY|WRITER_ACCESS_KEY_ID|WRITER_SECRET_ACCESS_KEY))=" /t/parcel-publication.env' > ~/.parcel-202609.env
set -a; . ~/.parcel-202609.env; set +a; rm -f ~/.parcel-202609.env
export FOUNDATION_PLATFORM_RELEASE_DIR=/opt/foundation-platform/current
export FOUNDATION_PLATFORM_PUBLISHER_BIN=/opt/foundation-platform/artifacts/$(basename "$(readlink -f /opt/foundation-platform/current)")/foundation-outbox-publisher
export VWORLD_PARCEL_EDITION=202609

# 1) 변환: 시군구 256개 ZIP → 계약이 이 판에 정한 핸드오프 자리 (계획부터 본다; 첫 줄이 그 자리를 말한다)
/opt/foundation-platform/current/scripts/load/vworld-parcel-handoff-export.sh plan | tail -3
setsid nohup /opt/foundation-platform/current/scripts/load/vworld-parcel-handoff-export.sh run \
  > ~/parcel-export-202609.log 2>&1 < /dev/null & disown

# 2) 검사만(쓰지 않음) → 통과하면 적재
LAKEHOUSE_HANDOFF_SOURCE=r2 /opt/foundation-platform/current/scripts/load/lakehouse-batch-load.sh validate parcel_boundaries
LAKEHOUSE_HANDOFF_SOURCE=r2 setsid nohup /opt/foundation-platform/current/scripts/load/lakehouse-batch-load.sh load parcel_boundaries \
  > ~/parcel-load-202609.log 2>&1 < /dev/null & disown
```

| 단계 | 202606 실측 | 202609 예상 |
| --- | --- | --- |
| 변환 (동시 8) | 255 객체, 38 분 (2026-08-31) | 256 객체, 약 40 분 — 망 속도가 묶는다 |
| 적재 (`DRIVER_MEM=16g`, `local[8]`, 묶음 16) | 39,861,511 행, 16 묶음, 19 분 (2026-08-28) | 약 3,990만 행, 16 묶음, 약 20 분 + 유지보수(합치기) |
| 메모리 | 드라이버 16g (`compose.lakehouse.yml` 의 spark 상한이 이에 맞춰져 있다). Spark 기본 1g 는 OOM 이고 표만 생긴 채 0 행이 된다 | 같음. 16g 아래로 내리지 않는다 |

적재는 Airflow 밖에서 도는 16g Spark 다. spark 풀 전체를 잡는 작업(`building_register_floor`, `lineage_stewardship`,
`gold_panel_rebuild`)이 도는 동안에는 시작하지 않는다. Airflow 화면에서 그 셋이 끝난 것을 보고 시작한다. 매시
접기(`map_edit_fold_*`)는 함께 돌 수 있다(2026-08-28 의 전국 적재도 같은 호스트에서 19 분이었다).

### 확인

```sql
-- Trino (ai-server: docker exec -i foundation-platform-trino trino)
SELECT source_snapshot_id, count(*), min(valid_from_utc), max(valid_from_utc)
FROM r2.silver.parcel_boundaries GROUP BY 1;
-- 기대: vworldkr__parcel-202606 39,861,511 / 2026-06-01, vworldkr__parcel-202609 약 3,990만 / 2026-09-01
```

### 하면 안 되는 것

- 202606 판의 핸드오프 자리 바로 아래에 있는 `30563-*--sha256-*.jsonl.gz` 256개를 싣지 않는다. 2026-09-27 에 손으로
  변환한 9월 판으로, `vworldkr__parcel:202609` 와 수집 시각(`2026-09-27T00:54:42Z`)을 지닌다. 판의 핸드오프 자리가
  다르므로 적재기는 그것을 읽지 않고, 읽게 되더라도 잡이 다른 id 를 가진 행을 거부한다. 지우지 않는다(append-only).
- `silver.parcel_boundaries_202609_smoke`(12,352 행, 한 시군구, 2026-10-01T18:04Z 수동 실행)는 그대로 둔다. 정본이 아니다.
- `--iceberg-write-mode overwrite` 를 쓰지 않는다. 분할이 없는 표라 표 전체를 갈아 버린다.

## 5. 짝 맞추기의 판 쌍

`lineage_stewardship` 의 0a 단계(`scripts/ops/legal-dong-code-load.sh`)가 `--jibun-evidence editions` 로 돈다. 폐지된
동마다 계약에서 폐지일 전에 다 뽑힌 마지막 판과 뒤에 다 뽑힌 첫 판을 고른다(`vworld_parcel_editions.bracketing`).
추출일이 폐지일에 걸친 판은 어느 쪽도 아니다.

| 목록의 `jibun` | 뜻 | 할 일 |
| --- | --- | --- |
| `needs a parcel edition extracted before <날>; the source contract holds none` | 그 날보다 앞선 판이 없다 | 그 전 판을 구한다(6 절). 없으면 판단 대기로 남는다 |
| `needs a parcel edition extracted after <날>; …` | 그 날 뒤의 판이 아직 없다 | 다음 판을 기다린다(이 작업이 받는다) |
| `needs parcel edition <판> (vworldkr__parcel-<판>), which is not loaded into the parcel table` | 계약에는 있고 표에 없다 | 4 절의 적재 |
| `N codes below wait: …` | 시군구·시도 아래 동이 기다린다 | 위와 같다 |

2026-10-05 의 대기 코드: 인천 83(2026-07-01) → 202606·202609 적재 후 정해진다. 경기 219(2026-01-02, 01-31, 03-01)와
충북 13(2026-03-25)은 앞선 판이 없다. 대구 16(2026-09-30)은 202610 이후 판이 필요하다.

## 6. 더 오래된 판 (2026-01 이전)

MK/30563 페이지는 현재 판 하나만 준다(2026-10-05: 276 파일, 기준월 2026-09 272 + 폐지된 인천 구 2026-06 3 + 칸 정의서 1).
과거 판은 VWorld 의 다른 데이터셋 `NA/23`(일별 연속지적도형정보, 시도 단위, CC BY)에 있다. 2026-10-05 목록:

| 구분 | 수 | 기준일 |
| --- | --- | --- |
| 전체데이터 (시도 17 또는 16 파일) | 184 | 2025-11-04, 2025-12-04, 2026-01-04, 2026-02-08, 2026-03-08, 2026-04-08, 2026-05-08, 2026-06-08, 2026-07-19, 2026-08-08, 2026-09-08 |
| 변동데이터 (전국 하루치) | 341 | 2025-10-05 ~ 2026-10-01 매일 |

경기 2026-01-02 는 2025-12-04 와 2026-01-04 판, 2026-01-31 은 2026-01-04 와 2026-02-08 판, 2026-03-01 은 2026-02-08 과
2026-03-08 판, 충북 2026-03-25 는 2026-03-08 과 2026-04-08 판이 감싼다. 받는 것은 소유자 승인 뒤의 일이다(전국 판 하나가
약 10GB). 이 데이터셋은 칸 정의와 파일 단위가 MK/30563 과 달라 적재 경로를 따로 확인해야 한다.
