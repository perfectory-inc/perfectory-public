# ADR 0236: 읍면동 경계의 새 회차는 예약 작업이 Silver·발행 ID·타일까지 올린다

- Status: Accepted
- Date: 2026-10-14
- Builds on: [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md)(행정경계는 레이크하우스에서 굽는다),
  [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) §8·§9(개편에도 id 유지·발행 ID),
  [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)(입력이 바뀌면 시작),
  [ADR-0234](./0234-the-flow-map-says-job-missing-only-for-data-that-should-have-a-job.md)(작업 없는 데이터는 까닭을 적는다)

## Context

사장 결정(2026-10-14): 행정경계 회차에 예약 작업을 둔다. 지금 `silver.administrative_boundaries`(회차 202606·202609)와
`gold.place_id_*`(admin 단위, 2026-09-29 한 번)는 사람이 ai-server 에서 `scripts/tiles/admin-boundary/README.md` 를 따라
만들었다. 확인한 것(2026-10-14):

- 원천 `vworldkr__boundary_emd` 는 매일 수집 대상이 아니다(`daily_collection` 없음). 장부의 마지막 수집은 2026-07-02 이고,
  202609 회차를 만든 ZIP 은 장부에 없다. Silver 행의 `source_record_id` 는 객체가 아니라 접두사다.
- 회차(`YYYYMM`)는 ZIP 이름이 아니라 그 안의 레이어 이름(`LSMD_ADM_SECT_UMD_<시도>_<YYYYMM>.shp`)에만 있다. Bronze 는
  회차를 나란히 쌓는다.
- 시도 ZIP 은 몇 MB 씩, 회차 전체가 수십 MB 다. `convert.sh` 는 수집 env 에서 **쓰기** 키를 grep 해 받았다.
- 매시 접기(`map_edit_fold_admin`)는 편집이 없으면 굽지 않는다. 새 회차는 `FOUNDATION_MAP_EDIT_FOLD_FORCE=1` 로만 반영됐다.

## Decision

1. 매일 수집이 `boundary_emd` 를 받는다(엔드포인트 카탈로그 `daily_collection: source_sweep`). 같은 갱신일의 파일은 다시
   받지 않으므로 평소 비용은 목록 한 번이다.
2. 예약 작업 `admin_boundary_release`(`scripts/ops/admin-boundary-release.sh`, spark 3 자리, `started_by: inputs`,
   대체 09:30 UTC)가 한 실행에:
   1. Bronze 의 경계 ZIP 을 **읽기 키**로 받는다(`convert.sh download`).
   2. ZIP 안 레이어 이름에서 가장 새 회차를 고른다(`rounds.py`). 한 회차에 같은 시도 ZIP 이 둘이면 거부한다.
   3. `administrative_boundaries_handoff_to_silver.py --probe-only` 로 Silver 가 그 회차(`vworldkr__boundary_emd-30603-<회차>`)를
      가졌는지 묻는다. 가졌으면 받은 파일을 지우고 `unchanged` 로 끝난다.
   4. 그 회차의 ZIP 만 변환하고(`convert.sh convert`), 공식 코드 목록으로 병합한다(`merge.py`). 시도가 빠진 회차는 실패가
      아니라 `unchanged reason=incomplete_round` 다(제공자가 시도 파일을 나눠 올린다).
   5. 회차를 덧붙인다. `source_record_id` 는 그 회차의 Bronze 키들이다. 합쳐진 동의 무게는 지도가 쓰는 필지 판으로 잰다.
   6. 발행 ID(`place_id_release_to_gold.py --unit admin`), 그다음 매시 접기의 스크립트를 FORCE 로 불러 타일을 굽는다. 3 자리를
      쥐고 있으므로 매시 접기(2 자리)와 겹치지 않는다.
   7. 마지막 줄 `foundation-job-outcome changed|unchanged`(ADR-0171).
3. 작업은 꺼진 채 등록된다. 켜기 전 감독 실행은 `by-pnu-pack-operator.sh job start admin_boundary_release`(ADR-0235)다.
4. 작업이 쓰는 네 칸(`silver-administrative-boundaries`, `gold-place-id-registry`·`-bridge`·`-changelog`)은 그래프의
   `unscheduled` 를 지운다 — ADR-0234 의 가드가 그것을 요구한다.

## Consequences

- 회차 반영이 사람 손에서 빠지고, 회차의 계보가 실제 Bronze 객체를 가리킨다.
- 평소 하루 비용: ZIP 수십 MB 받기(R2 바깥 전송 없음)와 Spark 확인 한 번(1–2 분). 받은 파일은 남기지 않는다(루트 디스크).
- 장부에 없는 손 시절 객체(202609 회차의 ZIP)는 이 작업이 읽지 않는다 — 다음 회차부터 장부·계보가 맞는다.
- `convert.sh` 의 손 실행용 쓰기 키 grep 과 `alpine` 이미지를 없앴다.
- 시험: `infra/lakehouse/spark/tests/test_admin_boundary_rounds.py`(회차 고르기·거부),
  `test_administrative_boundaries_handoff_to_silver.py`(probe 인자), `orchestration/tests` 의 작업 등록 시험들.
