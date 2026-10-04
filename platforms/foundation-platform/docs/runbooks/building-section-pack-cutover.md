---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-04
---

# 건물 by-PNU 묶음 파일 전환 런북

건물 상세 문서를 PNU 하나당 R2 객체에서 항목별 묶음 파일로 옮기는 절차다.
결정은 [루트 ADR-0147](../../../../docs/adr/0147-by-pnu-documents-are-served-from-section-packs.md)과
그 Revision 절이다. 패치·고정 규칙은 [ADR-0141](../../../../docs/adr/0141-by-pnu-serving-publishes-changed-documents-as-patch-generations.md),
[ADR-0146](../../../../docs/adr/0146-a-by-pnu-lane-is-re-based-by-verifying-what-it-serves-and-its-reflected-snapshot-is-pinned.md)이다.

전환 관문을 모두 통과하기 전에는 지금의 객체 방식이 그대로 서빙한다. 옛 객체는 지우지 않는다.

## 0. 명령과 환경변수

모든 명령은 발행기(`foundation-outbox-publisher`)의 하위 명령이다. 환경변수 앞머리는
`FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_` 다(아래에서는 `…_` 로 줄인다).

| 명령 | 하는 일 | 쓰기 |
|---|---|---|
| `export-building-by-pnu-section-packs` | Gold → 묶음(기본 세대 또는 패치) | 묶음, create-only |
| `verify-building-by-pnu-section-pack-equality` | 관문 (가): 모든 PNU 의 묶음 응답 = 객체 | 증거 파일만 |
| `probe-building-by-pnu-section-pack-latency` | 관문 (나): 첫 조회 p50·p95, 운영 경로 대 미리보기 | 증거 파일만 |
| `publish-building-by-pnu-section-packs` | manifest 의 `section_packs` 블록 | manifest(비교 후 교체)·이력·고정 태그 |
| `inspect-building-by-pnu-section-packs` | PNU 하나의 항목별 조각과 응답 | 없음 |

| 변수 | 쓰는 명령 | 뜻 |
|---|---|---|
| `…_OUTPUT_STORAGE_DRIVER`, `…_OUTPUT_ROOT` | 모두 | `r2` 또는 `local`(+ 경로) |
| `…_PACK_GENERATION` | 굽기·관문·확인 | 묶음 세대 |
| `…_PACK_SECTIONS` | 굽기 | 일부 항목만(쉼표). 없으면 계약의 모든 항목 |
| `…_PNU_PREFIX` | 굽기 | 샤드(1~10자리, 법정동을 쪼개지 않는다) |
| `…_PACK_SUMMARY_PATH` | 굽기 | 이 실행의 요약 |
| `…_TARGET_PATCH`, `…_PNU_ALLOWLIST_PATH`, `…_DELETE_LIST_PATH` | 굽기(패치) | 변경 집합 |
| `…_PACK_SUMMARY_DIR`, `…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID`, `…_PACK_EXPECTED_DOCUMENT_COUNT` | 발행 | 굽기 요약 모음, 스냅숏, Gold 행 수 |
| `…_PACK_EQUALITY_EVIDENCE_PATH`, `…_PACK_LATENCY_EVIDENCE_PATH` | 관문·첫 발행 | 증거 파일 |
| `…_PACK_PREVIEW_BASE_URL`, `…_PACK_LIVE_BASE_URL` | 관문 (나) | 미리보기와 운영 경로 |
| `…_CHANGE_SET_SUMMARY_PATH`, `…_UPSERT_LIST_PATH` | 발행(패치) | 변경 집합 잡의 결과 |
| `…_INSPECT_PNU` | 확인 | PNU |

## 1. 측정 (운영자, 쓰기 없음)

릴리스가 설치된 뒤, 운영 환경 파일을 읽을 수 있는 계정으로 한 번 잰다. 묶음은 데이터 디스크의 빈
디렉터리에만 쓰고, R2 쓰기 키는 환경에서 지운다.

```bash
sudo systemd-run --wait --collect --pipe -p User=foundation-platform -p MemoryMax=14G \
  -p EnvironmentFile=/etc/foundation-platform/recovery.env \
  -p EnvironmentFile=/etc/foundation-platform/source-sweep.env \
  -p EnvironmentFile=/etc/foundation-platform/map-edit-fold.env \
  /opt/foundation-platform/current/scripts/ops/measure-building-section-packs.sh \
  /data/foundation-platform/by-pnu-bake/building-pack-measure
```

출력 `measurement.json` 의 `bake_seconds`, `peak_resident_bytes`, `packs`, `pack_bytes`,
`projected_r2_class_a_writes` 를 PR 과 이 런북의 측정 기록에 남긴다.

## 2. 운영 버킷에 첫 세대 굽기 (manifest 는 그대로)

건물 레인 잠금(`/data/foundation-platform/by-pnu-bake/building/lane.lock`)을 잡고, 예약 굽기와 겹치지 않게 한다.
샤드는 레인의 `shard-plan.txt` 를 따른다. 모든 샤드에 첫 샤드의 Gold 스냅숏을 `…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID` 로 준다.

```bash
export …_OUTPUT_STORAGE_DRIVER=r2 …_CONFIRM_PACK_EXPORT=true …_PACK_GENERATION=1
for prefix in $(cat /data/foundation-platform/by-pnu-bake/building/shard-plan.txt); do
  …_PNU_PREFIX=$prefix …_PACK_SUMMARY_PATH=$WORK/summaries/shard-$prefix.json \
    "$PUBLISHER_BIN" export-building-by-pnu-section-packs
done
```

묶음은 `serving/buildings/packs/{section}/g1/{법정동}.pack` 에 create-only 로 쓰인다. 같은 스냅숏으로
다시 돌리면 같은 바이트라 재사용되고, 다른 스냅숏을 같은 세대에 섞으려 하면 첫 쓰기 전에 거부된다.

## 3. 관문 (가): 전수 비교

```bash
…_PACK_GENERATION=1 …_PACK_EQUALITY_EVIDENCE_PATH=$WORK/equality.json \
  "$PUBLISHER_BIN" verify-building-by-pnu-section-pack-equality
```

- 지금 manifest 가 서빙하는 객체(최신 패치, 없으면 기본 세대)를 PNU 마다 모두 읽고, 묶음 응답과 `source`
  를 뺀 내용으로 비교한다. 읽기 전용이다.
- `passed` 는 `different`·`only_served`·`only_packs`·`unreadable` 이 모두 0 이고 비교 수가 manifest 의
  `object_count` 와 같을 때만 참이다. 증거는 비교한 객체 상태(기본 세대·최신 패치·반영 스냅숏·객체 수)를
  적는다. 그 뒤 객체 패치가 발행되면 다시 비교해야 한다.

## 4. 관문 (나): 미리보기 Worker 로 첫 조회 시간 재기

1. 이 PR 의 Worker 를 **운영 경로가 아닌** 버전으로 올린다(버전 업로드의 미리보기 주소 또는 별도 경로).
   그 버전에만 바인딩 `FOUNDATION_PLATFORM_BUILDING_PACK_PREVIEW=true` 를 준다.
2. 운영 경로에서 `?packs=g1` 이 404 인지 확인한다(manifest 가 그 세대를 가리키기 전에는 404 여야 한다).
3. 잰다.

```bash
…_PACK_GENERATION=1 …_PACK_PREVIEW_BASE_URL=https://<미리보기 주소> \
…_PACK_LATENCY_EVIDENCE_PATH=$WORK/latency.json …_OUTPUT_STORAGE_DRIVER=r2 \
  "$PUBLISHER_BIN" probe-building-by-pnu-section-pack-latency
```

- 표본은 1세대의 기준 항목 묶음에서 고르게 고른다(크기는 계약의 `latency_sample_size`).
- PNU 마다 운영 경로와 미리보기를 한 번씩, 번갈아 먼저 읽는다. 둘 다 200 이고 내용이 같아야 한다.
- 묶음의 p50·p95 가 객체보다 계약의 `latency_max_increase_ms` 이상 늘지 않아야 통과다.
- 주소가 로컬·사설이면 증거에 `local-simulation` 이 적히고, 그 증거로는 발행이 거부된다.

## 5. 첫 발행 (전환)

운영 Worker 를 이 PR 의 코드로 배포한다(`_capabilities` 가 `[1, 2, 3]`). 그 다음:

```bash
…_CONFIRM_PACK_PUBLISH=true …_PACK_SUMMARY_DIR=$WORK/summaries \
…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=<스냅숏> …_PACK_EXPECTED_DOCUMENT_COUNT=<Gold 행 수> \
…_PACK_EQUALITY_EVIDENCE_PATH=$WORK/equality.json …_PACK_LATENCY_EVIDENCE_PATH=$WORK/latency.json \
  "$PUBLISHER_BIN" publish-building-by-pnu-section-packs
```

발행은 다음 중 하나라도 어기면 manifest 를 바꾸지 않는다.

- 항목마다 목록 = 요약의 묶음, 문서 수 = Gold 행 수, 표본 묶음의 sha256·머리가 요약과 같음.
- 세대가 그 항목에 묶음이 있는 모든 세대 중 가장 큼.
- 게이트웨이가 `section_packs` 를 읽는다고 답함.
- 두 증거가 이 세대의 것이고 통과(수치로 다시 판정), 전수 비교가 지금 객체 상태의 것.

발행은 v2 필드를 그대로 두고 `section_packs` 만 더한다. 이전 manifest 는 이력에 남는다.

## 6. 확인과 되돌리기

```bash
…_INSPECT_PNU=<PNU> "$PUBLISHER_BIN" inspect-building-by-pnu-section-packs
```

항목마다 어느 묶음(`g1`, `g1/p3`)이 답했는지, 상태, 조각, 합친 문서를 낸다. 객체 서빙으로 돌아가려면
`section_packs` 가 없는 이력 manifest 로 되돌린다(`publish-building-by-pnu-serving-manifest` 의 되돌리기).

## 7. 남은 일

- 예약 굽기(`by-pnu-serving-bake.sh`)는 아직 객체를 굽는다. 매일 변경을 묶음 패치로 굽고 발행하도록 잇는
  일은 후속 PR 이다. **5절의 전환은 그 PR 이 배포된 뒤에 한다.** 그 전에 전환하면 객체 발행이
  `section_packs` 블록을 그대로 옮기므로 서빙은 끊기지 않지만, 묶음은 새 변경을 받지 못해 낡는다.
  변경 집합 잡은 묶음의 반영 스냅숏(`section_packs.reflected_gold_iceberg_snapshot_id`)을 기준으로 돌아야 한다.
- 필지 레인은 건물 관문을 통과한 뒤 같은 절차로 옮긴다.
