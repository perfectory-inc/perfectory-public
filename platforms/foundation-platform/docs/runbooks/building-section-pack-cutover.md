---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-05
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
| `verify-building-by-pnu-section-pack-equality` | 관문 (가): 굽기가 비교한 수를 모두 더하고(Gold 행 수 전부), 표본을 뽑는다 | 증거 파일만(R2 읽기 없음) |
| `probe-building-by-pnu-section-pack-latency` | 관문 (나): 표본 10,000건을 운영 경로와 미리보기로 한 번씩 읽어 내용·첫 조회 p50·p95 비교 | 증거 파일만 |
| `publish-building-by-pnu-section-packs` | manifest 의 `section_packs` 블록 | manifest(비교 후 교체)·이력·고정 태그 |
| `inspect-building-by-pnu-section-packs` | PNU 하나의 항목별 조각과 응답 | 없음 |

| 변수 | 쓰는 명령 | 뜻 |
|---|---|---|
| `…_OUTPUT_STORAGE_DRIVER`, `…_OUTPUT_ROOT` | 모두 | `r2` 또는 `local`(+ 경로) |
| `…_PACK_GENERATION` | 굽기(기본)·관문·확인 | 묶음 세대. 패치는 이것을 받지 않는다(항목마다 서빙 중인 세대 아래에 쓴다) |
| `…_PACK_SECTIONS` | 굽기 | 일부 항목만(쉼표). 없으면 계약의 모든 항목. 일부만 굽는 것은 반영 스냅숏에서만 된다 |
| `…_PNU_PREFIX` | 굽기 | 샤드(1~10자리, 법정동을 쪼개지 않는다) |
| `…_PACK_SUMMARY_PATH` | 굽기 | 이 실행의 요약 |
| `…_TARGET_PATCH`, `…_PNU_ALLOWLIST_PATH`, `…_DELETE_LIST_PATH` | 굽기(패치) | 변경 집합 |
| `…_PACK_SUMMARY_DIR`, `…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID` | 관문 (가)·발행 | 굽기 요약 모음(반영만 하는 발행에는 없음), 스냅숏 |
| `…_PACK_EXPECTED_DOCUMENT_COUNT` | 관문 (가)·발행 | 선택. 적으면 카탈로그가 기록한 Gold 행 수와 같아야 한다. 기준은 언제나 카탈로그다 |
| `…_INSTALLED_JOBS_PATH` | 첫 발행 | 설치된 릴리스의 `orchestration/jobs.v1.json`. 없으면 `/opt/foundation-platform/current/` 의 것 |
| `…_PACK_EQUALITY_EVIDENCE_PATH`, `…_PACK_LATENCY_EVIDENCE_PATH` | 관문·첫 발행 | 증거 파일. 관문 (나)는 (가)의 증거에서 표본을 읽는다 |
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

## 3. 관문 (가): 전수 비교 (R2 비용 없음)

비교는 2절의 굽기 안에서, 법정동마다 그 법정동을 쓰기 전에 끝난다. 굽기는 법정동마다 항목 묶음을
메모리에 만들고, 그 바이트에서 문서를 다시 읽어 게이트웨이와 같은 방법으로 합친 뒤, 같은 Gold 행이 그리는
객체 문서와 바이트 단위로 비교한다.

- 다른 문서가 하나라도 있으면 그 법정동은 하나도 쓰지 않고, 샤드는 실패하고, 요약을 남기지 않는다.
- 법정동은 동시에 처리되므로, 먼저 통과한 다른 법정동의 묶음은 이미 쓰였을 수 있다. create-only 라 해가
  없다. 발행되지 않은 묶음은 서빙되지 않고, 같은 스냅숏으로 다시 돌리면 같은 바이트로 재사용된다.
- 같은 수(`equal`)는 비교한 수(`compared`)와 따로 센다. 그래서 `equal == compared` 는 실제로 틀릴 수 있는
  판정이다.
- 요약에는 비교한 수, 같은 수, 표본 후보(계약의 `sample_seed` 로 정한 PNU 해시), 굽기가 Gold 매니페스트에서
  센 스냅숏 행 수가 남는다.

```bash
…_PACK_SUMMARY_DIR=$WORK/summaries …_PACK_GENERATION=1 …_PACK_EQUALITY_EVIDENCE_PATH=$WORK/equality.json   "$PUBLISHER_BIN" verify-building-by-pnu-section-pack-equality
```

- 요약 파일과 카탈로그의 표 메타데이터만 읽는다. R2 의 묶음은 읽지 않는다.
- 기준 Gold 행 수는 운영자가 적는 값이 아니라 카탈로그가 그 스냅숏에 기록한 행 수(`total-records`)다.
  `…_PACK_EXPECTED_DOCUMENT_COUNT` 를 적으면 그 값과 같아야 하고, 요약이 센 행 수도 같아야 한다.
- `passed` 는 모든 요약이 한 Gold 스냅숏, 1세대, 모든 항목의 기본 굽기이고, 비교한 수의 합과 같은 수의
  합이 모두 그 Gold 행 수와 같을 때만 참이다.
- 증거에는 관문 (나)의 표본이 들어 있다. 후보 중 순위가 낮은 `latency_sample_size`(10,000)건과 그 sha256 이다.

## 4. 관문 (나): 업로드 뒤 실서빙 표본 비교와 첫 조회 시간

1. 이 PR 의 Worker 를 **운영 경로가 아닌** 버전으로 올린다(버전 업로드의 미리보기 주소 또는 별도 경로).
   그 버전에만 바인딩 `FOUNDATION_PLATFORM_BUILDING_PACK_PREVIEW=true` 를 준다.
2. 운영 경로에서 `?packs=g1` 이 404 인지 확인한다(manifest 가 그 세대를 가리키기 전에는 404 여야 한다).
3. 잰다.

```bash
…_PACK_GENERATION=1 …_PACK_PREVIEW_BASE_URL=https://<미리보기 주소> …_PACK_EQUALITY_EVIDENCE_PATH=$WORK/equality.json …_PACK_LATENCY_EVIDENCE_PATH=$WORK/latency.json   "$PUBLISHER_BIN" probe-building-by-pnu-section-pack-latency
```

- 표본은 관문 (가)의 증거에 든 10,000건이다. 증거에 적힌 sha256 과 맞지 않으면 거부한다.
- PNU 마다 운영 경로와 미리보기를 한 번씩, 번갈아 먼저 읽는다. 둘 다 200 이어야 하고, 내용은 `source`
  를 빼고 같아야 한다.
- 묶음의 p50·p95 가 객체보다 계약의 `latency_max_increase_ms` 이상 늘지 않아야 통과다.
- 주소가 로컬·사설이면 증거에 `local-simulation` 이 적히고, 그 증거로는 발행이 거부된다.

### 비용

| 단계 | R2 요청 |
|---|---:|
| 2절 굽기 (묶음 쓰기) | Class A 약 76,040 |
| 3절 관문 (가) | 0 |
| 4절 관문 (나): 운영 경로 10,000건 | Class B 약 10,000 (객체 하나씩) |
| 4절 관문 (나): 미리보기 10,000건 | Class B 약 10,000 요청분. 차가운 묶음은 요청 하나에 머리·문서 범위 읽기가 항목마다 붙어, R2 GET 으로는 최대 약 80,000 |
| 5절 첫 발행 | Class A 2 (manifest·이력) + 표본 묶음 다시 읽기(항목마다 16) |

## 5. 첫 발행 (전환)

운영 Worker 를 이 PR 의 코드로 배포한다(`_capabilities` 가 `[1, 2, 3]`). 그 다음:

```bash
…_CONFIRM_PACK_PUBLISH=true …_PACK_SUMMARY_DIR=$WORK/summaries \
…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=<스냅숏> \
…_PACK_EQUALITY_EVIDENCE_PATH=$WORK/equality.json …_PACK_LATENCY_EVIDENCE_PATH=$WORK/latency.json \
  "$PUBLISHER_BIN" publish-building-by-pnu-section-packs
```

발행은 다음 중 하나라도 어기면 manifest 를 바꾸지 않는다.

- 항목마다 목록 = 요약의 묶음, 문서 수 = Gold 행 수(카탈로그가 기록한 수), 표본 묶음의 sha256·머리가 요약과 같음.
- 세대가 그 항목에 묶음이 있는 모든 세대 중 가장 큼.
- 게이트웨이가 `section_packs` 를 읽는다고 답함.
- 설치된 릴리스의 예약 굽기 잡(`by_pnu_serving_bake`)이 계약의 능력(`building_section_pack_patches`)을
  선언함(7절). 선언하지 않으면 그 릴리스의 예약 굽기는 계속 객체를 굽고, 묶음은 매일 변경을 받지 못한다.
- 두 증거가 이 세대·이 Gold 스냅숏·말한 Gold 행 수의 것이고 통과(수치로 다시 판정), 두 증거의 표본 sha256 이 같음.

발행은 v2 필드를 그대로 두고 `section_packs` 만 더한다. 이전 manifest 는 이력에 남는다.

## 6. 확인과 되돌리기

```bash
…_INSPECT_PNU=<PNU> "$PUBLISHER_BIN" inspect-building-by-pnu-section-packs
```

항목마다 어느 묶음(`g1`, `g1/p3`)이 답했는지, 상태, 조각, 합친 문서를 낸다. 객체 서빙으로 돌아가려면
`section_packs` 가 없는 이력 manifest 로 되돌린다(`publish-building-by-pnu-serving-manifest` 의 되돌리기).

## 7. 예약 굽기와 묶음 (완료)

예약 굽기(`scripts/ops/by-pnu-serving-bake.sh`, 잡 `by_pnu_serving_bake`)는 건물 manifest 에
`section_packs` 블록이 있으면 객체 대신 묶음을 굽고 발행한다. 블록이 없으면(전환 전) 지금처럼 객체를 굽는다.

| 고르는 것 | 묶음 레인에서 |
|---|---|
| 할 일이 있나 | Gold 현재 스냅숏 = 묶음의 반영 스냅숏(`section_packs.reflected_gold_iceberg_snapshot_id`)이면 없다 |
| 변경 집합 | 묶음의 반영 스냅숏에서 Gold 현재 스냅숏까지 (`by_pnu_panel_delta.py`) |
| 반영만 | 빈 변경 집합. 굽기 없이 `publish-building-by-pnu-section-packs` 에 변경 집합만 준다 |
| 패치 | `export-building-by-pnu-section-packs` + `TARGET_PATCH`. 항목마다 서빙 중인 세대 아래 `g{n}/p{m}/` 에 쓰고, 삭제는 툼스톤이다. 패치 번호는 최신 패치와 묶음이 있는 모든 패치 번호보다 크다 |
| 전체 | 문서 스키마가 바뀜, 어느 항목이 읽는 패치가 `max_patches` 에 닿음, 누적 변경이 비율을 넘음, 운영자 강제. 모든 항목의 새 세대(서빙 중인 세대와 묶음이 있는 모든 세대보다 큼) |

- 한 항목만 새 세대로 다시 구운 뒤에도 매일 패치는 그 항목의 새 세대 아래로 간다(항목마다 세대가 다르다).
- 묶음 실행의 작업 파일은 `runs/<스냅숏>-packs-…/` 와 `in-progress-packs.json` 이다. 굽기 요약은 실행 디렉터리의
  `summaries/` 에 따로 둔다(발행이 그 디렉터리의 `*.json` 을 모두 읽는다).
- 패치 대상에 다른 스냅숏의 묶음이 이미 있으면 굽기가 첫 쓰기 전에 거부하고, 다음 실행은 그 위 번호에서 시작한다.
- 잡 항목은 이것을 `capabilities: ["building_section_pack_patches"]` 로 선언한다. 5절의 첫 발행은 설치된
  릴리스의 잡이 이것을 선언하지 않으면 거부한다. 그래서 **5절의 전환은 이 능력이 있는 릴리스를 설치한 뒤에만 된다.**
- 객체 발행(전량·패치·반영)은 `section_packs` 블록을 그대로 옮기고, 바꾸거나 떼면 거부된다. 블록을 떼는 것은
  되돌리기(6절)뿐이다.

### 한 항목만 다시 굽기

`…_PACK_SECTIONS=<항목>` 과 `…_PACK_GENERATION=<그 항목의 새 세대>` 로, 반영 스냅숏에서만 굽는다. 굽기는
법정동마다 새 항목을 지금 서빙 중인 다른 항목들(기본 세대와 패치, R2 에서 그 법정동의 묶음만 읽는다)과 합쳐
모든 문서를 객체 문서와 비교하고, 그 법정동에서 서빙 중인 다른 PNU 가 문서로 답하지 않는지도 본다. 어긋나면 그
법정동을 쓰지 않고 실패한다. 발행은 그 항목의 세대만 바꾸고 `patch_floor` 를 최신 패치로 둔다.

## 8. 남은 일

- 필지 레인은 건물 관문을 통과한 뒤 같은 절차로 옮긴다.
