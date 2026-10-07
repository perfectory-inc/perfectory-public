---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-05
---

# 건물 by-PNU 묶음 파일 전환 런북

건물 상세 문서를 PNU 하나당 R2 객체에서 항목별 묶음 파일로 옮기는 절차다.
결정은 [루트 ADR-0147](../../../../docs/adr/0147-by-pnu-documents-are-served-from-section-packs.md)과
그 Revision 절, 그리고 서빙 형태를 문서 통째(`documents` 항목 하나)로 고친
[루트 ADR-0151](../../../../docs/adr/0151-building-packs-serve-each-document-whole.md)이다. 패치·고정 규칙은 [ADR-0141](../../../../docs/adr/0141-by-pnu-serving-publishes-changed-documents-as-patch-generations.md),
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
| `check-building-gateway-version-health` | 단계 배포 한 단계의 판정(Cloudflare 분석) | 없음 |
| `monitor-building-by-pnu-serving` | 운영 주소 매시 감시 | 보고 파일만 |

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
| `…_PACK_PREVIEW_BASE_URL`, `…_PACK_LIVE_BASE_URL` | 관문 (나) | 미리보기, 비교할 객체 경로(기본은 미리보기 자신의 객체 경로 — 계약 `cutover_gate.comparison_reason`) |
| `…_PACK_PROBE_CONCURRENCY` | 관문 (나) | 선택. 동시에 읽는 PNU 수. 없으면 계약의 `probe_concurrency` |
| `FOUNDATION_PLATFORM_CLOUDFLARE_ACCOUNT_ID`, `FOUNDATION_PLATFORM_CLOUDFLARE_ANALYTICS_TOKEN` | 관문 (나) | 앞머리 없음. 계약 `by_pnu_section_packs.cloudflare_analytics` 가 이름과 파일(`/etc/foundation-platform/cloudflare-analytics.env`, root 0600)을 정한다. Workers 분석(GraphQL)으로 미리보기 Worker 의 CPU 를 읽는다. 토큰 권한은 Account Analytics Read 하나. 없으면(파일이 아직 없으면) 탐침과 카나리아 판정이 시작 전에 파일과 변수 이름을 대며 거부한다 |
| `…_CHANGE_SET_SUMMARY_PATH`, `…_UPSERT_LIST_PATH` | 발행(패치) | 변경 집합 잡의 결과 |
| `…_INSPECT_PNU` | 확인 | PNU |

## 1. 측정 (운영자, 쓰기 없음)

릴리스가 설치된 뒤, 운영 환경 파일을 읽을 수 있는 계정으로 한 번 잰다. 묶음은 데이터 디스크의 빈
디렉터리에만 쓰고, R2 쓰기 키는 환경에서 지운다.

```bash
# 환경 파일은 계약이 정한다(config/runtime-secrets.contract.json 의 run measure-building-section-packs, 루트 ADR-0153).
sudo systemd-run --wait --collect --pipe -p User=foundation-platform -p MemoryMax=14G \
  $(python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py properties measure-building-section-packs) \
  /opt/foundation-platform/current/scripts/ops/measure-building-section-packs.sh \
  /data/foundation-platform/by-pnu-bake/building-pack-measure
```

출력 `measurement.json` 의 `bake_seconds`, `peak_resident_bytes`, `packs`, `pack_bytes`,
`projected_r2_class_a_writes` 를 PR 과 이 런북의 측정 기록에 남긴다.

측정 기록 (2026-10-04, ai-server, 릴리스 eebf1ff6, Gold 스냅숏 하나, 샤드 27):

| 무엇 | 값 |
|---|---:|
| 문서 | 5,945,767 (항목 넷 모두 같은 수, 툼스톤 0) |
| 묶음 | 76,040 (항목마다 19,010 법정동) |
| 묶음 바이트 | 6.96GB (머리·색인 0.70GB) |
| 가장 큰 묶음 / 머리 | 6.56MB / 472KB |
| 굽기 시간 | 2,329초 (약 39분, 가장 느린 샤드 236초) |
| 최대 상주 메모리 | 10.5GiB |
| 예상 R2 Class A 쓰기 | 76,042 (객체 방식 약 5,945,767) |

굽기 안의 전수 비교(3절)도 이 측정에서 모든 문서가 같았다(다르면 그 샤드가 실패한다).

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

묶음은 `serving/buildings/packs/documents/g1/{법정동}.pack` 에 create-only 로 쓰인다(항목은 계약의 `documents` 하나, ADR-0151). 같은 스냅숏으로
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

1. 미리보기 Worker 를 올린다. 운영 Worker 와 이름·주소가 다른 별도 Worker(`foundation-building-gateway-preview`,
   `buildings-preview.perfectory.io`)다. 계약 `building_by_pnu_gateway.section_packs.preview_worker` 가 정본이고
   `wrangler.jsonc` 의 `env.preview` 는 그 투영이다. 사용자 지정 도메인이라 엣지 캐시가 운영 주소와 같게 동작한다.
   ```bash
   cd services/foundation-building-gateway
   corepack pnpm@9.12.0 install --frozen-lockfile
   npx wrangler deploy --env preview --var FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS:<운영 Worker 와 같은 값>
   ```
   바인딩 `FOUNDATION_PLATFORM_BUILDING_PACK_PREVIEW=true` 는 그 env 에만 있다. 이 명령은 운영 Worker 와 운영
   주소를 건드리지 않는다.
2. 운영 경로에서 `?packs=g1` 이 404 인지 확인한다(manifest 가 그 세대를 가리키기 전에는 404 여야 한다).
3. 잰다.

```bash
# 분석 토큰은 EnvironmentFile 로만 읽는다(셸·저장소에 두지 않는다).
PUBLISHER_BIN=/opt/foundation-platform/artifacts/$(basename "$(readlink -e /opt/foundation-platform/current)")/foundation-outbox-publisher
sudo systemd-run --wait --collect --pipe -p User=foundation-platform \
  $(python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py properties section-pack-latency-probe) \
  -E FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_PACK_GENERATION=1 \
  -E FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_PACK_PREVIEW_BASE_URL=https://buildings-preview.perfectory.io \
  -E FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_PACK_EQUALITY_EVIDENCE_PATH=$WORK/equality.json \
  -E FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_PACK_LATENCY_EVIDENCE_PATH=$WORK/latency.json \
  "$PUBLISHER_BIN" probe-building-by-pnu-section-pack-latency
```

- 표본은 관문 (가)의 증거에 든 10,000건이다. 증거에 적힌 sha256 과 맞지 않으면 거부한다.
- 계약의 `probe_concurrency`(8)개 PNU 를 동시에 읽는다. PNU 마다 운영 경로와 미리보기를 한 번씩, 번갈아 먼저
  읽고, 읽기마다 따로 잰다. 둘 다 200 이어야 하고, 내용은 `source` 를 빼고 같아야 한다.
- 법정동의 첫 읽기가 차가운 읽기다. 판정은 계약의 SLO(`cutover_gate.slo`)로 한다: 묶음의 차가운 p50·p95·p99 가
  같은 PNU 의 운영 경로보다 `latency_max_increase_ms.cold` 이상, 따뜻한 것이 `.warm` 이상 늘지 않아야 하고, 미리보기
  읽기의 200 비율(`availability`)이 `availability_min`(99.95%) 이상이어야 한다.
- 실패는 쪽·종류별(`pack:http-503`, `live:body-timeout` …)로 센다. 운영 경로 쪽 실패는 망이나 객체 경로의 것이라
  묶음 가용성에 넣지 않지만, 답한 PNU 가 표본의 `availability_min` 에 못 미치면 통과가 아니다.
- 짝 읽기 뒤 부하 단계(`cutover_gate.load_test`, 초당 25건 10분 = 15,000건, 동시 최대 64)가 미리보기만 읽는다.
  표본에서 시드 해시로 뽑아 법정동이 되풀이되므로 차가운 읽기와 따뜻한 읽기가 섞인다. 동시 한도에 걸려 시작하지
  못한 요청(`shed`)도 가용성에서 뺀다. 그 구간의 Worker CPU·`exceededResources` 도 분석에서 읽어 같은 한도로 본다.
- 모든 읽기는 브라우저처럼 `Accept-Encoding: gzip` 을 보낸다. 미리보기 답이 `Content-Encoding: gzip` 이 아니면 저장된
  덩어리를 그대로 낸 것이 아니므로 실패(`pack:not-gzip`)다. 증거의 `encodings` 가 쪽마다 받은 인코딩을,
  `server_timing.outcomes` 가 미리보기가 간 경로(`document`·`document-decompressed`·…)를 센다.
- 짝 읽기 뒤 표본 앞 `no_gzip_sample_size`(200)건을 미리보기에서 `Accept-Encoding: identity` 로 다시 읽는다(증거
  `no_gzip`). Worker 가 푸는 유일한 경로다. 모두 200·`Content-Encoding` 없음·운영 경로와 같은 내용이어야 통과다.
- 미리보기의 `Server-Timing`(전체, R2 대기, 묶음이 `r2-whole`·`r2-head+range`·`edge-…`·`memory-…` 중 어디서 왔는지)을
  모아 증거의 `server_timing` 에 적는다.
- 차가운 읽기는 미리보기가 어디서 답했는지로 다시 센다(증거 `cold_read_paths`: `r2`·`edge-copy`·`memory`·`untimed`).
  모든 항목을 R2 에서 읽은 것(`r2`)만 첫 읽기다. 나머지가 `slo.cold_reads_not_from_r2_max_share`(1%)를 넘으면 차가운
  한도가 캐시를 잰 것이므로 통과가 아니다. 2026-10-07 에는 같은 표본을 앞서 읽은 실행이 남긴 엣지 사본에서 차가운
  읽기 10,000건 중 9,952건이 답해 첫 읽기를 45번만 재고도 통과했다. 미리보기의 엣지 사본 이름에는 Worker 버전이
  들어가므로, **다시 잴 때는 이 절 첫 단계의 미리보기 올리기(`wrangler deploy --env preview`)부터 다시 해** 새 버전으로 차갑게 시작한다.
- 비교 상대(객체 경로)도 같은 미리보기 Worker 가 답한다(질의 없는 주소). 같은 코드·같은 배치이고 엣지 사본도 그
  버전 것이라, 새로 올린 미리보기에서는 양쪽 모두 R2 에서 처음 읽는다. 객체 경로도 미리보기에서는
  `Server-Timing` 에 `object;desc="r2-object"`·`"edge-answer"` 로 어디서 답했는지 말하고, 증거
  `cold_live_read_paths` 가 그것을 센다. 운영 주소는 그것을 말하지 않으므로 운영 주소와 비교한 증거로는 통과하지
  못한다. 2026-10-07 운영 주소와 비교했을 때는 앞선 측정들이 데운 운영 엣지 사본(캐시)과 R2 첫 읽기를 비교한 셈이었다.
- 탐침 구간의 미리보기 Worker CPU 를 Workers 분석에서 읽는다(분석은 1–2분 늦게 센다. 탐침은 보낸 요청의 95% 가 셀
  때까지 최대 10분 기다린다). `exceededResources` 가 0 이고 p99 가 `worker_cpu_p99_max_ms`(10ms, ADR-0157) 이하여야 통과다.
  2026-10-05 에는 계정이 Workers Free(요청당 10ms)라 넘은 요청이 오류 1102, 곧 503 이 되었다. 2026-10-06 부터 Workers
  Paid 이고 운영 Worker 의 CPU 한도는 계약 `building_by_pnu_gateway.cpu_limit_ms` 다(ADR-0151 Revision 9).
- 주소가 로컬·사설이면 증거에 `local-simulation` 이 적히고, 그 증거로는 발행이 거부된다.

### 비용

| 단계 | R2 요청 |
|---|---:|
| 2절 굽기 (묶음 쓰기) | Class A 약 76,040 |
| 3절 관문 (가) | 0 |
| 4절 관문 (나): 운영 경로 10,000건 | Class B 약 10,000 (객체 하나씩) |
| 4절 관문 (나): 미리보기 10,000건 | Class B 최대 약 20,000. 256KiB 이하 묶음은 GET 하나, 큰 묶음은 머리 + 범위 둘이고, 같은 법정동의 다음 읽기는 메모리·엣지 사본에서 R2 없이 답한다 |
| 5절 첫 발행 | Class A 2 (manifest·이력) + 표본 묶음 다시 읽기(항목마다 16) |

## 5. 전환: 버전 비율로 단계 배포 (ADR-0151 Revision)

전환은 세 단계다. 사용자가 묶음을 읽기 시작하는 것은 셋째 단계이고, 그 단계는 Worker 버전 비율로 나눠
올리고 내린다. manifest 는 둘째 단계에서 한 번 바뀌고, 셋째 단계의 되돌리기는 manifest 를 건드리지 않는다.

| 단계 | 하는 일 | 사용자가 읽는 것 | 되돌리기 |
|---|---|---|---|
| 1. 코드 | `building-gateway-canary.sh --execute code`: 이 릴리스의 Worker 를 `FOUNDATION_PLATFORM_BUILDING_PACK_SERVING=off` 로 올려 지금 버전에서 계약의 `canary.steps_percent` 로 옮긴다 | 객체 | 이전 버전 100% |
| 2. manifest | 아래 `publish-building-by-pnu-section-packs` (꺼진 버전이 `_capabilities` 에 `[1, 2, 3]` 으로 답한다) | 객체 (꺼진 버전은 블록을 읽지 않는다) | 6절 manifest 되돌리기 |
| 3. 묶음 | `building-gateway-canary.sh --execute packs <1단계의 버전>`: 같은 코드를 켜서 올리고 꺼진 버전에서 단계마다 옮긴다 | 묶음 | `building-gateway-canary.sh --execute rollback <1단계의 버전>` |

- 시작 전 준비: 분석 파일(계약 `cloudflare_analytics.env_file`)에 계정 id·**존 id**·토큰을 둔다. 토큰 권한은 계약
  `token_scope` 그대로 둘이다(Account Analytics: Read, 운영 주소 존의 Analytics: Read). 존 id 가 없거나 토큰이 둘 중
  하나를 못 읽으면 `--preflight` 가 두 GraphQL 질의를 실제로 돌려 보고 아무것도 올리기 전에 거부한다. 표본은 감시가
  쓰는 파일(`/etc/foundation-platform/building-serving-monitor.env` 의 `…_MONITOR_SAMPLE_PATH`)에서 읽는다.
- 각 단계는 `canary.hold_seconds` 동안 머문 뒤 `scripts/ops/building-gateway-health.sh <새> <옛>`(분석 토큰이 있는
  호스트에서 root 로)이 판정한다. 먼저 운영 주소에서 표본 PNU 를 `canary.synthetic_load` 만큼 두 버전 각각에
  `Cloudflare-Workers-Version-Overrides` 로 고정해 읽는다(출시 전 실트래픽 1% 로는 요청 수를 못 채운다). 그리고:
  고정 읽기의 200·gzip 비율 ≥ `slo.availability_min`(버전마다), 새 버전이 **직접 답한** 고정 읽기 수(답마다 Worker 가
  계약 `version_header` 에 자기 버전 id 를 적는다) ≥ `canary.min_requests_per_step`, Workers 분석이 그 답을 센 비율 ≥
  `canary.analytics_min_coverage`(분석은 표본추출되고 몇 분 늦다. 도달 판정에는 쓰지 않는다, ADR-0157),
  `exceededResources` 0, 예외·내부 오류 비율과 운영 주소의 5xx 비율(클라이언트 요청만, `requestSource: eyeball`;
  하나도 안 세어지면 위반) ≤ 1 − `slo.availability_min`, CPU p99 ≤ `worker_cpu_p99_max_ms` 이고 옛 버전 p99 대비 증가 ≤ `worker_cpu_p99_max_increase_ms`, wall p50·p99 증가 ≤
  `slo.latency_max_increase_ms.warm`. 옛 버전과의 비교(CPU·wall 증가)는 두 버전이 함께 배포된 단계에서만 한다. 100% 단계에서는 옛 버전이 배포에 없어 고정 읽기가 닿지 않으므로 새 버전의 절대 한계만 본다(2026-10-06 실측: 100% 에서 옛 버전 비교를 요구해 건강한 전환이 되돌려졌다). 어긋나면 스크립트가 모든 요청을 옛 버전으로 즉시 되돌리고 멈춘다(exit 1).
- 배포 명령 자체가 실패하면(단계 배포 도중) 스크립트는 지금의 배포 상태를 찍고 옛 버전 100% 로 되돌린 뒤 exit 2 로
  멈춘다. 되돌리기마저 실패하면 갈라진 상태와 손으로 마칠 명령(`--execute rollback <옛>`)을 찍고 exit 3 이다.
  `packs <버전>` 은 그 버전이 지금 100% 가 아니면 거부하고, 올리기가 버전 id 를 내지 않으면 아무것도 배포하지 않는다.
- 스크립트는 기본이 dry-run 이다(명령만 찍는다). 노트북에서 돌릴 때는 판정을 분석 호스트에서 하게 한다:
  `CANARY_HEALTH_COMMAND='ssh <host> sudo /opt/foundation-platform/current/scripts/ops/building-gateway-health.sh @NEW@ @OLD@'`.
- 3단계가 100% 가 되면 감시를 켠다(6절).

2단계 발행:

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

## 6. 감시, 확인과 되돌리기

### 감시 (3단계 100% 뒤)

`foundation-building-serving-monitor.timer`(매시 17분)가 운영 주소에서 표본 앞 `monitor.pnus`(20)건을 읽어
레인이 서빙하는 문서(발행기가 R2 묶음에서 직접 푼 것)와, 객체가 남아 있는 동안 객체 문서와 비교하고 p95 를
`monitor.latency_p95_max_ms` 와 견준다. 어긋나면 유닛이 실패하고 `OnFailure` 가 Slack 에 알린다. 릴리스가 타이머를
설치만 하므로 켜는 것은 이 단계다.

```bash
echo 'FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_MONITOR_SAMPLE_PATH=<WORK>/equality.json' |
  sudo install -m 0640 -o root -g foundation-platform /dev/stdin /etc/foundation-platform/building-serving-monitor.env
sudo systemctl enable --now foundation-building-serving-monitor.timer
```

### 객체는 남긴다

객체 레인은 감시가 `fallback.objects_kept_clean_days`(30일) 동안 깨끗할 때까지 발행된 채, manifest 되돌리기로
돌아갈 수 있는 상태로 둔다. 객체를 정리할지는 그 뒤 소유자가 따로 정한다. 승인 없이 지우지 않는다(append-only).

### 확인과 되돌리기

```bash
…_INSPECT_PNU=<PNU> "$PUBLISHER_BIN" inspect-building-by-pnu-section-packs
```

항목마다 어느 묶음(`g1`, `g1/p3`)이 답했는지, 상태, 조각, 합친 문서를 낸다. 객체 서빙으로 돌아가려면
`section_packs` 가 없는 이력 manifest 로 되돌린다(`publish-building-by-pnu-serving-manifest` 의 되돌리기).

**전환 뒤에는 릴리스를 묶음 패치 이전(#337 이전)으로 되돌리지 않는다.** 그 릴리스의 예약 굽기는 객체만
굽고, 객체 발행은 `section_packs` 블록을 그대로 옮긴다. Worker 는 계속 묶음을 읽으므로 묶음은 낡은 채
서빙되고, 새 변경은 아무도 읽지 않는 객체에만 쌓인다. 릴리스를 그 이전으로 내려야 하면 먼저 위의
manifest 되돌리기로 객체 서빙으로 돌아간다. (#336 보다 오래된 발행기는 모르는 칸이 있는 manifest 를 거부하므로
안전하다.)

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

건물 레인은 항목이 `documents` 하나라(ADR-0151) 이 절은 지금 쓰이지 않는다. 패널에 새 칸이 생기면 모든 묶음을 새
세대로 굽는다(2절, 법정동 약 19,010개). 발행기는 계약이 항목을 여럿 적는 레인을 위해 아래 동작을 그대로 갖고 있다.

`…_PACK_SECTIONS=<항목>` 과 `…_PACK_GENERATION=<그 항목의 새 세대>` 로, 반영 스냅숏에서만 굽는다. 굽기는
법정동마다 새 항목을 지금 서빙 중인 다른 항목들(기본 세대와 패치, R2 에서 그 법정동의 묶음만 읽는다)과 합쳐
모든 문서를 객체 문서와 비교하고, 그 법정동에서 서빙 중인 다른 PNU 가 문서로 답하지 않는지도 본다. 어긋나면 그
법정동을 쓰지 않고 실패한다. 발행은 그 항목의 세대만 바꾸고 `patch_floor` 를 최신 패치로 둔다.

## 8. 남은 일

- 필지 레인은 건물 관문을 통과한 뒤 같은 절차로 옮긴다.
