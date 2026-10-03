---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-03
---

# 필지 by-PNU R2 서빙 — 굽기·발행·검증 런북

루트 ADR-0096 레인의 운영 절차다. 2026-09-09 서울 실증(필지 898,741행 → 3필지 표본 굽기 →
`catalog.gongzzang.app` 실서빙 → postgres 전 필드 대조 21검사 일치)에서 실측한 순서와 함정을
그대로 적는다. 코드 정본은 세 곳이다:

- Spark 잡: `infra/lakehouse/spark/jobs/parcel_panel_silver_to_gold.py`
- 굽기·발행: `services/foundation-outbox-publisher` 의
  `export-parcel-by-pnu-serving` / `publish-parcel-by-pnu-serving-manifest`
- 서빙 Worker: `services/foundation-parcel-gateway` (경로·캐시·CORS 의 정의는
  `config/r2-connections.contract.json` 의 `parcel_by_pnu_gateway`)

## 0. 전제

- 무거운 실행은 전부 배치 호스트(ai-server)에서. 배포는 릴리스 스크립트로만, 장기 실행은
  `setsid` 로 세션과 분리한다 — ssh 가 끊기면 작업도 죽는다.
- 카탈로그·R2 자격증명은 배치 호스트의 systemd 환경 파일(root 전용)에 있다. 실행자는
  거기 없는 두 값만 보충한다: `FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_*` 보충분과
  **`FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER`** (익스포터 필수, systemd 파일엔 없다.
  유효 값은 `crates/lakehouse/lakehouse-infrastructure/src/lakehouse_config.rs` 가 정의).
- 값은 절대 출력하지 않는다. 존재 여부(SET/MISSING)만 말한다.

## 1. gold.parcel_panel 생성 (Spark)

리허설을 먼저 돌린다. `--validate-only` 는 표를 건드리지 않으므로 덮어쓰기 승인 플래그가
필요 없다(시험 `tests/test_parcel_panel_validate_args.py` 가 고정).

아래 `--region-prefix 11`은 서울 실증 범위다. 더 넓은 운영 표에 그대로 덮어쓰면 그 범위를
잃을 수 있다. 운영 재생성 전에는 모든 입력의 수집 범위와 현재 필지 수를 독립적으로
확인하고 `--expected-count`로 고정한다. 검증용 쓰기는 별도 `_smoke` 표를 사용한다.
검증용 표에서 운영 표로 승격하는 기능은 현재 이 생산자에 없다.

Iceberg 입력에는 `--source-snapshots-path`가 필수다. JSON 객체는 사용할 모든 Silver 표를
키로, 실제 Iceberg 판 번호를 값으로 갖는다. 표 목록은 생산자의 `input_sources`에서 나온다.
`--iceberg-snapshot-id`는 이 파일의 `silver.parcel_boundaries` 값과 일치해야 한다.
공통 판 해석기는 파일을 Spark 시작 전에 한 번만 읽고 모든 조회와 실행 요약에 사용한다
([ADR-0130](../../../../docs/adr/0130-panel-input-snapshots-are-complete-and-bound-before-spark.md)).
검증과 재생성에는 동일한 판 파일을 사용하며, 수집 범위·업무 시점의 적합성은 별도로 확인한다.
기본 동작은 `silver.parcel_lineage`의 지번 변경 관계도 읽는다. 입력이 없다는 이유로
`--no-carry-lineage`를 넣어 정상 운영 검증을 통과시킨 것으로 계산하지 않는다.

```bash
# 리허설 — 수치만 판정
parcel_panel_silver_to_gold.py \
  --input-mode iceberg --write-mode iceberg \
  --region-prefix 11 \
  --iceberg-snapshot-id <silver.parcel_boundaries 현재 스냅숏> \
  --source-snapshots-path <사용할 모든 입력 표의 판 번호 JSON> \
  --validate-only --summary-output <검증 요약 경로>

# 요약의 row_count·품질 지표 확인 후 실쓰기 (덮어쓰기는 명시 승인)
#   같은 인자에서 --validate-only 제거, --allow-non-smoke-overwrite 추가
```

- 서울(20코어, `local[2]`·driver 6g): 검증·쓰기 각 10분대, 898,741행.
- **단일 스냅숏 게이트**: 소스 7개 중 하나라도 스냅숏이 2개면 사유를 말하며 거부한다(설계).
- 중복 PNU 거부가 뜨면 수치를 확인하고 `--allow-intra-snapshot-duplicates` 로 재실행한다
  (드랍 수는 요약에 남는다).
- `zoning_unresolved_skipped` 가 커 보여도 투영 로더와 같은 규칙이다 — 판정은 스팟 필지의
  postgres 대조로 한다(6절).

## 2. R2 굽기 (Rust 익스포터)

```bash
# 표본 먼저: allowlist 파일(줄당 PNU 하나)로 3~10필지
FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_CONFIRM_EXPORT=true \
FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER=r2 \
FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION=1 \
FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PNU_ALLOWLIST_PATH=<allowlist> \
FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_SUMMARY_PATH=<요약 경로> \
foundation-outbox-publisher export-parcel-by-pnu-serving

# 전량: allowlist 변수만 빼고 병렬을 올린다 (기본 8, 상한 32)
#   FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_MAX_CONCURRENCY=32
```

- 굽기는 **create-only + 바이트 동일 재사용**이다. 같은 gold 스냅숏에서 재실행하면 기존
  객체는 reused 로 세고 다시 쓰지 않는다 — 중단 후 재실행이 안전하다.
- **함정: 배치 호스트의 퍼블리셔 바이너리는 배포가 갱신하지 않는다.** 릴리스는 소스만
  나른다. 실증에서는 배포된 소스를 읽기 전용 마운트로 컨테이너 빌드해 해결했다
  (rust 이미지 + `cmake`·`libssl-dev` 선설치, `--locked -p foundation-outbox-publisher`,
  20코어 약 3분). 정식 패키징 전까지는 이 절차가 필요하다.

## 3. manifest 발행

```bash
FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_CONFIRM_PUBLISH=true \
FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPORT_SUMMARY_PATH=<굽기 요약 경로> \
foundation-outbox-publisher publish-parcel-by-pnu-serving-manifest
# 최초 발행(기존 manifest 부재)만 FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_FIRST_PUBLICATION=true
```

발행 로그의 `current_generation`·`gold_iceberg_snapshot_id`·`object_count` 가 굽기 요약과
일치해야 한다.

## 4. Cloudflare (코드로 전부 가능, 대시보드 불필요)

`wrangler login`(브라우저 1클릭) 뒤에는 전부 명령이다:

```bash
cd services/foundation-parcel-gateway
corepack pnpm install --frozen-lockfile && corepack pnpm run config:render
npx wrangler deploy --var "FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS:<쉼표구분 origin>"
# Worker 생성 + R2 binding 은 deploy 가 wrangler.jsonc 에서 만든다.
# keep_vars: true 라 CORS 값은 다음 deploy 에도 보존된다.
```

- **커스텀 도메인은 wrangler 명령이 없다.** REST API 로 붙인다(wrangler OAuth 토큰의 Bearer):
  `PUT /accounts/{account}/workers/domains` 에
  `{zone_id, hostname, service: "foundation-parcel-gateway", environment: "production"}`.
- R2 비공개 확인: `wrangler r2 bucket dev-url get <bucket>` 이 disabled,
  `domain list` 가 비어 있어야 한다.
- zone 설정 조회(url_normalization 등)는 wrangler OAuth 스코프 밖이다 — 정규화는 행동으로
  검증한다: 닷세그먼트를 넣은 GET 이 200 이면 정규화가 살아 있다.

## 5. HTTP 검증 (엣지)

| 검사 | 기대 |
|---|---|
| `GET /parcels/by-pnu/{pnu}` | 200, 두 번째부터 `cf-cache-status: HIT` |
| 비정규 PNU·다른 경로 | 404 |
| POST | 405 |
| 미허용 Origin | 403 |
| `If-None-Match` 재요청 | 304 |

지연 실측(2026-09-09, 무료 플랜): PoP 가 LAX 로 잡혀 캐시 HIT TTFB ~0.47s(그중 TLS ~0.30s,
연결 재사용 시 ~0.17s). 한국 PoP 라우팅은 유료 플랜 영역이다 — 코드 결함이 아니다.

## 6. 값 대조 (postgres 와 전 필드)

표본 필지들의 엣지 JSON 과 postgres `catalog.parcel_*` 행을 섹션별로 대조한다. 실증에서는
7섹션(zonings·price·characteristics·forest_ledger·transfer_history·land_rights·
land_right_total) × 3필지 = 21검사 전부 일치했다. 대조는 양쪽 공통 필드의 다중집합 비교로
하고, 명칭 차이는 매핑한다(엣지 `history_seq` ↔ pg `transfer_history_seq`).

함정: R2 응답 JSON 파일은 반드시 `encoding='utf-8'` 로 열 것 — Windows 기본 cp949 가
한글 필드에서 죽는다.

## 7. 실패 시

- Worker 가 503: manifest 부재·비파싱이다. 3절 발행 상태부터 본다(설계된 전면 거부).
- 굽기 중단: 같은 세대로 재실행하면 된다(2절의 create-only 재사용).
- 세대 복구: `check_generation_transition`은 현재보다 낮은 세대의 발행을 거부한다.
  과거 세대를 그대로 재발행하는 롤백 명령은 지원하지 않는다. 기존 객체를 보존하고,
  검증된 과거 내용을 더 높은 새 세대로 준비·검증·발행하는 복구 경로를 먼저 리허설한다.
  이 런북은 그 복구 리허설이 완료됐다는 증거가 아니다.
- Gold 복구: 기존 표의 스키마와 Iceberg 스냅샷을 먼저 보존한다. 생산자는 스키마 추가 후
  직접 덮어쓰기와 재조회 검증을 수행하므로, `--validate-only`만으로 실제 쓰기·복구까지
  검증됐다고 표시하지 않는다. `row_digest`가 없는 과거 판은 일일 변경분 비교의 기준으로
  사용할 수 없으며, 새 계약으로 전체 굽기를 검증한 기준 판이 먼저 필요하다.

## 8. 예약 굽기 (Scheduled bake — 필지·건물 공통)

2026-10 이전에는 두 레인(필지 by-PNU 약 3,986만 객체, 건물 by-PNU 약 594만 객체)을 서버에 손으로 둔
스크립트로만 구웠다. 이제 정본은 저장소의 작업이다([루트 ADR-0122](../../../../docs/adr/0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)).

| 무엇 | 정본 |
| --- | --- |
| 작업 목록·일정·풀·켜짐 | `orchestration/jobs.v1.json` 의 `by_pnu_serving_bake` (두 레인을 한 작업이 차례로) |
| 실행 계정·환경·시간 상한·메모리 상한·쓰기 경로 | `infra/systemd/foundation-by-pnu-serving-bake.service` |
| 한 번의 실행 | `scripts/ops/by-pnu-serving-bake.sh all` (필지 다음 건물; 레인 하나만은 `parcel`·`building`) |
| 할 일이 있는지 | `foundation-outbox-publisher show-<레인>-by-pnu-serving-state` (읽기 전용) |

한 번의 실행은 이렇다.

1. 상태 명령이 Gold 표의 현재 스냅숏, manifest 가 서빙 중인 세대·스냅숏, 그리고 버킷에 객체가 하나라도
   있는 세대 전부(`generations_with_objects`, 구분자 목록 한 번)를 파일로 적는다. 스냅숏이 같으면
   `nothing to do` 를 남기고 성공으로 끝난다. manifest 를 읽을 수 없으면 실패한다(첫 발행은 3절의
   운영자 단계이며 예약 작업이 추정하지 않는다).
2. 이 상태 루트가 같은 스냅숏으로 시작해 기록한 세대(`in-progress.json`)가 있으면 그 세대를 이어 굽는다.
   아니면 새 세대 = (발행 세대, 버킷에 객체가 있는 가장 높은 세대, 기록된 세대) 중 최댓값 + 1. 기록 없이
   객체만 있는 세대 — 손 스크립트가 반쯤 쓴 세대, 상태 루트를 지우기 전의 세대 — 는 이어 굽지 않는다:
   익스포터가 목록에 있는 키를 다시 읽지 않고 완료로 세므로, 다른 스냅숏의 객체가 섞여도 행 수 합과 목록
   개수가 모두 맞는다. 새 세대의 샤드는 `FRESH_GENERATION=true` 로 돌며, 익스포터가 그 샤드 범위에 이미 객체가
   있으면 거부하고 작업은 재시도 없이 실패한다. 범위가 비었음을 확인하면 익스포터가 첫 쓰기 전에
   `shard-<접두사>.fresh-checked` 를 남기고, 그 표시가 생긴 뒤에만 재시도가 이어 굽기로 바뀐다 — 스캔 중 끊긴
   시도는 확인에 닿지 않았으므로 다음 시도도 빈 범위를 요구한다. 세대는 샤드 하나가 이 확인을 통과한 뒤에야
   `in-progress.json` 에 기록되고, 거부되면 기록을 지운다: 다음 실행은 거부된 세대를 이어 굽지 않고 그 위로 간다.
3. PNU 앞자리 샤드로 굽는다. 시작은 `1`…`9`, 익스포터가 행 상한(실행당 90만)을 넘는다며 거부한 샤드는
   10개로 쪼개고(스캔은 상한을 넘는 첫 행에서 멈추므로 거부되는 샤드도 상한만큼만 쥔다), 다음 실행을 위해 잎 샤드 목록(`shard-plan.txt`)을 기억한다. 충돌(429 등)은 같은 샤드를
   재시도하며, 재시도는 목록에 이미 있는 객체를 건너뛴다. 객체는 create-only 다 — 스크립트가
   `ALLOW_OVERWRITE`·`ALLOW_REPOINT`·`FIRST_PUBLICATION` 을 환경에서 지운다. 샤드마다 굽기 대상 스냅숏을
   `EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID` 로 넘기므로, 굽는 중 Gold 가 바뀌면 다음 샤드가 스캔 전에 거부하고
   작업이 그 자리에서 실패한다(다음 실행이 새 세대로 시작한다).
4. 모든 샤드가 같은 스냅숏·같은 세대이고 샤드들의 `exported_row_count` 합이 Gold 행 수(`scanned_row_count`)와
   같을 때만 `PUBLISH_FROM_LISTING` 으로 manifest 를 옮긴다(발행 명령이 목록 개수를 다시 대조한다). 하나라도
   어긋나면 발행하지 않고 실패한다 — 2026-09-10 에 495만 개가 모자란 굽기가 조용히 끝났던 일을 막는 자리다.
5. 발행 뒤 샤드 요약에서 객체 목록을 지우고 개수만 남긴다.

건물 레인은 승인된 건물 연결을 운영 DB 에서 읽는다. `DATABASE_URL` 은 FLOOR 와 같이 compose 의 API 연결을
루프백 포트로 옮겨 얻으며(`runtime-database-url.py`), 얻지 못하면 굽기 전에 78 로 끝난다.

작업 파일은 `/data/foundation-platform/by-pnu-bake/<레인>/` 에만 있다. unit 은 `ProtectSystem=strict` 이고 이
경로만 쓸 수 있으며, `foundation-release.sh timers` 가 디렉터리를 만든다. 환경은 FLOOR 와 같은
`recovery.env`·`source-sweep.env`·`map-edit-fold.env`(카탈로그와 R2 lakehouse writer)와, 선택 파일
`/etc/foundation-platform/by-pnu-bake.env`(비밀 없는 조정값: `FOUNDATION_BY_PNU_BAKE_MAX_CONCURRENCY`, 기본 128)다.

### 켜는 순서 (감독 실행 뒤 별도 배포)

작업은 `enabled: false` 로 등록돼 있다. 호스트는 꺼진 작업의 시작을 거부하므로 Airflow 로는 돌지 않는다.

### 풀과 메모리 ([루트 ADR-0138](../../../../docs/adr/0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md))

굽기는 3슬롯 `spark` 풀의 1슬롯을 쓴다. FLOOR·계보 검토는 3슬롯(혼자), 접기는 2슬롯이라 매시 접기 하나가 굽기
옆에서 돈다. 그 대가로 FLOOR 와 계보 검토가 밀린다. 얼마나 밀리는지는 다음 셋이 정한다.

- 무게(`priority_weight`): FLOOR·계보 10 > 접기 5 > 굽기 1. 슬롯이 비면 Airflow 는 들어가는 작업 중 무거운 것부터 시작한다.
- 굽기는 재시도하지 않는다(`retries: 0`). 시간 상한에 걸리면 다음 차례에 이어 굽는다.
- 차례(`takes_turns`): 굽기는 FLOOR 와 계보가 자기 마지막 시작 뒤에 한 번씩 시작하기 전에는 다시 시작하지 않는다.
  `start-scheduled-job.sh` 가 시작을 `/var/lib/foundation-scheduler/starts/` 에 적고, 차례가 아니면
  `deferred: ...` 를 남기고 아무것도 시작하지 않는다(Airflow 에는 성공).

그래서 최악의 대기는(재시도까지 시간 상한을 다 쓴 경우) **FLOOR 약 34시간, 계보 검토 약 38시간, 매시 접기 약 17시간**
이다(굽기 한 실행 + 차례가 올 때 이미 돌던 접기 + 같은 무게의 상대). `job_specs.pool_starvation` 이 이 상한을 계산해
자기 일정 주기 20번과 비교한다. 굽기는 Gold 에 새 스냅숏이 있을 때만 돈다.

Gold 가 전국 굽기 한 번(실행 여러 번에 걸침)보다 자주 바뀌면 굽기마다 "moved during the bake" 로 멈춘다. 그때는:
발행하지 않는다(서빙은 이전 세대 그대로), 반쯤 쓴 세대는 서빙되지도 이어 굽지도 않은 채 남는다(로그에
`abandoned: generation N holds the objects ...`), 다음 실행은 그 위 세대로 처음부터 굽는다. 남은 세대를 지우는 것은
수동이며(append-only 원칙상 예약 작업은 지우지 않는다), Gold 를 만드는 쪽이 굽기 주기에 맞춰야 끝까지 굽힌다.

단위의 `MemoryMax=14G` 와 행 상한 90만은 2026-10-03 ai-server scratch 측정에서 정했다. **로컬 출력으로 잰 값이다**:
R2 경로는 동시 쓰기 128개와 최대 90만 개의 목록 키를 더 쥐므로 감독 실행(아래 1)에서 `systemctl show -p MemoryPeak
foundation-by-pnu-serving-bake.service` 로 다시 재고, 14G 의 1.3배 여유를 넘으면 행 상한을 낮춘다. 한 레인이 상한에
걸려 OOM 으로 죽어도(`OOMPolicy=continue`) 다른 레인은 돈다.

| 실행 | 행 | 익명 메모리 최대 | 걸린 시간 | 스캔한 파일 |
|---|---:|---:|---:|---:|
| 필지 샤드 `471` | 1,970,539 | 15,025,704,960 B | 4분 13초 | 24개, 39,861,511행 |
| 필지 샤드 `11` | 898,741 | 11,108,622,336 B | 3분 20초 | 24개, 39,861,511행 |
| 건물 샤드 `5` | 867,696 | 6,073,765,888 B (실패 시점) | 56초 | 10개 |

- 최대 메모리 ≈ (남긴 행 + 디코딩 중인 파일 하나) × 약 4KB. 스캔은 데이터 파일(필지 약 166만 행)을 통째로 디코딩한
  뒤 거르므로 약 7.8GB 의 바닥이 있다 — 접두사 필터를 스캔 안으로 밀어 넣는 것이 후속 작업이다.
- 14G = 90만 행 측정 × 1.3. 상한 200만이면 18.2GiB 가 필요해 접기 옆에 둘 수 없다.
- 두 레인은 한 단위가 차례로 돈다. 나란히 두면 예산에 들어간다고 보일 수 없다(건물 측정이 실패 시점까지뿐).
- **건물 레인은 현재 Gold 에서 실패한다**: 샤드 `5` 가 `Gold unit is missing its source unit_pnu` 로 거부됐다.
  감독 실행 전에 Gold 건물 패널을 고쳐야 한다. `all` 은 필지 레인을 그대로 굽고 실패로 끝난다.
- 전국 첫 필지 굽기 추정: 잎 샤드 252개 × 전 표 스캔 약 155초(약 11시간) + R2 쓰기 3,986만 개 × 실측 약
  160개/초(약 69시간) ≈ 81시간. 21시간 시간 상한마다 끊기고 다음 날 목록 이어 굽기로 이어 가 약 4번에 끝난다.

이 작업을 넣은 PR(#318) 이전에 빌드한 릴리스는 `build.json` 에 `tippecanoe_image` 가 없어 새 스크립트에서
`verify-current` 를 통과하지 못한다. 감독 실행 전에 새 릴리스로 전환되어 있어야 한다
([lakehouse-compute-engines.md](lakehouse-compute-engines.md) 4절, 롤백 영향 포함).

1. 감독 실행은 메모리 예산 밖이므로(직접 시작은 Airflow 풀을 거치지 않는다) **여러 날 걸리는 실행 내내** 같은 풀의
   작업을 멈춘다:
   - Airflow 에서 `foundation_building_register_floor`, `foundation_lineage_stewardship`,
     `foundation_map_edit_fold_admin`, `foundation_map_edit_fold_complex` 를 일시정지한다.
   - 시작 전에 하나도 돌고 있지 않은지 확인한다:
     `systemctl is-active foundation-building-register-floor.service foundation-lineage-stewardship.service
     foundation-map-edit-fold@admin.service foundation-map-edit-fold@complex.service` 가 모두 `inactive`
     (또는 `failed`)여야 한다.
   - 데이터 호스트에서 root 가 `sudo systemctl start --no-block foundation-by-pnu-serving-bake.service` 로 시작하고
     `journalctl -u foundation-by-pnu-serving-bake.service -f` 로 지켜본다. 시간 상한(20시간)에 걸리면 같은 확인 뒤
     다시 시작한다(이어 굽는다). 직접 시작은 차례 기록(`starts/`)에 남지 않는다.
   - 레인마다 `nothing to do` 이거나 `published generation N: <개수> objects` 로 끝나야 한다. 엣지에서 5·6절 검증을
     하고, 위의 `MemoryPeak` 를 기록한다. 끝나면 네 DAG 를 다시 켠다.
2. 두 레인이 확인되면 `jobs.v1.json` 에서 `enabled: true` 로 바꾸고 `disabled_reason` 을 지운 변경을 병합·배포한 뒤
   `airflow-runtime.sh up -d` 로 DAG 를 켠다.

### 손 스크립트에서 옮겨 온 것과 버린 것

손 스크립트(2026-09-12, 서버 보관본만 있음)의 하위 명령 분류는 이 작업을 넣은 PR 본문에 표로 있다. 요지:
샤드 굽기·목록 발행·건물 굽기·건물 발행은 위 작업이 되었고, Gold 생성(Spark)은 각 레인의 기존 Spark 잡이
정본이며, 개수 세기·목록 보기·접두사 삭제 같은 점검·수리 명령은 옮기지 않는다(삭제는 append-only 원칙에 어긋나고
개수 대조는 발행 명령이 한다).
