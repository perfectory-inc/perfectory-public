---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-09-09
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

```bash
# 리허설 — 수치만 판정
parcel_panel_silver_to_gold.py \
  --input-mode iceberg --write-mode iceberg \
  --region-prefix 11 \
  --iceberg-snapshot-id <silver.parcel_boundaries 현재 스냅숏> \
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
- 세대 롤백: manifest 를 이전 세대로 재발행하면 끝이다. 객체는 지우지 않는다.
