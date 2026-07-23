# ADR 0022 — Bronze scraping = 격리 Python service (`services/scraper-py/`)

| | |
|---|---|
| 작성일 | 2026-05-07 |
| 상태 | Historical implementation superseded by ADR 0034/0048; isolation invariant retained by [ADR 0025](./0025-bronze-scraping-workflow-orchestrator-not-rust-spawn.md) |
| 선행 | [ADR 0016](./0016-medallion-base-layer-postgis-silver-pmtiles-gold.md), [ADR 0021](./0021-static-vector-tile-decomposition.md), [AGENTS.md § 1](../../AGENTS.md) |
| Amendment | [ADR 0025](./0025-bronze-scraping-workflow-orchestrator-not-rust-spawn.md) retains the producer-isolation invariant without binding it to a workflow filename |

## 결정

*HTML scraping + anti-bot bypass + 정부 사이트 자동 다운* 이 필요한 ETL Bronze 작업은 **격리된 Python service** (`services/scraper-py/`) 에서 [Scrapling](https://github.com/D4Vinci/Scrapling) 라이브러리로 구현. 메인 Rust 시스템 (`crates/`, `services/api`, `services/etl-base-layer`) 에 Python 의존성 0.

ETL Rust orchestrator (`services/etl-base-layer/`) 가 `tippecanoe` / `ogr2ogr` / `tile-join` 와 *동일한 subprocess pattern* 으로 Python script 를 spawn → JSON summary 받음.

## 컨텍스트

### 요구사항

1. **연속지적도 전국 SHP archive** (V-World dtmk selector `dsId=30563`) 자동 수집 + R2 영구 저장 → 정적 타일 input
2. **runtime API 호출 0** — 정적 데이터 (필지 polygon, 행정구역, 산단) 는 *모두 우리 R2/DB 영구 저장*. 외부 API 의존 0
3. **scheduled diff** — provider inventory 변경을 감지해 변경된 artifact만 다시 수집
4. **미래: 대법원 경매 scraping** — anti-bot 강함, Scrapling 의 stealth 필수

### 검토한 대안

#### A. Rust 직접 구현 (`reqwest` + `cookie_store` + `scraper` crate)
- 장점: 메인 언어 일관성, single binary, dependency tree 단순
- 단점:
  - **Reinvent the wheel** — Scrapling 의 anti-bot bypass 패치를 우리가 따라잡아야 함 (Cloudflare/Akamai 가 *상시 패치*)
  - 경매 같은 *anti-bot 강한 사이트* 에서는 우리 재구현이 *항상 뒤처짐*
- **거부 이유**: 미래 대법원 경매 (anti-bot 강함) 에서 무조건 재구현 부담 발생. V-World (지금) 도 정부 정책 변화 시 anti-bot 강화 가능성

#### B. 메인 Rust crate 안에 Python embed (PyO3)
- 장점: single binary 환상
- 단점:
  - Rust binary 가 Python runtime 의존 → 배포 시 *Python 환경 + venv* 동봉
  - 의존성 격리 안 됨 (Python lib 충돌이 Rust 빌드까지 영향)
  - PyO3 의 GIL 핸들링 복잡
- **거부 이유**: AGENTS.md § 1 의 "메인 시스템 의존성 0" 정책과 강하게 충돌

#### C. 격리 Python service + subprocess pattern (본 ADR 채택)
- 장점:
  - Scrapling 그대로 사용 (anti-bot 패치 자동 — `pip install --upgrade scrapling`)
  - 메인 Rust 시스템 영향 0 (`services/scraper-py/` 만 Python)
  - tippecanoe / ogr2ogr 와 *동일 subprocess pattern* — ETL Rust 의 orchestrator 가 spawn
  - 격리 — Python venv 의 의존성이 Rust crate `Cargo.toml` 에 안 섞임
- 단점: Python runtime과 별도 dependency lifecycle을 운영해야 함
- **채택 이유**: anti-bot 패치 자동성 + 격리 + 패턴 일관성. *진짜 SSS = 표준 도구 그대로 + 우리 glue 코드 최소*.

#### D. 외부 SaaS scraping (예: ScrapingBee, Apify)
- **거부**: 비용 + 정부 사이트 약관 + 우리 데이터 제3자 노출 risk

## 채택 조건

격리 worker는 다음 계약을 자동 검증해야 합니다.

1. 인증이 필요한 provider listing/detail 요청을 세션 범위 안에서 수행합니다.
2. 응답을 메모리에 전부 올리지 않고 스트리밍합니다.
3. HTTP metadata와 archive magic/content를 함께 검증한 뒤에만 Bronze에 commit합니다.
4. stdout summary와 log에 credential, cookie, provider account 식별자를 남기지 않습니다.
5. 실제 요청·응답과 실행 측정치는 공개 ADR이 아니라 private operations evidence에 둡니다.

## 채택

### 디렉토리

```
services/scraper-py/
├── .venv/                  # gitignored
├── .gitignore
├── README.md               # 운영 가이드
├── requirements.txt        # scrapling, curl_cffi, boto3
├── dtmk_vworld.py          # 본 ADR 의 첫 구현 — V-World dtmk SHP zip → R2
└── (미래)
    ├── court_auction.py    # 대법원 경매 (anti-bot 강함)
    └── ...
```

### Python 측 책임

- HTML scraping (Scrapling)
- form login + session cookie persist (curl_cffi)
- streaming download → R2 PUT (boto3, S3-compatible)
- idempotent skip (R2 의 같은 size object 면 skip)
- summary JSON stdout (Rust 가 parse)

### Rust 측 책임 (ETL orchestrator)

- subprocess spawn (`tippecanoe` 와 동일 pattern):
  ```rust
  build_command(host, "python", &[
      Arg::Path(scripts_dir.join("services/scraper-py/dtmk_vworld.py").as_path())
  ])
  ```
- stdout JSON parse → manifest 갱신
- R2 Bronze prefix 의 zip → ogr2ogr → tippecanoe → flat tile (Gold)
- TileJSON publish (ADR 0021)

### 데이터 흐름 (전체)

```
[V-World dtmk 사이트]
  ↓ Scrapling (Python 격리 service)
[R2 Bronze archive]
  bronze/<YYYY-MM>/parcel-dtmk-30563/SYNTHETIC_PARCEL_<region>.zip
  ↓ Rust ETL (services/etl-base-layer)
  ↓ unzip → ogr2ogr → tippecanoe → tile-join
[R2 Gold]
  gold/v<N>/parcels/{z}/{x}/{y}.pbf  (flat vector tile, ADR 0021)
  gold/v<N>/parcels.json             (TileJSON, ADR 0021)
  gold/manifest.json                 (artifact 메타 + sha256)
  ↓ 클라
[Naver SDK + mapbox-gl 자동 fetch]
  지도에 전국 필지 폴리곤
```

## 영향

### 신규
- `services/scraper-py/` 패키지 (README, requirements.txt, .gitignore, dtmk_vworld.py)
- `docs/adr/0022-bronze-scraping-isolated-python-service.md` (본 파일)

### 수정 (다음 세션)
- `services/etl-base-layer/src/bronze/dtmk.rs` 신규 — Rust 가 Python script spawn
- `services/etl-base-layer/src/main.rs` — `bronze --source dtmk-vworld` subcommand
- `crates/parcel-lookup/` — runtime V-World API 의존 폐기, **DB 우선 lookup** (사용자 정신: *정적 데이터 = 우리 저장, runtime API 호출 0*)

### 폐기 (검토)
- `services/etl-base-layer/scripts/fetch-vworld-sig.mjs` — Node prototype, 본 ADR 채택 후 삭제

## 후속

### Scheduled diff

- provider inventory의 안정 식별자와 checksum/size marker를 이전 manifest와 비교합니다.
- 변경된 artifact만 다시 수집하고 영향받은 Gold partition만 재생성합니다.
- manifest watermark가 비교 기준의 SSOT이며, 배포 주기와 실행 결과는 private operations에서
  관리합니다.

### 다음 sub-project (SP-court-auction)
- `services/scraper-py/court_auction.py`
- 대법원 경매정보 (anti-bot 강함 — Scrapling 의 stealth 필수)
- *우리 platform 의 매물 검색에 경매 매물 포함*

### 메인 시스템 영향 0 검증
- `services/api/` (Rust 백엔드) — Python dep 추가 0
- `services/etl-base-layer/Cargo.toml` — Python 의존 0 (subprocess spawn 만)
- `crates/` — Python dep 0
- `apps/web/` — Python dep 0
- AGENTS.md § 1 정책 부합 ✅

## 참고

- Scrapling repo: https://github.com/D4Vinci/Scrapling
- V-World dtmk selector `dsId=30563`: https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?dsId=30563
