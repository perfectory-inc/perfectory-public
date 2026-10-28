# AGENTS.md — perfectory monorepo router

AI 에이전트 공용 라우터(루트). 영역 안에서 작업할 때는 **그 영역의 AGENTS.md가 우선**한다.

## ★ 최상위 원칙 — 근본 해결 (다른 모든 규칙에 우선)

**표면적 땜질 금지.** 모든 변경은 세 가지를 동시에 만족해야 한다:

1. **근본 원인 제거** — 증상이 아니라 왜 생겼는지를 없앤다.
2. **우수한 구조(SSOT)** — 해결이 단일 정의를 남긴다. 같은 지식이 두 곳에 복제되면 그 자체가 결함이다.
3. **재발 불가** — 같은 부류의 문제가 다시 생길 수 없도록 기계적 차단(가드/타입/단일출처)을 남긴다.

문제를 만나면 먼저 물어라: *"이걸 어떻게 하면 다시는 발생 불가능하게 만드나?"* 그 답이 없는 수정은 미완이다.
근거: [ADR 0004 — 검증 SSOT](./docs/adr/0004-verification-ssot.md).

### 해결 접근 순서 — 근거와 재사용 우선

구조·운영·공통 기능을 설계하거나 중요한 문제를 해결할 때는 다음 순서를 따른다.

1. **근본 원인과 불변식 정의** — 직접 원인에서 멈추지 말고, 같은 부류의 문제를 가능하게 한 구조적 원인과 반드시 지켜야 할 불변식을 먼저 밝힌다.
2. **현행 근거 우선** — 실제 코드, ADR, 계약, 운영 증거를 먼저 확인한다. 추측으로 이미 존재하는 구조를 다시 만들지 않는다.
3. **검증된 사례 조사** — 관련된 성숙한 오픈소스와 대규모 프로덕션의 공식 엔지니어링 문서·저장소·표준을 조사한다. 블로그 요약보다 1차 자료를 우선한다. 사례의 복잡성을 그대로 복사하지 말고 우리 규모와 제약에 맞는 보장만 가져온다.
4. **오픈소스 우선(Build vs. Reuse)** — 직접 구현하기 전에 유지보수 상태, 라이선스, 보안, 호환성, 운영비용, 교체 가능성을 기준으로 기존 표준·라이브러리·도구를 평가한다. 적합한 오픈소스가 있으면 재사용이 기본값이다. **이미 평가가 끝난 것부터 본다:** [`docs/technology-stack.md` §1.1 채택 애플리케이션 라이브러리](./docs/technology-stack.md)가 어떤 필요에 무엇을 쓰는지 고정해 둔 명단이다. 거기 있는 필요를 직접 구현하는 것은 이 항의 위반이며, 없는 필요만 아래 5항으로 넘어간다.
5. **커스텀 코드는 마지막 수단** — 도메인 고유의 미충족 부분만 최소한으로 구현한다. 검토한 후보와 채택하지 못한 이유를 중요한 설계/ADR에 남기고, 성숙한 데이터 플레인을 다시 만들지 말고 얇은 어댑터·제어 계층만 만든다.
6. **규모에 맞는 산업 수준 품질** — “대기업식”은 서비스와 프레임워크를 많이 추가한다는 뜻이 아니다. 명확한 소유권과 계약, 불변·감사 가능한 변경, 멱등성, 검증 후 승격, 롤백, 관측성, 기계적 가드를 의미한다.

판단 순서:

`증상 → 근본 원인/불변식 → 현행 SSOT → 검증된 사례 → 오픈소스 평가 → 최소 커스텀 → 기계적 가드`

조사의 깊이는 결정의 영향에 비례해야 한다. 단순하고 국소적인 변경에 형식적인 시장 조사를 강제하지 않는다.

### 검증 결과를 그 문면대로 읽어라 (반복 발생한 오류)

근거: [ADR-0012 검증 결과는 그 문면대로여야 한다](./docs/adr/0012-verification-results-must-mean-what-they-say.md).
아래는 실제로 이 저장소에서 반복 발생한 오류다. 규칙이 아니라 **이미 지불한 비용**이다.

- **"통과"가 "첫 실패까지만 봤다"일 수 있다.** `cargo test`는 fail-fast고, `cargo xtask verify`의
  Python 스위트는 순차 실행이다. 앞선 실패는 뒤 타깃을 **실행조차 시키지 않는다.** 결함 6개를
  찾는 데 세 번의 왕복이 걸렸고, 중간에 두 번 "다 고쳤다"고 잘못 보고했다. 감사할 때는 스위트를
  개별 실행해 실패 전체를 모아라.
- **파이프가 종료코드를 먹는다.** 판정 자리에 `cmd | tail`이나 `cmd; echo "EXIT=$?"`를 쓰지 마라.
  실패 4개를 `RC=0`으로 보고한 사례가 있다. `if cmd > out 2>&1; then …; else …; fi`로 분리한다.
- **"전수 확인"에는 스캔 범위를 함께 써라.** `*.rs`만 훑고 완전성을 주장해 Python 테스트 4개를
  놓친 사례가 있다. 같은 계약을 Rust·Python·셸·TS·SQL·JSON이 함께 읽는다.
- **문서·SQL 단정은 산문이 아니라 메커니즘을 앵커로 써라.** 섹션 제목·식별자·테이블/함수/컬럼
  이름·상태값·환경변수·플레이스홀더만 쓴다. 사람이 읽는 서술 문장은 [ADR-0009](./docs/adr/0009-korean-first-documentation-and-multilingual-readiness.md)
  한글 우선 정책이 언제든 다시 쓴다 — 그 단정은 **정책이 요구한 편집에** 깨진다.
- **나중 파일이 앞 파일을 대체하는 구조에서는 유효 집합 전체를 읽어라.** 마이그레이션 디렉터리가
  그렇다. `CREATE OR REPLACE`로 교체된 함수 본문을 검사하며 초록불이던 테스트가 있었다.

### 새 메커니즘 전에 "이미 있는지" 증명하라 (강제 — 반복 발생한 오류)

스위치·플래그·창(window)·게이트·필드·헬퍼·엔드포인트·"방식"을 **새로 만들기 전에, 코드 한 줄 쓰기 전에** 같은 일을 하는 것이 이미 있는지 **증명한다.** 이것은 위 2항(현행 근거 우선)의 실행 절차이며 건너뛸 수 없다:

1. **전 트리를 조사한다** — 눈앞 파일 하나가 아니라 저장소 전체를 grep 하고, 스캔 범위를 함께 쓴다(아래 "전수 확인" 규칙). 같은 개념이 다른 영역(API·수집·계약·운영 스크립트·다른 언어)에 살아 있을 수 있다.
2. **`docs/adr/*`, 해당 영역의 AGENTS.md·런북**을 확인한다.
3. **결과를 변경 설명/PR에 남긴다.** 있으면 **재사용·확장이 기본값**이고, 없으면 "왜 없는지"를 적은 뒤에만 5항(커스텀)으로 넘어간다.

눈앞 파일에 없다고 "없다"고 단정하지 마라. 같은 개념의 **두 번째 구현은 그 자체가 결함**이다(§원칙 2 SSOT). 증명을 보이지 못하면 새로 만들지 않는다.

이미 지불한 비용: 2026-10-23, 공매 cut-over의 시간창을 넘기려고 cut-over 스크립트 하나만 보고 "없다"고 단정한 뒤 CLI 플래그를 새로 만들었다. 실제로는 같은 일을 하는 운영자 스위치([ADR-0282](./docs/adr/0282-operator-switches-are-issued-by-foundation-api-and-land-as-files-the-runner-reads.md) `set_window`, 법원경매 수집 상시화에 이미 사용)가 있었고, 새 플래그는 SSOT를 쪼개고 그 ADR의 안전 원칙까지 어겼다.

### 계층을 넓게 쌓기 전에 한 경로를 끝까지 뚫어라

명령·타입·테스트를 계층별로 넓게 만들고 나중에 연결하면, 연결 시점에야 드러나는 제약이 그때까지
쌓인 설계를 무효화한다. v2 타일 발행에서 **19번째 커밋에** 스키마 유니크 키가 정적 승격을 아예
막고 있다는 것이 드러났다. 첫 커밋에서 알 수 있었다 — 한 경로를 DB까지 뚫어 봤다면.

세로로 먼저 뚫고, 그 다음 넓힌다. 프로덕션에서 호출되지 않는 계층은 "완료"가 아니다.

### 스키마·제약 변경은 ADR을 남겨라

테이블·제약·유니크 키·함수 게이트 변경은 마이그레이션 주석이 아니라 ADR에 근거와 기각한 대안을
남긴다. 특히 이 저장소에 선례가 없는 형태(`DROP CONSTRAINT` 등)는 반드시 ADR을 만든다.

### 한 번만 하는 일은 저장소에 쌓지 마라

저장소에 영구히 남는 것은 계속 도는 것(제품 코드, 정기 작업, 계약, 그 시험)과 오래 가는 결정뿐이다.

| 종류 | 남기는 것 | 남기지 않는 것 |
|---|---|---|
| 일회성 운영 작업(호스트 이전, 데이터 일회 보정, 옛 표 정리) | 런북 한 절 + 실행 기록 | 전용 계약·ADR·가드. 일회용 스크립트는 작업이 끝난 PR에서 지운다 |
| 사고 | 사후 검토서 한 장(원인·영향·재발 방지 PR 링크). 비공개 저장소 `perfectory-inc/perfectory-ops` 의 `postmortems/` 에 둔다 | 사고마다 새 ADR |
| ADR | 오래 가는 결정(정본 위치, 키, 경계, 대가를 치른 선택) | 작업 절차, 측정 일지, 임시 환경 |
| 임시 환경·실험 | 내리는 명령 하나 | 시험·계약 |

일회성 스크립트를 PR로 올릴 때는 맨 위 주석에 `one-off: <끝나는 조건>`을 적고, 그 조건이 끝나는 PR에서 지운다.

## 영역 지도

- `products/gongzzang` — B2C 제품. 규칙: [products/gongzzang/AGENTS.md](./products/gongzzang/AGENTS.md)
- `platforms/foundation-platform` — 데이터 원장 SSOT. 규칙: [platforms/foundation-platform/AGENTS.md](./platforms/foundation-platform/AGENTS.md)
- `platforms/identity-platform` — 인증/인가. 규칙: [platforms/identity-platform/AGENTS.md](./platforms/identity-platform/AGENTS.md)
- `platforms/intelligence-platform` — LLM 정규화 제안. 규칙: [platforms/intelligence-platform/AGENTS.md](./platforms/intelligence-platform/AGENTS.md)
- `consoles/dawneer` — 직원 통합 콘솔(화면만 소유, 서버는 로그인·중계 BFF). 규칙: [consoles/dawneer/README.md](./consoles/dawneer/README.md)

## 모노레포 절대 규칙 (docs/adr/0001 요약)

- GitHub 설정은 루트 `.github/`에만. 영역 내부 `.github/workflows` 금지.
- Rust 툴체인 핀은 루트 rust-toolchain.toml 하나뿐(값은 `tools/technology-versions.contract.json` 의 `rust_toolchain`). 영역 안에 rust-toolchain 파일을 만들지 말 것(루트를 가림).
- 헬스는 `/healthz`·`/readyz`·`/metrics`. 마이그레이션은 `YYYYMMDDHHMMSS_snake.sql`.
- Cargo 패키지명은 모노레포 전역 유일. 범용 이름엔 `<area>-` 접두사.
- 영역 간 결합은 published HTTP 계약/이벤트만. Cargo path 의존 금지.
- 시크릿 스캔(gitleaks)은 루트 설정 하나. 커밋 전 lefthook이 실행.

## 영역 간 경계 (요약)

- 필지/건물/산단 카탈로그와 공공데이터 수집 = Foundation 소유. gongzzang은 계약 소비만.
- Staff/서비스 인증 = Identity 소유. Zitadel 직접 참조는 수렴 대상(전환기 예외 존재).
- Intelligence는 Foundation에 proposal만 제출(쓰기 권한 없음).

## 가드 · 검증

- `scripts/guard/monorepo-guard.sh` 가 위 규칙을 기계 검사한다(CI + pre-push).
- **검증 SSOT**: `cargo xtask verify <area>` 하나를 로컬 하네스와 CI가 똑같이 부른다([ADR-0004](./docs/adr/0004-verification-ssot.md)). 워크플로우에 raw `cargo clippy/fmt` 금지.
- **CI 시간 예산**: PR 벽시계 상한은 [`tools/ci/ci-time-budget.contract.json`](./tools/ci/ci-time-budget.contract.json) 하나뿐이다. 주간 `ci-time-budget` 워크플로가 직전 주 p50과 비교한다([ADR-0203](./docs/adr/0203-pull-request-ci-has-one-wall-clock-budget.md)). CI를 느리게 하는 변경은 이 숫자 안에서 설명한다.
- **번호는 열린 PR까지 본다**: ADR 번호와 마이그레이션 버전은 main만이 아니라 다른 열린 PR이 이미 잡은 것도 피한다. PR CI의 `numbering_collisions.py`(`required/repository`)가 main이나 더 먼저 연 PR이 가진 번호를 거부하고 다음 빈 번호를 알려 준다. 나중에 연 쪽이 번호를 바꾼다.
- **훅은 조언, CI가 권위**([ADR-0005](./docs/adr/0005-hooks-advisory-ci-authoritative.md)): git 훅은 빠른 로컬 편의일 뿐 권위 게이트가 아니다. host 도구(cargo/pnpm)가 없으면 훅은 실패가 아니라 **skip**한다. 무거운/툴체인 검사의 권위는 CI에 있다.

## 위생 불변식

기계가 강제하는 위생 규칙만 둔다: 규칙 + 그것을 강제하는 장치 + 결정 ADR([ADR-0299](./docs/adr/0299-engineering-hygiene-is-a-set-of-machine-enforced-invariants.md) §5).

- **서빙 신선도(B·C)**: 모든 by-PNU 레인은 사용자가 받는 manifest 의 나이(`published_at_utc`)를 계약 예산(`by_pnu_section_packs.freshness_budget_hours`) 안에 둔다 — 작업 상태가 아니라 산출물로 잰다. 장치: 매시 `foundation-by-pnu-serving-monitor@<lane>` 실패 → Slack([ADR-0306](./docs/adr/0306-a-by-pnu-lane-alerts-when-what-it-serves-is-older-than-its-freshness-budget.md)).

## 작업 목록 SSOT

- `AGENTS.md`에는 작업 내용을 복제하지 않는다. 운영 출시 작업은 [루트 README](./README.md)의 문서 지도에서 연결한 단일 TODO/로드맵을 따른다.
- 새 작업을 시작하는 AI 에이전트는 [루트 README](./README.md) → [문서 지도](./docs/README.md) → 연결된 문서의 미완료 항목 순서로 읽는다.
