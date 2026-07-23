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
4. **오픈소스 우선(Build vs. Reuse)** — 직접 구현하기 전에 유지보수 상태, 라이선스, 보안, 호환성, 운영비용, 교체 가능성을 기준으로 기존 표준·라이브러리·도구를 평가한다. 적합한 오픈소스가 있으면 재사용이 기본값이다.
5. **커스텀 코드는 마지막 수단** — 도메인 고유의 미충족 부분만 최소한으로 구현한다. 검토한 후보와 채택하지 못한 이유를 중요한 설계/ADR에 남기고, 성숙한 데이터 플레인을 다시 만들지 말고 얇은 어댑터·제어 계층만 만든다.
6. **규모에 맞는 산업 수준 품질** — “대기업식”은 서비스와 프레임워크를 많이 추가한다는 뜻이 아니다. 명확한 소유권과 계약, 불변·감사 가능한 변경, 멱등성, 검증 후 승격, 롤백, 관측성, 기계적 가드를 의미한다.

판단 순서:

`증상 → 근본 원인/불변식 → 현행 SSOT → 검증된 사례 → 오픈소스 평가 → 최소 커스텀 → 기계적 가드`

조사의 깊이는 결정의 영향에 비례해야 한다. 단순하고 국소적인 변경에 형식적인 시장 조사를 강제하지 않는다.

## 영역 지도

- `products/gongzzang` — B2C 제품. 규칙: [products/gongzzang/AGENTS.md](./products/gongzzang/AGENTS.md)
- `platforms/foundation-platform` — 데이터 원장 SSOT. 규칙: [platforms/foundation-platform/AGENTS.md](./platforms/foundation-platform/AGENTS.md)
- `platforms/identity-platform` — 인증/인가. 규칙: [platforms/identity-platform/AGENTS.md](./platforms/identity-platform/AGENTS.md)
- `platforms/intelligence-platform` — LLM 정규화 제안. 규칙: [platforms/intelligence-platform/AGENTS.md](./platforms/intelligence-platform/AGENTS.md)

## 모노레포 절대 규칙 (docs/adr/0001 요약)

- GitHub 설정은 루트 `.github/`에만. 영역 내부 `.github/workflows` 금지.
- Rust 툴체인 핀은 루트 rust-toolchain.toml 하나뿐(1.96.0). 영역 안에 rust-toolchain 파일을 만들지 말 것(루트를 가림).
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
- **훅은 조언, CI가 권위**([ADR-0005](./docs/adr/0005-hooks-advisory-ci-authoritative.md)): git 훅은 빠른 로컬 편의일 뿐 권위 게이트가 아니다. host 도구(cargo/pnpm)가 없으면 훅은 실패가 아니라 **skip**한다. 무거운/툴체인 검사의 권위는 CI에 있다.
