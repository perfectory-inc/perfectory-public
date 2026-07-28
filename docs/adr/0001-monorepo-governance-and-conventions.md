---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-28
---

# ADR 0001: 모노레포 거버넌스와 규칙

> 이 결정이 참조하는 운영 계획과 날짜별 구현 증거는 [ADR-0007](./0007-public-code-private-operations-boundary.md)이
> 관리하는 비공개 전환 보관소에 보존한다.

- 상태: 채택
- 작성일: 2026-07-19

## 배경

`products/gongzzang`과 `platforms/{foundation,identity,intelligence}-platform`은
비공개 전환 중 이 모노레포로 합쳐지기 전까지 별도 저장소였다. 공개 정본 저장소는 검토된
게시 스냅샷에서 시작하며, 공개 전 이력은 ADR-0007의 비공개 전환 보관소에 남아 있다.
통합 과정에서 파일은 옮겼지만 운영 모델은 통합하지 않아 각 영역이 자체 `.github/`(GitHub는
루트 저장소의 workflow만 읽으므로 비활성), 툴체인 고정, 생성 시기의 규칙을 계속 가졌다.
그 결과 health endpoint, migration 이름, Rust 툴체인, 패키지명, 보안 검사, 문서 경로가
달라졌다. Gongzzang 0048과 Foundation 0021 같은 영역 ADR도 여전히 다중 저장소 세계를
설명한다.

## 결정

1. **이 저장소가 네 영역의 모노레포 SSOT다.** 영역 ADR에 남은 다중 저장소 배치는
   역사적 기록이다. 영역 경로는 `products/gongzzang`,
   `platforms/{foundation,identity,intelligence}-platform`이다.
2. **GitHub 설정은 루트 `.github/`에만 둔다.** 영역 workflow는 path filter와
   `defaults.run.working-directory`를 사용한다. 하위 `.github/workflows/`는 금지한다
   (가드: `scripts/guard/no-subdir-github.sh`).
3. **모노레포 전체 Rust 툴체인은 1.96.0 하나다.** 루트 `rust-toolchain.toml` 하나만
   두며 rustup은 부모 디렉터리를 따라 찾는다. 영역별 파일은 루트 고정을 가리므로
   금지하고, `rust-version`은 각 workspace manifest에 유지한다. 버전 상승은 네 영역을
   한 커밋에서 함께 바꾼다(가드: `scripts/guard/toolchain-consistency.sh`).
4. HTTP를 제공하는 모든 workspace는 **axum 0.8**을 사용한다.
5. **Health endpoint:** liveness `/healthz`, readiness `/readyz`, metrics `/metrics`.
   의존성별 진단은 `/readyz/<dep>` 아래에 둔다(가드: `scripts/guard/health-route-conformance.sh`).
6. **경로 namespace:** 플랫폼 고유 HTTP API는 `/<area>/v1/...`에 탑재한다
   (`/catalog/v1`과 `/map/v1`은 Foundation이 공개한 구간이므로 유지). OpenAI 호환 표면
   (`/v1/chat/completions`, `/v1/models`)은 생태계가 요구하는 경로이므로 예외로 기록한다.
7. **Migration:** 각 영역 `migrations/`의 `YYYYMMDDHHMMSS_<snake_case>.sql`(UTC 14자리,
   sqlx 기본 형식)을 사용한다(가드: `scripts/guard/migration-naming.sh`). 출시 전 기존
   파일은 한 번 이름을 바꿀 수 있으며 로컬 데이터베이스는 다시 만든다.
8. 영역별 OpenAPI 산출물은 `docs/openapi/<name>.v<major>.json` JSON이다.
9. 어디서나 **PostgreSQL 17**, 캐시 런타임은 **Valkey 8**을 사용하고 컨테이너 이미지는
   SHA로 고정한다(Gongzzang ADR-0028과 ADR-0007을 상속). local·CI·staging·production은
   endpoint·자격 증명·용량이 다를 수 있지만 정본 runtime major 버전은 다르지 않다.
10. **Cargo 패키지명은 모노레포 전체에서 유일해야 한다.** 범용 라이브러리는 `<area>-`
    접두사를 붙인다(가드: `scripts/guard/unique-package-names.sh`).
11. **환경변수 접두사:** 각 영역은 `FOUNDATION_*`, `IDENTITY_*`, `INTELLIGENCE_*`로
    namespace를 만들며 Gongzzang의 접두사 없는 legacy 변수는 기존 예외로 둔다. 영역 간
    소비는 소유 영역의 접두사를 사용한다.
12. **공급망:** 루트 worktree의 gitleaks(`.gitleaks.toml`,
    `.github/workflows/secret-scan.yml`)와 각 workspace의 cargo-deny(`deny.toml`)를
    네 영역 모두 CI에서 실행한다. 의존성 업데이트는 사람이 검토한 변경으로 수행하며,
    별도 ADR에서 다시 활성화하기 전까지 의존성 bot은 사용하지 않는다.
13. **병합 전 형제 경로 금지:** 추적 파일에 `../<former-sibling>/...`나
    `<local-home>/<repo>`를 남기지 않는다(가드: `scripts/guard/no-stale-sibling-paths.sh`).

## 가드 정책(모노레포 전체에 Gongzzang 제품 우선 규칙 적용)

모든 가드는 막으려는 구체적인 실패 모드를 식별해야 한다. 새 가드는 추측이 아니라 그
실패 모드를 재현할 수 있는 증거를 필요로 한다. 운영 증거는 ADR-0007의 비공개 운영
시스템에 보관한다.

## 결과

- 일회성 정렬에서 툴체인·axum 0.8·health/route 이름·migration 이름·crate 이름·문서
  연결을 통합했다.
- 여러 **저장소** 간 흐름을 설명하던 영역 문서는 이제 여러 **영역** 간 흐름을 뜻하며,
  해당 문구를 필요한 곳에서 고쳤다.
- migration 이름 변경은 기존에 적용된 로컬 sqlx 이력을 무효화하므로 로컬 데이터베이스를
  다시 만들어야 한다.
