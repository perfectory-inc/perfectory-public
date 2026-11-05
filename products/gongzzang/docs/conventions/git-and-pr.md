---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# Git + PR 컨벤션

## 1. 브랜치

| 종류 | 형식 | 예시 |
|------|------|------|
| 메인 | `main` | (보호 브랜치, PR + merge queue 로만 반영) |
| 기능 | `feat/<topic>` | `feat/listing-search-api` |
| 버그 | `fix/<topic>` | `fix/auth-token-leak` |
| 잡일 | `chore/<topic>` | `chore/update-deps` |
| 문서 | `docs/<topic>` | `docs/sub-project-2-spec` |
| 리팩터 | `refactor/<topic>` | `refactor/listing-status-machine` |
| 성능 | `perf/<topic>` | `perf/parcel-search-index` |
| CI | `ci/<topic>` | `ci/add-mutation-test` |

`<topic>` = kebab-case 짧게 (3-5단어).

## 2. 커밋 메시지 (Conventional Commits)

```
<type>(<scope>?): <subject>

<body 선택>

<footer 선택>
```

### type (필수)

- `feat` — 새 기능
- `fix` — 버그
- `chore` — 잡일 (deps, 설정)
- `docs` — 문서만
- `test` — 테스트만
- `refactor` — 리팩터 (동작 변경 X)
- `perf` — 성능 개선
- `ci` — CI 설정
- `build` — 빌드 시스템
- `style` — 포맷 (rustfmt, biome)
- `revert` — 되돌리기

### scope (선택, 권장)

- 도메인: `listing`, `parcel`, `user`, `auth`, ...
- 인프라: `db`, `cache`, `api`, `ui`, ...
- 도구: `monorepo`, `quality`, `ci`, ...

### 예시

```
feat(listing): 매물 검색 API 추가
fix(auth): JWT 만료 시 401 대신 200 반환 버그 수정
chore(deps): Biome 2.4 → 2.5 업그레이드
docs(adr): ADR-0012 검색 엔진 결정 추가
refactor(parcel): PNU 값 객체로 캡슐화
perf(db): listing_geom_gist_idx 추가로 공간 쿼리 100ms → 10ms
```

자동 강제: **없음** — 루트 `lefthook.yml` 에 commit-msg 훅이 없고 PR 제목 검사 workflow 도 없다.
형식은 수동 준수한다. squash merge 라 main 의 커밋 제목은 PR 제목이다.

## 3. PR 룰

### 크기

- **권장 ≤500줄** (생성/수정 합산)
- 1,000줄 초과 = 분할 검토
- PR 크기 자동 차단은 없다. 자동 차단은 **파일 하나**가 1,500줄을 넘을 때다
  (`scripts/guard/file-line-limit.sh`, 예외 `tools/ci/file-line-limit.exceptions.json`)

### 1 PR = 1 관심사

- ❌ "feat + fix + refactor" 섞기
- ✅ 각각 분리 PR

### PR 제목 형식

커밋 메시지 첫 줄과 동일 형식:
```
feat(listing): 매물 검색 API 추가
```

### PR 본문

루트 `.github/pull_request_template.md` 가 정본이다(변경 요약·동기·범위·테스트·SSS 7 기둥·옵션 A·Breaking
Change·관련 이슈). 이 문서는 그 내용을 복사하지 않는다.

## 4. 머지 전략과 보호 룰

정본은 `../../../../tools/github/main-ruleset.json` 이다. 요지:

- PR 필수, **squash merge** 만 허용, merge queue 로 병합 (한 PR = main 의 한 커밋)
- main 직접 push·force push·삭제 금지, linear history
- 필수 `required/*` status check 전부 통과 + 리뷰 스레드 해결
- 필수 승인 수는 단독 maintainer 기간 `0` — 두 번째 maintainer 가 생기면 올린다

## 5. 태그 + 릴리즈

현재 릴리즈 태그·Changesets·릴리즈 노트 자동화는 없다.

## 6. 커밋 자체 규칙

- ✅ Atomic — 한 커밋 = 한 논리적 변경
- ✅ Bisect 가능 — 각 커밋이 컴파일·테스트 통과
- ✅ rebase 친화 — fixup/squash 자유
- ❌ "WIP", "tmp", "fix typo" 같은 의미 없는 메시지

## 7. 자동 강제 흐름

단계별 도구는 [enforcement-flow.md](./enforcement-flow.md)와 [README.md](./README.md)의 매핑 표가 정본이다.
커밋 메시지 형식은 자동 강제가 없다.

## 8. 금지

- ❌ `git push --force` to main
- ❌ `git rebase -i main` 후 force push (PR 외)
- ❌ 시크릿 git history에 남기기 (gitleaks가 차단)
- ❌ 큰 바이너리 git 직접 커밋 (R2 등 객체 저장소에 둔다)
- ❌ `--no-verify` 사용 (훅 우회)
