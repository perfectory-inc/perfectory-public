# ADR 0586: 변경이 건드린 셸·Python 파일은 ShellCheck 와 ruff 를 통과한다 — 기준선은 줄기만 한다

- Status: Accepted
- Date: 2026-11-19
- Builds on: [ADR-0012](./0012-verification-results-must-mean-what-they-say.md)(검증 결과는 문면대로),
  [ADR-0197](./0197-ci-downloads-pinned-release-binaries-only-through-one-cached-retrying-fetcher.md)(고정 내려받기),
  [ADR-0207](./0207-a-pull-request-runs-only-the-guards-whose-inputs-it-changes.md)(가드는 입력이 바뀔 때만),
  [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md)(바뀐 것만)

## Context

저장소에는 셸 스크립트 287개와 Python 파일 539개가 있고(2026-11-19 main), 둘 다 린터가 없었다. `AGENTS.md` 는 "파이프나
명령 치환이 종료 코드를 가리면 안 된다" 를 글로 요구하고 검토가 지켰다. 2026-10-29 에는 파이프로 넘긴 출력만 보고 사진
확인이 성공이라고 보고한 일이 있었다. 같은 규칙을 도구가 이미 갖고 있다.

상류 문서(2026-11-19 에 읽음):

- ShellCheck 0.11.0([CHANGELOG](https://github.com/koalaman/shellcheck/blob/v0.11.0/CHANGELOG.md),
  `shellcheck --list-optional`): 선택 검사 `check-extra-masked-returns` 가 SC2312("명령 치환 안에서 종료 코드가 가려진다")를
  낸다. 설정은 `.shellcheckrc`, 출력은 `--format=json1`. 배포판은 Linux·macOS 가 아키텍처별 tar, Windows 는 zip 하나다.
- ruff 0.17.5(`ruff rule PLW1510`): `subprocess.run` 에 `check` 가 없으면 반환 코드를 확인하지 않아 조용히 실패할 수 있다.
  출력은 `--output-format=json`.
- ESLint [Bulk Suppressions](https://eslint.org/docs/latest/use/suppressions): 규칙을 켤 때 이미 있는 위반을 파일·규칙별
  개수로 적어 두고, 새 코드에는 규칙을 적용한다. 이 결정의 기준선이 그 모양이다.

첫 전체 실행(실측, Windows 와 WSL 리눅스에서 같은 결과):

| 도구 | 파일 | 건수 | 많은 규칙 |
|---|---:|---:|---|
| ShellCheck | 153 / 287 | 874 | SC2312 682, SC2016 56, SC2034 31, SC2154 30 |
| ruff | 156 / 539 | 340 | PLW1510 119, B905 50, B023 42, E402 33, F401 31, F821 24 |

전체 실행은 24초다(노트북).

## Decision

1. **검사 하나**: `scripts/ci/lint_changed.py check` 가 ShellCheck(`.shellcheckrc`)와 ruff(`ruff.toml`)를 돌린다. 가드
   `shell-and-python-lint` 가 부르고, `tools/ci/change-scope.json` 에 입력(`**/*.sh`, `**/*.py`, 두 설정, 기준선, 핀)을
   적는다. 막는 사고: 종료 코드를 가린 검사가 성공으로 보고된 일(2026-10-29).
2. **바뀐 파일만**: PR 과 병합 큐에서는 그 변경이 건드린 파일만 검사한다(`CHANGE_SCOPE_BASE...HEAD`). 그 밖의 실행(밤
   전체 실행, 손으로 돌릴 때)은 전체다.
3. **기준선은 파일·규칙별 개수이고 줄기만 한다**(`tools/ci/lint-baseline.json`).
   - 검사한 파일에서 어떤 규칙의 건수가 기준선보다 많으면 실패한다. 기준선에 없는 파일은 0건이어야 한다. 한 규칙을
     고친 만큼 다른 규칙을 더할 수 없다.
   - 검사한 파일이 나아졌으면 기준선도 내려야 한다(`lint_changed.py baseline` 이 다시 쓴다).
   - 기준선은 변경의 base 와 비교해 새 항목이나 더 큰 수를 가질 수 없다.
4. **규칙 묶음**: ShellCheck 기본 + `check-extra-masked-returns`. SC1091("source 한 파일을 열 수 없다")은 끈다: 스크립트가
   아니라 검사기가 닿는 범위에 대한 말이다. ruff 는 기본 규칙(E4·E7·E9·F) + flake8-bugbear(B) + PLW1510. 형식은 판정하지
   않는다.
5. **도구는 고정 내려받기다**: `tools/shellcheck.env`, `tools/ruff.env`, `tools/pinned-artifacts.json`. ShellCheck 는
   Windows 를 고정하지 않는다(아키텍처별 배포가 없다). Windows 에서는 셸 파일을 건너뛴다고 알리고, 판정은 CI 가 한다.
6. **한 줄을 봐줄 때는 그 줄에 이유와 함께 적는다**: `# shellcheck disable=SCnnnn`, `# noqa: CODE`.

## Consequences

- 새 파일과 손댄 파일은 깨끗해야 한다. 손대지 않은 1,214건은 그대로 있고, 파일을 고칠 때 줄어든다.
- bash heredoc 안의 Python 52개(27개 파일)는 어느 도구도 보지 않는다. 파일로 꺼낼 때 검사 대상이 된다.
- F821(정의되지 않은 이름) 24건과 B023(반복문 변수를 붙잡는 클로저) 42건은 실제 버그일 수 있다. 이 PR 은 고치지 않는다.
  목록은 기준선에 있고, 그 코드의 주인이 고친다.
- 도구 판을 올리는 PR 에서 새 규칙이 건수를 늘리면, 그 PR 이 그 규칙을 끄거나 먼저 고친다. 기준선은 늘지 않는다.
- Renovate 는 `tools/*.env` 의 핀을 올리지 않는다(재발명 목록 26번, 별도).

## Rejected alternatives

| 대안 | 왜 아닌가 |
|---|---|
| 전부 고친 뒤 켠다 | 1,214건을 한 PR 에서 고치면 운영 스크립트 153개가 한꺼번에 바뀐다. 검토할 수 없다. |
| 바뀐 줄만 본다(diff 기반) | 줄 번호가 밀리면 옛 위반이 새 것으로 보인다. 파일·규칙별 개수가 도구 없이도 안정적이다. |
| 파일 단위 예외 목록 | 예외 파일 안에서는 새 위반이 얼마든지 는다. |
| lefthook 에서만 돌린다 | 훅은 조언이다(ADR-0005). 권위는 CI. 훅 추가는 뒤에 한다. |
