---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-28
---

# ADR 0005: Git 훅은 조언, CI는 권위

## 배경

`markdown-links` pre-push 훅이 `cargo xtask docs`를 호출했지만 이 저장소는 Rust를
Docker 안에서만 빌드하고 개발 호스트에는 cargo가 없었다. 그 결과 문서와 무관한
`git push`도 `cargo: command not found`로 전부 막혔다. “호스트 전용 도구를 훅에서
금지하자”는 즉시 처방은 성숙한 조직의 방식과 맞지 않아 채택하지 않았다.

주요 근거는 같은 방향을 가리킨다.

- Pro Git의 Git Hooks 설명은 클라이언트 훅이 clone 때 복사되지 않고, 정책을 강제하려면
  서버 측에서 해야 하며, `git commit --no-verify`로 우회할 수 있다고 설명한다.
- Google의 Software Engineering은 presubmit에 빠르고 신뢰할 수 있는 검사만 두고,
  실제 테스트는 Forge/TAP 같은 서버 인프라에서 실행한다.
- `facebook/react`는 저장소에 Git 훅 도구를 제공하지 않고, `rust-lang/rust`의
  `src/etc/pre-push.sh`는 복사해 활성화하는 선택형 예제다. `oxidecomputer/omicron`도
  저장소 내부 훅 없이 CI와 선택형 Nix 개발 셸을 사용한다.

## 결정

**`.github/workflows/*`의 CI가 유일한 권위 검증 게이트이고, lefthook은 빠른 로컬
조언 도구다.** 구체적인 규칙은 다음과 같다.

1. 필요한 호스트 도구가 없으면 훅은 반드시 건너뛰고 실패시키지 않는다. 호스트 도구
   훅은 Lefthook의 `skip.run`에서 실제로 호출할 구체 명령을, 해당 명령의 `root:`에서
   검사한다. `pnpm`이나 `cargo` 같은 실행기만 확인하는 것으로는 불충분하다. 이
   저장소는 Docker에서만 Rust를 실행하므로 호스트 하위 도구가 없거나 사용할 수
   없어도 commit·push를 막지 않는다.
2. 무겁거나 툴체인에 의존하는 전체 `cargo xtask verify`, 전체 저장소 Lychee,
   DB 통합 테스트는 CI에서 강제한다. 훅에도 복사본을 둘 수 있지만 Docker로 감싸고
   건너뛰기 가드를 둔다(예: Docker가 내려가면 `scripts/ci/lychee-docs.sh`가 건너뜀).
3. 훅은 CI가 강제하지 않는 정책을 강제하는 장소가 아니다. 중요한 검사는 CI에 두고,
   훅은 빠른 피드백만 앞에서 제공한다.

호스트 전용 도구를 훅에서 일괄 금지하는 정책은 채택하지 않는다. 빠른 로컬 도구는
계속 유용하다. 대신 좁은 범위의 `lefthook-advisory-policy` 가드가 각 직접 패키지
도구 명령과 `root:`에서 필요한 가용성 검사를 도출한다. 이 가드의 변이 테스트는
실행기만 검사하는 경우, 잘못된 하위 도구·잘못된 root·검사 누락·간접 또는 모호한
검사·복합 명령·지원하지 않는 YAML 형태·중복 키를 거부한다. 권위 게이트가 Docker/CI에
이미 있으므로 무거운 호스트 Cargo 명령도 거부한다.

## 결과

- cargo 없는 호스트에서 임시 `LEFTHOOK_EXCLUDE=…` 우회가 필요하지 않다. cargo 훅은
  스스로 건너뛴다.
- 모든 구체적인 하위 도구가 설정된 root에서 실행되는 완전한 개발 환경에서는 기존처럼
  훅이 실행된다. PATH에 실행기가 있다는 사실만으로는 충분하지 않다.
- 권위 보장은 ADR-0004의 검증 SSOT가 재현 가능하게 만드는 CI에 있다.
