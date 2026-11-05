# ADR 0422: 구조화된 설정의 정책은 실제 파서가 만든 모델 위에서 고정된 Conftest 가 평가하는 Rego 다

- Status: Accepted
- Date: 2026-11-06
- Builds on: [ADR-0001](./0001-monorepo-governance-and-conventions.md)(가드는 막는 사고를 말한다),
  [ADR-0197](./0197-ci-downloads-pinned-release-binaries-only-through-one-cached-retrying-fetcher.md)(고정 내려받기 하나),
  [ADR-0385](./0385-the-host-says-when-its-disk-memory-or-a-cap-runs-out-and-each-rule-is-tested-by-promtool.md)(엔진 소켓·호스트 네임스페이스 금지의 이유)

## Context

가드의 상당수는 YAML 을 손으로 읽었다. 고급 정리 인벤토리(`docs/roadmap/high-end-cleanup-inventory.md` A-3)의 G026·G061·G063 이 그렇다.
- `scripts/guard/check-container-runtime-policy.sh`(710줄)는 Compose 를 줄 정규식과 들여쓰기로 읽는 YAML 부분집합 파서였다.
- `scripts/guard/check-workflow-policy.sh`(714줄)는 워크플로를 awk 로 읽는다.

파서가 손으로 짠 부분집합이라 YAML 문법 자체를 금지해야 했다. 탭, 따옴표 키, `key :`, `? key`, 흐름 매핑, 앵커·별칭 위치,
사용자 태그가 그렇다. 금지하지 않으면 그 모양 뒤에 가변 이미지나 열린 포트가 숨어도 파서가 보지 못한다. 이 금지는 사고를 막는
규칙이 아니라 파서의 사각을 막는 규칙이었다. 그런데도 실제 사각이 남았다. 목록형 `environment:`(`- POSTGRES_PASSWORD=…`)의 평문
비밀은 매핑형만 읽는 파서를 그대로 지나갔다.

2026-11-05 #1090 이 Compose 절반을 옮기며 측정했다.
- `docker compose config --no-interpolate --no-env-resolution --no-path-resolution --format json` 은 저장소의 Compose 17개를 모두
  정규화했다. 앵커·`extends`·`include`·`!reset` 이 풀린 최종 서비스가 나온다.
- 첫 CI 에서 러너의 옛 Compose 가 보간 안 한 볼륨 기본값 `${NAME:-./path}:/target:ro` 를 "too many colons" 로 거부해 6개 파일이
  실패했다. 파서의 판(version)이 정책의 결과를 바꾼 것이다.

업계의 같은 문제에 대한 답은 Open Policy Agent(CNCF 졸업 프로젝트)의 Rego 정책을 Conftest 로 구조화된 입력에 돌리는 것이다.
Kubernetes 의 Gatekeeper, Terraform plan 검사, Dockerfile·CI 설정 검사에 같은 엔진이 쓰인다. 정책은 데이터 위의 규칙이고, 단위
시험(`conftest verify`)을 엔진이 직접 돌린다.

## Decision

1. **엔진.** 구조화된 설정(Compose, GitHub Actions 워크플로, 이후 다른 YAML/JSON 설정)의 저장소 정책은 Rego 로 쓰고 Conftest 로
   평가한다. 정책은 `tools/conftest/<입력 종류>/` 에 있고, 패키지는 입력 종류의 이름이다(`compose`, `compose_source`, `workflow` …).
2. **입력은 실제 파서의 출력이다.** 정책은 원본 텍스트를 다시 파싱하지 않는다.
   - Compose: `docker compose config` 의 JSON. 위 플래그로 env 파일은 읽지 않으므로 로컬 비밀이 모델에 들어오지 않는다. 경로는 쓴
     그대로 남는다.
   - 워크플로: Conftest 의 YAML 파서가 읽은 문서.
   - 정규화가 숨기는 것은 원본에 대한 별도 패키지가 본다. 예: 원격 `include:` 는 정규화가 내려받아 합쳐 버리므로 `compose_source`
     가 본다.
   - 손 파서의 YAML 문법 금지는 옮기지 않는다. 파서의 사각이었지 사고가 아니었다.
3. **엔진과 파서는 둘 다 고정한다.** Conftest(`tools/conftest.env`)와 정규화에 쓰는 Docker Compose(`tools/docker-compose.env`)는
   플랫폼마다 체크섬을 가진 단일 릴리스이고, ADR-0197 의 하나뿐인 내려받기(`scripts/ci/fetch_pinned.py`)로만 받는다.
   - 러너나 노트북에 깔린 판을 쓰지 않는다. 판이 결과를 바꾸는 것을 측정했다(Context).
   - 판을 올리는 것은 리뷰되는 고정값 변경이다.
4. **가드를 옮기는 방법.** 손 파서 가드를 Rego 로 옮기는 PR 은 다음을 모두 지킨다.
   - 옮긴 규칙마다 ADR-0001 의 사고를 정책 파일 머리에 적는다.
   - Rego 단위 시험(`*_test.rego`)이 규칙마다 사고를 심은 입력 하나와 허용되는 모양 하나를 가진다. 그 가드의 셸 자기 시험이
     `conftest verify` 를 먼저 돌린다.
   - 셸 자기 시험은 실제 파서와 실제 엔진을 거치는 끝에서 끝 사례를 유지한다. 옛 자기 시험의 사례는 그대로 통과하거나, 바뀐
     이유를 PR 에 적는다. 바뀌는 사례는 앵커가 허용되는 대신 앵커 뒤의 가변 이미지가 거부되는 것처럼, 문법 금지를 그 문법이 숨기던
     사고로 바꾼 것이다.
   - 규칙을 약하게 바꾸는 변이를 한 번 돌려 시험이 실패하는 것을 PR 에 적는다.
   - 실제 트리 전체가 통과한다.
   - 옛 가드가 그 사고를 잡던 입력은 새 가드도 잡는다. 범위가 좁아지면 PR 에 이유와 함께 적는다.
5. **줄 수가 늘어도 되는 경우.** 옮긴 결과 총 줄 수(정책 + Rego 시험 + 남은 스크립트)가 옛 가드보다 늘 수 있다. 다음 중 하나를
   얻을 때만 받아들이고, 어느 것인지 PR 의 전후 표에 적는다.
   - (a) 손 파서를 실제 파서로 바꾼다.
   - (b) 옛 가드가 놓치던 사고 입력을 잡는다.

   #1090 이 둘 다 했다. 파이썬은 710→553줄, Rego 187줄 + Rego 시험 165줄로 총 줄 수가 늘었다. 대신 YAML 부분집합 파서가
   `docker compose config` 로 바뀌었고, 목록형 `environment` 의 평문 비밀을 이제 잡는다. 둘 중 아무것도 얻지 못하면서 줄만 늘어나는
   이전은 하지 않는다.
6. **역할 나눔.** 저장소 파일 하나의 구조(키·값·모양)는 Rego 다. 코드의 모양(호출·import·문자열)은 Semgrep 규칙
   (`tools/semgrep/rules/`, #1060)이다. 워크플로의 일반 보안 감사(고정 SHA·이미지 digest·자격 유지·위험한 트리거)는 zizmor(#1061)다.
   셋이 같은 검사를 두 번 하지 않는다. 한 규칙이 다른 저장소 파일(계약 JSON, 영역 목록)의 값을 필요로 하면 그 값은
   `--data` 로 넘긴다. 정책에 복사하지 않는다.

## Consequences

- 첫 적용은 #1090(G026 의 Compose 절반)이다. 다음은 G061 의 워크플로 계약(`check-workflow-policy.sh` 에 남은 정확한 권한 모양·
  terminal `required/*` 작업·영역 working-directory·Action 허용 목록)이다. 그 뒤 lane freeze 가 풀리면
  `every-container-has-a-memory-cap`·`batch-containers-are-run-scoped` 를 옮긴다.
- 가드가 고정 바이너리 둘(Conftest, Compose)을 더 받는다. 고정 캐시가 있으면 네트워크를 쓰지 않는다(ADR-0197).
- Compose 정규화는 파일마다 수 초가 걸려 병렬로 돈다. #1090 측정은 로컬 Windows 에서 트리 전체가 직렬 70s → 병렬 14s 였다.
  해당 가드의 change-scope 예산은 30s 다.
- Rego 를 읽고 쓸 줄 알아야 한다. 대신 정책과 시험이 같은 언어이고, 엔진이 시험을 돌린다.
- 남은 손 파서는 인벤토리 A-3 의 행으로 추적한다(끝난 행은 `끝남(#PR)`, 절반은 `절반(…)`).
