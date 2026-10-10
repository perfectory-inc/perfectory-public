# ADR 0178: 허가된 운영자 스크립트가 Silver 레인의 계획·시작·상태도 맡는다

- Status: Accepted
- Date: 2026-10-10
- Builds on: [ADR-0161](./0161-pack-cutover-root-steps-run-through-one-granted-script.md)(허가된 운영자 스크립트),
  [ADR-0169](./0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md)(Silver 레인)

## Context

Silver 레인 12개(ADR-0169)는 켜기 전에 레인마다 계획(`silver-refresh.sh <레인> --plan`)과 감독된 첫 실행이 필요하다.
둘 다 root 가 읽는 환경 파일이 있어야 해서 `sudo systemd-run` 으로만 돌았다. 2026-10-10 운영자(사장)는 sudo 비밀번호를
치고 화면 출력을 대화창에 붙여야 했다: 비밀번호가 틀려 레인마다 조용히 실패했고, 결과는 실행한 화면에만 남았다.

필지 묶음 전환은 같은 문제를 ADR-0161 로 풀었다: sudoers 가 운영자 계정에 스크립트 하나
(`scripts/ops/by-pnu-pack-operator.sh`, 제어 체크아웃 경로)만 비밀번호 없이 허락하고, 그 스크립트는 정해진 동작과 인자만
받는다.

## Decision

1. 그 스크립트에 `silver` 를 더한다: `by-pnu-pack-operator.sh silver plan|start|status <레인>`.
   - `plan`: 서비스 계정으로 `silver-refresh.sh <레인> --plan` 을 돌리고(환경은 runtime-secrets 계약의
     `silver-refresh-plan`), 결과를 호출자에게 그대로 보인다. 아무것도 쓰지 않는다.
   - `start`: 그 레인의 예약 유닛 `foundation-silver-refresh@<레인>.service` 를 한 번 시작한다(감독된 첫 실행).
     다른 Silver 레인이나 Gold 재생성이 돌고 있으면 75 로 거부한다.
   - `status`: 그 유닛의 상태와 journal 끝 40 줄.
2. 레인 목록은 다시 적지 않는다: `orchestration/jobs.v1.json` 에서 템플릿 유닛을 쓰는 작업이 레인이다. 그 밖의 이름, 다른
   동작, 남는 인자는 64 로 거부한다.
3. sudoers 허가는 바뀌지 않는다(같은 스크립트 경로). 새 설치 단계가 없다.

## Consequences

- 운영자 계정(과 그 계정으로 일하는 AI 작업자)이 비밀번호와 화면 붙여넣기 없이 레인 계획·첫 실행·상태를 본다.
- 허가 범위가 그만큼 넓어진다: 12개 레인의 계획(읽기만)과 예약된 유닛 시작. 유닛 정의와 환경은 릴리스가 정하고 인자로
  바꿀 수 없다.
- 시험: `orchestration/tests/test_by_pnu_pack_operator.py` 의 silver 시험 넷(계획 환경, 유닛 이름, 동시 실행 거부,
  목록 밖 이름 거부). 목록 검사를 빼면 거부 시험이 실패한다.
