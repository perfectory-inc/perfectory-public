# ADR 0181: 허가된 운영자 스크립트가 RAON 대용량 레인의 감독 실행도 맡고, 도는 oneshot 작업을 도는 것으로 본다

- Status: Accepted
- Date: 2026-10-10
- Builds on: [ADR-0161](./0161-pack-cutover-root-steps-run-through-one-granted-script.md)(허가된 운영자 스크립트),
  [ADR-0170](./0170-large-vworld-files-come-through-the-raon-agent-on-the-data-host.md)(RAON 대용량 레인),
  [ADR-0178](./0178-the-granted-operator-script-also-drives-the-silver-refresh-lanes.md)(Silver 레인)

## Context

VWorld 대용량 파일(약 500MB 초과, 2026-10-10 기준 14개·12.7GB)은 RAON 레인으로만 받는다(ADR-0170). 그 레인의
상한은 0(꺼짐)이고, 상한을 정하려면 운영자가 `sudo systemd-run ... raon-large-files.sh build|plan|run` 으로 한 번
지켜봐야 한다(런북 provider-acquisition-fargate.md). 운영자 계정은 그 명령에 비밀번호 없는 sudo 가 없어서, 이
감독 실행은 사람이 비밀번호를 치고 출력을 대화창에 붙여야만 돌았다. ADR-0178 이 Silver 레인에서 없앤 문제와 같다.

같은 날 그 스크립트를 읽다가 결함을 찾았다: `refuse_while_active` 가 `systemctl is-active` 로 "도는 중"을 물었다.
oneshot 작업(Gold 재생성, Silver 레인, 굽기)은 도는 내내 `activating` 이고 `is-active` 는 그것을 "멈춤"이라고 답한다.
그래서 "Gold 재생성이 도는 동안 묶음 굽기를 시작하지 않는다"(ADR-0166)는 실제로는 막지 못했다.

## Decision

1. 그 스크립트에 `raon` 을 더한다.
   - `raon build`, `raon plan`: 서비스 계정으로 `raon-large-files.sh build`, `plan <매일 훑기 증거>` 를 돌리고 기다린다
     (환경은 runtime-secrets 계약의 `raon-large-files`). 아무것도 받지 않는다.
   - `raon run <max-files|all> <new-bytes-budget>`: 같은 스크립트의 `run` 을 분리된 유닛
     `foundation-raon-large-files-operator.service` 로 시작한다. 파일 수(1–9999 또는 all)와 바이트 상한(양의 정수)은
     그 실행 하나의 환경으로만 간다. 카탈로그의 하루 상한은 바뀌지 않는다.
   - `raon status`: 그 유닛의 상태와 journal 끝.
   - 증거 경로는 고정이다. 매일 훑기(`foundation-source-sweep.service`)나 이 유닛이 도는 동안에는 75 로 거부한다. 그 밖의
     동작·인자는 64 로 거부한다.
2. "도는 중"은 `list-units --state=active,activating` 으로 묻는다. 스크립트의 모든 거부가 이 하나를 쓴다.
3. sudoers 허가는 바뀌지 않는다(같은 스크립트 경로).

## Consequences

- RAON 첫 감독 실행과 그 뒤 측정한 상한의 PR 을, 비밀번호 없이 운영자 계정에서 끝낼 수 있다.
- Gold 재생성·레인·굽기가 도는 동안의 거부가 실제로 동작한다. 시험(`test_by_pnu_pack_operator`)의 가짜 systemctl 은
  `activating` 을 따로 흉내 내고, 도는 Gold 재생성 위에서 굽기가 거부되는 것을 확인한다.
