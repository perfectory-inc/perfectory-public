# ADR 0153: 운영 환경 파일과 그것을 읽는 단위·실행은 계약 하나가 정하고, 나머지는 거기서 만들거나 그것으로 검사한다

- Status: Accepted
- Date: 2026-10-06
- Related: [ADR-0152](./0152-bronze-keys-of-reused-provider-file-numbers-name-their-bytes.md), [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md) (작업 = systemd 단위), [ADR-0071](./0071-a-deploy-that-leaves-the-schema-behind-has-not-finished.md) (배포의 환경 확인)

## Context

이틀 사이 같은 결함이 두 번 났다.

- 2026-10-04 건물 묶음 측정 스크립트가 Gold 읽기에 필요한 키 쌍을 잃고 첫 샤드에서 실패했다.
- 2026-10-05 필지고유번호변동연혁 단위의 스크립트가 읽기 키를 요구했지만 단위의 환경 파일 어디에도 없었다(ADR-0152).

어느 파일이 어떤 이름을 담는지, 단위가 어느 파일을 읽는지, 스크립트가 무엇을 요구하는지, 런북의 `systemd-run` 이 어느
파일을 넘기는지가 각각 손으로 쓴 목록이었다. 파일 경로는 단위 17개 줄·런북·스크립트 주석·`r2-connections` 계약
(`cloudflare_analytics.env_file`, 코드는 읽지 않음)에 흩어져 있었다.

## Decision

1. **정본은 `platforms/foundation-platform/config/runtime-secrets.contract.json` 하나다.** 이름만, 값은 없다.
   - 그룹 = 호스트 환경 파일: 경로, 소유자·그룹, 가장 넓은 허용 모드, 담는 이름. 이름은 그것을 읽는 코드가 읽는 곳을
     가리킬 수 있다(`@<파일>`, `@<파일.json>#/<포인터>`) — 복사하지 않는다.
   - 소비자 = 단위 또는 운영 실행: `needs`(이름 → 그것을 대는 그룹), 아직 나열하지 못한 바이너리 설정 때문에 읽는
     그룹(`unenumerated`, 이유 필수), 선택 그룹.
   - 최소 권한: 읽기 키는 `lakehouse-reader` 그룹(`/etc/foundation-platform/lakehouse-reader.env`, root 0600)에만
     있다. 쓰기 키와 같은 파일에 두지 않는다.
2. **만든다.** 단위의 환경 파일 줄은 계약에서 렌더한다(`scripts/deploy/runtime_secrets.py render`). 런북의
   `systemd-run` 은 `runtime_secrets.py properties <실행>` 으로 인자를 받는다.
3. **유도해서 검사한다.** `runtime_secrets.py check`(시험 `orchestration/tests/test_runtime_secrets_contract.py`,
   가드 `scripts/guard/runtime-secrets-contract.sh`)가 거부한다: 계약과 다른 단위의 환경 파일 줄; 단위·실행의
   스크립트(와 그것이 source 하는 것)가 요구하는 이름(`${NAME:?}`, `required_env=(...)`, Python
   `os.environ["NAME"]`) 중 계약이 그 소비자에게 대지 않는 것; 뒤에 읽히는 그룹이 같은 이름을 덮어쓰는 `needs`;
   계약에 없는 단위; 런북·스크립트에 손으로 쓴 환경 파일 경로. 가드가 막는 사고: 2026-10-05 처럼 스크립트가 요구하는
   키를 단위가 주지 않아 실행이 부작용 뒤에 멈추는 것.
4. **배포에서 호스트를 확인한다.** `foundation-release.sh` 의 `verify`(install 포함)와 `timers` 는 단위가 읽는 모든
   그룹 파일이 있고, 소유자가 맞고, 모드가 계약보다 넓지 않고, 선언한 이름을 다 담는지 root 로 확인하고(`host`),
   아니면 65 로 거부한다. 이름만 출력한다.
5. **시험의 가짜도 계약을 따른다.** 가짜 퍼블리셔는 계약이 그 소비자에게 준 이름이 없으면 실패하고, 시험의 환경도
   계약에서 만든다. 시험마다 따로 쓴 요구 목록은 없앴다.

## Consequences

- 단위 14개 전부와 운영 실행 셋(건물 묶음 측정, 법정동 짝 검증, 묶음 지연 측정)이 계약에 있다. 바이너리(퍼블리셔·Spark)가
  읽는 설정은 아직 이름으로 나열하지 않았고 `unenumerated` 에 이유와 함께 남는다 — 그 그룹 안 이름의 누락은 배포
  확인이 못 본다. 나열은 바이너리가 자기 요구를 출력하게 하는 후속이다.
- 계약에 없는 수동 레시피(손으로 환경 파일에서 이름을 골라 셸에 싣는 것: 연속지적도 판 적재 런북, 행정경계 변환
  스크립트)는 이 검사 밖이다.
- 계약의 이름이 호스트와 다르면 다음 배포가 멈춘다. 그 전에 운영자가 `runtime_secrets.py host` 로 확인한다.
