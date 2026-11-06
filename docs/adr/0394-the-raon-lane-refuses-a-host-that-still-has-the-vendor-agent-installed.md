# ADR 0394: RAON 레인은 제조사 에이전트가 직접 깔린 호스트를 거부한다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0170](./0170-large-vworld-files-come-through-the-raon-agent-on-the-data-host.md)(RAON 에이전트는 데이터 호스트의 image 안에서), [ADR-0001](./0001-monorepo-governance-and-conventions.md)(가드는 막을 실제 사고를 적는다)
- Amends: `config/provider-agent-packages.contract.json` 의 `raonk-2018` 항목에 `host_install_forbidden` 을 더한다. `scripts/ops/raon-large-files.sh` 의 전제 확인이 그것을 읽는다.

## Context

ADR-0170 은 RAON K 에이전트를 데이터 호스트의 `raon-batch` image 안에서만 돌린다. image 는 제조사 패키지를
계약의 주소에서 받아 sha256 으로 확인한 뒤 설치한다. 그런데 2026년 7월, 에이전트를 처음 시험할 때 같은 패키지를
호스트 자체에도 손으로 깔았고, 그것이 남아 있다. 2026-11-03 읽기 전용으로 확인했다(ai-server):

| 확인 | 값 |
|---|---|
| 패키지 | `raonk-2018` 2018.2.8 `install ok installed`, `/opt/raonk-2018` |
| cron | 패키지 postinst 가 root crontab 에 매분 `service.sh` 와 `@reboot` 를 넣는다. journal 의 `CRON[...] (root) CMD (/opt/raonk-2018/scripts/service.sh)` 가 매분 |
| 하는 일 | `service.sh` 가 5초마다 12번 `raonk.sh` 를 부르고, `raonk.sh` 는 tty1 사용자로 `su -` 해 화면 없는 Electron 앱을 띄운다. 매번 `cannot open display: :tty1` 로 끝난다 |
| 권한 | `/opt/raonk-2018`·`scripts/service.sh`·`scripts/raonk.sh` 가 `rwxrwxrwx root` 이고, `chrome-sandbox` 는 setuid root |
| journal (24시간) | apparmor `userns_create` 감사 17,281줄, `pam_unix(su-l:session)` 34,562줄, `raonk` 18,720줄. journal 은 2.0G |
| 레인이 쓰는가 | 아니다. 저장소 어디에도 `/opt/raonk-2018` 이 없고, image 가 같은 바이트를 따로 받는다 |

root 의 cron 이 누구나 쓸 수 있는 스크립트를 매분 실행한다. 이 호스트의 어느 계정이든, 탈취된 컨테이너가 쓰는
계정이든, `raonk.sh` 한 줄을 고치면 1분 안에 root 가 된다. 이것은 로그 소음보다 큰 문제다.

## Decision

1. **호스트 사본은 지운다(사장, 한 번).** 명령은 [제공기관 수집 런북](../../platforms/foundation-platform/docs/runbooks/provider-acquisition-fargate.md)
   "호스트에 직접 깔린 제조사 패키지 지우기" 에 있다. PowerShell·cmd 에 그대로 붙일 수 있게 달러 기호를 쓰지 않는다.
   패키지의 prerm 이 root crontab 의 줄을 지운다. 지운 뒤 crontab·디렉터리·프로세스를 확인하는 세 줄이 따른다.
   에이전트는 sudo 가 필요하므로 이 일을 하지 않는다.
2. **계약이 금지 경로를 적는다.** `packages.raonk-2018.host_install_forbidden.paths` 가 `/opt/raonk-2018` 을
   적는다. 패키지의 주소·sha256 이 있는 곳과 같은 곳이다(ADR-0170 §4, 고정값은 한 곳).
3. **레인이 거부한다.** `raon-large-files.sh` 는 `build`·`plan`·`run` 모두 전제 확인에서 그 경로 하나라도 있으면,
   무엇도 쓰기 전에 78(EX_CONFIG)로 끝난다. 계약에 목록이 없거나 비어 있어도 "금지 없음" 으로 읽지 않고 거부한다.
   `orchestration/tests/test_raon_large_files_command.py` 가 합성 호스트(그 경로를 만든 임시 디렉터리)로 거부를,
   지운 뒤 통과를, 목록이 빠진 계약의 거부를 증명한다.

대기업 방식과 같다: CIS Benchmark 의 "cron 이 실행하는 파일은 root 만 쓸 수 있다"(Ubuntu 5.1.x)와, 쓰지 않는
패키지는 호스트에서 지운다는 최소 설치 원칙. 레인이 전제 확인에서 거부하는 것은 Kubernetes 의 node readiness
gate(조건이 맞지 않는 노드에 일을 보내지 않는다)와 같은 모양이다.

## Consequences

- 사장이 지우기 전에는 RAON 레인이 데이터 호스트에서 돌지 않는다. 지금 레인은 카탈로그 상한 0(꺼짐)이라
  매일 훑기는 스크립트를 부르지 않는다. 운영자가 첫 감독 실행을 하면 그때 거부가 보인다.
- 다른 호스트에 같은 패키지를 깔면 같은 가드에 걸린다.
- 가드는 레인이 돌 때만 본다. 레인을 쓰지 않는 동안 호스트에 남은 구멍은 이 가드가 알리지 않는다. 그래서 1항의
  삭제가 먼저다.

## Alternatives

- **가드 없이 지우기만** — 다음에 누군가 시험하려고 다시 깔면 같은 구멍이 조용히 돌아온다. 기각.
- **호스트 사본을 고쳐 쓴다(권한만 755)** — 레인이 쓰지 않는 패키지를 남길 이유가 없고, 매분 cron 과 setuid
  `chrome-sandbox` 는 남는다. 기각.
- **service-checks 로 매시간 호스트를 본다** — 금지 경로가 하나뿐이고 지운 뒤에는 늘 비어 있다. 레인 전제 확인으로
  충분하다고 보고 미룬다. 다시 생기면 그때 더한다.
