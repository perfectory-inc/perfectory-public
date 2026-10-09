# ADR 0161: 묶음 전환의 루트 단계는 허용된 스크립트 하나로 실행한다

- Status: Accepted
- Date: 2026-10-08
- Builds on: [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)(정확한 경로만 허용),
  [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(환경 파일은 계약 하나),
  [ADR-0160](./0160-one-by-pnu-gateway-source-serves-both-lanes.md)(레인 인자)

## Context

필지 묶음 전환(ADR-0147 §8)의 운영 단계 가운데 관문 (가)·(나), 발행, 단계 배포 한 단계의 판정, 감시 표본 파일
쓰기는 모두 서버 루트가 필요하다. 이 단계들이 root:root 0600 환경 파일(Cloudflare 분석 토큰, R2 쓰기 키)을
읽고, 서비스 사용자로 유닛을 띄우기 때문이다.

- 건물 전환(2026-10-05~07) 때는 docker 그룹을 거쳐 루트 셸을 얻었다. 이 방법은 무엇이든 실행할 수 있다.
- 그 대신 사람이 단계마다 `ssh -t ... sudo` 로 비밀번호를 넣고 실행하면, 단계 배포 판정처럼 15분마다 도는
  단계는 진행이 사람을 기다린다.
- 같은 문제를 배포는 이미 풀었다(ADR-0134 §2): sudo 는 제어 체크아웃의 `foundation-release.sh` 한 경로만
  비밀번호 없이 허용하고, 그 스크립트는 정해진 부속 명령만 받는다.
- 사례: 운영 절차를 미리 정한 문서로 묶어 그 문서만 실행 권한을 주는 방식(AWS Systems Manager 문서,
  Rundeck 작업)은 루트 셸을 주지 않고 반복 작업을 맡기는 일반적인 방법이다.

## Decision

1. **스크립트 하나.** `scripts/ops/by-pnu-pack-operator.sh <building|parcel> <동작> <세대 | 버전>` 이 묶음
   전환의 루트 단계를 맡는다. 동작은 `equality`, `latency`, `publish`, `monitor-sample`, `health`, `status` 뿐이다.
2. **경로나 명령을 받지 않는다.** 인자는 레인, 동작, 세대 번호(1~9999), Worker 버전 id(UUID)뿐이다. 이 밖의
   인자는 아무것도 실행하기 전에 거부한다(종료 코드 64). 작업 위치는 레인과 세대로 정해진다
   (`/data/foundation-platform/by-pnu-bake/<레인>-pack-g<세대>`).
3. **실행하는 것은 승인된 릴리스뿐이다.** 발행기는 `admitted-writer-runtime.sh --current` 가 고른 현재 릴리스의
   바이너리다. 서비스 사용자로, 임시 유닛 안에서 돈다. 환경 파일은 runtime-secrets 계약이 그 실행
   (`section-pack-operator`, `section-pack-latency-probe`)에 정한 것만 쓴다.
4. **증거는 덮어쓰지 않는다.** `latency` 는 앞선 `latency.json` 을 시각을 붙여 보관한 뒤 새로 잰다.
   `publish` 는 두 관문의 증거가 모두 `passed` 일 때만 시작한다.
5. **허용은 한 경로다.** `foundation-release.sh operator-access <계정>` 이
   `/etc/sudoers.d/foundation-operator` 에 제어 체크아웃의 이 스크립트 경로 하나만 비밀번호 없이 허용한다.
   제어 체크아웃은 자동 배포(ADR-0159)가 검사를 통과한 main 으로만 옮기므로, 병합되지 않은 코드는 이
   경로에 닿지 않는다.
6. **시험.** `orchestration/tests/test_by_pnu_pack_operator.py` 가 다음을 확인한다. 잘못된 인자(레인 오타, `;`,
   `../`, 0, 없는 동작, 버전이 아닌 값)는 아무것도 실행하지 않고 거부된다. 관문 (나)는 그 레인의 미리보기
   주소와 계약의 환경 파일을 쓴다. 발행은 두 관문이 통과하기 전에는 거부된다. 세대 검사를 지우면 이 시험이
   실패하는 것을 확인했다(2026-10-08).

## Consequences

- 운영자 계정은 묶음 전환의 정해진 단계를 비밀번호 없이 실행한다. 단계 배포 판정
  (`CANARY_HEALTH_COMMAND`)도 이 스크립트를 부른다.
- 이 허용은 서버 보안을 조금 넓힌다. 그 대가로 docker 그룹을 통한 루트 셸을 쓸 이유가 사라진다. 운영자
  계정의 docker 그룹 탈퇴는 별도 결정으로 남긴다.
- 첫 설치는 사람이 한 번 실행한다: `sudo .../foundation-release.sh operator-access <계정>`.
- 묶음 굽기(1세대)는 아직 이 스크립트에 없다. 이후 세대는 예약 굽기(`by-pnu-serving-bake.sh`)가 굽는다.

---

2026-10-09 개정 주: 동작 `bake`(묶음 세대 굽기)와 `gold-rebuild`(무조건 패널 Gold 재생성)가
[ADR-0166](./0166-pack-generation-bake-and-gold-rebuild-run-through-the-operator-script.md) 으로 더해졌다.
