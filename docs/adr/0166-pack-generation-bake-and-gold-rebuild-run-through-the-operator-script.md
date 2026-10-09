# ADR 0166: 묶음 세대 굽기와 무조건 Gold 재생성도 허용된 운영 스크립트로 실행한다

- Status: Accepted
- Date: 2026-10-09
- Amends: [ADR-0161](./0161-pack-cutover-root-steps-run-through-one-granted-script.md) §1(동작 목록)
- Builds on: [ADR-0139](./0139-panel-gold-is-rebuilt-when-a-silver-input-changes.md)(패널 Gold 재생성),
  [ADR-0141](./0141-by-pnu-serving-publishes-changed-documents-as-patch-generations.md)(예약 굽기의 샤드),
  [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md) §8(필지 전환 순서),
  [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(환경 파일은 계약 하나)

## Context

ADR-0161 의 운영 스크립트(`by-pnu-pack-operator.sh`)는 관문 (가)·(나), 발행, 감시 표본, 단계 판정만 맡는다.
묶음 세대 하나를 만드는 데 남은 루트 단계는 둘이다.

- **세대 굽기.** 2026-10-08 필지 묶음 세대 굽기는 사람이 루트로 임시 유닛을 띄워 손 스크립트를 돌렸다. 필지 레인
  잠금을 잡고, PNU 앞자리 샤드마다 `export-parcel-by-pnu-section-packs` 를 4개씩 동시에, 샤드마다 3번까지
  시도했다(R2 본문 읽기가 일시적으로 실패했다). 첫 샤드의 Gold 스냅숏을 나머지에 `…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID`
  로 고정했다. 샤드(2~5자리)는 손으로 골랐다. 수출기는 샤드가 900,000행을 넘으면 거부한다.
- **무조건 Gold 재생성.** 묶음은 굽기 직전 Gold 에서 나와야 한다. 예약 재생성은 Silver 입력이 바뀔 때만
  다시 만든다. 그래서 `gold-panel-rebuild.sh all --unconditional "<이유>"` 를 루트로 손으로 돌렸다.
- 예약 굽기(`by-pnu-serving-bake.sh`)에는 이미 샤드 규칙이 있다: 1..9 에서 시작해 행 상한에 걸린 샤드를 한 자리
  긴 열 개로 나눈다. 손 스크립트는 이 규칙을 따로 다시 썼다. 같은 규칙이 두 곳에 있으면 한쪽만 바뀐다.
- 재생성 스크립트는 systemd 단위가 하나라는 사실 말고는 동시 실행을 막지 않는다. 같은 일을 다른 이름의 유닛으로
  돌리면 두 재생성이 각자 compose `spark` 상한(20g)을 쓰며 함께 돌 수 있다.

## Decision

1. **동작 둘을 더한다.** `by-pnu-pack-operator.sh <레인> bake <세대>` 와 `by-pnu-pack-operator.sh <레인> gold-rebuild`.
   인자 규칙은 ADR-0161 §2 그대로다. 세대는 1~9999 이고, `gold-rebuild` 는 레인 뒤에 아무것도 받지 않는다(이유
   문자열도 받지 않는다). 생성 번호 동작은 인자를 정확히 하나만 받는다. 그 밖은 실행 전에 64 로 거부한다.
2. **굽기는 저장소 스크립트 하나다.** `bake` 는 현재 릴리스의 `scripts/ops/by-pnu-pack-bake.sh <레인> <세대>` 를
   임시 유닛(`foundation-<레인>-pack-bake-g<세대>`)에서 서비스 사용자로 돌린다. 출력은 `<작업>/logs/bake.log`, 환경
   파일은 runtime-secrets 계약의 실행 `section-pack-bake` 다. 스크립트는:
   - 예약 굽기와 같은 레인 잠금(`…/by-pnu-bake/<레인>/lane.lock`)을 기다리지 않고 잡는다(잡혀 있으면 75).
   - 샤드 규칙은 `scripts/ops/by-pnu-bake-shards.sh` 하나다. 예약 굽기·측정 스크립트도 이것을 읽는다: 첫 계획 1..9,
     수출기 거부 문구(`shard the run with`)로 행 상한을 알아보기, 열 자식으로 나누기, 런타임 DB 주소 찾기, 완료 판정.
     나눈 샤드는 `shards/shard-<앞자리>.split` 에 남겨 다시 돌릴 때 바로 자식으로 간다.
   - 요약이 있는 샤드는 건너뛴다(이어 굽기).
   - 첫 샤드는 혼자, 고정 없이 돈다. 그 요약의 스냅숏이 나머지 샤드를 고정한다. 다시 돌릴 때는 이미 있는 요약에서
     고정한다. 스냅숏이 둘인 요약 모음은 이어 굽지 않는다(65).
   - 동시 샤드 수와 시도 수는 `by-pnu-bake-shards.sh` 의 `BY_PNU_PACK_BAKE_WORKERS`(4)·`BY_PNU_PACK_BAKE_ATTEMPTS`(3)
     한 곳이다. 시도 사이 대기는 예약 굽기의 `FOUNDATION_BY_PNU_BAKE_RETRY_SECONDS` 다.
   - 끝나면 예약 굽기의 완료 판정(겹침 없음, 한 스냅숏, 모든 Gold 행)을 통과해야 성공이다. **발행하지 않는다.**
     관문과 발행은 ADR-0161 의 동작으로 따로 실행한다.
   - 유닛의 `MemoryMax` 는 예약 굽기 유닛의 `MemoryMax`(샤드 하나 수출의 측정 상한) × 동시 샤드 수다.
     `OOMPolicy=continue` 라 한 샤드가 죽으면 그 샤드만 재시도된다.
3. **무조건 재생성은 예약 유닛의 복사본이다.** `gold-rebuild` 는 현재 릴리스의
   `infra/systemd/foundation-gold-panel-rebuild.service` 를 읽어 그 설정(계정, 환경 파일, 시간 상한, `ExecStopPost`
   정리, 샌드박스, 상태 디렉터리, 실패 알림)을 그대로 임시 유닛 `foundation-gold-panel-rebuild-unconditional` 에
   옮긴다. 명령은 그 유닛의 `gold-panel-rebuild.sh all` 에 `--unconditional "<고정 이유>"` 를 붙인 것이다. 이유는
   이 ADR 과 동작을 적은 고정 문자열이다. 유닛 파일이 그대로 옮길 수 없는 모양이면(다른 명령, 펼 수 없는 지정자)
   65 로 거부한다. 행 하한·품질 검사·판 기록은 재생성 스크립트가 그대로 한다.
4. **재생성은 하나씩.** `gold-panel-rebuild.sh` 가 상태 디렉터리의 `rebuild.lock` 을 기다리지 않고 잡는다(잡혀 있으면
   75). 예약 유닛, 운영자 복사본, 감독 실행 모두 같은 잠금이다. `all` 은 두 표 동안 잠금을 쥔다.
5. **서로 비켜 간다.** `gold-rebuild` 는 예약 재생성·자신의 복사본·예약 굽기·묶음 굽기 유닛이 돌거나 어느 레인
   잠금이든 잡혀 있으면 75 로 거부한다(잠금 파일은 확인만 하고 만들지 않는다). `bake` 는 예약 재생성이나 그 복사본이
   돌면 거부한다. 굽기 도중 Gold 가 움직이면 고정한 스냅숏 때문에 굽기가 멈추고, 그 세대는 다시 이어 구울 수 없다.
6. **`status <세대>`** 는 `bake` 유닛과 두 재생성 유닛, 예약 굽기 유닛의 상태와 재생성 로그 끝을 함께 보인다.
7. **시험.** `orchestration/tests/test_by_pnu_pack_operator.py`(새 동작의 인자 거부, 띄우는 유닛·환경 파일·메모리,
   서로 비켜 가기, 유닛 파일 복사), `test_by_pnu_pack_bake.py`(행 상한 나누기, 이어 굽기, 스냅숏 고정, 재시도,
   동시 수, 레인 잠금), `test_gold_panel_rebuild.py`(재생성 잠금). 2026-10-09 에 행 상한 문구 인식을 깨면 굽기 시험과
   예약 굽기 시험이, 고정을 지우면 굽기 시험이, 재생성 잠금을 지우면 재생성 시험이 실패하는 것을 확인했다.

## Consequences

- 묶음 세대 하나의 루트 단계(Gold 재생성 → 굽기 → 관문 (가)·(나) → 발행 → 감시 표본)가 모두 이 스크립트를 거친다.
  루트 셸이 필요한 단계가 남지 않는다.
- 동시 4샤드의 메모리는 측정하지 않았다. 상한은 측정된 샤드 하나 × 4(14G × 4 = 56G)로 62g 호스트에서 다른 Spark
  작업과 함께 들어가지 않는다. 굽기는 감독 실행처럼 예약 DAG 를 멈추고 돌린다(런북). 측정 뒤 동시 수를 바꾸는 것은
  `BY_PNU_PACK_BAKE_WORKERS` 한 줄이다.
- 예약 재생성이 이미 잠금을 쥔 동안 Airflow 가 예약 유닛을 시작하면 그 실행은 75 로 실패하고 다음 날 다시 계획한다.
- 이 스크립트는 자동 배포가 main 을 제어 체크아웃에 옮긴 뒤부터 쓸 수 있다(ADR-0161 §5). 허용(sudoers)은 바뀌지
  않는다.
