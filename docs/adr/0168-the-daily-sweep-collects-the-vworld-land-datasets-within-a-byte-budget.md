# ADR 0168: 매일 훑기는 VWorld 토지 데이터셋도 받고, 하루 새 바이트 예산 안에서만 받는다

- Status: Accepted
- Date: 2026-10-09
- Amends: [ADR-0077](./0077-the-pipe-looks-at-its-sources-every-day.md) §2(훑기 범위와 레인 목록의 자리),
  [ADR-0152](./0152-bronze-keys-of-reused-provider-file-numbers-name-their-bytes.md) §1(내용 주소 키의 256MiB 메모리 상한)과 Consequences(내용 주소 실행이 번호 키로
  건너뛰지 않음)
- Related: [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md)(같은 파일 번호의 새 판),
  [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(환경 파일 계약)

## Context

매일 훑기(`foundation-source-sweep.service`, 작업 `source_sweep`, `scripts/ops/daily-source-sweep.sh`)는 hub.go.kr 건축물대장
벌크만 받았다. Gold 패널을 먹이는 VWorld 토지 데이터셋 — 토지특성(NA/4), 개별공시지가(NA/6), 임야(NA/29), 토지이동이력(NA/13),
토지이용계획(NA/14), 대지권등록(NA/20), 용도지역지구코드(MK/30508) — 은 사람이 손으로 받을 때만 Bronze 에 들어왔다. 제공자가
새 판을 올려도 아무도 몰랐다. ADR-0077 이 막으려던 침묵이 이 데이터셋들에 그대로 남아 있었다.

받는 기계는 이미 있다: `plan-vworld-dataset-collection` → `inventory-vworld-dataset-files` → `ingest-vworld-dataset-files`
(원장 `catalog.bronze_object`, 단일 커미터). 붙이기 전에 코드에서 셋을 확인했다.

1. **첫 실행이 무엇을 받는가.** 건너뛰기는 같은 파일 번호의 최신 원장 행이 목록의 제공자 갱신일을 가질 때(`holds_listed_release`,
   ADR-0148)였고, 여기에 키 모양 조건(`holds_key_form`)이 더 있었다: 내용 주소 실행(`content_addressed`, ADR-0152)은 번호 키
   (`<ds>-<no>.zip`) 객체를 가진 것으로 치지 않았다. 지금까지 이 데이터셋들은 모두 번호 키로 받혔다. 같은 파일 번호에 새 판이
   올라오는 원천이라(ADR-0148) 매일 훑기는 내용 주소 키를 써야 하므로, 그대로 붙이면 **첫 실행이 이미 가진 파일을 전부 다시
   받는다** — 이 데이터셋들의 Bronze 는 시도 17개 × 판 3–12개(토지특성 AL_D195 51개, 임야 AL_D003 204개, ADR-0087·0088)를
   이미 갖고 있고, 목록에 오른 모든 판이 대상이다.
2. **목록에는 판이 여럿 있다.** 제공자 목록은 최신 판만이 아니라 과거 판 파일도 보인다. 무엇을 "새 것"으로 볼지 정해야 한다.
3. **목록과 예산이 있을 곳.** ADR-0077 §2 는 "레인 추가는 sweep 스크립트에 명령 한 줄"이라 했지만, 데이터셋 일곱 개의 목록을
   스크립트에 적으면 엔드포인트 카탈로그와 같은 사실이 두 곳에 산다. 기존 두 VWorld 수집 스크립트는 카탈로그를 잘라 내고 요약
   CSV 를 지어내는 파이썬 도우미를 따로 갖고 있다.

## Decision

1. **훑는 데이터셋 목록은 엔드포인트 카탈로그 하나다.** `docs/catalog/public-source-endpoint-catalog.v1.json` 의
   `vworld_dataset` 엔드포인트가 `"daily_collection": "source_sweep"` 을 가지면 매일 훑기가 받는다. 같은 파일의
   `daily_collections.source_sweep` 이 그 수집을 선언하고 하루 예산 `new_bytes_budget`(바이트)을 담는다. 선언되지 않은
   이름을 가진 엔드포인트는 계획을 막고(`blocked`), 선언되지 않은 수집을 고르면 계획이 실패한다 — 철자 하나로 데이터셋이 조용히
   빠지지 않는다. 스크립트에는 데이터셋 목록이 없다. 연속지적도(30563)와 필지고유번호변동연혁(30527)은 자기 작업이 받으므로
   표시하지 않는다(시험이 두 작업이 이름 대는 엔드포인트와 겹치지 않음을 본다).
2. **계획은 요약 CSV 없이 돈다.** `plan-vworld-dataset-collection` 은 `FOUNDATION_PLATFORM_VWORLD_DATASET_DAILY_COLLECTION`
   이 있으면 그 표시를 가진 엔드포인트만 계획하고, `FOUNDATION_PLATFORM_VWORLD_DATASET_INVENTORY_SUMMARY_PATH` 가 없으면
   요약을 읽지 않는다. 그때 작업은 `expected_counts_known: false` 를 달고, 목록 단계는 모르는 기대 수와 비교하지 않는다(어긋남
   경고 없음). 계획 보고서는 `daily_collection` 과 `new_bytes_budget` 을 옮겨 적는다. 선택자 없이 부르는 기존 호출자는 예전과
   같다.
3. **가진 것은 키 모양과 무관하다.** 같은 파일 번호의 최신 원장 행이 목록의 제공자 갱신일을 갖고, 원장이 그 바이트의 SHA-256
   (소문자 16진 64자)을 알면 가진 것이다(`holds_known_checksum`). `holds_key_form` 은 없앤다. 키 모양이 필요한 소비자는 키를
   스스로 검사한다 — 30527 넘김은 내용 주소 키가 아니면 거부하고(ADR-0152 §3) `FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1`
   로 받는다. 건너뛴 증거는 찾은 객체의 키를 있는 모양 그대로 적는다.
4. **새 것 = 목록에 있고 Bronze 가 갖지 않은 파일 전부.** 최신 판만 고르지 않는다 — Bronze 는 덧붙이기만 하고, 과거 판도 원천의
   사실이다. 대신 **하루 새 바이트 예산**을 둔다. `ingest-vworld-dataset-files` 는
   `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_NEW_BYTES_BUDGET` 이 있으면 live 실행에서 본문을 하나도 열기 전에 선택 파일마다
   가졌는지 묻고, 갖지 않은 파일의 목록 크기(`size_kib` × 1024) 합이 예산을 넘으면 **아무것도 받지 않는다**: 증거 상태
   `blocked_new_bytes_budget`, 그 파일들은 `deferred_new_bytes_budget`, 0 이 아닌 종료. 일부만 받고 나머지를 미루지 않는다 —
   부분 진척은 무엇이 남았는지를 매일 다르게 만든다. 첫 예산은 16GiB(17,179,869,184 바이트)다: 데이터셋 하나의 새 판(시도 17개)이
   하루에 들어오고, 밀린 판 여러 개는 들어오지 않는 크기다. 실측 한 달 뒤 조정한다.
5. **밀린 것은 운영자가 일부러 받는다.** 예산 초과는 슬랙 🔴 로 건수·바이트·예산을 말한다. 운영자는 계약의 run
   `source-sweep-vworld-backlog` 로 같은 스크립트를 한 번, `FOUNDATION_SOURCE_SWEEP_VWORLD_NEW_BYTES_BUDGET` 을 올려 돌린다
   (절차는 [VWorld 데이터 파일 Bronze 수집 런북](../../platforms/foundation-platform/docs/runbooks/vworld-dataset-file-bronze-ingest.md)의
   "VWorld 밀린 파일 받기"). journal 줄에 `budget_override=1` 이 남는다.
6. **매일 훑기의 VWorld 레인.** `daily-source-sweep.sh` 는 hub 레인 다음에 계획 → 목록 → 수집을 돈다: 내용 주소 키,
   RAON 선택 묶음(`SelectionArchive`) 제외·보류, 강제 재수집 없음, 예산은 계획이 카탈로그에서 옮긴 값. 증거는 상태
   디렉터리의 `vworld-evidence.json`, journal 한 줄에 두 레인이 함께(`hub ... | vworld ... deferred= pending_bytes= budget=
   status=`), 신규·실패는 같은 슬랙 메시지에 실린다. 한 레인이 실패해도 다른 레인은 돈다. 실행 시작에 지난 증거를 지워, 오늘 아무것도
   쓰지 못한 레인은 어제 증거가 아니라 실패로 읽힌다.
7. **부작용 전에 확인한다.** 스크립트 첫머리가 `required_env` 목록(DB 비밀번호, Bronze 쓰기 설정)과 VWorld 로그인을 확인하고,
   없으면 상태 디렉터리도 만들기 전에 78 로 끝난다. 로그인 이름은 정식 이름과 폐기 예정 별칭이 있으므로 스크립트가 이름을 적지
   않고 `config/environment-variable-naming.contract.json` 을 읽어 확인한다. 값은 어디에도 찍지 않는다. 런타임 비밀 계약은
   단위 `foundation-source-sweep.service` 의 recovery 사용처에 VWorld 로그인을 적고, 운영자 run 을 더한다. 로그인 이름을
   recovery 그룹의 `holds` 로 올리지 않는 것은 서버 파일이 정식 이름과 별칭 중 무엇을 쓰는지 저장소가 모르기 때문이다 — 올리면
   배포 시 호스트 검사가 틀린 이름으로 배포를 막는다.
8. **Silver 반영은 여전히 사람이다(ADR-0077 §5).** 이 결정은 Bronze 착지까지다.
9. **내용 주소 키의 본문은 메모리가 아니라 디스크 스풀에 받는다(ADR-0152 의 256MiB 상한을 없앤다).** 운영 원장 실측
   (`catalog.bronze_object`, 읽기 전용): 256MiB 를 넘는 파일이 토지이용계획 96개(최대 1,524MiB), 개별공시지가 16개(최대
   1,369MiB), 토지이동이력 4개(최대 367MiB)다 — 상한이 있으면 이 데이터셋들의 새 판은 매일 실패한다. 수집기
   (`content_spool`)는 본문을 선언된 스풀 디렉터리(`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR`, `content_addressed`
   에 필수)의 임시 파일에 쓰며 SHA-256 을 재고, 키를 정한 뒤 그 파일에서 올린다(저장소 어댑터가 크기로 멀티파트를 고른다).
   같은 내용 키에 같은 바이트가 있으면 쓰지 않는다(객체 잠금·덮어쓰기 금지 의미 그대로). 임시 파일은 그 본문을 가진 마지막
   소유자가 사라질 때 지워진다 — 성공·실패·업로드 전 거부 모두. 본문을 읽기 전에 선언된 길이를 스풀 파일시스템의 여유
   공간과 비교하되, **진행 중인 파일의 예약을 모두 더하고** 2GiB 를 남긴다(`MAX_IN_FLIGHT` 4 × 1.5GiB ≈ 6GiB 정점). 모자라면
   그 파일은 본문을 읽지 않고 실패한다. 매일 훑기의 스풀은 기본값이 데이터 디스크의
   `/data/foundation-platform/source-sweep/spool`(설정 `FOUNDATION_SOURCE_SWEEP_SPOOL_DIR`)이다: 상태 디렉터리
   `/var/lib/foundation-platform` 은 작은 루트 디스크에 있고 두 번 0 까지 찼다(2026-10-02). 단위의 `ReadWritePaths` 가 그
   경로를 대고 릴리스 설치가 만든다. 실행 시작에 지난 실행이 남긴 스풀 파일(`.provider-*.part`)을 지운다 — 그래서 매일
   단위와 운영자의 밀린 파일 실행을 동시에 돌리지 않는다. 30527 수집은 같은 경로를 실행 디렉터리 안 스풀로 쓴다(파일이
   작다).

## Consequences

- 첫 실행이 받는 것: 목록에 있고(선택 묶음 제외) 같은 파일 번호의 최신 원장 행이 목록의 갱신일과 체크섬을 갖지 않은 파일. 즉 손으로
  받은 적 없는 판, 받은 뒤 제공자가 갱신일을 바꾼 파일, 원장 행이 없거나 갱신일이 비어 있던 행. 저장소에서는 그 크기를 알 수 없다
  (운영 원장을 조회하지 않았다). 예산을 넘으면 첫 실행은 아무것도 받지 않고 슬랙으로 크기를 알린다 — 그 수치가 첫 측정이다.
  결정 3 이 없었다면 첫 실행은 이미 가진 수백 개 파일을 모두 다시 받으려 했다.
- 결정 9 로 내용 주소 키의 크기 상한은 스풀 디스크의 여유 공간뿐이다. 실행 하나가 스풀에 동시에 두는 양은 최대
  `MAX_IN_FLIGHT` 개 파일이다. 목록에서 500MB 를 넘는 파일은 여전히 선택 묶음(RAON)이라 이 레인이 받지 않는다 — 운영 원장의
  1.5GiB 파일들이 그 경로로 받혔다면, 새 판도 선택 묶음으로 목록에 오르는 한 이 레인에서는 보류되고 수동 수집 몫이다.
- 시험(스풀): 큰 본문이 디스크를 거쳐 바이트 그대로 올라가고 파일이 남지 않음, 업로드 실패·다른 바이트 거부·짧은 본문에도
  파일이 남지 않음, 여유 공간이 모자라면 본문을 읽기 전에 거부, 진행 중 예약이 여유 공간에 더해지고 끝난 파일은 돌려줌,
  `content_addressed` 에 스풀이 없으면 거부, 스크립트가 스풀을 넘기고 죽은 실행의 스풀 파일만 지움, 단위가 기본 스풀 경로를
  쓸 수 있음. 각각 규칙을 깨 실패함을 확인했다.
- 시험: Rust(계획의 수집 선택·요약 없는 계획·선언되지 않은 수집 거부·엔드포인트의 미선언 이름 차단, 목록 단계의 기대 수 무시, 키
  모양과 무관한 보유·체크섬 없는 행 비보유, 예산이 가진 것을 세지 않음·강제 재수집, 예산 경계), Python
  (`orchestration/tests/test_daily_source_sweep_command.py`: 실제 스크립트를 설치된 릴리스 배치에서 돌려 카탈로그 예산 전달,
  조용한 날, 예산 초과, 운영자 덮어쓰기, 레인 독립, 지난 증거, 요건 하나씩 빼기, 별칭 로그인, 카탈로그 성질). 스크립트 쪽 시험은
  규칙을 하나씩 깨 실패함을 확인했다.
- 기존 두 VWorld 수집 스크립트(30527, 30563)는 자기 도우미로 자른 카탈로그와 요약을 그대로 쓴다. 그 도우미를 이 선택자로 바꾸는
  것은 후속이다.

---

2026-10-10 개정 주석: §4 의 하루 새 바이트 예산과 §5 의 운영자 밀린 파일 실행은 [ADR-0172](./0172-the-daily-vworld-sweep-fetches-everything-it-lacks.md)가 없앴다 — VWorld 레인은 가지지 않은 것을 전부 받고, 큰 날은 안내 한 줄(`landed_bytes_notice`)만 보낸다.
