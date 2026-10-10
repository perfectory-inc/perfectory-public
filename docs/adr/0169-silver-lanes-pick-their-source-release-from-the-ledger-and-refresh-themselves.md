# ADR 0169: Silver 레인은 원천 판을 장부에서 고르고, 예약 작업이 스스로 채운다

- Status: Accepted
- Date: 2026-10-09
- Builds on: [ADR-0077](./0077-the-pipe-looks-at-its-sources-every-day.md)(매일 수집),
  [ADR-0128](./0128-floor-inputs-bind-to-bronze-and-scalar-retries-bind-to-their-append.md)(층별 사이클),
  [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md)(필지 판),
  [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(예약 작업)
- Amends: ADR-0077 §5(새 파일 반영은 사람이 한다), [ADR-0092](./0092-hub-registers-land-through-layout-contracts-and-streams.md)
  §1·§8(계약의 `selected_vintage`, `land-use-batch-load.sh` 적재), [ADR-0094](./0094-the-exclusive-register-bridges-prices-and-units.md)
  §1(같은 것, 전유부 다리) — ②에서; [ADR-0083](./0083-a-parcel-learns-its-zoning-from-the-land-use-ledger.md)·0085·0087·0088·0089·0091 의
  VWorld 토지 손 적재(계약의 `objects`·`selected_vintage`, 실행 경로) — ③에서

## Context

2026-10-09 전수 조사(파이프라인 그래프 + 코드 + 운영 장부 읽기):

- Gold 두 표가 읽는 Silver 14개 중 예약 작업이 채우는 것은 `silver.building_register_floors` 하나다(ADR-0128). 나머지
  13개는 사람이 손으로 적재한다. 새 원천 파일이 Bronze 에 와도 누가 적재하기 전에는 서비스에 닿지 않는다.
- VWorld 토지 원천 7종은 계약 `infra/lakehouse/contracts/vworld-*-source-objects.json` 의 `objects[]`(사람이 잰 목록)와
  `selected_vintage`(사람이 고친 판)로 적재 대상을 정한다. 판을 올리는 일이 PR 이다.
- 사람이 재야 했던 이유: 장부(`catalog.bronze_object`)의 `provider_file_name` 은 데이터셋 제목("개별공시지가정보")뿐이고,
  어느 시도·어느 판인지는 ZIP 안 파일 이름(`AL_D151_<시도>_<날짜>.csv`)에만 있다. 운영 장부 실측: 토지특성 807개,
  이용계획 241개(판 11개), 임야 542개(판 12개)가 모두 그렇다.
- ZIP 안 이름은 중앙 디렉터리만 읽으면 된다. 필지 판(ADR-0148)이 이미 이렇게 잰다
  (`vworld_parcel_edition_members.py`, 13 GB 판을 수 KB 범위 요청으로).
- 건축HUB 원천은 장부의 `snapshot_date` 와 파일 이름에 판(월)이 있다. 층별 사이클은 그것으로 "둘 다 있는 최신 판"을
  고른다(ADR-0128). 표제부·전유부·전유공용면적은 같은 원천인데 사람이 입력 객체를 골라 넣는다.
- 모든 적재는 같은 입력으로 다시 돌면 아무것도 쓰지 않는다(`append_batch_once`, 내보내기 `already_present`). 그래서
  매일 돌려도 중복은 없다.

## Decision

1. **ZIP 안 이름은 장부 옆에 쌓는다.** 새 표 `catalog.bronze_object_member`(쌓기만, ADR 전 데이터 원칙)에 Bronze 객체마다
   안 파일 이름·크기·날짜를 남긴다. 매일 수집(ADR-0168) 뒤 같은 단위가 아직 잰 적 없는 객체만 범위 요청으로 잰다.
   과거 객체는 같은 명령을 한 번 돌려 채운다. 재는 일은 필지 판의 도구를 일반화한 하나다.
   구현(마이그레이션 `20261009150000_a_bronze_object_keeps_its_zip_members.sql`):
   - 표 둘. `catalog.bronze_object_measurement` 는 객체 한 번의 읽기다: `outcome` 이 `zip`·`not_zip`·`unreadable`
     (정해진 결과, 객체마다 하나 — 부분 유일 색인 `bronze_object_measurement_settled_key`)이거나 `failed`(바이트를
     못 받음; 이력으로 쌓이고 다음 실행이 다시 잰다). `catalog.bronze_object_member` 는 중앙 디렉터리 항목마다
     한 행이다: 위치(`member_index`, 이름은 겹칠 수 있어 키가 아니다), 이름, 이름을 읽은 방식
     (`member_name_encoding`: UTF-8 표시·Info-ZIP 유니코드 경로·표시 없는 UTF-8·CP949·대체 문자), 두 크기(ZIP64
     추가 필드 포함), 머리의 MS-DOS 시각(시간대 없음). 두 표 모두 UPDATE·DELETE·TRUNCATE 를 거부하고, 읽기는
     세어 둔 개수만큼의 항목을 같은 트랜잭션에서 가져야 한다(트리거 둘).
   - 명령 `measure-bronze-object-members`(발행기). 장부의 `size_bytes` 로 끝을 알므로 `HeadObject` 없이 끝 128 KiB 를
     한 번 읽는다. 중앙 디렉터리가 그 앞에 있으면 한 번, ZIP64 끝 레코드가 그 앞에 있으면 또 한 번: 객체마다
     GET 1–3 번이고 객체 전체는 받지 않는다. 같은 객체를 두 번 재면 아무것도 더하지 않는다.
   - `zip` 크레이트로 열지 않는다. 열 때 끝 레코드를 객체 앞까지 거꾸로 훑어(zip 2.4 `find_central_directory`)
     ZIP 이 아닌 객체를 범위 읽기로 통째로 받게 되고, UTF-8 표시 없는 이름을 CP437 로 읽어 한글 이름을 바꾼다.
     그래서 끝 레코드·디렉터리 해석만 직접 두고(`bronze_object_members/zip_directory.rs`), 시험은 `zip` 크레이트가
     쓴 파일을 그 크레이트의 읽기와 견준다.
   - 실행은 `scripts/ops/bronze-object-members.sh` 하나다. 잴 원천의 기본 목록(`vworldkr__land*,hubgokr__*`)은 그
     스크립트에만 있고, 읽기 전용 키 쌍으로만 읽는다(`config/runtime-secrets.contract.json` 의 실행
     `bronze-object-members`, ADR-0153). 과거 채우기는 운영자가 이 실행으로 돌린다. 매일 수집
     (`daily-source-sweep.sh`)이 수집 뒤 이 스크립트를 부르는 연결은 sweep 쪽 변경이 하고, 그때 sweep 단위가
     `lakehouse-reader` 묶음을 함께 싣는다.
2. **계약에는 규칙만, 사실은 장부에.** 각 레인 계약은 안 파일 이름에서 시도와 판을 읽는 규칙(이름 형식), 완전성
   (`load_granularity` 와 그 개수, 예: 시도 17), 넘김 경로를 갖는다. `objects[]` 와 `selected_vintage` 는 지운다.
   판 선택은 "완전한 판 중 가장 새것" 하나다: 각 시도에 그 판의 객체가 있고(같은 시도·판이 둘이면 제공자 갱신일이 늦은
   것), 개수가 계약의 개수와 같다.
3. **레인마다 예약 작업 하나, 모양은 하나.** `foundation-silver-refresh@<레인>.service`(템플릿) 가
   `silver-refresh.sh <레인>` 을 돌린다. 순서는 층별 사이클과 같다: 장부에서 판을 고른다 → Silver 가 그 판을 이미
   가졌으면 `unchanged` 로 끝난다 → 내보내기 → 적재 → 다시 읽기. 레인 목록은 `orchestration/jobs.v1.json`(작업마다
   파이프라인 그래프 간선)이고, 레인의 실행 값(내보내기 종류, Spark 계약, 메모리)은 그 레인 계약에 있다. 스크립트에
   레인 목록을 다시 적지 않는다.
4. **바뀐 것만 다음을 부른다.** 작업은 `changed` 와 `unchanged` 를 구별해 남긴다. 다음 단계(Gold 재생성 → 굽기)를
   시간표 대신 이 신호로 잇는 일은 이 다음 결정이다(데이터 기반 예약).
5. **순서.** ① 장부 옆 기록과 과거 채우기 ② 건축HUB 레인(표제부·전유부·전유공용면적·공동주택가격·전유부 다리)
   ③ VWorld 토지 레인 7종 ④ 공동주택가격 조합(`unit_official_price`)·필지 경계. 각 단계는 따로 병합하고, 레인마다
   첫 실행은 운영자가 지켜본 뒤 켠다(ADR-0122 §4).

   **② 구현(2026-10-09).** 레인 다섯: 표제부(`building-register-titles`), 전유부(`building-register-units`),
   전유공용면적(`building-register-unit-areas`), 공동주택가격(`building-register-apartment-price`), 전유부
   다리(`building-register-exclusive-unit`). 실행 절차는
   [`silver-refresh.md`](../../platforms/foundation-platform/docs/runbooks/silver-refresh.md) 런북에 있다.
   - **판.** 레인 계약의 `silver_refresh.roles`(역할 → 원천 slug)의 장부 행을 한 번 읽어(`run-silver-refresh`,
     `remote_lakehouse_job/silver_refresh/release.rs`) 모든 역할이 있는 가장 새 제공자 월을 고른다. 허브는 그 월을
     `snapshot_date` 1일로 적는다. 빠진 역할이 있는 더 새 월은 건너뛰고 이름을 남긴다. 한 역할의 객체가 둘이면
     `provider_updated_at` 이 늦은 것이고, 같거나 둘 다 없으면 거부한다. 파일 이름 `OPN<YYYYMMDD>` 는 그 월 안이어야
     하고, 여러 역할(전유부의 표제부·기본개요)은 같은 내보내기 날이어야 한다(호실 부모 증거, ADR-0125). FLOOR 의
     선택과 같은 규칙이다. 다만 FLOOR 는 둘을 비기면 거부하고, 이것은 갱신일로 가른다.
   - **이미 반영.** 새 등록부를 만들지 않았다. `append_batch_once` 가 행과 같은 커밋에 남기는 스냅숏 요약
     `foundation.ingest-batch-objects` 를 Iceberg REST 로 읽는다(`IcebergRestCatalog::load_ingested_batch_objects`,
     Spark 없이 수 초). 한 판이 한 `source_snapshot_id` 인 세 레인(표제부·전유부·면적)은 그 값을
     `silver-refresh-<레인>-<YYYYMM>-<읽는 바이트의 digest 16자>` 로 정한다. 같은 판을 다시 돌리면 같은 값이고,
     다시 받은 파일은 다른 값이다. 부분 레인 둘은 R2 매니페스트의 부분 키가 그 값이다. 표가 그 판을 가졌으면
     `unchanged`(`already_loaded`)다. 더 새 판을 가졌으면 옛 판으로 되돌리지 않고 `unchanged`(`newer_release_loaded`)다.
   - **실행.** `foundation-silver-refresh@<레인>.service` → `scripts/ops/silver-refresh.sh <레인>` →
     `run-silver-refresh` 하나다. 레인마다 다른 셸 분기는 없다. 바뀐 판이면 장부의 크기·SHA-256 로 확인하며 ZIP 을
     `/data/foundation-platform/silver-refresh/<레인>/work` 에 받고, 그 레인의 내보내기를 발행기의 자식으로 돌린다
     (유닛 `MemoryMax=4G`, FLOOR 내보내기 컨테이너의 4 GiB 와 같은 한계). 그다음 compose `spark` 에서
     `silver_scalar_handoff_to_lakehouse.py` 로 적재하고 표의 기록을 다시 읽는다. 세 레인은 한 묶음 `overwrite` 다:
     `gold.building_panel` 이 `source_snapshot_id` 하나만 읽기 때문이다(ADR-0142 Consequences). 대지 PNU NULL
     게이트는 적재기가 표 계약대로 건다. 부분 레인은 `append` 이고 4 부분씩 넣는다. 이미 넣은 부분은 적재기가
     건너뛴다. Spark 는 invocation 이름의 Compose 프로젝트에서 돌고, ExecStopPost 가 FLOOR·Gold 재생성과 같은 코드
     (`invocation_cleanup`)로 치운다. 마지막 줄은
     `silver-refresh-outcome lane=… outcome=changed|unchanged reason=… release=<YYYYMM> identity=… rows=…` 다.
     `--plan` 은 같은 판단만 하고 `silver-refresh-plan … outcome=would_change|unchanged` 로 끝난다. 받기·쓰기는 없다.
   - **계약.** 허브 두 계약에서 `selected_vintage`·`objects`·`granularity_counts` 를 지웠다. 세 레인의 계약
     (`hub-building-register-{title,unit,unit-area}-source-objects.json`)을 새로 두었다. 계약에는 판 규칙과 실행 값만
     있고, 계약에 판을 적으면 발행기가 거부한다. 허브 공통 내보내기는 입력 객체와 장부 크기
     (`*_INPUT_OBJECT_BYTES`)를 받는다. `vintage` 는 파일 이름의 월이다. 허브 매니페스트 검사는
     `source_handoff_inputs.py` 에서 발행기로 옮겼다. 그 계획기는 허브 계약을 거부하고 VWorld 계약만 계획한다(③까지).
     `unit_official_price.py` 는 `--vintage` 가 없을 때 두 표가 함께 가진 가장 새 vintage 를 쓴다. 한쪽만 새 월이면
     거부한다.
   - **예약은 아직 아니다.** `orchestration/jobs.v1.json` 에 레인을 올리지 못했다. `spark` 풀의 굶주림 상한이 이미
     꽉 찼다. 매시간 접기가 1,200/1,200 분을 기다린다(`job_specs.pool_starvation`). 1 슬롯 30 분 작업 하나만 더해도
     1,230 분, 3 슬롯 240 분이면 1,440 분이다. 그래서 레인을 등록하는 일은 이 단계와 따로 결정한다. 선택지는 셋이다.
     (가) 기존 작업의 점유 시간을 줄인다(접기의 retries 0 이면 레인 하나 몫). (나) 레인을 한 작업에 묶어
     FLOOR 단위 뒤에 잇는다. code.go.kr 적재가 `lineage_stewardship` 안에 탄 것과 같다. (다) 상한(ADR-0138)을
     바꾼다. 그때까지 유닛은 운영자가 시작한다(DAG 를 멈추고 한 레인씩, 스크립트가 잠금으로 강제한다). 릴리스는
     작업 목록 밖의 이 템플릿에도 릴리스 승인을 설치하고 상태 폴더를 만든다(`foundation-release.sh timers`).
   - **FLOOR 는 그대로다.** FLOOR 의 선택은 역사 증인과 확정 입력에 묶여 있어 층 전용이다. 일반화해도 단순해지지
     않았다. 함께 쓰는 것은 정리 코드(`invocation_cleanup`), Spark 요약 검증, 장부 조회 모양이다.
   - **④로 넘긴 것.** `unit_official_price` 는 시도마다 한 번(17 번), 두 부분 레인이 같은 월을 가진 뒤에 돈다.
     공동주택가격 레인의 하류 단계로 붙이면 한 레인이 다른 레인을 기다리게 된다. 그래서 ④에서 데이터 기반 예약
     (4항)과 함께 잇는다.

   **③ 구현(2026-10-10).** VWorld 토지 레인 일곱: 토지특성(`land-characteristic`), 임야대장(`land-forest-ledger`),
   개별공시지가(`land-individual-price`), 대지권등록(`land-right-registration`), 토지이동이력(`land-transfer-history`),
   토지이용계획(`land-use-plan`), 용도지역 코드표(`land-use-zone-code`). 모양은 ②와 하나다(같은 유닛 템플릿·스크립트·
   `run-silver-refresh`). 다른 것은 판을 고르는 1 단계와 판의 단위뿐이다(`remote_lakehouse_job/silver_refresh/land.rs`).
   - **판.** 레인 원천의 장부 행을 ①의 ZIP 안 이름과 함께 한 번 읽는다(정해진 `zip` 읽기만). 계약의
     `silver_refresh.member_name`(이름 형식, 이름 붙은 묶음 `region`·`vintage`)에 맞는 안 이름이 하나인 객체가 후보다.
     `completeness`(시도 17, 코드표는 전국 1)를 모두 덮는 판 중 가장 새것이 판이다. 같은 시도·판이 둘이면 ②와 같은
     규칙(`release::newest`)으로 가르고, 가를 수 없으면 거부한다. 덮지 못한 더 새 판은 `skipped_incomplete_release` 로
     남긴다. 아직 정해진 읽기가 없는 객체가 하나라도 있으면 거부하고 측정 명령을 이름 짓는다: 그 객체가 가장 새 판일 수
     있다. 내보내기가 거부할 객체(읽을 CSV 가 둘, 날짜가 아닌 판)는 후보에서 빼고 `refused_object` 로 남긴다.
   - **이미 반영.** 토지 표의 적재 단위는 Bronze 객체(`source_record_id`)다. 판의 객체가 모두 표의 스냅숏 요약에
     있으면 `already_loaded`, 더 새 판의 객체가 있으면 `newer_release_loaded` 다(②와 같이 Iceberg REST 로 읽는다).
     손 적재로 들어간 판도 그래서 같은 판으로 읽힌다.
   - **적재.** gold.parcel_panel 은 토지 표마다 `source_snapshot_id` 하나만 읽는다. 그래서 새 판은 표를 바꿔 쓴다:
     객체마다 기존 내보내기(`export-land-*-silver-handoff`)를 발행기의 자식으로 돌려 R2 핸드오프 하나를 쓰고(이미 있으면
     건너뛴다), 시도 순서로 `input_file_batch_size`(4) 개씩 적재한다. 첫 묶음이 `overwrite`, 나머지가 `append` 다.
     `source_snapshot_id` 는 원천 slug 를 하이픈으로 쓴 이름과 판이다(`vworldkr-land-use-plan:<판>`, 손 적재와 같다).
     묶음마다 Spark 요약의 `source_snapshot_ids` 가 그 하나인지 확인한다. 끊긴 실행은 기록에 없는 첫 묶음부터 잇는다
     (`append_batch_once`).
   - **계약.** 일곱 계약에서 `objects`·`selected_vintage`·`granularity_counts`·`excluded_objects` 와 손 측정 설명을
     지우고 `release_rule` 과 `silver_refresh` 블록을 두었다. 내보내기 종류·환경 접두사·표는 내보내기 코드
     (`land_use_silver_export::export_command`)에서 읽어 레인 쪽에 다시 적지 않는다. 계약에 판을 적으면 거부한다.
   - **은퇴.** 손 적재 경로(`land-use-plan-handoff-export.sh`, `land-use-batch-load.sh`, `source_handoff_inputs.py`)를
     지웠다. 남은 계약이 하나도 측정 목록을 갖지 않아 그것들이 읽을 것이 없다.
   - **예약.** 레인 일곱은 `orchestration/jobs.v1.json` 의 `silver_refresh_land_*`(ADR-0171, source_sweep 이 시작)이고
     꺼져 있다. 켜기 전 조건은 둘이다: 레인마다 첫 감독 실행([런북 5 절](../../platforms/foundation-platform/docs/runbooks/silver-refresh.md)),
     그리고 매일 수집 뒤의 ZIP 안 이름 측정(①의 sweep 연결). 그 연결 전에는 새 객체마다 레인이 거부한다.

## Consequences

- 새 원천 판이 사람 없이 Silver 에 닿는다. 판을 올리는 PR 이 없어진다.
- 매일 레인마다 장부 조회 한 번과 Silver 확인 한 번이 돈다. 바뀐 것이 없으면 수 초다.
- 과거 객체 측정은 객체마다 수 KB 범위 읽기다(수천 개, R2 읽기 몇 원).
- 손으로 잰 `objects[]` 는 과거 적재의 증거였다. 그 증거는 Silver 스냅숏 요약(`source_snapshot_id`)과 새 표가 대신한다.
- 필지 판의 측정 도구(`vworld_parcel_edition_members.py`)는 필지 판 제안이 새 표를 읽게 될 때(5항 ④, 필지 경계)까지
  남는다. 그때 지운다; 그 전까지 ZIP 안 이름을 재는 곳이 둘이다.

## 개정 기록

- 2026-10-10: §4 의 데이터 기반 예약과 §5 ②의 "예약은 아직 아니다"는 [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md) 이 정했다. 레인 다섯은 `orchestration/jobs.v1.json` 의 작업(`silver_refresh_building_register_*`)이 되었고, source_sweep 이 새 파일을 받으면(`foundation-job-outcome changed`) 시작하며 07:00 이 대체 시각이다. 등록 방법은 선택지 (가)~(다)가 아니라 굶주림 셈법의 정정이다: 접기보다 가볍고 슬롯이 같거나 많은 작업은 접기를 앞지르지 못한다. 그래서 레인은 무게 4, 재시도 0, 유닛 `TimeoutStartSec=150m` 이고, 첫 감독 실행 전까지 꺼져 있다. 릴리스의 템플릿 승인 설치는 작업 목록 반복문이 맡는다. 위 결정 본문은 고치지 않았다.
- 2026-10-10: §1 의 sweep 연결을 했다. `daily-source-sweep.sh` 가 세 레인(hub·vworld·raon) 뒤에
  `scripts/ops/bronze-object-members.sh` 를 인자 없이 부른다 — 잴 원천의 기본 목록은 그 스크립트에, 한 실행 개수
  (`FOUNDATION_PLATFORM_BRONZE_MEMBER_LIMIT`, 기본 2000)와 동시 객체 수(8)는 명령에 있고 sweep 은 다시 적지 않는다.
  레인이 실패해도 돈다. sweep 단위는 `lakehouse-reader` 묶음을 함께 싣고(`config/runtime-secrets.contract.json`,
  렌더한 `EnvironmentFile`), sweep 은 읽기 키 쌍을 부작용 전 필수 목록에 넣는다: 그 파일이 없으면 레인이 돌기 전에
  78 로 멈춘다. journal 줄 끝에 `| members measured= failed= selected= members=` 가 붙고, 요약이 없으면
  `| members status=no-summary rc=`.
  - **측정 실패는 실패한 레인처럼 빨갛다.** 읽지 못한 객체가 있거나(`failed>0`, 명령이 0 이 아닌 값으로 끝남) 요약이
    없으면 sweep 은 슬랙 🔴 와 함께 1 로 끝나고 결과 줄(`foundation-job-outcome`)을 내지 않는다. 받은 파일은 같은 줄의
    `new=` 에 그대로 남는다(숨지 않는다). 결과 줄을 내지 않는 까닭: 실패한 실행은 결과를 말하지 않는다는 ADR-0171 의
    규칙과 같고, 그날 결과 줄로 Silver 레인을 시작해도 못 잰 객체 앞에서 레인이 거부할 뿐이다. 실패로 남은 측정은
    다음 실행이 다시 고르고(시도 횟수가 적은 것부터), Silver 레인에는 07:00 대체 시각이 있다.
  - **과거 채우기.** 첫 실행들은 새 VWorld 파일(운영 약 880 개)과 지난 객체 전부를 잰다. 한 실행이 2000 개까지이므로
    매일 sweep 만으로는 (못 잰 객체 수 ÷ 2000)을 올림한 만큼의 실행이 걸린다 — 1만 2천 개면 7 번. 객체마다 범위 GET
    1–3 번(≤128 KiB)을 8 개씩이라 한 실행의 측정은 분 단위이고 단위 상한(8 시간)에 영향이 없다. 더 빨리 채우려면 운영자가
    `bronze-object-members` 실행을 거듭 돌린다(겹쳐도 같은 객체를 두 번 쓰지 않는다). 줄의 `selected=2000`(상한)은 아직
    남았을 수 있다는 뜻이고, 그보다 적으면 그 실행이 남은 것을 다 골랐다.
  - 실패한 레인의 파일별 이유가 유닛 저널로 간다(ADR-0174 개정 기록).
