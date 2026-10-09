# ADR 0169: Silver 레인은 원천 판을 장부에서 고르고, 예약 작업이 스스로 채운다

- Status: Accepted
- Date: 2026-10-09
- Builds on: [ADR-0077](./0077-the-pipe-looks-at-its-sources-every-day.md)(매일 수집),
  [ADR-0128](./0128-floor-inputs-bind-to-bronze-and-scalar-retries-bind-to-their-append.md)(층별 사이클),
  [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md)(필지 판),
  [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(예약 작업)
- Amends: ADR-0077 §5(새 파일 반영은 사람이 한다)

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

## Consequences

- 새 원천 판이 사람 없이 Silver 에 닿는다. 판을 올리는 PR 이 없어진다.
- 매일 레인마다 장부 조회 한 번과 Silver 확인 한 번이 돈다. 바뀐 것이 없으면 수 초다.
- 과거 객체 측정은 객체마다 수 KB 범위 읽기다(수천 개, R2 읽기 몇 원).
- 손으로 잰 `objects[]` 는 과거 적재의 증거였다. 그 증거는 Silver 스냅숏 요약(`source_snapshot_id`)과 새 표가 대신한다.
- 필지 판의 측정 도구(`vworld_parcel_edition_members.py`)는 필지 판 제안이 새 표를 읽게 될 때(5항 ④, 필지 경계)까지
  남는다. 그때 지운다; 그 전까지 ZIP 안 이름을 재는 곳이 둘이다.
