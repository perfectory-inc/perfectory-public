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
