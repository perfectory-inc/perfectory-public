# ADR 0361: 필지의 소유자는 자기 by-PNU 종류다 — parcel-ownership

- Status: Accepted
- Date: 2026-10-30
- Builds on: [ADR-0360](./0360-a-parcel-learns-its-ownership-from-the-land-ownership-file.md)(silver.land_ownership),
  ADR-0193(데이터 종류마다 자기 Gold 와 by-PNU 레인), ADR-0287 §9(자기 생산자를 가진 종류), ADR-0292(종류의 행은 호스트의 부분집합),
  ADR-0303(자기 생산자 종류의 굽기 자리), ADR-0312 §2·ADR-0344(바뀐 PNU 만), ADR-0336(스키마 레지스트리), ADR-0272(시도 커버리지)

## Context

소유는 필지의 사실이지만 필지 패널(`gold.parcel_panel`)의 섹션으로 넣으면 안 된다. 필지 문서(`parcel_by_pnu_profile`)는 패널의 모든
섹션을 싣는다(`contracts/by-pnu-documents/foundation-platform.parcel_by_pnu_profile.v2.json`). 섹션 하나를 더하면 문서 판이 바뀌고,
약 4천만 개 필지 문서를 새 세대로 다시 굽게 된다. 그 굽기는 13시간이 넘게 걸려 배포를 막은 적이 있다(10-23).

종류 레인(ADR-0193)은 바로 이것을 위해 있다. 자기 생산자를 가진 종류(ADR-0287 §9, 공매)는 패널에 묶이지 않고 자기 Silver 에서 만들어진다.

## Decision

1. **종류 한 줄.** `config/r2-connections.contract.json` `by_pnu_lanes.parcel-ownership`:
   - 호스트는 필지, 종류는 `ownership`, 섹션은 `ownership`이다.
   - 열은 `ownership_json`, `owner_total:long`이다.
   - 생산자는 `land_ownership_silver_to_gold`다.
   - Gold `gold.parcel_ownership`, 경로 `/parcels/ownership/by-pnu/`, 묶음 파일은 그 한 줄에서 유도된다.
2. **문서(`ownership`).** 필지의 현재 소유 행에서 만든다. 원천의 코드와 말만 쓰고, 사람 이름은 원천에 없다.
   - `owners`: 땅의 공유인. `집합건물일련번호`가 모두 0인 행이다.
     - 공유인일련번호 순서로, 100명까지 싣는다.
     - 한 명마다 소유구분·거주지구분·국가기관구분·소유권변동원인·변동일자·공유인수를 싣는다.
     - `owner_total`(Gold 열)이 전부를 센다.
   - `unit_owner_total`, `unit_owner_kinds`: 필지 위 집합건물(아파트, 지식산업센터)의 호 소유 행 수와, 그 소유구분별 수(많은 순)다.
     호는 하나하나 싣지 않는다. 한 PNU 에 2,246행이 있었다(10-30 측정).
   - `latest_changed_on`, `base_year_month`, `data_reference_date`: 행들 중 가장 새것이다.
   - 널도 쓴다. 문서의 경로는 늘 레지스트리의 경로다.
3. **서빙 필지판의 PNU 만, 계보는 따르지 않는다.**
   - 필지 집합은 `read_served_parcels`(서빙판, ADR-0148)다. 종류의 행은 호스트의 부분집합이다(ADR-0292).
   - 개편으로 바뀐 코드의 행은 서빙 코드로 읽는다(`renumbered`, 모든 필지 섹션과 같다).
   - 소유는 지금 등록된 필지의 사실이라 앞 필지에서 물려받지 않는다. `attached_via` 는 늘 널이다.
4. **바뀐 PNU 만 다시 계산한다(ADR-0312 §2).**
   - Silver 판은 바뀐 필지의 행만 덧붙인다(ADR-0360). 그래서 Gold 는 핀 이후 덧붙은 행의 PNU를 Iceberg 증분 읽기
     (`read_appended`, ADR-0189)로 얻는다. 덧붙임이 아닌 커밋 뒤에는 행 차이로 얻는다.
   - 서빙 필지판이 바뀌면 들고 난 PNU 를 더해 그것만 병합한다. 병합은 merge-on-read 다(ADR-0344).
   - 코드 변경표가 바뀌면 통째로 짓는다(`WHOLE_TABLE_INPUTS`). 개편은 동 전체의 이름을 바꾸고 드물게 오므로, 통째가 더 싼 규칙이다.
   - 소유는 등록부라서 ADR-0272 의 시도 커버리지 관문(5%)을 받는다. 공매와 달리 하루하루 크게 흔들리지 않는다.
5. **작업.**
   - `gold_parcel_ownership_rebuild`가 `gold-panel-rebuild.sh parcel-ownership`을 돈다. 자기 단위
     `foundation-gold-parcel-ownership-rebuild.service`, scope `changed-keys`, 입력이 시작한다.
   - 공매처럼 자기 작업인 까닭: 자기 생산자 종류는 `kinds` 의 표가 아니다.
   - 굽기는 자기 굽기 작업 `by_pnu_parcel_ownership_bake`(`by-pnu-serving-bake.sh parcel-ownership`)가 한다. 자기 생산자 종류는
     모두 서빙 간선을 자기 작업이 진다(ADR-0303 §2): 느린 전국 필지 굽기가 이 레인을 붙잡지 않는다.
   - 그 작업은 스파크 풀 두 자리, `MemoryMax=5G` 다. 정기 실행은 반년마다 바뀐 필지의 패치만 굽고, 레인 전체 세대는 운영자의
     전환 굽기가 만든다(ADR-0190). 두 자리인 까닭: 패널 굽기(14G)·공매 굽기(5G)와 셋이 함께 돌면 24G 로 호스트 예산의
     작업 몫 22G 를 넘는다(`every-container-has-a-memory-cap`, ADR-0138). 두 자리면 둘 중 하나와만 함께 돈다.
   - 첫 발행 전에는 굽기가 할 일이 없다(묶음 전용 관문, ADR-0190).
6. **문서 스키마는 레지스트리에 있다(ADR-0336).**
   - 주제는 `foundation-platform.parcel_ownership_by_pnu`, 판 1 리비전 1, `FULL_TRANSITIVE`다.
   - 소비자는 발행기의 `pack_lane_document::check`다. 웹 화면의 소비자는 화면 PR 이 더한다.
   - Spark 테스트(`test_land_ownership_gold.py`)가 쓰인 JSON 의 경로를 레지스트리와 같게 묶는다(`SPARK_HELD`).
7. **첫 전량 빌드는 감독 실행이다.**
   - Gold 작업과, 그것이 먹이는 굽기 작업은 꺼진 채로 들어간다(`enabled: false`). 켜진 작업의 입력을 쓰는 작업이 모두 꺼져
     있으면 작업 목록이 거부한다(`job_specs`).
   - 전국 약 1억 2천만 소유 행의 첫 빌드를 단위의 150분 상한 안에서 재본 뒤, 다른 배포에서 켠다.
     ADR-0193 §7 의 "첫 전량 빌드는 예약 작업 밖에서 재고 돈다"를 따른다.

## Consequences

- **비용(계산, R2 $0.015/GB-월, 1달러 1,400원).**
  - Gold 는 필지 약 3,900만 행이다. 세종에서 PNU 207,385개 중 206,588개가 땅 소유 행을 가졌다. JSON 압축 Parquet 로 약 3GB, 월 약 60원이다.
  - 묶음 한 세대는 약 18,800 묶음이다. 쓰기 요청은 약 $0.09 한 번이고, 저장은 약 3–5GB 로 월 약 100원이다.
  - 반년마다 바뀐 필지만 패치한다.
- **운영자 단계(사장, 순서대로).**
  1. `silver_refresh_land_ownership` 이 첫 판을 적재한 뒤 첫 Gold 를 잰다:
     `ssh -tt ai-server sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh gold rebuild parcel-ownership`
  2. 레인 첫 발행(묶음 전용 관문, 창 안):
     `ssh -tt ai-server sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh parcel-ownership cutover 1`
  3. 첫 빌드가 150분 안이었고 발행이 끝났으면, Gold 작업과 굽기 작업을 함께 켜는 PR.
- 스파크 풀의 기다림 상한(FLOOR)은 굽기 한 번(115+30분)만큼 늘어 2,645분이다. 작업 목록의 상한 안이다(`pool_starvation`).
- 화면(공짱 필지 패널의 "소유" 칸)은 다음 PR 이 이 레인을 읽어 붙인다.
