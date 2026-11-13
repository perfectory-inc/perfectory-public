---
status: current
owner: repository-maintainers
doc_type: roadmap
last_reviewed: 2026-11-07
---

# 운영 준비 작업 목록

이 문서는 perfectory의 현재 구현을 운영 단계까지 가져가기 위한 단일 실행 목록이다. 기술
스택 이름을 늘리는 문서가 아니라, 각 단계의 완료 조건과 다음 작업의 순서를 고정한다.

## 현재 기준

- 원본 데이터는 Foundation 소유 R2 Bronze에 보존한다.
- 처리 상태와 이벤트 원장은 Foundation PostgreSQL이다.
- Kafka는 Postgres outbox에서 파생 이벤트를 전달하는 통로다.
- 로컬/CI 브로커는 Redpanda `v24.3.6`, 스키마 레지스트리는 Karapace `6.2.0`이다.
- 운영 Kafka/Schema Registry는 관리형 서비스를 사용하며, 운영 자격증명은 저장소에 넣지 않는다.
- Kafka 원장(event sourcing) 전환은 기본 계획이 아니다. 특정 파생 영역에서 필요성이 증명될 때만
  별도 ADR로 결정한다.

## 운영 레이크하우스 자격증명·계정 분리 원칙

- 현재 버킷별 R2 개체 토큰은 유지한다. 개체 읽기·쓰기는 파일 API 권한이지 Iceberg 카탈로그
  권한이 아니다.
- 운영 전 카탈로그 권한은 서비스별로 분리한다. 쓰기 서비스는 카탈로그 쓰기와 운영
  레이크하우스 버킷 개체 읽기·쓰기를, 조회 서비스는 카탈로그 읽기와 버킷 개체 읽기만 갖는다.
- Cloudflare 대시보드의 `Admin Read & Write`는 계정 전체 버킷에 적용되므로 모든 서비스가
  공유하지 않는다. 사용자 지정 API 정책을 우선하고, 대시보드 어드민 토큰은 임시 대안 또는
  전용 쓰기 서비스에만 사용한다.
- 카탈로그 권한은 계정 수준이므로 계정 안의 다른 버킷과 강하게 격리해야 하면 출시 전에
  운영용 Cloudflare 계정을 별도로 둔다. 같은 로그인으로 여러 계정을 관리할 수 있지만,
  운영 중 분리하려면 새 버킷·카탈로그 생성, Iceberg 메타데이터와 객체 이전, 쓰기 동결,
  검증 후 주소·자격증명 전환과 롤백 절차가 필요하다.

## 최근 반영된 구현 조각

- Foundation Bronze 실시간 기록 어댑터는 객체 저장소를 실제로 만드는 경계에서 공통 런타임·버킷
  사전 점검을 통과한다. 호출자는 공급자 다운로드 전에 다시 점검하고, 수집 코드가 검증되지 않은
  빌더를 직접 호출하면 소스 가드가 거부한다.
- 환경변수를 바꾸는 publisher 테스트는 비동기 프로세스 전역 잠금 하나를 사용하고 원래 값을
  복원한다. 따라서 CI 환경 설정이 테스트 결과를 조용히 바꾸지 못한다.
- Kafka 계약 검사는 재시도에도 같은 `event_id`와 파티션 키가 유지되는지, 실제 Avro 레코드가
  원본 바이트가 아닌 Bronze claim-check 메타데이터를 노출하는지 확인한다.
- 자격증명 없이 실행하는 consumer 계약은 Avro claim-check를 디코드하고 Bronze checksum을
  검증하며 `event_id` 중복 전달을 버린다. 이것은 경계 증명이지 운영 Silver/Gold consumer 구현은
  아니다.
- Kafka 활성화는 어댑터 내부에서 정본 런타임 환경을 요구한다. 직접 호출자가 staging/production
  전송 경계를 생략할 수 없다.

## 진행 중 프로그램 (2026-10 현재)

마지막 대조: 2026-10-31 (origin/main·PR 상태·ADR 파일과 대조. 운영 호스트 실측은 "확인 필요"로 적는다.) 호스트 실측 전체는 §9(2026-11-07).

지금 실제로 진행 중인 작업이다. **각 항목의 상세·결정·남은 단계는 가리킨 ADR이 정본**이며,
여기서는 프로그램과 순서만 고정한다(작업을 복제하지 않는다 — 이 로드맵의 원칙 그대로). 아래가
"지금 무엇을, 왜, 어떤 순서로" 의 단일 출처다. 어떤 세션·에이전트도 이 절을 먼저 읽는다.
"초안"은 draft PR, "열림"은 리뷰 대기 PR 이다.

### 1. 사장 작업 순서 (2026-10-26)

사장이 정한 순서다. 앞 항목을 끝까지 가져간 뒤 다음으로 간다.

1. **공매(Onbid) → 화면.** ADR-0280·0292·0295·0296·0297·0298·0330·0339·0340.
   - [x] 사진 저장 결함 수정(#867), silver 사진표 + gold v2(#877), 엣지 미디어 서빙(#878), 웹 사진
     갤러리(#879), v2 재구축 수정(#881), 캐시 사진 500 수정(#927).
   - [x] 전체 필드 수집·서빙·화면 — ADR-0340 1~4단계(#942·#943·#945·#947).
   - [x] 사진 수집 한도 상향 — 계약 `media` 하루 45,000·run 당 3,000(#934·#941).
   - [ ] 사진 전량 백필이 끝났는지 실측(`onbid-media-summary-json`). 확인 필요.
   - [x] 입찰정보(15157251)·공고상세(15157218) data.go.kr 활용신청(ADR-0340 §3) — 사장 2026-10-30 신청 완료.
2. **행 버전.** ADR-0286 사슬. 1b 병합(#815). 남은 것: 1c #818 → 2단계 #816 → 3단계 #817(모두 초안).
3. **건물 패널 층 세대 버그.** 끝남 — 최신 층 묶음만 서빙(#830, 10-27 병합), 층 행 id 는 자연 키(ADR-0331, #932).
4. **에디션 관리·감사.** ADR-0291(PR #833 제목의 "ADR-0289"는 번호가 바뀐 것이다). 공매 종류가 서빙
   판만 읽게 한 수정(#832)만 병합. 준비도 점검·준비도 관문 승격·단일 판 읽기(ADR-0291 §2~§5)는
   main 에 없다 — 미착수.
5. **장소 정체성 수렴.** ADR-0288 여섯 단계 중 (1) 정본 해석기·우회 가드(#820·#823) 병합, (2) 분할
   흡수(#821) 병합. 나머지 개편 종류 흡수·(3) 경매 수렴·(4) 가드와 신뢰도 등급·(5) 감시·(6) 전수 백필은 미착수.
6. **법원경매 → 화면.** ADR-0280·0289(ADR-0281 대체)·0290. 수집·Silver 켜기 #969, 재수집 목록 ADR-0352
   #967, 필지 패널 경매 칸 ADR-0353 #978 — 모두 초안. ADR-0293 파일은 main 에 없다.

### 2. 엔지니어링·운영 위생

- 결정: ADR-0299(위생 = 기계 강제 불변식, A~O 15 계열) + ADR-0300(15 계열 전부 채택, 정합이 축,
  P1→P3). 각 불변식은 가드가 착지하는 PR 에서 AGENTS.md `위생 불변식` 절에 한 줄로 들어간다.
  지금 그 절의 규칙은 서빙 신선도(ADR-0306, #893)와 호스트 자원(ADR-0385, 초안)이다.
- P1 상태:

| 항목 | 상태 |
| --- | --- |
| 호스트 자원 경보 | 초안. ADR-0385: node-exporter 호스트 수집기(디스크·메모리·OOM·CPU) + cAdvisor, `foundation-host` 경보 6개 → Slack, promtool 규칙 시험 가드. 처음 잰 것: 10-28~11-03 OOM 100번(공매 Silver 2.5GiB 상한) |
| 수집 run 당 상한 + 가드 | 가드 있음(ADR-0386): 작업 69개 모두 `per_run_cap` + 잰 run 시간, `test_per_run_cap.py`. 안 맞는 열린 예외 6개(고침은 초안, 배포 뒤 다시 재고 예외를 지운다): `onbid_public_auction_silver`(10-30 timeout 5회·OOM 67회 — #1008), `silver_refresh_building_register_unit_areas`(제한의 95% — #1017), `silver_refresh_building_register_apartment_price`(10-20 timeout — #1010), `silver_refresh_land_transfer_history`(10-21 timeout, 11-03 82% — #1010), `gold_panel_rebuild`(timeout 10회, 마지막 11-03 — #1009), `gold_parcel_real_transaction_rebuild`(11-12 첫 전량 구축 약 11,000 s 가 9,000 s 유닛 둘에 걸쳤다: 시도 샤드 17개 8,309 s + 커밋 2,447 s. 단계 저장(ADR-0440)이라 이어지지만 첫 공개 뒤에는 둘째 유닛이 다음 날 일정에야 시작하고 첫 유닛은 timeout 으로 끝난다. 남은 일: 단계 경계에서 스스로 끝나고 곧바로 이어 시작하게 하거나 DuckDB 로 옮기는 Gold(ADR-0559)와 함께 Spark 를 떠난다; 11-13 09:50Z 첫 changed-keys run 을 재서 적는다) |
| postgres 복원드릴 타이머 + 백업 freshness | freshness 끝(#882, 타이머 있음). 주간 운영 복원 드릴 `foundation-postgres-restore-drill`(W6, 초안) — 병합·첫 일요일 실행 확인 남음 |
| R2 객체 ↔ silver 색인 대조 | 초안: ADR-0387 — 공매 미디어 `onbid/media` ↔ `silver.onbid_real_estate_item_photo` 매일 대조(`media_reconcile`), 없음·고아 경보. 고아 정리(삭제)는 보고서 뒤 사람 결정 |
| expand-contract 마이그레이션 가드 | 만들지 않기로 함 — #883 닫음(10-28): 막을 실제 사고가 없고 기존 두 장치가 막는다 |

- P2/P3 는 ADR-0300 소유.

### 3. 부분 처리 프로그램

- 결정: ADR-0307~0347(부분 처리가 기본, 전량은 계약 예외 — ADR-0314). 대부분 병합됐다.
- 남은 것:
  - 남은 설계(추적 초안 #951·#952·#953 은 11-06 ADR 진행 기록으로 접고 닫았다): Gold MERGE 바뀐 행만은
    ADR-0344 §6 측정만 남음(1·2단계 #959·#960 병합; `gold_panel_rebuild` 가 11-03 부터 매 실행 증분 계산 중
    끝나 첫 merge-on-read 커밋이 아직 없다), FLOOR Silver 바뀐 층만 ADR-0322 §3, 서빙 필지 Gold 시도별 ADR-0322 §4.
  - 아직 `full` 로 선언된 작업: 9개 / 45개(`orchestration/jobs.v1.json`, #956 으로 23 → 9). 각 주인은
    ADR-0314·0322.
  - 스냅숏 만료 #938(ADR-0338, 초안, 보류 — 운영 dry-run 먼저).
  - 단가 vintage ADR-0317 3단계 #919(초안) — 운영자 2단계(`vintage_versions.py`, missing = extra = 0)가 먼저.

### 4. 구조 전환

- 결정: ADR-0333(분할 자산, Accepted) · ADR-0334(여정 SLO·WAP, Proposed) · ADR-0335(연산 격리,
  Proposed) · ADR-0336(스키마 레지스트리, Accepted).
- 진행: ADR-0333 1단계(#940)·조정기(#950) 병합. 스키마 레지스트리는 공매 주제 하나만(#939).
  Dagster airlift 관찰 0단계 #946 초안.

### 5. 전체 수집 (사장 승인 2026-10-30: 미수집 원천 전부, 벌크 우선)

- **실거래가 12종.** 수집·Silver ADR-0348·0349 병합(#980). Silver 적재 수정 ADR-0364 #983(초안). 가린
  지번 필지 추정 ADR-0351 #982(초안, 사장 10-31 "해보자"). Gold·화면 ADR-0350 #977(초안).
  - 백필 속도는 공급자 하루 100회가 정한다(우리 상한 하루 90회): 산업 3종 약 23일, 12종 전부 약 86일.
  - 화면은 산업 3종 백필이 끝난 뒤 켠다.
- **건축HUB.** 판정·순서 ADR-0355(#966). 인허가 기본개요 ADR-0356(#971) 병합. 총괄표제부 ADR-0357
  #972 열림. 다음: 부속지번 → 월별 전기·가스 → 철거멸실(ADR-0355 §2).
- **VWorld.** 파일 23개 매일 훑기 ADR-0359(#968) 병합. 토지소유 ADR-0360·0361 #973 열림 + 웹 #976
  초안. 용도지역지구 규제 구역 ADR-0362 #974 초안. 500MB 넘는 파일은 RAON 레인(ADR-0170).
- **주소 전자지도 11종.** 사장의 juso.go.kr 이용 신청이 먼저(ADR-0355 §3).
- **기타 공공 일괄 12종.** 원천마다 실제 제공자를 먼저 적고 data.go.kr 파일 수집기를 넓힌다(ADR-0355 §4). 미착수.
- **법원경매.** 수집·Silver 켜기 #969, 재수집 목록은 lakehouse 에서 계산 ADR-0352 #967, 필지 패널 경매 칸
  ADR-0353·0382 #978. 사진은 물건상세 수집(ADR-0289/0290 2단계) 뒤.
- **공장 가동 신호.** 건축HUB 에너지 파일에는 산업용이 빠져 있다. 원천 조사는
  [ADR-0406](../adr/0406-the-factory-operating-signal-comes-from-pension-workplaces-and-kicox-complex-statistics-not-electricity.md)
  (Proposed)다. 추천은 필지에 국민연금 사업장, 단지에 KICOX 가동률·현황통계다. 사장의 순위 결정을 기다린다.
- **규칙: 모든 데이터셋은 법정동 코드 변경을 따른다.** 대체가 닿는 코드 아래 PNU 만 다시 계산한다
  (ADR-0329 §3 `views.changed_codes`). 지금 쓰는 곳은 필지 패널 Gold 하나다. 실거래 Gold(ADR-0350)는
  여기에 배선해야 한다.

### 6. 사장 할 일 (대기)

- [ ] 스냅숏 만료 #938 — 첫 예약 실행 전 `lakehouse-maintenance.sh --dry-run` 한 번.
- [ ] 단가 vintage 2단계 실행 → 그 뒤 #919 병합.
- [ ] `parcel_boundaries` rewrite-all 한 번, 약 ₩300(ADR-0316 §4).
- [ ] #973 병합 뒤 토지소유 Gold 첫 재구축 시간 재기와 cutover(그 PR 본문의 운영자 명령).
- [ ] RAON 레인은 사람 단계 없이 매일 8개씩 새것부터 받는다(ADR-0514) — 배포 뒤 첫 실행(03:40 UTC)의 `| raon` 줄과 용도지역지구 20261109 의 경기·경북 Bronze 도착, parcel-zoning 재발행을 확인한다.
- [ ] juso.go.kr 전자지도 이용 신청(ADR-0355 §3, 처리 10일).
- [ ] 규제 구역 지도 타일을 만들지 결정(#974: 타일은 계획만, 월 ₩100 미만 추정).
- [ ] `MERGE` 로 쓰는 Gold 가 Spark 를 떠나는 길 결정([ADR-0561](../adr/0561-a-gold-that-merges-leaves-spark-by-becoming-a-table-of-appended-row-versions-the-publisher-commits.md), Proposed):
      아직 없는 두 Gold(주변·실거래)를 "바뀐 행을 쌓는 표"로 태어나게 하고 나머지는 그 둘을 잰 뒤 정한다는 권고. 받아들이기 전에는 짓지 않는다.
- [x] 가린 지번 추정 진행 결정 — 승인(10-31 "해보자"), 구현은 #982.

### 7. 미룬 것

| 항목 | 이유 | 다시 꺼낼 때 | 결정 기록 |
| --- | --- | --- | --- |
| KAMCO 국유·공공 부동산 API | 사장 "일단 뒤로, 하던 것부터"(10-29) | 사장이 다시 꺼낼 때 | 결정 기록 없음 |
| Kafka 하나로 합치기 | 운영 Kafka 소비자가 없다 | 데이터 계약 뒤, 또는 Kafka 이벤트 소비자가 생길 때 | [ADR-0118](../adr/0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) ③ (보류는 [ADR-0336](../adr/0336-every-published-schema-lives-in-one-registry-and-ci-holds-every-consumer-to-it.md)이 적음) |
| 저장소 비공개 전환 | 개발 먼저, 비공개면 CI 비용 발생(10-03) | 사장이 다시 꺼낼 때 — 새 ADR 로 | 결정 기록 없음 |
| 열쇠 오프사이트 보관(Zitadel masterkey·백업 키) | 사장 "나중에"(10-12) | 실사용자 오픈 전, 직원 추가, 서버 이전 | 결정 기록 없음 |
| 크기 상한 건물 묶음(256 KiB 넘는 동 분할) | Workers Paid 라 실트래픽 근거를 기다림(10-06) | 큰 동에서 p95/p99 문제가 보일 때 | 결정 기록 없음 |

### 8. 하이엔드 정리 (사장 지시 2026-11-04)

- 결정: [ADR-0410](../adr/0410-the-repository-is-held-to-five-testable-cleanup-rules-and-every-artifact-comes-from-one-declared-definition.md)(정리 규칙 다섯 개, 선언 하나에서 생성, 범주마다 0 을 지키는 가드).
- 전수 목록과 순서: [하이엔드 정리 인벤토리](./high-end-cleanup-inventory.md). 항목·순서·진행은 거기가 정본이다.

### 9. 준비도 일괄 점검 (2026-11-07, ADR-0442)

- 결정: [ADR-0442](../adr/0442-production-readiness-is-one-table-a-day-built-from-the-definitions-sorted-by-what-users-see.md).
  막힘을 하나씩 고치며 다음 막힘을 찾지 않는다. `foundation-readiness-report.timer` 가 매일 22:10 UTC 에
  `scripts/ops/readiness_report.py` 로 모든 작업·레인·원천·호스트를 정의에서 읽어 이 표를 만든다
  (`/var/lib/foundation-platform/readiness/readiness.md`). 아래는 그 첫 판(2026-11-07 11:30–11:45Z, ai-server 읽기 전용 실측)에
  주인을 붙인 사본이다. 사용자 영향 순.
- 주인: W8c 법원경매 · W16 가드 · W17c Cloudflare · W18b 자동 승격 · W19 parquet · W20 낡은 필지/배포 보류 · W21 실패 유닛,
  또는 NEW(이 점검이 고친 PR, 또는 맡길 brief).

| # | 영향 | 막힘 | 실측 | 주인 |
| --- | --- | --- | --- | --- |
| 1 | 화면 | by-PNU 레인 8 개가 신선도 예산 48 h 를 넘김 | manifest 나이: `parcel` 196 h · `building` 194 h · `parcel-forest-ledger` 196 h · `parcel-transfer-history` 196 h · `parcel-zoning`·`parcel-price`·`parcel-characteristics`·`parcel-land-rights` 491 h. 표본 20 PNU 는 실 호스트명에서 20/20 응답(모니터·엣지 프로브 통과) — 깨진 것이 아니라 옛 데이터 | W19 + W20 (아래 3·4) |
| 2 | 화면 | `unit-price` 레인이 예산 직전 | manifest 46.7 h / 48 h. Gold `gold.unit_price` 11-06 15:20 커밋이 마지막 굽기(11-06 12:05) 뒤 | W18b |
| 3 | 화면 | `zoning_tile_bake` 가 11-03 22:22 부터 매 run 실패(14 일 무성공) | `maxzoom tiles miss 1820 of the 655599 served ids: 1178 sub-pixel (allowed, ADR-0262) and 642 that are not` | NEW — brief B3 |
| 4 | 화면 먹이 | `gold_panel_rebuild` 11-02 08:47 이후 무성공 | 증분 병합이 160 분 상한에 timeout(11-05·11-06), 배포 정지로 signal 3 회. `gold.parcel_panel` 10-30 커밋이 입력 `silver.land_transfer_history` 11-04 커밋보다 뒤처짐 | W20 (ADR-0438·0440 반영 뒤 재측정) |
| 5 | 화면 | 5 레인 미서빙 — `parcel-regulation-zones`·`-sub-parcels`·`-energy`·`-demolitions`·`-real-transaction` | 굽기 작업 꺼짐(첫 발행은 자동 승격). `parcel-regulation-zones` cut-over g1 이 10:27 부터 도는 중 | W18b |
| 6 | 화면 먹이 | 매시 1 슬롯 작업이 4.5 h 넘게 시작되지 않음 | `onbid_public_auction_silver` 마지막 시작 06:52, `map_edit_fold_admin` 06:35. 그 사이 spark 풀 전부를 07:00 Silver 새로 고침·10:27 cut-over 가 잡음 | W18b (용량·우선순위) |
| 7 | Silver | VWorld 토지 Silver 8 레인이 매일 실패 | `Parquet error: Disabled feature at compile time: flate2`(11-04 이후). Bronze 대 Silver: 토지권리 11-06 vs 10-16, 토지이용계획 10-20 vs 10-15, 임야 11-06 vs 10-26, 토지이동 11-06 vs 11-04 | W19 |
| 8 | Silver | `rt_molit_real_transaction_silver` 51 h 무성공 | `ModuleNotFoundError: lakehouse_ingest` — 호스트가 `python3 -I` 로 부르는데 #1008 이 모듈 맨 위에 import 를 둠 | NEW — #1141 |
| 9 | Silver | `silver_refresh_building_register_unit_areas` 실패 | `Error: the release has no source object`(11-07 07:00 첫 실패, 11-06 성공) | NEW — brief B4 |
| 10 | 수집 | `court_auction_collect` 거부 | `courtauction.go.kr refused the run (ipcheck=false); stopped after 1528 requests`(11-06 17:31) | W8c |
| 11 | 수집 | `legal_dong_code_changes` 11-04 10:01 이후 무성공 → `lineage_stewardship` 실패 | 파서 `row 1: 1 cells, expected 11`(code.go.kr 응답). 같은 날 다른 IP 에서는 53,403 코드가 보임. 계보는 `industrial-complex region codes did not follow` 로 실패, 지도가 경계 발행·공매 Gold·품질·유지보수·미디어 대조를 막힘으로 표시. 법정동 변경이 멈추면 ADR-0329 `views.changed_codes` 도 멈춘다 | NEW — brief B2 |
| 12 | 운영 | `shape_ledger_check` 10-31 이후 매일 실패 | 원장 48 h+ 낡음(28 표), 호스트에 원장 버킷 열쇠 없음 | W50 — ADR-0476: 호스트가 버킷을 코드로 만들고 실행마다 20분 열쇠로 올리며 CI 가 공개 주소에서 읽는다. 사장 할 일 없음 |
| 13 | 운영 | 수집 경보 세 개가 울릴 수 없다 | foundation-api 쿼리가 없는 열 `bronze_object.payload_size_bytes`(실제 `size_bytes`)를 읽어 `foundation_platform_ingestion_run_*` 가 0 줄. 열만 고치면 24 h 규칙이 원천 약 85/105 개에 울린다 — 기대 주기가 105 개 중 11 개에만 있다. `catalog.lakehouse_batch_run` 도 규칙이 거르는 행 0 개 | NEW — brief B1 |
| 14 | 호스트 | 실패 유닛 35 개 | 대부분 위 실패의 남은 상태, `foundation-by-pnu-public-auction-bake.service` 는 not-found | W21 |
| 15 | 호스트 | 정상 | 메모리 가용 42.2/62.6 GiB, 스왑 3.0/4.0 GiB 사용, 부하 19; 디스크 `/` 33%·`/data` 54%; `MemoryMax` 90% 넘은 유닛 0 | — |
| 16 | 호스트 | `foundation-postgres-restore-drill.timer` 한 번도 안 돎 | 첫 실행 11-08 05:50(일요일) 예정 — 막힘 아님 | (W6 첫 실행 확인) |

수집 원천(Bronze 마지막 객체, `catalog.bronze_object`): 공매 6 종 0 h, 실거래 12 종 11-06 17:43(공급자 하루 한도), 법원경매
물건 11-06 16:05, VWorld 매일 훑기 파일 11-06, 건축HUB 61 종 10-20(월간), code.go.kr 10-28(위 11), 등록공장 10-19.
`vworldkr__sandan_*`·`vworldkr__boundary_census_*` 는 07-02 이후 객체가 없는데 기대 주기가 선언되지 않아 판정할 수 없다(B1).

### 10. 수집한 데이터가 화면에 닿는가 (사장 목표 "보여 주는 것이 이긴다", 2026-11-08)

- Silver 표마다 그 표가 닿는 by-PNU 레인·타일과, 그 접점이 공짱 화면을 먹이는지(`gongzzang-panel` 로 가는 연결 또는
  레지스트리의 `web` 소비자)를 정의(`platforms/foundation-platform/definitions`)에서 만든다. 손으로 고치지 않는다 —
  `python3 scripts/catalog/render-showing-inventory.py` 가 아래 표를 다시 쓰고, `--check` 가 낡은 표를 거부하며,
  `--summary` 가 판정별 개수를 찍는다(합계 줄을 두지 않는 이유: ADR-0176).
- 판정: **화면** = 켜진 작업이 채우는 접점이 화면을 먹인다. **길 있음·꺼짐** = 화면까지 길은 선언됐지만 채우는 작업이 꺼져
  있다(대개 첫 발행 대기, ADR-0435 자동 승격). **서빙만** = 켜진 접점이 있으나 화면이 읽지 않는다. **적재 꺼짐** = 길도 없고
  Silver 를 채우는 작업이 모두 꺼져 있어 표에 행이 아직 없다 — 다음 걸음은 Gold 가 아니라 적재 작업의 첫 실행이다(ADR-0386 §3).
  **없음** = Gold·서빙 길이 없다.
- 순서는 산업 부동산 사용자가 먼저 찾는 것(공장·창고 → 토지 → 건물 → 거래·경매 → 물류 입지)이다. "없음"인 위쪽 줄부터
  Gold 종류 또는 기존 레인의 칸 + 공짱 패널 칸으로 잇는다(한 데이터셋 = PR 하나).

<!-- BEGIN GENERATED showing-inventory: python3 scripts/catalog/render-showing-inventory.py -->

| 사용자 가치 | Silver 표 | by-PNU 레인 | 타일 | 공짱 화면 |
| --- | --- | --- | --- | --- |
| 공장·산업단지 | 산업단지 경계 (`silver.industrial_complex_boundaries`) | — | `complex-tiles` | 화면 |
| 공장·산업단지 | 산업단지 고시 첨부 (`silver.industrial_complex_notice_attachment`) | — | — | 화면 |
| 공장·산업단지 | 산업단지 고시 읽은 기록 (`silver.industrial_complex_notice_detail`) | — | — | 화면 |
| 공장·산업단지 | 산업단지 기본 정보 (`silver.industrial_complexes`) | — | `complex-tiles` | 화면 |
| 공장·산업단지 | 전국 등록공장 (`silver.registered_factory`) | `parcel-factories` | — | 화면 |
| 공장·산업단지 | 산업단지와 필지 소속 (`silver.complex_parcel_memberships`) | — | — | 없음 |
| 토지 | 필지별 토지특성 (`silver.land_characteristic`) | `parcel`<br>`parcel-real-transaction` | — | 화면 |
| 토지 | 필지별 임야대장 (`silver.land_forest_ledger`) | `parcel` | — | 화면 |
| 토지 | 필지별 공시지가 (`silver.land_individual_price`) | `parcel` | — | 화면 |
| 토지 | 필지별 토지소유 (`silver.land_ownership`) | `parcel-ownership`<br>`parcel-real-transaction` | — | 화면 |
| 토지 | 필지별 대지권 등록 (`silver.land_right_registration`) | `parcel` | — | 화면 |
| 토지 | 필지별 토지이동이력 (`silver.land_transfer_history`) | `parcel`<br>`parcel-real-transaction` | — | 화면 |
| 토지 | 필지별 토지이용계획 (`silver.land_use_plan`) | `parcel` | — | 화면 |
| 토지 | 용도지역지구 구역 (`silver.land_use_zone_area`) | `parcel-regulation-zones (꺼짐)` | `zoning-overlay-tiles` | 화면 |
| 토지 | 용도지역 코드 사전 (`silver.land_use_zone_code`) | `parcel` | — | 화면 |
| 토지 | 필지 경계 (`silver.parcel_boundaries`) | `parcel`<br>`parcel-nearby (꺼짐)`<br>`parcel-real-transaction`<br>`parcel-regulation-zones (꺼짐)` | `parcel-tiles` | 화면 |
| 토지 | 필지 계보 (`silver.parcel_lineage`) | `parcel`<br>`parcel-real-transaction` | — | 화면 |
| 건물 | 건물 전기 사용량(월별) (`silver.building_energy_monthly_electricity`) | `parcel-energy` | — | 화면 |
| 건물 | 건물 가스 사용량(월별) (`silver.building_energy_monthly_gas`) | `parcel-energy` | — | 화면 |
| 건물 | 건축물 정기점검이력 (`silver.building_maintenance_regular_inspection_history`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축인허가 부설주차장 (`silver.building_permit_annex_parking`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축인허가 기본개요 (`silver.building_permit_basis_outline`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축인허가 동별개요 (`silver.building_permit_building_outline`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축인허가 철거멸실관리대장 (`silver.building_permit_demolition_loss`) | `parcel-demolitions` | — | 화면 |
| 건물 | 건축인허가 층별개요 (`silver.building_permit_floor_overview`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축인허가 대수선 (`silver.building_permit_major_repair`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축인허가 주차장 (`silver.building_permit_parking`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축인허가 대지위치 (`silver.building_permit_site_location`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축인허가 공작물관리대장 (`silver.building_permit_structure_ledger`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축인허가 가설건축물 (`silver.building_permit_temporary_building`) | `parcel-building-permits` | — | 화면 |
| 건물 | 건축물대장 공동주택가격 (`silver.building_register_apartment_price`) | `building`<br>`unit-price` | — | 화면 |
| 건물 | 건축물대장 전유부 (`silver.building_register_exclusive_unit`) | `building`<br>`unit-price` | — | 화면 |
| 건물 | 건물 층별 정보 (`silver.building_register_floors`) | `building` | — | 화면 |
| 건물 | 건축물대장 총괄표제부 (`silver.building_register_master`) | `parcel-register-masters` | — | 화면 |
| 건물 | 건축물대장 부속지번 (`silver.building_register_sub_parcel`) | `parcel-sub-parcels` | — | 화면 |
| 건물 | 건물 표제부 (`silver.building_register_titles`) | `building`<br>`parcel`<br>`parcel-building-permits`<br>`parcel-real-transaction`<br>`unit-price` | — | 화면 |
| 건물 | 호별 전유·공용 면적 (`silver.building_register_unit_areas`) | `building` | — | 화면 |
| 건물 | 건물 호별 정보 (`silver.building_register_units`) | `building`<br>`unit-price` | — | 화면 |
| 건물 | 주택인허가 기본개요 (`silver.housing_permit_basis_outline`) | `parcel-building-permits` | — | 화면 |
| 건물 | 세대별 기준일 공시가격 (`silver.unit_official_price`) | `building`<br>`unit-price` | — | 화면 |
| 건물 | 건축인허가 지역지구구역 (`silver.building_permit_district_zone`) | — | — | 없음 |
| 건물 | 건축인허가 전유공용면적 (`silver.building_permit_exclusive_common_area`) | — | — | 없음 |
| 건물 | 건축인허가 주택유형 (`silver.building_permit_housing_type`) | — | — | 없음 |
| 건물 | 건축인허가 도로명대장 (`silver.building_permit_road_ledger`) | — | — | 없음 |
| 건물 | 건축인허가 오수정화시설 (`silver.building_permit_sewage_facility`) | — | — | 없음 |
| 건물 | 건축인허가 호별전유공용면적 (`silver.building_permit_unit_exclusive_common_area`) | — | — | 없음 |
| 건물 | 건축인허가 호별개요 (`silver.building_permit_unit_outline`) | — | — | 없음 |
| 건물 | 주택인허가 행위개요 (`silver.housing_permit_act_outline`) | — | — | 없음 |
| 건물 | 주택인허가 행위호전유공용면적 (`silver.housing_permit_act_unit_exclusive_common_area`) | — | — | 없음 |
| 건물 | 주택인허가 부대시설 (`silver.housing_permit_ancillary_facility`) | — | — | 없음 |
| 건물 | 주택인허가 부설주차장 (`silver.housing_permit_annex_parking`) | — | — | 없음 |
| 건물 | 주택인허가 동별개요 (`silver.housing_permit_building_outline`) | — | — | 없음 |
| 건물 | 주택인허가 지역지구구역 (`silver.housing_permit_district_zone`) | — | — | 없음 |
| 건물 | 주택인허가 전유공용면적 (`silver.housing_permit_exclusive_common_area`) | — | — | 없음 |
| 건물 | 주택인허가 층별개요 (`silver.housing_permit_floor_overview`) | — | — | 없음 |
| 건물 | 주택인허가 관리공동부대복리시설 (`silver.housing_permit_managed_communal_ancillary_welfare_facility`) | — | — | 없음 |
| 건물 | 주택인허가 관리공동형별개요 (`silver.housing_permit_managed_communal_type_outline`) | — | — | 없음 |
| 건물 | 주택인허가 주차장 (`silver.housing_permit_parking`) | — | — | 없음 |
| 건물 | 주택인허가 오수정화시설 (`silver.housing_permit_sewage_facility`) | — | — | 없음 |
| 건물 | 주택인허가 대지위치 (`silver.housing_permit_site_location`) | — | — | 없음 |
| 건물 | 주택인허가 호별개요 (`silver.housing_permit_unit_outline`) | — | — | 없음 |
| 건물 | 주택인허가 복리분양시설 (`silver.housing_permit_welfare_sale_facility`) | — | — | 없음 |
| 거래·공매·경매 | 온비드 공고 (`silver.onbid_announcement`) | `parcel-public-auction` | — | 화면 |
| 거래·공매·경매 | 온비드 공고 입찰결과 (`silver.onbid_announcement_bid_result`) | `parcel-public-auction` | — | 화면 |
| 거래·공매·경매 | 온비드 공고상세 (`silver.onbid_announcement_detail`) | `parcel-public-auction` | — | 화면 |
| 거래·공매·경매 | 온비드 물건 입찰정보 (`silver.onbid_item_bid_info`) | `parcel-public-auction` | — | 화면 |
| 거래·공매·경매 | 온비드 물건 입찰결과 (`silver.onbid_item_bid_result`) | `parcel-public-auction` | — | 화면 |
| 거래·공매·경매 | 온비드 부동산 물건 (`silver.onbid_real_estate_item`) | `parcel-public-auction` | — | 화면 |
| 거래·공매·경매 | 온비드 물건상세 (`silver.onbid_real_estate_item_detail`) | `parcel-public-auction` | — | 화면 |
| 거래·공매·경매 | 온비드 물건 사진 (`silver.onbid_real_estate_item_photo`) | `parcel-public-auction` | — | 화면 |
| 거래·공매·경매 | 실거래가 (`silver.real_transaction`) | `parcel-real-transaction` | — | 화면 |
| 거래·공매·경매 | 법원경매 감정평가 (`silver.court_auction_appraisal`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 감정요항표 (`silver.court_auction_appraisal_point`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 감정평가서 (`silver.court_auction_appraisal_report`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 사건 (`silver.court_auction_case`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 목록별 종국결과 (`silver.court_auction_case_object`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 당사자 구분 (`silver.court_auction_case_party`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 관련사건 (`silver.court_auction_case_related`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 공통코드 (`silver.court_auction_code`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 진행 물건 (`silver.court_auction_item`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 제시외건물 (`silver.court_auction_item_annex`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 목록내역 (`silver.court_auction_item_object`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 사건 사진 (`silver.court_auction_item_photo`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 기일내역 (`silver.court_auction_item_schedule`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 매각물건명세서 요약 (`silver.court_auction_item_spec`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 매각결과 (`silver.court_auction_sale_result`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 현황조사 (`silver.court_auction_survey`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 현황조사 목록 (`silver.court_auction_survey_object`) | `parcel-court-auction (꺼짐)` | — | 길 있음·꺼짐 |
| 거래·공매·경매 | 법원경매 감정평가서 읽은 기록 (`silver.court_auction_appraisal_report_read`) | — | — | 없음 |
| 거래·공매·경매 | 법원경매 사건 문서 읽은 기록 (`silver.court_auction_case_document_read`) | — | — | 없음 |
| 거래·공매·경매 | 법원경매 물건상세 읽은 기록 (`silver.court_auction_item_detail`) | — | — | 없음 |
| 물류 입지 | 공항 (`silver.airport`) | `parcel-nearby (꺼짐)` | — | 길 있음·꺼짐 |
| 물류 입지 | 고속도로 출입시설 (`silver.highway_access_point`) | `parcel-nearby (꺼짐)` | — | 길 있음·꺼짐 |
| 물류 입지 | 철도역 (`silver.railway_station`) | `parcel-nearby (꺼짐)` | — | 길 있음·꺼짐 |
| 물류 입지 | 도시철도역 (`silver.subway_station`) | `parcel-nearby (꺼짐)` | — | 길 있음·꺼짐 |
| 물류 입지 | 항만 (`silver.port`) | — | — | 없음 |
| 주변 생활 | 상가(상권) (`silver.commercial_district`) | `parcel-nearby (꺼짐)` | — | 길 있음·꺼짐 |
| 주변 생활 | 초중등학교 위치 (`silver.school_location`) | `parcel-nearby (꺼짐)` | — | 길 있음·꺼짐 |
| 주변 생활 | 대학·전문대학 (`silver.university`) | `parcel-nearby (꺼짐)` | — | 길 있음·꺼짐 |
| 주변 생활 | 행정동 성별 연령별 인구 (`silver.dong_population`) | — | — | 없음 |
| 주변 생활 | 행정동 세대원수별 세대수 (`silver.household_count`) | — | — | 없음 |
| 행정·내부 | 행정경계(읍면동) (`silver.administrative_boundaries`) | — | `admin-tiles` | 화면 |
| 행정·내부 | 지도 편집 원장 (`silver.map_edit_ledger`) | — | `admin-tiles`<br>`complex-tiles`<br>`parcel-tiles` | 화면 |
| 행정·내부 | 법정동 연계정보 (`silver.adm_dong_code_linkage`) | — | — | 없음 |
| 행정·내부 | 건축물대장 공동주택가격 판 버전 (`silver.building_register_apartment_price_versions`) | — | — | 없음 |
| 행정·내부 | 필지고유번호변동연혁 (`silver.parcel_number_change_history`) | — | — | 없음 |
| 행정·내부 | 필지 ID 원장 (`silver.parcel_registry`) | — | — | 없음 |
| 행정·내부 | 중개사무소 소속 인원 (`silver.real_estate_broker_member`) | — | — | 없음 |
| 행정·내부 | 중개사무소 등록 (`silver.real_estate_broker_office`) | — | — | 없음 |
| 행정·내부 | 실거래가 적재 거부 줄 (`silver.real_transaction_rejected`) | — | — | 없음 |
| 행정·내부 | 세대별 기준일 공시가격 판 버전 (`silver.unit_official_price_versions`) | — | — | 없음 |

<!-- END GENERATED showing-inventory -->

### 11. 대기업 수준과의 간극 (2026-11-11)

운영을 대기업 기준(관측·가용성·환경 분리·비밀 보관)에 대 본 간극. 근거는 링크한 ADR 이 정본이고, 여기는 행마다 주인·상태·다음 단계만 둔다.

| # | 간극 | 주인 | 상태 | 다음 단계 |
| --- | --- | --- | --- | --- |
| 1 | 조용한 실패: 유닛은 성공했는데 결과가 없다(Dagster 코드 위치 3 일, 법원경매 물건상세 0 건, 원장 사슬 미설치, 적재 10 개 거절, data_go_kr 미적재) | W56 | 결정·구현 draft PR — [ADR-0480](../adr/0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md)(작업 90 개 결과 기대 선언, 러너 결과 원장, 주인·런북 경보, 주간 합성 화면 탐침) | 배포 뒤 첫 기한이 지나면 울린 알림을 원장과 대조. 법원경매 물건상세는 오늘도 Bronze 0 건(사건 2 열림) — 수집 경로 brief |
| 2 | 운영 호스트 하나: 신원(Zitadel)·운영 DB·서빙 제어에 HA 없음. 서빙 자체는 Cloudflare Worker·서울 CloudFront 앞문([ADR-0461](../adr/0461-korean-visitors-enter-through-a-cloudfront-front-door-in-seoul-in-a-separate-aws-account-applied-by-the-host.md))이라 호스트가 죽어도 살아남는다 | platform | 미착수. 백업·주 1 회 복원 리허설만 있음 | 대기 호스트와 DB 복제(스트리밍 복제 + 자동 승격) 설계 ADR, 비용 계산 후 사장 보고 |
| 3 | 표본 데이터가 있는 스테이징 없음: 검증이 운영에서 일어난다 | platform | 미착수 | 합성 표본(PNU 99999 대역)으로 같은 compose·같은 정의를 띄우는 스테이징 brief; 배포 전 합성 화면 탐침([ADR-0480](../adr/0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md) §6)을 그 위에서 |
| 4 | 오케스트레이터 둘(Airflow·Dagster) | lakehouse | 이행 중 — [ADR-0431](../adr/0431-scheduled-jobs-move-from-airflow-to-dagster-one-family-at-a-time-after-a-dual-run-in-which-both-decide.md)·[ADR-0458](../adr/0458-a-job-moves-from-airflow-to-dagster-on-agreeing-decisions-of-every-kind-it-makes-not-on-calendar-days.md), 작업별 `dagster_stage` | 가족마다 일치 결정 문턱을 넘으면 `proxied` → `dagster`; 두 쪽 모두 같은 결과 원장(ADR-0480 §2)에 쌓인다 |
| 5 | 오프사이트 키 보관: Zitadel masterkey·백업 암호 키 | 사장 | 보류(사장 "나중에") | 사장이 꺼낼 때 착수 |
| 6 | 남은 손 가드: 도구 규칙(Semgrep·cargo-deny·check-jsonschema 등)으로 바꿀 셸 가드 | W16 | 진행 중 — [ADR-0410](../adr/0410-the-repository-is-held-to-five-testable-cleanup-rules-and-every-artifact-comes-from-one-declared-definition.md), 2026-11-08 실물 대조 38 개 중 끝남 12·절반 3·남음 23 | [하이엔드 정리 인벤토리](./high-end-cleanup-inventory.md) A-3 표의 순서대로; 행 상태는 그 표가 정본 |

## 우선순위 0 — 출시 전 필수 게이트

### Kafka 이벤트 전달

- [ ] 운영 Kafka와 Schema Registry 소유자·배포 대상 확정
- [ ] TLS/SASL, Schema Registry HTTPS/CA, Secret Manager 주입 검증
- [ ] `foundation-platform.catalog.collection-raw-written.v1` 토픽의 파티션·복제·보존·ACL 확정
- [x] 소비자가 `event_id`로 중복 제거하고 Bronze claim-check를 읽는 계약 테스트 추가
- [ ] 발행 지연·실패·재시도·격리·consumer lag·스키마 오류 알림 연결
- [ ] `dual_publish_legacy=1` 관찰 기간과 Kafka 비활성화 롤백 절차 증명
- [ ] GitHub `kafka-integration` 필수 게이트가 실제 보호 브랜치에서 통과하는지 확인

현재 코드와 실행 명령은
[`foundation-kafka-outbox-contract-test.md`](../../platforms/foundation-platform/docs/runbooks/foundation-kafka-outbox-contract-test.md)와
[`0028-foundation-kafka-raw-written-design.md`](../../platforms/foundation-platform/docs/adr/0028-foundation-kafka-raw-written-design.md)에 있다.

> **2026-10-28:** [ADR-0118](../adr/0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) §3은
> Foundation 이벤트와 DataHub 가 Kafka 하나를 같이 쓰기로 정했지만, 그 실행 순서 ③(Kafka)은 아직
> 착수 전이다 — main 에서 Kafka 이미지는 DataHub 전용 compose(`platforms/foundation-platform/infra/datahub/compose.yml`)에만
> 있다. 위 항목은 그 착수 때까지 열어 둔다.

### 데이터 원장·복구

- [ ] 운영 R2/Postgres 버킷·DB를 개발/CI와 분리하고 런타임 가드로 강제
- [ ] 운영 R2 Data Catalog writer/reader 토큰을 사용자 지정 API 정책으로 분리하고, 어드민
  토큰을 공용 런타임 자격증명으로 사용하지 않도록 검증

  > **2026-08-06 현재 상태:** 라이브 레인을 처음 실행하기 위해 **어드민 범위 카탈로그 토큰을
  > 임시로 쓰고 있다.** 위 원칙이 허용하는 "임시 대안"에 해당하며, 그 전 토큰은 개체 권한만 있어
  > 카탈로그가 `403 Forbidden`으로 거절했다 — 개체 API 권한과 Iceberg 카탈로그 권한이 별개라는
  > 이 문서의 서술이 실측으로 확인됐다. **좁히기 전에는 닫지 말 것.** 필요한 최소 권한은 이제
  > 관측으로 정할 수 있다: 스모크가 실제로 부른 것은 `GET /v1/config`, 네임스페이스 조회,
  > 테이블 로드뿐이고 쓰기는 없었다.
- [ ] 계정 수준 카탈로그 격리가 필요하면 출시 전에 운영 Cloudflare 계정 분리 여부를 결정하고,
  분리 시 객체·Iceberg 메타데이터 이전과 무중단 전환 계획을 승인
- [ ] Bronze 불변성, Postgres 백업/복구 리허설, RPO/RTO 증거 확보
- [ ] 수집 원본·ledger·outbox·quarantine의 보존 기간과 삭제 승인 절차 확정

## 우선순위 1 — 실제 파이프라인 완성

층 자동화의 구현은 [ADR-0127](../adr/0127-floor-duckdb-staging-preserves-normalization-and-native-resource-contracts.md)과
[ADR-0128](../adr/0128-floor-inputs-bind-to-bronze-and-scalar-retries-bind-to-their-append.md)를 따른다.
기존 Bronze 원장에서 완전한 원본 쌍을 선택하고, 입력·완료 산출물·Iceberg 커밋을 대조한 뒤
필요한 단계만 실행한다. `orchestration/jobs.v1.json`이 예약·서비스·계보의 단일 정의다.
[ADR-0132](../adr/0132-release-activation-preserves-enabled-job-identities.md)는 활성화 때
기존 enabled 작업의 누락을 거부한다. 코드 병합과 운영 설치·실행 검증은 별개다.

- [ ] 병합된 릴리스로 FLOOR 서비스·설정을 설치하고 실제 예약 및 재시도 결과 검증
- [ ] 새 기준월의 Bronze → FLOOR Silver 및 후속 조회 반영 검증
- [ ] 실제 R2 Class A 요청 감소와 원본/완료 산출물 재사용 비용 측정
- [ ] [고정 입력 판](../adr/0130-panel-input-snapshots-are-complete-and-bound-before-spark.md)으로 전체 범위 패널을 검증 후 승격
- [ ] [지번 연결 입력 근거](../adr/0131-parcel-lineage-binds-every-input-and-derivation-identity.md)의 실제 원장·표·시점 대조 후 실행


- [ ] 국가 수집 대상별 bulk/API 선택과 실제 공급자 자격증명·쿼터 검증
- [ ] Bronze → Silver → Gold를 실제 R2/Iceberg backend 자격증명으로 실행하고 결과를 검증

  > **2026-08-06 실측.** 라이브 레인 셋을 처음 돌렸고, 실버가 **어디까지 와 있는지가 관측으로
  > 확정됐다.**
  >
  > - **R2 객체 경로: 통과.** 쓰기·읽기·삭제 왕복과 인벤토리 조회가 실제 버킷에서 성공했다.
  > - **Iceberg 카탈로그: 통과.** 네임스페이스는 `silver`, `gongzzang_silver`,
  >   `tiles_slice_proof`이고 `silver`에는 `building_register_units`와
  >   `building_register_unit_areas`가 있다. 그 테이블의 현재 스냅샷을 읽는 데 성공했다.
  > - **`silver.industrial_complexes`는 존재하지 않는다.** 스모크의 기본 대상이 이것이어서 레인이
  >   실패했고, 원인은 파이프라인이 아니라 **없는 테이블을 가리킨 설정**이었다.
  > - **data.go.kr: 상대 서버 오류.** HTTP 502에 `returnReasonCode 04`(HTTP 에러)로, 인증 단계에
  >   도달하지도 못했다. 서비스 키 문제가 아니므로 재시도 대상이다.
  >
  > 남은 구간은 실버에서 Postgres canonical로 넘어오는 길이다. 건축물대장은 실버까지 와 있는데
  > `catalog.building`·`catalog.building_unit`에는 생산자가 없다
  > ([기반 목표](./foundation-goals.md) G1의 21표에 둘 다 포함된다). **읽을 데이터가 없어서가
  > 아니라 읽는 코드가 없어서다.**
  >
  > **2026-10-28:** 두 표는 [ADR-0194](../adr/0194-retire-the-five-postgres-copies-that-r2-already-holds.md)가
  > 지웠다(`platforms/foundation-platform/migrations/20261014021000_drop_the_catalog_parcel_building_and_unit_copies.sql`).
  > 건물은 R2 by-PNU 묶음이 서빙하므로 이 생산자 공백은 더 이상 열린 일이 아니다.
- [ ] dbt Gold 모델 또는 Spark Gold projection 중 하나를 정식 Gold 계약으로 확정
- [ ] Trino/Spark/Iceberg catalog 연결, snapshot 승격·롤백·재처리 증명
- [ ] LLM 정규화 provider, 비용/쿼터, proposal 승인·적용 권한과 감사 로그 확정
- [x] production orchestrator 선택, 소유자·스케줄·재시도·취소·롤백을 ADR로 결정 — Airflow
  ([ADR-0118](../adr/0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md),
  [ADR-0122](../adr/0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md))

### 카탈로그 도메인 모델 교정

결정과 근거는 [ADR-0019](../adr/0019-membership-is-a-dated-fact-not-a-column.md)가 정본이며,
그 문서의 `남은 부채` 절이 아직 열린 항목을 소유한다. 여기서는 착수 순서만 고정한다.

- [ ] 1단계 — `catalog.parcel_complex_membership`을 추가하고 기존 `parcel.complex_id`에서
      백필한다. 기존 컬럼을 건드리지 않으므로 단독 배포 가능하다.
- [x] 2단계 — `/complexes/{id}/anchor-summary`의 읽기를 소속 표로 옮기고 `parcel.complex_id`에
      쓰기 금지 트리거를 걸었다. "현재"는 오늘이며 그 술어는 `catalog.parcel_current_complex`
      하나가 소유한다 ([ADR-0022](../adr/0022-current-means-today-and-one-view-says-so.md)).
      나머지 산단 스코프 읽기 셋은 옮기지 않고 삭제했으므로
      ([ADR-0021](../adr/0021-an-unread-surface-is-deleted-not-migrated.md)) 이전 대상은 넷이
      아니라 하나였다.
- [x] 3단계 — `catalog.parcel.complex_id`와 그 인덱스·FK·얼림 트리거, `ParcelResponse.complex_id`를
      제거했다. OpenAPI 사본 둘과 계약 pin 해시를 함께 갱신했고, 타일 슬라이스 증명이 소속 표를
      경유해서도 통과한다. **산업단지에 속하지 않는 필지가 이제 표현 가능하다.**
- [ ] 4단계 — 부속지번 실버가 실물이 된 뒤 `catalog.building_parcel`과
      `building_unit.building_id`를 착수한다. 지금은 소스가 `planned`라 착수하지 않는다.

> 이 순서는 위 G1 문제와 같은 뿌리다. 소속 표의 **운영** 생산자는
> `silver.complex_parcel_memberships`를 읽는 코드이며, 그 코드가 없는 이유는
> 건축물대장과 같다 — 읽을 데이터가 없어서가 아니라 읽는 코드가 없어서다.

### 공간 데이터 발행 경로

파이프라인의 서빙 끝단이다. 발행 상태 기계와 불변식은
[단일 출처 공간 데이터 공개 아키텍처](../architecture/single-source-spatial-publication.md)가 정본이며,
용어는 [전역 용어집](../glossary.md)의 공간 데이터 발행 용어를 따른다. 여기서는 남은 순서만 고정한다.

- [x] 발행 단위마다 완전한 활성 소스를 하나로 강제하고 포인터를 CAS로만 전진시킨다
- [x] 투영 적재를 정체성 있는 사실로 만들어 릴리스가 지명한 적재의 행만 서빙한다
      ([ADR-0016](../adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md))
- [x] 발행 리비전을 그것이 개정하는 단위에 스코프해 교차 단위 참조를 쓰기 시점에 막는다
      ([ADR-0017](../adr/0017-a-data-revision-belongs-to-the-unit-it-revises.md))
- [ ] `catalog.parcel`에 쓰는 프로덕션 경로의 소유자를 정한다. **이것이 parcels 적재기의
      선행조건이고 지금 주인이 없다.** 현재 그 표에 INSERT하는 것은 테스트와 Docker fixture뿐이고
      정본 실버는 PostgreSQL 밖에 있어 적재기가 읽을 Cargo 의존성이 없다. 선행조건 없이 적재기를
      먼저 만들면 fixture 규모가 천장이 된다.
- [x] 적재 경로와 승격 경로를 CI가 실행하는 검사로 덮는다 — 발행이 적재를 열고 닫는 것부터
      런타임 포인터가 그 적재를 서빙하도록 전진하는 것까지
      ([ADR-0016](../adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md) 남은 부채 4 해소)
- [x] 타일이 실제로 나오는지를 CI에서 확인한다 — `boundary-slice` 잡이
      `scripts/tiles/boundary-slice-proof.sh`를 돌리고 `required/foundation`이
      그것을 센다 ([기반 목표](./foundation-goals.md) G2).

      > **정정 (2026-08-06):** 이 항목은 그 스크립트가 "Docker Compose가 필요하다"고 적었으나
      > **사실이 아니었다.** compose 참조가 하나도 없고, 다이제스트 고정 이미지로 자기 Postgres와
      > Martin을 직접 띄우며 자격증명도 쓰지 않는다. 필요한 것은 Docker뿐이고 CI 러너에는 이미
      > 있었다. 막고 있던 것은 의존성이 아니라 **아무도 부르지 않는다는 사실**이었다 — 워크플로도,
      > xtask도, 가드도 이 스크립트를 참조하지 않았다. 잘못된 장애물 서술 때문에 한 줄이면 될 일이
      > 큰 일로 남아 있었다.
- [ ] PMTiles(정적) 경로는 여전히 CI 밖이다. 동적 경로만 위 잡이 덮으며, 정적 승격은 구현 자체가
      아직 포트 기본 구현(에러)이다 ([ADR-0014](../adr/0014-serving-generation-tracks-one-unit-source-selection.md) 남은 부채 1).
- [ ] 지오메트리 체크섬 계약이 기대는 인코딩 가정을 명시적으로 만든다. 생산자는 자신이 쓸 값을,
      소비자는 파일에서 **다시 파싱한** 값을 해시하므로 `print(parse(print(v))) == print(v)`가
      성립해야 하는데, `serde_json`은 자신이 출력한 17자리 값(`37.300000000000004`)을 1 ULP 다른
      f64(`37.3`)로 파싱한다. 오늘 터지지 않는 이유는 생산자의 좌표가 전부 파싱 결과이고 산술로
      만든 f64가 없기 때문이며, 이는 설계가 아니라 우연이다. 좌표를 변환하는 단계(재투영·단순화·
      반올림)가 들어오면 정상 데이터가 `geometry_sha256 mismatch`로 거부되고 그 메시지는 입력
      손상처럼 읽힌다.

## 우선순위 2 — 규모 확장용 Kafka Connect 전환

현재 직접 `OutboxWorker` 전달기는 작은 규모에서 운영 가능한 경로다. 다음 조건이 발생할
때만 Debezium/Kafka Connect 전환을 시작한다.

- outbox polling 부하 또는 backlog가 운영 목표를 넘는다;
- Kafka 소비자·sink가 여러 개로 늘어 publisher 운영이 병목이 된다;
- DB 변경을 여러 Kafka topic/sink로 표준화할 필요가 생긴다;
- CDC offset, 재시작, replay를 플랫폼 공통 기능으로 관리해야 한다.

전환 순서:

1. Outbox 행 구조와 `event_id`, partition key, Avro 호환성 계약을 동결한다.
2. 분산 Kafka Connect worker와 Debezium PostgreSQL connector를 별도 환경에 배치한다.
3. 기존 publisher와 CDC 경로를 같은 event_id 기준으로 shadow 비교한다.
4. 중복 발행을 막을 단일 production publisher 경로를 선택한다. 두 경로를 동시에 켜지 않는다.
5. connector offset/config/status, schema registry, ACL, lag, replay, 장애 복구를 검증한다.
6. 검증 후에만 기존 polling publisher를 단계적으로 끄고 rollback 경로를 유지한다.

Debezium/Kafka Connect는 Kafka를 원장으로 만들기 위한 도구가 아니다. PostgreSQL outbox를
Kafka로 안정적으로 전달하기 위한 교체 가능한 전달 계층이다. Kafka를 원장으로 하는
event-sourcing은 원본 Bronze 바이트가 아닌, 별도 파생 도메인에서만 별도 ADR로 결정한다.

## 우선순위 3 — 운영 품질과 비용 통제

- [ ] 수집·outbox·Kafka·lakehouse 전체의 trace/run id와 lineage 연결
- [ ] provider outage/quota, Kafka outage, R2 outage, DB failover, schema incompatibility 훈련
- [ ] 부하 테스트로 수집량·outbox backlog·Kafka partition 수·consumer 처리량을 측정
- [ ] 비용 대시보드: R2 저장/egress, Postgres, Kafka 보존/네트워크, LLM 호출 비용
- [ ] 모든 외부 운영 변경은 ADR·runbook·rollback 증거와 함께 반영

## 우선순위 4 — 검증과 빌드 시스템

ADR-0011의 후속 작업은 발견한 테스트를 나중에 잡는 데서 멈추지 않고, 실행 경로 누락이
구조적으로 생기지 않도록 만드는 것이다.

- [ ] 라이브 테스트 자원 요구사항을 테스트 선언에서 직접 파생한다.
- [ ] 일회용 하네스가 자기 저장소를 흘리지 않는지 기계적으로 검사한다. `scripts/verify/integration.sh`와
      `scripts/tiles/boundary-slice-proof.sh`는 `docker rm`에 `-v`가 빠져 실행마다
      PostgreSQL 익명 볼륨을 하나씩 남겼다. 107개(개당 약 115MB)가 쌓여 디스크가 차고 검증 실행이
      죽고 나서야 드러났다. 두 스크립트는 고쳤지만 세 번째가 같은 형태로 들어오는 것을 막는 것은 없다.
- [x] R2 환경변수 가드를 알려진 레거시 거부 목록에서 허용 목록으로 강화한다.
- [x] 문서 메타데이터 마이그레이션 후 문서 CI를 strict 모드로 전환한다.
- [ ] 현재 Cargo 검증을 유지하고, 두 번째 상시 엔지니어·CI 병목·원격 캐시 도입 때만
      Bazel/Buck2 재검토를 시작한다.

`AREAS`·`LiveLane`·`covers` 선언을 별도 구조화 입력으로 통합하는 안은 검토 후 기각했다.
세 선언은 이미 `tools/xtask/src/main.rs` 한 곳에 있고 셸 가드가 같은 파일을 읽는다. 중간
형식을 하나 더 두면 Rust 선언과 그 형식이 어긋날 수 있는 새 경로가 생기며, 이는 통합으로
제거하려던 문제와 같은 종류다.

## ADR이 기록한 남은 부채

각 ADR은 자신이 닫지 못한 것을 `남은 부채` 절에 기록한다. **그 항목의 본문은 ADR이 소유하고
이 표는 어디에 속하는지와 순서만 고정한다.** 항목을 여기에 옮겨 적지 않는다 — 복사본은 한쪽만
고쳐지는 순간 어긋나며, 그것이 아래에서 금지하는 중복과 같은 것이다.

`scripts/guard/roadmap-owns-recorded-debt.sh`가 `남은 부채`를 기록한 ADR이 이 표에서 빠지면
실패한다. 규칙이 산문뿐이던 동안 ADR 8개의 37개 항목 전부가 이 목록 밖에 있었다.

**기록된 항목 수는 남은 일의 수가 아니다.** `남은 부채` 절에는 세 가지가 섞여 있다 — 아직 열린
부채, ADR 자신에 대한 정정, 그리고 그 작업 중에 발견하고 **이미 고친 것**의 기록. 그리고 항목이
닫힐 때 이 절이 갱신된다는 보장이 없다. 가드는 목록이 **닿을 수 있는지**를 검사할 뿐 **최신인지**는
검사하지 않으며, 그것은 기계적으로 판정할 수 없다.

2026-08-05에 **여덟 ADR을 모두 실물과 대조했다.** 기록된 37항목 중 열린 것은 **17개**다. 나머지
20개는 이미 닫혔거나(10), 애초에 부채가 아니거나(정정·감사 결과·교훈 기록 3), 지켜야 할 코드가 아직
없어 도달할 수 없거나(4), 의도적 결정이다(3). 닫힌 10개 중 **셋은 아무도 ADR에 적지 않은 채로**
닫혀 있었고(그중 하나는 자기를 인용한 CI 주석까지 달려 있었다), 일곱은 이 대조를 하면서 닫았다.

`열림` 열이 실제 남은 일이다. `기록` 열은 그 절의 항목 수이며 위 세 가지가 섞여 있다.

| ADR | 기록 | 열림 | 열린 것이 무엇인가 | 우선순위 |
|---|---|---|---|---|
| [0010 라이브 자원 테스트 레인](../adr/0010-live-resource-test-lanes.md) | 11 | 2 | (자격증명 항목은 2026-08-06 해소 — 아래 참조) 정적 가드(부분 해소 — CI는 차단 밖), 임포트 게이트 부재 | 4 |
| [0011 테스트 실행 집합 완전성](../adr/0011-test-execution-set-completeness.md) | 4 | 3 | 정적 가드(부분 해소 — CI는 차단 밖), 자격증명 레인, 임포트 게이트 | 4 |
| [0012 검증 결과의 의미](../adr/0012-verification-results-must-mean-what-they-say.md) | 3 | 0 | — 전부 닫힘 | 4 |
| [0013 릴리스 유일성](../adr/0013-release-uniqueness-admits-both-source-kinds.md) | 2 | 0 | — 전부 닫힘 | 1 |
| [0014 제공 세대](../adr/0014-serving-generation-tracks-one-unit-source-selection.md) | 2 | 1 | 정적 승격이 아직 포트 기본 구현(에러)이라 이 규칙을 지난 적 없음 | 1 |
| [0015 멱등성 원장](../adr/0015-one-idempotency-ledger-for-keyed-catalog-mutations.md) | 5 | 3 | 키를 발급하는 클라이언트 부재, `validate()` 결합, 원장 우회 경로 둘 | 1 |
| [0016 투영 적재의 정체성](../adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md) | 6 | 4 | 적재 보존 기간, `failed` writer 부재, parcels 적재 경로, 지문 v1 의미 변화 | 1 |
| [0017 리비전의 소속](../adr/0017-a-data-revision-belongs-to-the-unit-it-revises.md) | 4 | 3 | `superseded` writer 부재, 계보를 CHECK로 강제할 수 없음, 사실 원장(`parcel_identifier`) 분리 미완 | 1 |
| [0018 두 언어의 어휘 대조](../adr/0018-a-vocabulary-written-in-two-languages-is-compared.md) | 3 | 2 | catalog 밖 도메인 어휘 미대조, 제약 읽기가 단일 목록 형태에 한정 | 2 |
| [0019 소속은 기간을 가진 사실](../adr/0019-membership-is-a-dated-fact-not-a-column.md) | 4 | 4 | 필지 전이표 부재, 건물↔필지·호실→건물 미교정, 소속 표의 운영 생산자 부재, 산단의 시군구 코드가 스칼라 | 1 |
| [0020 도형은 사실의 근거가 아니다](../adr/0020-geometry-is-not-evidence-for-a-fact.md) | 4 | 3 | `sandan_parcel` 수집기 부재, 한 필지 한 산단 미실측, 레이크하우스 계약의 도형 어휘 잔존, 덮어쓰기형 표 전환 | 1 |
| [0021 안 읽히는 표면은 지운다](../adr/0021-an-unread-surface-is-deleted-not-migrated.md) | 3 | 2 | (`anchor-summary` 읽기 이전은 0022가 해소) 산단 SSOT 제안 문서 전면 재검토, 등록부와 실물 라우트 대조 검사 부재 | 2 |
| [0022 "현재"는 오늘이다](../adr/0022-current-means-today-and-one-view-says-so.md) | 3 | 2 | `CURRENT_DATE` 시간대 미지정, `as_of` 파라미터 보류 (`parcel.complex_id` 항목은 3단계가 컬럼을 지워 해소) | 2 |
| [0023 편집은 원장의 행이다](../adr/0023-an-edit-is-a-row-in-the-ledger-not-only-in-the-row.md) | 4 | 4 | 필지 지오메트리 편집 경로 부재, `kind`의 기준값이 R2에 없음, 원장 표 이름이 내용보다 좁음, `building_register_floor` writer 부재 | 1 |
| [0024 서빙 투영은 계약이 지명한 것만 싣는다](../adr/0024-the-serving-projection-carries-only-what-the-tile-contract-names.md) | 3 | 3 | 발행 표에 생산자 부재(채우는 표와 서빙하는 표가 다름), 뷰가 계약보다 많이 싣는 것을 막는 검사 부재, 두 서빙 표의 `complex_id` 처분 미정 | 1 |

2026-08-06에 0018이 더해졌다(기록 3, 열림 2). 세 번째 항목은 0017 남은 부채 3과 **같은 건**이므로
열림으로 세지 않는다 — 같은 일을 두 번 세는 것이 이 표가 피하려는 것이다.

2026-08-07에 0019가 더해졌다(기록 4, 열림 4). 네 항목 모두 오늘 기록됐으므로 닫힌 것이 없다.
셋째 항목(운영 생산자 부재)은 0016 남은 부채 3과 **뿌리가 같지만 다른 표**다 — 저쪽은
`catalog.parcel`을 채우는 적재기이고 이쪽은 소속 표를 채우는 경로이며, 둘 다 Rust가 Iceberg를
읽을 수 있어야 열린다. 뿌리가 같다는 이유로 합쳐 세지 않는 대신 그 사실을 여기 적어 둔다.
표 합계는 기록 44, 열림 23이다.

2026-08-09에 0020이 더해졌다(기록 4, 열림 3). 첫째 항목(`sandan_parcel` 수집기 부재)은 0019 남은
부채 3(소속 표의 운영 생산자 부재)과 **같은 건**이므로 열림으로 세지 않는다 — 0020이 그 생산자가
읽어야 할 소스를 지목했을 뿐 새 일을 만들지 않았다. 넷째 항목(덮어쓰기형 표 전환)은 이 저장소의
사실 표 전체에 걸린 방향이며 한 증분이 아니라 프로그램이므로, 범위를 정하는 별도 ADR이 먼저다.
표 합계는 기록 48, 열림 26이다.

2026-08-09에 0021이 더해졌다(기록 3, 열림 3). 첫째 항목(`anchor-summary` 읽기 이전)은 0019
§이행 순서 2가 이미 담고 있던 일을 **좁힌** 것이지 새로 만든 것이 아니다 — 대상이 넷에서 하나로
줄었으므로 이 표에서는 0019 쪽이 아니라 여기에 적어 남은 범위를 정확히 보이게 한다.
표 합계는 기록 51, 열림 29다.

2026-08-10에 0022가 더해졌다(기록 3, 열림 3). 동시에 0021의 첫 항목(`anchor-summary` 읽기 이전)이
닫혔으므로 0021은 열림 3 → 2가 된다. 0022의 첫 항목(`CURRENT_DATE` 시간대)은 **이 결정이 만든 것이
아니라** `parcel_current_identifier`가 이미 갖고 있던 문제를 하나 늘린 것이며, 둘을 함께 고쳐야
하므로 한 항목으로 센다. 표 합계는 기록 54, 열림 31이다.

2026-08-10에 0023이 더해졌다(기록 4, 열림 4). 같은 날 3단계가 `parcel.complex_id`를 지워 0022의
셋째 항목이 닫혔으므로 0022는 열림 3 → 2가 된다. 0023의 첫 항목(필지 지오메트리 편집 경로 부재)은
ADR-0006이 "승인된 편집이 있는 단위"를 PostGIS로 제공한다고 적은 것의 **전제**이며, 그 편집을 만드는
명령이 저장소에 없다는 사실이 이번 조사에서 처음 기록된다. 표 합계는 기록 58, 열림 34다.

2026-08-11에 0024가 더해졌다(기록 3, 열림 3). 첫 항목(발행 표에 생산자 부재)은 **채우는 표와
서빙하는 표가 다르다**는 사실을 처음 기록한다 — 전국 재적재기는
`serving_postgis.parcel_boundary_mirror`를 채우고 Martin은 `parcel_boundary_publication`을 읽는다.
0024는 그 표에 필지를 넣지 못하게 막던 제약을 치웠을 뿐 둘을 잇지 않으며, 잇는 것이 필지 타일
슬라이스의 본체다. 표 합계는 기록 61, 열림 37이다.

대조에서 나온 두 가지를 여기 적어 둔다. **2026-08-05 대조 시점의 열린 19개 중 상당수는 코드로 닫히지 않는다** — 자격증명
(5타깃), 정본 실버의 소유자(parcels 적재기), 클라이언트 구현(멱등성 키 발급)처럼 결정이 먼저인
것들이다. 그리고 **`남은 부채` 절은 닫힐 때 갱신되지 않는다.** 여섯 항목이 코드로는 닫힌 채 목록에
살아 있었고, 그중 하나는 자기를 인용한 CI 주석까지 달려 있었다. 이 표의 `열림` 수도 대조한 날짜의
사실일 뿐이며, 같은 방식의 재대조가 유일한 유지 수단이다.

## 문서 정리 상태

- [ ] `doc_type: documentation`인 28개 문서를 실제 종류로 좁힌다. 이 값은 아무것도 분류하지
      않으며(`intelligence-platform/docs/architecture.md`가 이 값을 달고 있다), 파일마다 판단이
      필요해 2026-08-06에 어휘를 강제할 때 **잠정 허용**으로 남겼다. 좁히고 나면
      `audit-documentation.py`의 `ALLOWED_DOC_TYPES`에서 뺀다.

문서의 미완료 작업도 이 문서가 정본이다. README·ADR에 작업을 복제하지 않는다.

- [x] 루트 작업 목록을 `docs/roadmap/` 아래에 배치
- [x] 루트·영역·주요 하위 문서 폴더의 README 색인 연결
- [x] 비표준 `@` 경로 표기를 상대 Markdown 링크로 교체
- [x] 사람이 읽는 유지 서술 문장의 영문 설명을 한글 정본으로 전환 (기술 식별자·외부
      표준명·명령·계약 field는 원래 표기를 유지하며 감사 가드가 영문 문장 재발을 차단)
- [x] 유지 문서 메타데이터를 보강하고 계약·ADR·초안·법률 문서의 예외를 감사 규칙에 명시
- [x] 사용되지 않는 계획·초안 문서를 참조 검사 후 비공개 기록으로 전환

현재 제안·초안 문서는 카탈로그 색인과 현행 설계 문서에서 모두 참조되며, 감사 보고서의
유입 링크 수가 0인 초안은 없다. `audit-documentation.py --check`가 앞으로 승인 전 문서의
유입 링크 0건을 실패시킨다. 승인 전 문서는 `proposed` 또는 파일명 `.draft.` 상태를 유지하고
운영 계약으로 오인되지 않게 한다.

한글화 감사에서 예외를 제외한 유지 서술 문서의 `english` 0개와 명백한 영문 서술 문장 0개를
확인했다. 감사 보고서의 `mixed` 수는 API·제품명·schema field 같은 기술 표기를 포함한
혼합 표기 문서 수이며, 이를 번역해 식별자를 훼손하지 않는다. 계약·fixture JSON,
`AGENTS.md`/`CLAUDE.md` 라우터,
법률 고지는 원문 표기를 유지한다. 기술 식별자와 외부 제품명은 원래 표기를 보존하되,
사람이 읽는 설명은 한글로 작성한다. 상세 결과는 `python3 scripts/catalog/audit-documentation.py` 출력에서 확인한다(ADR-0176).

## 완료 판정

“운영 준비 완료”는 코드가 빌드되는 상태가 아니다. 우선순위 0의 모든 체크가 실제
운영 계정/CI에서 통과하고, 우선순위 1의 핵심 Bronze→Silver→Gold 경로가 실제 backend에서
재현되며, 장애·복구·롤백 증거가 남아 있을 때만 완료로 표시한다.

## 문서 정리 완료 판정

문서 정리는 파일을 옮기거나 메타데이터를 채운 것만으로 완료하지 않는다.

- [x] 모든 문서가 정본 유형·소유자·상태를 갖거나, 계약·ADR·초안·법률·에이전트 예외가 감사 보고서에 명시된다.
- [x] 문서가 있는 모든 폴더에 README 색인이 있고, README는 내용을 복제하지 않는다.
- [x] 작업 목록은 이 로드맵 하나만 사용한다.
- [x] 깨진 상대 링크와 비표준 문서 참조가 자동 검사된다.
- [x] 사람이 읽는 유지 문서의 설명을 한글 정본으로 전환한다(코드·명령·식별자·외부 원문은
      원래 표기 유지). `audit-documentation.py --check`가 영문 전용 문서와 명백한 영문 문장을
      모두 차단한다.
- [x] 파일명 중복 후보를 범위별 정본으로 구분하고, 의도적 중복은 감사 보고서에 소유권을 기록한다.
      대체된 결정은 ADR supersession으로 연결한다.
- [ ] 문서 CI와 오프라인 링크 검사가 실제 보호 브랜치에서 통과한다.

### 2026-07-30 검증 기록

- 로컬 감사: `audit-documentation.py --check` 통과(예외를 제외한 영문 전용 유지 문서 0개,
  혼합 표기 유지 문서 85개(기술 표기 포함), 명백한 영문 서술 문장 0개, 메타데이터 누락 0개, 링크 위반 0개,
  비의도적 파일명 중복 0개).
- 자동 색인: `render-document-catalog.py` 생성 성공(ADR-0176 이후 색인은 커밋하지 않는다).
- 감사 단위 테스트: 17개 통과.
- `git diff --check` 통과.
- Git Bash 경로에서 Docker 기반 `scripts/ci/lychee-docs.sh`가 통과했다
  (`857` 링크 입력, 오류 `0`). PowerShell의 `bash.exe` 래퍼로 직접 호출하면 Docker 엔진 연결
  시간 초과가 발생하므로 CI와 같은 Git Bash 실행 경로를 사용한다.
- 통합 `scripts/guard/monorepo-guard.sh`는 180초 실행에서 `container-runtime-policy`까지
  성공했지만 종료하지 않아 전체 통과로 표시하지 않았다. 개별 문서 감사·색인·lychee 증거와
  보호 브랜치 CI 결과를 분리해 기록한다.
