# ADR 0371: 실거래 종류는 법정동 단위로 다시 짓고, 첫 발행은 산업용 이력이 다 찬 뒤다

- Status: Accepted
- Date: 2026-11-04
- Implements: [ADR-0350](./0350-a-parcel-shows-its-own-transactions-and-the-factory-deals-of-its-legal-dong.md)
  §1–§3·§6, [ADR-0351](./0351-a-masked-lot-is-inferred-only-from-measured-evidence-and-every-link-follows-code-changes.md) §3
- Builds on: [ADR-0287](./0287-public-sale-is-a-parcel-kind-built-from-its-own-silver.md) (자기 생산자 종류),
  [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md)
  (바뀐 키만, 바뀐 키를 남긴다), [ADR-0329](./0329-official-evidence-supersedes-a-recorded-inference-automatically.md) §3
  (`views.changed_codes`), [ADR-0336](./0336-every-published-schema-lives-in-one-registry-and-ci-holds-every-consumer-to-it.md)
  (스키마 레지스트리), ADR-0370(#993, 추정 수준 판정)

## Context

ADR-0350 은 실거래를 필지 화면에 싣는 종류를 정했다. ADR-0351 §3 은 그 연결이 적재 때 한 번 정하고 끝나면 안 된다고
했다. 사장 규칙(2026-10-31): 모든 데이터는 법정동 코드 변경을 따라간다. 사장 일반 원칙: 모든 처리는 바뀐 것만 한다.

실거래 문서의 세 부분은 모두 한 법정동 안에서 정해진다.

- 한 필지의 거래: 거래의 주소가 가리키는 현재 법정동 안의 필지다. 추정 후보도 같은 법정동 안에서만 찾는다.
- 비교 거래: 같은 현재 법정동의 공장창고 거래다.
- 지역 통계: 그 법정동의 거래다.

예외는 계보 하나다. 분할·합병된 필지의 거래는 다른 법정동의 현재 필지로 옮겨질 수 있다.

같은 날 Silver 상태(읽기만, 2026-11-03): 828,897행이 실려 있다. 공장창고는 173/250달, 토지·상업업무는 3/250달이다
(ADR-0348 §5 백필 진행 중).

## Decision

1. **종류 줄과 생산자.** `parcel-real-transaction`(호스트 필지, 절 `real_transaction`, 열 `real_transaction_json`,
   `transaction_total`)이다. 생산자는 `real_transaction_silver_to_gold.py` 다. Gold 는 `gold.parcel_real_transaction`
   이다. 재빌드 작업은 `gold_parcel_real_transaction_rebuild` 이고, 굽기 작업은 `by_pnu_parcel_real_transaction_bake`
   다. 소유·공매 종류와 같은 모양으로 둔다.
2. **매 빌드 원천 열에서 다시 잇는다.**
   - 생산자는 Silver 의 `pnu`·`legal_dong_code`·`place_identity_grade` 를 읽지 않는다. 시험이 열 목록으로 이를 막는다.
   - 법정동은 `address_raw` 를 가장 새 `reference.legal_dong_code_snapshot` 의 이름에 맞춰 정한다
     (`rt_molit_real_transaction.dong_index`). 그 코드를 `code_crosswalk` 로 현재 코드로 옮긴다.
   - 안 가린 지번은 정본 해석기 `resolve` 를 지난다. 현재 필지가 아니면 공매의 `placed` 로 계보를 따른다.
     계보로 옮길 때 해석기가 낸 현재 형태가 필요하다. 그래서 `resolve` 에 `keep_candidate` 를 더한다(덧붙이기만).
   - 가린 지번은 `infer_masked` 를 지난다. 계약이 켠 수준만 쓴다(ADR-0370 판정, 지금은 없음).
3. **바뀐 것만: 단위는 법정동이다.** 실행은 변경이 닿는 법정동의 PNU 만 다시 계산하고 합친다(`gold_incremental`).
   바뀐 PNU 는 `gold.<t>__changes` 에 남는다(ADR-0312). 닿는 법정동은 다음과 같다(`affected_dongs`).
   - 적재가 덧붙인 거래의 법정동. 덧붙인 행만 읽는다(ADR-0189).
   - 코드 변경 표에서 바뀐 쌍의 두 코드. `views.changed_codes` 로 찾는다(ADR-0329 §3). 옛 코드로 실린 거래가 새 코드의
     필지로 옮겨진다. 합성 자료 시험이 이를 증명한다.
   - 이름이 바뀐 코드(법정동 스냅숏).
   - 필지판·토지특성·건축물대장·토지소유·계보에서 바뀐 행의 PNU 앞 10자리.
   - 빌드 달이 넘어가 36개월·12개월 창에서 빠진 달에 거래가 있는 법정동.

   어떤 입력도 종류를 통째로 다시 짓게 하지 않는다(`WHOLE_TABLE_INPUTS` 없음). 입력 제한은 키의 법정동과 그
   계보 선행 필지의 법정동이다. 코드 표·스냅숏·계보는 통째로 읽는다(수천 행). 패리티 표본은 1,000 PNU 다
   (`kind_tables.overrides`): 표본 PNU 하나가 법정동 하나를 통째로 읽기 때문이다(계약의 이유). 빌드의 창은 빌드 달에
   맞춘다. 그래서 같은 달 안의 다시 빌드는 같은 문서를 낸다.
4. **스키마.** 레지스트리 주제는 `foundation-platform.parcel_real_transaction_by_pnu` major 1 rev 1 이다. Spark 시험이
   쓴 JSON 경로를 레지스트리와 같게 묶는다(`SPARK_HELD`). 지역 통계 Gold 표(ADR-0350 §4)는 이 PR 에 없다. 문서의
   `region` 은 그 법정동의 12개월 건수와 m² 금액 사분위만 싣는다. 시군구 통계는 범위가 법정동을 넘으므로 그 표의
   PR 로 미룬다.
5. **첫 발행은 이력이 찬 뒤다.**
   - 레인 줄의 `first_publish_waits_for_history` 가 기다릴 데이터셋을 적는다. 값은 계약 `priority` 의 산업용 세 종이다.
   - 첫 컷오버(`by-pnu-lane-cutover.sh`)의 첫 단계 `history` 가 `scripts/ops/serving_history_readiness.py` 를 부른다.
     이 단계는 그 데이터셋의 첫 달부터 지난 온달까지 모든 달이 적재됐을 때만 지난다.
   - 무엇이 적재됐는지는 새로 기록하지 않는다. `silver.real_transaction` 커밋이 이미 기록한 Bronze 객체를 읽는다
     (`foundation.ingest-batch-objects` 와 ADR-0332 의 지속 사본). REST 카탈로그로 읽고 Spark 는 쓰지 않는다.
     `plan-silver-load-objects` 가 빚진 것을 정하는 그 기록이다. `--report` 는 12종 각각의 적재 달 수를 보인다.
   - 알 수 없으면(카탈로그 없음) 단계는 실패하고 아무것도 시작하지 않는다.
   - 레인 줄의 손 필드 목록(`DERIVED_LANE_HAND_FIELDS`)에 두 필드를 더한다. Rust 계약 해석기는 이 필드를 읽지 않는다.
   - 막는 사고: 2026-11-03 실측으로 토지·상업업무는 250달 중 3달만 실렸다. 그 상태로 서빙하면 대부분 필지가
     "거래 없음" 으로 보인다(ADR-0350 §6).
6. **작업은 꺼진 채로 들어간다.** 재빌드는 매일 09:50 이다. 공매 Gold 처럼 예약으로 돈다. Silver 가 매시간 적재되므로
   입력 시작으로 두면 spark 풀의 대기 상한(20주기 = 20시간)을 넘는다. 첫 전량 빌드의 시간을 재고 컷오버를 한 뒤에
   두 작업을 켠다. 켜는 것은 별도 배포 단계다(ADR-0122 §4).

## Consequences

- 대기업 관행과 같다. 바뀐 파티션만 다시 계산한다(Dagster 의 partitioned asset, Airflow 의 dataset 트리거). 발행 전에
  완전성으로 막는다(Netflix WAP 의 audit 단계).
- 비용(계산): 지금 거래 828,897행이다. 이력이 다 차면 월 약 21만 행 × 250달, 약 5천만 행이다. 하루 적재가 닿는
  법정동은 하루 거래 수(약 7천 건)의 법정동이다. 대부분 필지는 매일 다시 계산되지 않는다. 첫 전량 빌드 시간은
  재지 않았다(작업이 꺼진 이유).
- 추정 연결이 켜지는 날(ADR-0370 판정), 계약 변경은 코드 버전 변경이다. 그날 키별 발행(ADR-0321 §5)이 바뀐 문서만
  싣는다.
- **첫 전량 빌드 계획.**
  - 잰 비교 대상: 같은 하드웨어와 같은 크기(`local[8,8]`, driver 16g, compose `spark` 20g)의 소유 종류 전량 빌드가 걸린
    시간이다(2026-11-03 15:24→15:59Z, 35분). 입력은 토지소유 80.4M 행과 서빙 필지판이었고, 39,396,906 행을 썼다.
  - 계산: 실거래 빌드도 필지판·토지특성(35.9M)을 같은 방식으로 한 번씩 읽는다. 거래(오늘 0.83M 행)는 작다.
    추정 약 30분이다. 이력이 다 찬 뒤(약 50M 거래)는 소유 빌드의 입력 규모와 비슷해서 약 45–60분이다.
  - 두 경우 모두 단위의 `TimeoutStartSec=150m` 안이다.
  - 추정이 켜지지 않은 동안 건축물대장·토지소유는 캐시하지 않는다(`INFERENCE_ONLY`). 메모리는 소유 빌드와 같은
    설정 안에 든다.
  - 운영자가 한 번 시작해서 잰다. 다른 Spark 작업이 돌면 운영 스크립트가 거부한다(exit 75). PowerShell·cmd 에서
    그대로 쓸 수 있게 `$`·`%이름%` 이 없다.

    ```
    ssh -tt ai-server "date +%s | xargs -o -I{} sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh gold rebuild parcel-real-transaction --requested-at {} --reason 'ADR-0371 first full build, timed'"
    ```

  - 걸린 시간은 실행의 `summary.json` 과 로그(`/var/lib/foundation-gold-panel-rebuild/parcel-real-transaction/runs/<run>`)
    에 남는다. 150분 안에 끝나면 두 작업을 켜는 PR 을 낸다.
- 남은 일: 지역 통계 Gold 표(ADR-0350 §4), 웹 `실거래` 탭(별도 PR), 첫 전량 빌드 시간 측정, 컷오버.
