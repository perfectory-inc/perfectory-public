# ADR 0095: 세대 공시가격은 전유부를 통해 세대 정체성으로 재색인된다

- Status: Accepted
- Date: 2026-09-08

## Context

공동주택가격 2.02억 행에는 동·호가 없고, 전유부 1,982만 행에는 같은 관리번호와
동·호가 함께 있다. PNU만으로 가격을 붙이면 다른 세대의 가격을 반환한다.
불변식은 **관리번호가 하나의 세대 정체성만 지목하며, 서빙 가격의 키는
PNU·동명·호명·기준연도이고 건물 적재가 발급한 UUID에는 의존하지 않는다**다.

코디네이터의 전국 실측에서 가격 관리번호 13,078,558개 중 전유부에 존재하는 것은
13,077,926개(99.9952%)다. 전유부 관리번호별 `(pnu,dong_name,ho_name)`는 전국 1:1이며
`conflicting=0`이다. 서울 세대표 표본 2,000건은 정확일치 99.65%, 숫자 정규화 시
99.75%였다. 종로 표본의 `(mgmt_key,base_date)`와 `(mgmt_key,base_date,price_won)`는
각각 234,972개로 같았다. 반복 관측은 `notice_date`의 재발행이다.

코디네이터가 서버에서 실행한 서울 시도 조인은 **17초**, 세대·연도 결과
**29,070,232행**, 세대 **1,872,891개**였다. 코디네이터는 이 실측을 근거로 전국
17개 시도 분할과 shuffle join을 확정했으며 Trino의 15GB 한도 안에서 실행 가능하고
broadcast가 필요 없다고 전달했다. 이는 코디네이터 제공 실행 증거이며 이 워커가
Spark 전국 적재·운영 COPY를 실행했다는 뜻은 아니다.

## Decision

1. `unit_official_price.py --sido <두 자리>`는 해당 시도만 처리한다. 두 입력의 Iceberg
   snapshot을 고정하고 선택 `vintage`를 읽는다. 기본 vintage는 두 원천 객체 계약의
   `selected_vintage`에서 읽으며 서로 다르면 명시적 선택을 요구한다. 전국 17회 실행은
   운영자가 담당한다. Spark의 일반·적응형 broadcast를 모두 끄고 shuffle join을 쓴다.
2. 전유부의 `(mgmt_key,pnu,dong_name,ho_name)` DISTINCT로 사전을 만든다. 시도 필터보다
   먼저 전국 사전의 관리번호별 정체성 개수를 검사해 시도 경계를 넘는 충돌도 계수한다.
   `conflicting`을 항상 기록하고 0이 아니면 append 전에 거부한다. 가격은
   `(mgmt_key,base_date,price_won)` DISTINCT이며 연도는 `base_date`의 앞 네 자리다.
   해석 불가능한 날짜·가격은 `invalid_prices`로 세고 원천 Silver에서 보존한다.
3. 조인 결과는 `silver.unit_official_price`에 append한다. 같은 연간 키의 서로 다른
   가격도 원천 관측으로 남기며 원천 두 표는 수정하지 않는다. `source_snapshot_id`는
   두 입력 snapshot과 vintage를, `source_record_id`는 여기에 시도를 더한 배치를 지목한다.
   기존 `append_batch_once`가 Iceberg snapshot summary 안에서 배치의 재실행을 판정한다.
   새로운 append 원장이나 브로커를 만들지 않는다.
4. 다리1 누락 632건과 숫자 정규화 후 다리2 미일치 0.25%는 신축 미등재 등 원천 간
   시차에서 생기는 **정상 손실**로 취급한다. 개별 누락 원인이 모두 신축임을 주장하지
   않는다. 정확일치 읽기는 표본에서 0.35%가 미일치하므로 정규화의 0.25%와 구별한다.
   이번 읽기는 동·호를 임의로 정규화하지 않는다. 조인 실패는 `unmatched_prices`로
   기록하며 세대 없는 가격으로 원천 Silver에 남는다.
5. Rust `load-unit-official-price-projection`은 완료된 Silver snapshot과 원천 조합을
   명시적으로 선택한다. Trino 공식 CLI로 시도 목록을 읽고 17개가 아니면 거부한다.
   시도별 결과를 메모리에 모으지 않고 PostgreSQL 임시표에 COPY한 뒤에만 승격한다.
   R2 핸드오프 파일이나 새로운 Trino HTTP 프로토콜 구현은 만들지 않는다.
6. `catalog.unit_official_price`의 PK는 `(pnu character(19),dong_name,ho_name,base_year
   smallint)`다. `price_won bigint`는 음수가 아니며 PNU CHECK는 기존 가격 투영과
   같은 19자리 규칙을 쓴다. 건물·세대표 FK는 두지 않아 건물 재적재와 가격 재적재를
   분리한다. `source_snapshot_id`는 가격의 원천 조합을 보존한다.
7. 같은 키의 복수 가격은 `conflicting_prices`로 기록하고 `DISTINCT ON`의
   `price_won DESC`로 결정론적으로 하나를 고른다. 공시일만 다른 같은 가격도 접는다.
   이것은 사실 수정이 아니라 서빙 선택이며 모든 대안은 Silver에 남는다. 전체 새
   투영을 검증한 후 잠금·단일 트랜잭션 안에서 기존 투영을 교체한다. 읽기는 커밋 전후의
   완전한 투영 중 하나만 본다. 재시도는 같은 선택을 재현하며 실패한 COPY는 승격하지 않는다.
8. append-only는 Bronze·Silver 관측에 적용한다. 이 catalog 표는 재생성 가능한 서빙
   투영이므로 빈티지 재적재에 따른 교체가 허용된다. `base_year`가 연간 자연 버전이고,
   이전 투영으로 돌아갈 때는 보존된 Silver snapshot과 원천 조합을 다시 선택한다.
9. PNU별·건물별 페이지 세대 응답 모두 `official_price_history`를 제공한다. 연혁은
   연도 내림차순이고 미일치 세대는 빈 배열이다. 단일 저장소 port는 동·호와 가격을
   반환하며 쿼리에 `$1::character(19)`를 써 PK 인덱스 비교 연산자를 보존한다.
   Gongzzang은 published HTTP 계약을 소비해 제품 응답까지 동일한 정수 원을 전달한다.
10. 합성 `99999` PNU의 관계형 fixture로 사전·충돌·연도·가격 중복을 검사하고, Rust로
    동호 매칭·구 응답 호환·제품 HTTP 전달을 검증한다. PostgreSQL이 필요한 교체·조회
    증명은 `#[ignore]` 레인에 둔다. 그래프·OpenAPI·클라이언트 핀·생성 문서를 함께 갱신한다.

## 대안과 재사용 근거

- 전국 단일 조인: 메모리 한도가 있고 시도 분할이 실측으로 충분하므로 기각한다.
- PostgreSQL에 전유부 사전을 먼저 적재: 추가 임시 적재·인덱스가 필요하고 시도별
  shuffle join이 측정에서 성공했으므로 채택하지 않는다.
- Trino에서 곧바로 catalog COPY: 최초 요청의 후보였으나 코디네이터가 확정한 시도별
  Spark append 경로로 변경했다. 재색인된 Silver를 남겨 조인 재실행과 서빙 재적재를 분리한다.
- 세대 UUID에 가격을 고정: 건물 파이프라인 재적재와 불필요하게 결합하므로 기각한다.
- 별도 Trino 클라이언트·적재 라이브러리: 기존 Apache Trino CLI(Apache-2.0), Spark·Iceberg,
  SQLx, Tokio를 사용한다. 새 의존성은 없다. 표준 데이터 처리는 기존 엔진이 담당하고
  새 코드는 도메인 조인·배치 선택·승격 제어에 한정한다.

[Trino 조인 방식](https://trino.io/docs/current/optimizer/cost-based-optimizations.html),
[Trino CLI 배치 출력](https://trino.io/docs/current/client/cli.html),
[PostgreSQL COPY](https://www.postgresql.org/docs/17/sql-copy.html)를 기준으로 재사용했다.

## 실행과 복구

Foundation 릴리스 루트에서 기존 lakehouse 환경을 불러온 운영자가 실행한다. 아래는
한 시도의 제출 예다. `PRICE_SNAPSHOT_ID`, `EXCLUSIVE_SNAPSHOT_ID`는 첫 실행 전에
확정한 두 입력의 Iceberg snapshot ID이며 **17회 모두 같은 값을 사용한다**.
`SIDO`는 이번에 실행할 시도의 두 자리 코드다. 기본 vintage는 원천 계약에서 읽는다.

```bash
PACKAGES=$(PYTHONPATH=infra/lakehouse/spark/jobs python3 -c 'from lakehouse_engine import iceberg_packages; print(iceberg_packages())')
docker compose -p foundation-platform-compute -f compose.lakehouse.yml --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN \
  -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT \
  -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID \
  -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY \
  spark spark-submit --master 'local[8]' --driver-memory 12g \
  --packages "$PACKAGES" --conf spark.jars.ivy=/home/spark/.ivy2 \
  /workspace/infra/lakehouse/spark/jobs/unit_official_price.py \
  --sido "$SIDO" --price-snapshot-id "$PRICE_SNAPSHOT_ID" \
  --exclusive-snapshot-id "$EXCLUSIVE_SNAPSHOT_ID"
```

각 실행의 `conflicting`, `invalid_prices`, `unmatched_prices`, `rows`, `elapsed_seconds`,
`source_snapshot_id`와 append 결과를 보관한다. 같은 시도·입력 snapshot의 재실행은
기존 Iceberg 배치 기록으로 건너뛴다. 17회 종료 후 새 표의 **완료 snapshot ID**를 읽고,
그 값과 Spark가 출력한 `source_snapshot_id`를 아래 환경변수로 전달한다.
`DATABASE_URL`은 마이그레이션을 적용한 Foundation DB를 가리켜야 한다.

```bash
export FOUNDATION_PLATFORM_UNIT_PRICE_PROJECTION_LOAD_CONFIRM=true
export FOUNDATION_PLATFORM_UNIT_PRICE_ICEBERG_SNAPSHOT_ID="$COMPLETED_SNAPSHOT_ID"
export FOUNDATION_PLATFORM_UNIT_PRICE_SOURCE_SNAPSHOT_ID="$SOURCE_SNAPSHOT_ID"
foundation-outbox-publisher load-unit-official-price-projection
```

기본 Trino 컨테이너는 compose의 `foundation-platform-trino`다. 다르면
`FOUNDATION_PLATFORM_UNIT_PRICE_TRINO_CONTAINER`로 지정한다. 각 시도의 staged 행수,
`conflicting_prices`, `folded`, 최종 `unit-official-price-projection-load-ok`를 확인한다.
실패 시 기존 catalog 투영이 유지된다. 이전 가격 투영으로 복구할 때는 보존한 이전
완료 snapshot과 원천 조합으로 같은 로더를 재실행한다.

## 개정 각주 (2026-09-08)

전국 조인 실행 중 실측: 202608 vintage 의 건축HUB 데이터는 표준 17개 시도가 아니라
**16개 시도 코드**를 쓴다. 광주광역시(29)와 전라남도(46)가 "전남광주통합특별시"(코드 12)로
통합돼 두 코드가 사라지고 하나가 생겼다(원문 주소 "전남광주통합특별시 동구 대인동…"으로
확인). 전유부에서 `substr(pnu,1,2) = substr(sigungu_cd,1,2)` 는 전 행 일치(불일치 0)라
파이프라인 내부 정합성은 유지된다. 이에 따라 투영 로더의 완전성 검사는 상수 17 대신
**고정된 price Silver 스냅숏의 실제 시도 집합**과 대조하도록 고쳤다(행정구역 개편에도 옳게
작동). source_snapshot_id 에 박힌 `price:<n>` 계보로 그 스냅숏을 특정한다.
