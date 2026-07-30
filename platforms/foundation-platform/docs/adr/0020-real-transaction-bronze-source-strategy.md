# ADR 0020 - 실거래 Bronze 원천 전략

- **Status:** Accepted
- **Date:** 2026-06-30
- **Relates:** [ADR 0016](./0016-bronze-commit-protocol.md),
  [ADR 0017](./0017-bronze-collection-protocol.md),
  [ADR 0019](./0019-bronze-readable-object-lake-postgres-catalog-ssot.md)
- **Evidence boundary:** live scope, provider count, object key, checksum, commit 결과는
  [root ADR 0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md).

## 배경

Foundation Platform은 한국 실거래 원본 데이터에 대해 두 개의 공개 채널을 가진다.

1. `data.go.kr` RTMS Open API: operation·법정동 code·거래 month·page별 paged record.
2. `rt.molit.go.kr` 조건 기반 CSV export: 거래 유형·계약일 범위·지리적 scope별 provider CSV.

Open API는 제한된 probe와 동등성 확인에 유용하다. 하지만 페이지 단위 전국 수집은 지역·월·
페이지·거래 유형별 호출을 곱한다. CSV export 하나가 같은 제한 provider 범위를 변경 불가능한
Bronze 객체 하나로 보존할 수 있다.

## 결정

실거래 원본 수집의 **기본 Bronze 채널**은 `rt.molit.go.kr` CSV export로 한다.

`data.go.kr` RTMS API는 다음 경우에만 사용한다.

- 제한된 parity·schema-drift 확인 channel
- smoke 또는 조사 channel
- CSV export가 없거나 제한 비교에서 불완전하다고 증명될 때의 fallback

두 채널로 같은 업무 사실을 정기 수집하지 않는다. 원천 소유권을 중복하고 provider quota를
소비하며 경쟁하는 원본 이력을 만들기 때문이다. 두 경로 모두 provider 소유 원천이며 Bronze는
정규화 행이 아니라 원본 응답 바이트를 저장한다.

export 계획은 두 모드로 나눈다.

- **historical 또는 initial backfill:** provider download를 줄이도록 선택한 명시적이고 큰
  contract-date range
- **refresh:** 명시적 rolling contract-date window로 configured real-transaction dataset마다
  export job 하나를 만든다.

rolling export는 provider delta feed가 아니다. Silver와 Gold가 행 단위 변경 탐지,
insert/update/delete 해석, 현재 상태 projection을 소유한다.

## 필수 동등성 증거

데이터셋을 활성화하거나 기본 채널을 바꾸기 전에 다음 조건이 같은 제한 비교를 실행한다.

- transaction type;
- legal geographic scope;
- contract period;
- provider inclusion and cancellation semantics.

논리 record 수, 가능하면 안정적인 provider identity, schema, 대표 field 값을 비교한다.
불일치 원인을 설명할 때까지 승격을 차단한다. 한 범위에서 일치해도 영구적 동등성을 증명하지
않으므로 예정된 drift 확인을 계속한다.

공개 저장소에는 절차와 불변식만 기록한다. 선택 범위·count·sample·execution ID·객체 identity·
checksum·R2/Postgres 대조 결과는 비공개 운영 증거 시스템에 둔다.

## 객체 식별자 계약

객체 identity는 운영 버킷이나 실행 결과를 이 ADR에 고정하지 않고 provider 원천과 수집 범위를
명시해야 한다. 정본 템플릿은 다음과 같다.

```text
bronze/source=<source-slug>/period=<yyyy-mm>/sido=<sido-code>/sigungu=<sigungu-code>/export.csv
bronze/source=<source-slug>/contract_from=<yyyy-mm-dd>/contract_to=<yyyy-mm-dd>/scope=<scope>/export.csv
```

정확한 key 검증의 실행 SSOT는 이 문서가 아니라 Bronze key compiler와 catalog다.

## 영향

- 전국 계획은 많은 provider API 페이지보다 소수의 명시적인 CSV export 단위를 우선한다.
- Bronze는 provider CSV 인코딩·header·바이트를 변경하지 않는다.
- API 수집 코드는 동등성 확인·조사·fallback을 위해 유지한다.
- Silver와 Gold는 반복되는 rolling window를 수용하고 겹치는 provider 사실을 멱등적으로
  대조해야 한다.
- 운영자는 동등성 증거를 비공개·검토 가능하게 유지하고 승격하는 데이터셋 버전에 연결한다.

## 범위 밖

- Silver 또는 Gold 정규화가 완료됐다고 선언하는 것
- 전체 과거 전국 수집을 승인하는 것(확장은 운영자 gate를 따른다)
- `data.go.kr` RTMS 경로를 제거하는 것
