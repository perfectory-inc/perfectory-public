# Cost model

환경별 비용 산정 방법의 SSOT다. 승인 예산, 현재 지출, invoice, account별 할인율은
비공개 운영 증거이며 이 문서에 고정하지 않는다.

## 입력

- 트래픽: 활성 세션, 요청 종류별 호출 수, peak factor
- Compute/DB/cache: instance profile, 실행 시간, 가용성 수준
- Object storage: 저장 bytes, storage class, read/write 요청, egress, lifecycle
- 외부 API/SaaS: billable calls 또는 seats, 현재 단가, 포함 quota
- 보안·컴플라이언스: 선택한 검증 범위와 별도 승인 견적

공급자 가격과 quota는 변경 가능한 외부 값이다. 비용 산정 시점에 공식 가격표에서
가져오고, 그 출처와 기준 시각을 비공개 운영 기록에 남긴다.

## 산정식

```text
monthly_estimate = fixed_costs + sum(estimated_volume[i] * current_unit_price[i])
```

추정량은 관측된 단위 지표(`cost/request`, `cost/active-session`, `cost/stored-GiB`)로
보정한다. Reserved capacity나 장기 약정은 부하와 실제 청구 데이터가 안정된 후에만
평가한다.

## 운영 가드

- 환경별 budget과 경고 임계값을 비공개 운영 시스템에 설정한다.
- 승격 전에 예상치와 실제치의 차이 및 단위 비용 추세를 검토한다.
- 가격·quota 변경은 산정 입력 변경으로 처리하며 소스 코드 계약을 바꾸지 않는다.
- 사업 목표 사용자 수와 승인 예산을 공개 기술 문서에 복제하지 않는다.

## 관련 문서

- [TECH.md 비용 관리 계약](../../TECH.md#6-비용-관리-계약)
- [ADR 0008 — observability](../adr/0008-observability-grafana-otel-sentry.md)
