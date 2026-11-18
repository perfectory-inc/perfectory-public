---
status: current
owner: intelligence-platform
doc_type: documentation
last_reviewed: 2026-11-05
---

# 인텔리전스 플랫폼 아키텍처

## 소유권

인텔리전스 플랫폼은 모델 실행, 정규화 제안 생성, 지식 검색, 벡터·검색 증강 처리, 영속 AI 작업 흐름
전달을 소유한다. 정본 부동산 데이터를 소유하지 않으며 파운데이션 DB 에 직접 쓰지 않는다.

> 지식 검색은 Postgres 전문검색과 한국어 형태소 색인, 신호별 검색기의 RRF 융합으로 구현되어
> 있다([ADR 0003](./adr/0003-korean-morphology-in-rust.md)·[0004](./adr/0004-retriever-fusion-over-a-single-scorer.md)·[0005](./adr/0005-result-assembly-diversity-and-context.md)).
> 임베딩 제공자·벡터 색인과 운영 검색 증강 호출 주소는 [ADR 0002](./adr/0002-canonical-release-rag-design.md)의
> 승인 조건과 별도 ADR 없이 도입하지 않는다. 이 문서의 소유권 선언은 구현 완료를 뜻하지 않으며,
> 지원 기능의 정본은 코드와 공개 API 계약이다([ADR 0001](./adr/0001-canonical-implementation-rust.md) §영향).

## Rust 모듈 경계

```text
./
|-- crates/normalization/
|   |-- intelligence-normalization-domain
|   |-- intelligence-normalization-application
|   `-- intelligence-normalization-infrastructure
|-- crates/knowledge/
|   |-- knowledge-domain
|   |-- knowledge-application
|   `-- knowledge-infrastructure
|-- crates/messaging/
|   `-- messaging-infrastructure
|-- crates/intelligence-contracts
|-- crates/intelligence-workspace-hack
|-- services/intelligence-api
`-- services/intelligence-worker
```

- 도메인 크레이트는 업무 규칙과 값 타입을 담는다.
- 응용 크레이트는 사용 사례와 포트를 담는다.
- 인프라 크레이트는 HTTP·PostgreSQL·Kafka·모델 어댑터를 구현한다. 검색은 Postgres
  전문검색 어댑터(와 시험용 메모리 어댑터)뿐이며 벡터 저장소 어댑터는 아직 구현하지 않았다.
- `messaging-infrastructure`는 기술 전달 어댑터다. 메시지 계약은 `intelligence-contracts`와
  `schemas/`에 있으므로 도메인·응용 쌍을 일부러 두지 않는다.
- `intelligence-workspace-hack`은 빌드 기능 통합용 크레이트이며 코드를 담지 않는다.
- 서비스는 모듈을 조합하고 실행 경계를 드러낸다.

## 플랫폼 간 계약

1. 파운데이션은 버전이 붙은 계약으로 불변 원문·정본 참조를 공개한다.
2. 인텔리전스는 근거·확신도·계보·멱등 키가 있는 정규화 제안을 만든다.
3. 인텔리전스는 파운데이션 API 로 제안을 낸다.
4. 파운데이션이 제안을 저장·검토하며 인텔리전스는 승인·적용할 수 없다.
5. 승인된 제안을 정본 데이터에 적용하는 명령은 파운데이션만 실행한다.

`*.v1.avsc` 이름의 Avro 파일은 구현 반복 번호가 아니라 실제 이벤트 호환성 계약이므로 그대로 둔다.
호환되지 않는 이벤트 모양이 생기면 새 스키마 버전을 붙인다.
