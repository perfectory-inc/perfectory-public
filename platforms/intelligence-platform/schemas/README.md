---
status: current
owner: intelligence-platform
doc_type: README
last_reviewed: 2026-11-05
---

# schemas/

인텔리전스 플랫폼이 발행하는 이벤트의 Avro 스키마 파일(`.avsc`)을 둔다.

## 포함 내용

버전이 붙은 이벤트 토픽마다 파일 하나:

| 파일 | 토픽 | Kafka 키 |
|------|-------|-----------|
| `intelligence.normalization-proposal.submission-requested.v1.avsc` | `intelligence.normalization-proposal.submission-requested.v1` | `aggregate_id` (= `idempotency_key`) |

이 토픽을 발행하는 코드는 아직 없다. 이 파일은 정규화 outbox 기록(`NormalizationOutboxRecord`)의 전송 모양을
고정하는 계약이다 — Kafka 이벤트 백본(어댑터·Karapace 클라이언트·실패 메시지 토픽)은 발행자가 없어 지웠고
(루트 ADR-0587), 발행자가 생길 때 그 라이브러리와 함께 돌아온다.

## 스키마 변경 규칙 — 이전 판 전부와 하위 호환(`BACKWARD_TRANSITIVE`)

이 디렉터리의 모든 스키마는 **`BACKWARD_TRANSITIVE`** 호환성 규칙을 따른다.

규칙:
- **버전 안에서는 더하기만 허용:** 새 필드에는 Avro `default`가 있어야 하며 기존 필드 뒤에만
  더한다. 이전 스키마 버전의 소비자는 새 필드를 조용히 무시하고 새 버전의 생산자는
  그 값을 채운다.
- **게시된 필드는 절대 바꾸지 않는다:** 스키마 파일을 `main`에 커밋한 뒤에는 필드의
  이름·삭제·타입·순서를 바꾸지 않는다.
- **호환성을 깨는 변경 = 새 파일 + 새 토픽:** 구조 변경(이름 바꾸기, 타입 바꾸기, 필드 삭제)이
  불가피하면 새 파일(예: `.v2.avsc`)과 새 토픽을 만들고, 합의한 이전 기간 동안 옛·새
  토픽을 나란히 돌린 뒤 v1 토픽을 폐기한다.

## 스키마 레지스트리(Karapace)

**`TopicNameStrategy`**로 **Karapace**(Confluent 호환 스키마 레지스트리)에 등록한다. 등록 주제
이름은 `<topic>-value`다(예: `intelligence.normalization-proposal.submission-requested.v1-value`).

생산자는 Avro 본문 앞에 5바이트 Confluent 전송 형식 접두사
(`\x00 + schema_id_int32_big_endian`)를 직렬화한다. 소비자는 같은 레지스트리로 읽을 때 스키마 ID 를
해석한다.

## 계약 시험

`crates/normalization/intelligence-normalization-application/tests/event_schema_contract.rs` 가 스키마와 코드의 호환을 다음으로 고정한다:

1. 시험 때 `.avsc` 파일을 `apache_avro::Schema::parse_str` 로 해석한다.
2. `NormalizationOutboxRecord` 고정물을 직렬화 → 역직렬화로 끝까지 왕복한다.
3. 해석한 스키마의 모든 필드가 필수 필드 집합에 있거나 Avro `default`를 갖는지 확인한다.
   이것이 더하기만 하는 진화의 걸림 장치이며, `default` 없는 필드를 더하면 시험이 바로 실패한다.

실행: `cargo test -p intelligence-normalization-application --test event_schema_contract`.
