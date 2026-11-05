---
status: current
owner: foundation-platform
doc_type: reference
last_reviewed: 2026-11-05
---

# 원천 변경 감지 정책

Date: 2026-06-02
Owner: foundation-platform

## 결정

공급자 payload 필드는 카탈로그 데이터이며 변경 감지의 정본이 아니다.

Foundation Platform은 원천 계약이 요구하는 모든 공급자 필드를 보존해야 하지만, 생성일·수정일·
발급일·상태일 같은 의미 필드를 믿고 원천 페이지·feature·파일·논리 레코드의 변경 여부를
판정하면 안 된다.

변경 감지는 수집한 콘텐츠의 hash를 기준으로 한다.

- 같은 콘텐츠의 byte 응답이 안정적인 공급자: 원본 byte checksum
- 원본 byte는 달라져도 논리 콘텐츠가 같을 수 있는 공급자: 정규 의미 checksum
- 벌크 파일: 파일 checksum과 공급자 manifest checksum

공급자 날짜 파라미터는 운영자가 승인한 수집 범위 최적화에만 사용할 수 있다. 이것은 변경되지
않은 데이터를 놓치지 않았다는 증거도, 정본 변경 감지기도 아니다. 전체 또는 hash 검증 스냅샷이
정확성 기준이다.

## 필수 분리

모든 원천 연동은 다음 개념을 분리해야 한다.

| Concept | Meaning | Example |
|---|---|---|
| Request fingerprint | Same provider request shape | provider, endpoint, parameters, page, scope |
| Content checksum | Same collected data | raw payload hash or canonical semantic hash |
| Provider metadata | Data supplied by provider | `crtnDay`, update date, status, count |
| Collection scope | What we choose to ask for | full snapshot, file, page window, date window |

이미 검증된 Bronze 객체 manifest가 정확히 같은 요청과 스냅샷이 수집되었음을 증명할 때만
request fingerprint 재사용으로 공급자 호출을 생략할 수 있다.

content checksum이 새로 수집한 payload가 이전 콘텐츠와 같은지 결정한다.

공급자 메타데이터는 원천 데이터로 저장·정규화하지만 동일성을 결정하지 않는다.

### `provider_file_id` content-stability assumption (bulk-file pre-download skip)

벌크 파일 수집(hub.go.kr 건축물대장 벌크, V-World 데이터셋 파일)은 위 request fingerprint
재사용을 *다운로드 전* 최적화로 적용한다. 작업의 `source_partition_key`(여기에
`provider_file_id`가 포함됨)에 Bronze 객체가 이미 있으면 fetch *전에* 다운로드 전체를
생략하므로 수 GB 파일을 매번 다시 스트리밍하지 않는다.

hub.go.kr은 발행 파일마다 새 OPN ID를 부여하므로 `provider_file_id`만으로 다운로드 전 생략이
올바르다. V-World 데이터셋 파일(`{download_ds_id}-{file_no}`)은 그렇지 않다. 공급자가 같은 파일
번호에 새 판을 올린다(루트 [ADR-0148](../../../../docs/adr/0148-cadastral-parcel-editions-are-held-side-by-side.md)).
그래서 V-World 생략은 파일 번호에 더해 이미 가진 객체가 인벤토리가 적은 공급자 갱신일을 담고 있을
때만 일어나며, 같은 번호의 새 바이트는 내용 해시를 담은 키로 따로 쓴다(루트
[ADR-0152](../../../../docs/adr/0152-bronze-keys-of-reused-provider-file-numbers-name-their-bytes.md),
[ADR-0168](../../../../docs/adr/0168-the-daily-sweep-collects-the-vworld-land-datasets-within-a-byte-budget.md) §3).
빈 DB에서 처음 재수집할 때는 이 생략이 발생하지 않는다.

생략이 변경을 놓쳤다고 의심되면 opt-in 플래그 **`FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1`**를
사용한다. 설정하면 벌크 파일 수집이 생략을 우회해 다시 다운로드하고, 위에서 말한 "전체 또는 hash
검증 스냅샷이 정확성 기준"인 post-download content checksum을 다시 실행한다. 기본값은 off다.
같은 콘텐츠면 객체를 다시 쓰지 않고 원장 행만 맞추며(버킷은 덮어쓰기를 거부한다), 다른 콘텐츠는
새 객체에 기록한다.

## 저장소 정책

Bronze 저장소는 동일 콘텐츠의 중복 R2 쓰기를 피해야 한다.

1. payload bytes를 fetch한 뒤 checksum을 계산한다.
2. content-addressed object가 이미 있으면 동일한 원본 객체 전체를 다시 쓰지 않는다.
3. 기존 content object를 가리키도록 manifest/ledger pointer를 쓰거나 갱신한다.
4. request lineage, collection run id, provider scope와 object checksum을 각각 보존한다.

이렇게 하면 시스템이 다음 두 가지를 모두 증명할 수 있다.

- 어떤 요청을 보냈는지
- 정확히 어떤 content가 반환됐는지

## 속도 정책

다운로드 실행은 공급자가 허용한 한도를 넘지 않으면서 충분히 활용해야 한다.

- 중앙 provider lane scheduler를 사용한다(한도 정본: [`provider-rate-policy.v1.json`](./provider-rate-policy.v1.json)).
- job 병렬 실행은 그 scheduler를 통해서만 한다.
- provider lane마다 AIMD token-bucket 제어를 사용한다.
- 성공률과 latency가 정상일 때만 동시성을 높인다.
- HTTP throttling·provider quota code·timeout·p95 latency threshold에서 backoff한다.
- throttling 시 job을 버리지 말고 defer한다.

고정 sleep은 보수적 override로만 허용한다. 기본 속도 제어 수단으로 사용하지 않는다.

## R2 비용 정책

R2 쓰기는 월별 무료 구간 뒤 Class A 과금 작업이다. 불필요한 객체 쓰기가 유료 작업이 될 수
있으므로 content-addressed Bronze 재사용은 단순 정리 취향이 아니라 비용 통제 요구사항이다.

삭제 작업은 무료지만 저장·write/list·read/head 작업은 무료 구간 사용량과 저장소 클래스에 따라
과금될 수 있다.
