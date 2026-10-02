# ADR 0125: foundation-platform — 호실의 건물 연결은 원천 부모 키와 근거를 함께 갖는다

- Status: Accepted
- Date: 2026-10-02

## Context

호실의 `mgm_bldrgst_pk`는 호실 대장의 식별자다. 건물 대장의 식별자와 같은 칸 이름을
쓴다고 같은 대상이 되지는 않는다. 필지와 동 이름으로 부모 건물을 추정하면 같은 이름이
여럿이거나 원천의 필지가 다를 때 잘못 연결하거나 확실한 연결을 잃는다.
칸 구조가 같아도 계산 방법이 달라지면 값은 달라진다. 스키마 검사는 관계의 근거를 대신할 수 없다.

불변식: 자동 연결은 같은 원천 스냅숏에서 확인한 전유부 대장의 상위 표제부 키를 따른다.
그 관계를 만든 입력의 근거를 전달한다. 확인할 수 없는 관계는 NULL과 사유로 남긴다.
승인으로 바꾼 관계는 현재 활성 승인 원장과 대상·결과가 일치해야 한다.

## Decision

1. 기존 Rust 스트리밍 수출기와 Spark·Iceberg 적재 경로를 유지한다.
   `building_register_basis.rs`가 기본개요의 `mgmUpBldrgstPk`를 읽고 전유부 종류 `4`에서
   표제부 종류 `3`으로 이어지는 관계를 검증한다. 표제부 입력에도 해당 부모 키가 있어야 한다.
   부모 누락·충돌·잘못된 종류·자기 참조는 연결하지 않는다. 동 이름이나 단일 후보로 보충하지 않는다.
2. 입력 ZIP의 역할·날짜를 검증하고 실제 읽은 세 파일의 SHA-256을 기록한다. 읽기 전후
   크기와 해시로 실행 중 변조를 검사한다. PK가 중복돼도 종류·부모가 같으면 동일한 관계다.
   보조 속성의 충돌은 그 속성을 비우며, 확실한 PK 관계를 지우지 않는다.
3. `silver.building_register_units`에 nullable string 칸 세 개를 추가한다:
   `building_link_source_record_id`, `building_link_input_sha256`, `building_link_reason`.
   자동 연결은 기존 `building_link_method`의 `parent_key` 값과 출처·해시를 갖는다.
   기존 `unit_designation_normalized` 계산, 정규화 이유, 가격 이력은 유지한다.
4. 관계 근거의 필드·방법·형식은 기존 `building-unit-handoff.json`의
   `relationship_evidence_policy` 하나가 소유한다. Python 검증과 Rust 소비 경계가 읽는다.
   Silver 적재는 근거 없는 연결을 거부한다. 소비 경로는 근거 없는 과거 연결을 NULL로
   바꾸고 사유를 보존한다. 자동 근거와 승인 근거는 섞지 않는다.
5. Catalog 적재·건물 연결 복구 manifest를 `v2`로 올리고 `building_link_evidence`를 필수로
   전달한다. 부모 키의 명시적 NULL은 연결 철회다. 칸 생략은 NULL로 취급하지 않고 거부한다.
   복구 UPDATE도 기존 연결을 지울 수 있어야 하며, Catalog에 없는 부모를 옛 연결로 대신하지 않는다.
6. 정규화 제안은 `building_register_unit.normalized.v2`로 올린다.
   `proposed_record.mgm_bldrgst_pk`로 승인 대상을 원천 호실 PK에 묶는다.
   Intelligence는 모델이 만든 PK 대신 검증된 입력의 PK를 제출한다.
   기존 활성 승인 reader로 application ID·원천 행·호실 PK·부모 결과를 대조한다.
   부모 키 누락·잘못된 자료형을 철회로 해석하지 않는다. v1 제안과 대상 PK 없는 승인 산출물은
   거부하며, 과거 승인을 추정한 호실에 다시 붙이지 않는다.
7. Gold는 부모 PK로 연결하고 부모 건물의 필지에 호실을 넣는다. 호실 자신의 `unit_pnu`와
   `parcel_id`는 보존해 따로 검사한다. 확실한 부모 키가 있어도 해당 표제부가 제공 대상에서
   제외됐다면 호실은 미연결 목록에 남고 공개 `building_id`는 NULL이다. 원천 부모 근거는 유지한다.
   이미 저장된 serving 객체는 실제 바이트가 새로 검증한 내용과 같을 때만 재사용한다.
8. 세 칸이 정식 계약에 들어가는 이 변경에서 ADR-0124의 임시 예외를 제거한다.
   운영 쓰기는 병합된 릴리스만 실행한다. 소비 작업보다 스키마 마이그레이션을 먼저 수행하고,
   원천 입력·활성 승인 호환성을 확인한 뒤 새 수출·적재·handoff·Gold를 순서대로 만든다.
   데이터 스냅숏 롤백이 스키마도 되돌린다고 가정하지 않는다. 실행 증거는 비공개 운영 기록에 남긴다.

## Alternatives

- 이름·필지로 보충 연결: 동명 중복과 필지 차이가 관계를 잘못 확정하므로 기각한다.
- 호실과 부모의 필지 일치 강제: 원천이 명시한 관계를 버리므로 기각한다.
- 근거 칸만 추가하고 소비자 유지: 추정과 확정을 구분할 수 없어 기각한다.
- 별도 그래프 DB·매칭 엔진: 이미 제공되는 키 관계 확인에 필요하지 않다. 기존 Rust 컬렉션,
  serde, SQLx, Spark 조인, Iceberg 스키마 진화를 재사용한다. 원천 해석과 관계 판정만 도메인 코드다.
- 승인 원장 복제: 승인·철회 의미가 갈라지므로 기존 활성 reader를 재사용한다.

## Verification and limits

- 합성 원천으로 누락·충돌·종류·날짜·해시·자기 참조·승인 대상을 검사한다.
- 실제 Spark로 이름 추정 거부, 불완전 근거 거부, 필지가 다른 부모 연결, NULL 철회,
  압축 JSON의 근거 객체와 가격 이력 보존을 검사한다. 공유 Gold fixture는 실제 Spark로 만든다.
- 실제 PostgreSQL의 폐기 가능한 DB에서 복구 UPDATE와 재실행을 확인하고 기존 검증 lane에 등록한다.
- 활성 승인 확인은 실행 시작 시점의 조회다. 분산 트랜잭션 잠금을 제공하지 않는다.
- 원천 자체가 무오류이거나 전국 운영 값 검증이 끝났다는 뜻이 아니다. 전수 값 비교와 병합된
  릴리스 적용은 별도 운영 검증이다. 기존 v1 승인은 원천 확인 후 명시적인 재승인·철회로 처리한다.

## References

- [국토교통부 건축HUB 건축물대장정보 서비스](https://www.data.go.kr/data/15134735/openapi.do): 대장 유형·PK 체계 변경 안내.
- [건축HUB 제공기관 안내](https://www.hub.go.kr/portal/psg/idx-intro-openApi.do): 표제부·전유부 등 원천 제공 범위.
- [ADR 0007 — 공개 코드와 비공개 운영 경계](./0007-public-code-private-operations-boundary.md)
- [ADR 0107 — 정규형 호 지정자는 파생으로 얻는다](./0107-the-normalized-unit-designation-is-derived-not-approved.md)
