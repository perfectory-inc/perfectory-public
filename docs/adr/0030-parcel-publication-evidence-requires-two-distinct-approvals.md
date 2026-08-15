---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-14
---

# ADR 0030: 필지 발행 증거는 서로 다른 두 승인을 구별한다

- Status: Accepted
- Date: 2026-08-14
- Supersedes: [ADR-0029](./0029-parcel-publication-evidence-is-written-from-the-terminal-run.md) §2의 eligibility boolean 고정 결정만
- Implements: [ADR-0025](./0025-parcel-publication-names-one-sealed-iceberg-evidence.md), [ADR-0027](./0027-every-guard-declares-its-threat-model.md)

## 맥락

`ParcelPublicationExecutionEvidence::validate_publication_claims`는
`production_cutover_allowed`와 `national_rollout_allowed`가 모두 `true`일 때만 증거를 허용한다.
그러나 Foundation 운영 규칙은 명시적 운영자 승인 전까지 national rollout을 허용하지 않는다.
또한 기존 `foundation-platform.national_data_collection_rollout_approval.v1` check artifact는 national
rollout 승인과 선행 증거를 검증하지만, 스스로 `does_not_approve_production_cutover`라고 한계를 밝힌다.

저장소에는 production cutover 승인을 나타내는 불변 artifact 계약이 없다. 따라서 terminal run의
사실만으로 두 boolean을 고정 `true`로 쓰면 승인되지 않은 실행이 승인된 것처럼 보인다. 반대로
`false` 문서를 R2에 쓰면 봉인자가 영원히 거부하는 쓰레기 객체만 남는다.

## 결정

producer는 R2 PUT 전에 서로 성격이 다른 두 게이트를 모두 통과해야 한다.

1. **National rollout은 검증한다.**
   `FOUNDATION_PLATFORM_PARCEL_PUBLICATION_NATIONAL_ROLLOUT_APPROVAL_CHECK_PATH`가 가리키는 기존
   national rollout approval-check JSON을 읽고, schema, `status=ready`, `approved=true`,
   `approved_scope=national`, `national_rollout_allowed=true`, 빈 blocker 집합을 검사한다. 이 검사는
   기존 national approval 모듈이 소유하며 parcel producer가 계약을 복제하지 않는다.
2. **Production cutover는 현재 신뢰한다.**
   저장소에 승인 artifact SSOT가 없으므로
   `FOUNDATION_PLATFORM_PARCEL_PUBLICATION_PRODUCTION_CUTOVER_CONFIRM=1`이라는 기존 CONFIRM 관례의
   exact sentinel을 요구한다. 변수가 없거나 다른 값이면 실패한다. 이는 검증 가능한 증거가 아니라
   명령 실행 환경과 evidence-writer credential 소유자를 신뢰하는 임시 경계다.
3. 두 게이트 중 하나라도 실패하면 producer는 문서를 직렬화하거나 R2 PUT을 시도하지 않는다.
   `false` eligibility 문서를 기록하지 않는다. 오류는 실패한 env/path와 기대한 조건을 지명한다.
4. 두 게이트를 통과한 뒤에만 기존 strict contract의 두 boolean을 `true`로 만들고, contract validator를
   다시 실행한 다음 create-only/content-addressed write를 수행한다.

ADR-0029의 terminal run, snapshot, key, credential, threat-model 결정은 그대로 유지한다. 이 ADR은
§2에서 approval 확인 없이 boolean을 고정하던 부분만 대체한다.

## 기각한 대안

- **terminal run 성공이면 두 값을 자동으로 `true`로 둔다.** 실행 성공은 운영자 승인과 다른 사실이며
  Foundation 규칙을 우회하므로 기각한다.
- **national approval artifact 하나를 두 승인에 함께 쓴다.** 해당 artifact가 production cutover를
  승인하지 않는다고 명시하므로 문면을 거슬러 읽는 오류다.
- **미승인 상태를 `false` 문서로 R2에 남긴다.** 봉인 불가능한 객체만 영구히 남고 성공/실패 의미를
  흐리므로 PUT 전 실패한다.
- **env가 존재하기만 하면 production 승인을 인정한다.** 빈 값과 오타도 승인으로 읽히므로 exact
  `=1` sentinel만 허용한다.
- **이 작업에서 새 production approval artifact 계약을 만든다.** 운영 승인 주체·수명·취소·감사
  정책을 결정해야 하는 별도 설계이며 실행 증거 생산자 구현 범위를 넘는다.

## 위협 모델과 한계

National gate는 누락·blocked·다른 scope·미승인 check artifact가 발행 주장으로 승격되는 것을 막는다.
Exact sentinel은 실수로 production cutover를 허용하는 것을 막지만, env와 전용 R2 writer credential을
가진 주체의 고의 위조는 막지 못한다. producer가 승인 없이 PUT하지 않는다는 보장은 코드와 fixture로
검증하지만, production 배포 환경의 secret 정책 자체는 이 저장소만으로 증명하지 않는다.

## 후속 작업

Production cutover 승인 SSOT가 없다. 현재는 env sentinel이 그 자리를 대신하며 이것이 credential
소유자 신뢰 경계다. 승인 주체·시각·범위·취소 정책을 가진 불변 artifact 계약이 생기면 producer는
sentinel 대신 그 artifact를 검증해야 한다.
