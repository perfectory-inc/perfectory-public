# ADR 0152: 제공자가 파일 번호를 다시 쓰는 원천의 Bronze 키는 내용 해시를 담고, 수집은 부작용 전에 요건을 다 확인한다

- Status: Accepted
- Date: 2026-10-06
- Amends: [ADR-0150](./0150-the-official-parcel-number-history-decides-before-the-jibun-sets.md) (필지고유번호변동연혁 수집의 Bronze 키와 실행 순서)
- Related: [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md) (연속지적도 판, `30563-<n>--sha256-<해시>.zip`), [ADR-0153](./0153-runtime-secrets-have-one-contract.md) (환경 파일 계약)

## Context

2026-10-05 운영(ai-server)에서 `foundation-parcel-number-change.service` 첫 실행이 실패했다.

- 스크립트는 Bronze 되읽기용 레이크하우스 **읽기 키**를 요구했지만, 단위의 환경 파일(recovery, source-sweep) 어디에도
  없었다. 그 확인은 수집 **뒤**에 있었다: 20개 파일이 모두 Bronze 에 쓰인 다음(16:41:59–16:42:01Z,
  `bronze/source=vworldkr__parcel_number_change_history/30527-<번호>.zip`, `30527-1.xlsx`) 실행이 멈췄다.
- 읽기 키를 손으로 넣은 두 번째 실행은 모든 파일에서 R2 `409 ObjectLockedByBucketPolicy` 로 실패했다. 키가 파일 번호로만
  지어져 같은 키에 다시 쓰려 했고, 버킷은 객체 잠금으로 덮어쓰기를 거부한다. 이 거부는 쓰기-한-번 조정이 기다리는
  `412` 가 아니라서 재실행이 스스로 회복하지 못한다.
- 제공자는 같은 파일 번호에 새 판을 올린다(ADR-0148). 번호 키로는 새 판도 재실행도 받을 수 없다.

## Decision

1. **내용 주소 키.** 30527 의 Bronze 객체는 `<download_ds_id>-<file_no>--sha256-<내용 해시>.<확장자>` 로 쓴다.
   수집기(`ingest-vworld-dataset-files`)는 `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY=content_addressed`
   일 때 본문을 끝까지 받아(상한 256MiB, `Content-Length` 와 정확히 같아야 함) 해시를 알고 나서 키를 정한다. 기본값
   `provider_file_id` 는 지금까지와 같다. 키 모양은 `collection_domain::build_bronze_content_object_key` 하나가 만든다.
2. **같은 바이트의 재실행은 성공이다.** 그 키에 객체가 있으면 되읽어 해시·크기가 같을 때 쓰지 않고 원장 행만 맞춘다
   (단일 커미터의 회복 경로). 다른 바이트가 있으면 거부한다(키가 이름 붙인 해시와 다르므로).
3. **넘김은 내용 주소 키만 받는다.** `stage-handoff`·`landed-objects`·`load` 는 계약
   (`vworld-parcel-number-change-history.contract.json` 의 `bronze_objects.key_pattern`)에 맞지 않는 키, 다른 파일의
   키, 되읽은 바이트가 키의 해시와 다른 객체를 거부한다.
4. **2026-10-05 의 번호 키 20개는 대체됐다.** 지우지 않는다(덧붙이기만, 버킷 잠금). 어떤 넘김·수락 상태·Silver 행도
   그것을 가리키지 않는다(첫 실행은 넘김 전에 멈췄다). 같은 바이트를 다시 받으면 원장 행이 내용 키로 옮겨 가고, 옛 키는
   R2 재고 감사에 삭제 후보로 나타난다 — 삭제하지 않는다.
5. **부작용 전에 확인한다.** 수집 스크립트(`parcel-number-change-collect.sh`, `vworld-parcel-edition-collect.sh`)는
   `required_env=(...)` 목록과 DB 자격·도구를 맨 앞에서 확인하고, 없으면 상태 디렉터리도 만들기 전에 78 로 끝난다.
   측정 스크립트 `measure-building-section-packs.sh` 도 같다. 단위 파일이 그 이름을 대는지는 ADR-0153 이 본다.

## Consequences

- 시험: Rust(`vworld_dataset_file_ingest/tests.rs`: 내용 키, 같은 바이트 재실행 무쓰기, 다른 바이트 거부, 길이 불일치,
  번호 키로 건너뛰지 않음), Python(넘김 거부 셋), 실제 스크립트 실행(요건 하나씩 빼면 아무것도 안 부르고 끝남). 각
  시험은 수정 전 코드에서 실패함을 확인했다.
- 연속지적도(30563) 판 수집은 여전히 번호 키를 쓴다. 202609 판은 다른 경로로 내용 키를 얻었지만 다음 판은 번호 키가
  이미 있으면 같은 409 를 만난다. 같은 스위치를 켜는 것은 그 소비자(판 측정·계약)를 확인한 뒤의 후속이다.
