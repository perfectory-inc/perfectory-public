# ADR 0039: Gold 서빙 아티팩트는 lakehouse 버킷에 살고, 타일만 나간다

- Status: Accepted
- Date: 2026-08-18
- Supersedes: ADR-0038
- Amended by: ADR-0055 (공개 경계는 CDN prefix 설정이 아니라 비공개 버킷 앞의 허용 목록 Worker)

## Context

ADR-0038 은 "가져가라고 만든 산출물은 정본 바이트와 버킷을 같이 쓰지 않는다"고 정하고, Gold
프로필을 lakehouse 버킷에 쓰지 못하게 막았다. **그 근거가 틀렸다.** 실물을 확인한 결과:

1. **선례가 이미 lakehouse 안에 있다.** `parcel_marker_anchor_artifact_export.rs` 는
   `R2ObjectStorage::from_env()` — 즉 lakehouse 연결 — 로
   `gold/parcel-marker-anchors/artifacts/{artifact_id}/manifest.json` 과 그 옆의 JSONL 을 쓴다.
   화면·소비자가 가져가는 JSON 아티팩트가 이미 그 버킷의 `gold/` 아래에 산다.

2. **lakehouse 연결 계약이 공개 주소를 갖고 있다.**
   `config/r2-connections.contract.json` 의 `lakehouse.required_env` 에
   `FOUNDATION_PLATFORM_R2_LAKEHOUSE_PUBLIC_BASE_URL` 이 들어 있다. 그 버킷이 공개 주소로
   서빙된다는 전제가 계약에 이미 적혀 있다.

3. **ADR-0006 의 분리는 타일 한정이다.** 문면은
   *"Martin reads the dedicated private derivative R2 bucket with separate bucket-scoped read-only
   credentials ... The canonical lakehouse bucket is not a tile origin."* 이다.
   타일을 뗀 이유는 Martin 의 자격증명이 **버킷 단위**라서 타일 origin 을 lakehouse 로 두면
   Martin 에게 lakehouse 전체 읽기 권한을 줘야 하기 때문이다. "서빙 아티팩트는 lakehouse 에
   두지 마라"가 아니다.

ADR-0038 은 이 셋을 확인하지 않고 썼다. 그 결과 실제로 존재하는 배치를 코드가 거부하게 됐다.

## Decision

1. **산업단지 Gold 프로필은 lakehouse 버킷의
   `gold/industrial-complex/profiles/{artifact_id}.json` 에 산다.** 이미 그 자리에 쓰인
   1,442개는 옳은 자리에 있으므로 옮기거나 지우지 않는다. `export-industrial-complex-gold-profiles`
   의 `r2` 드라이버는 lakehouse 연결(`FOUNDATION_PLATFORM_R2_LAKEHOUSE_*`)로 쓴다.

2. **ADR-0038 이 도입한 "보호 버킷 거부 목록"과
   `serving_derivative_object_storage.rs` 는 철회한다.** 그 모듈이 있던 이유는 lakehouse 를
   막는 것이었고, 그 이유가 사라졌다. `TileDerivativeR2Config` 는 ADR-0038 이전 형태로
   되돌아가 자기 거부 목록을 다시 소유한다.

3. **타일 버킷 분리는 유지한다.** 근거는 ADR-0006 이 살아 있다: Martin 의 read-only 자격증명은
   버킷 단위이므로 타일 origin 은 전용 버킷이어야 하고, `TileDerivativeR2Config` 는 계속
   lakehouse 를 포함한 보호 버킷을 거부한다. 이 ADR 은 그 규칙을 건드리지 않는다.

4. **키 검사는 남긴다.** 프로필 export 는 쓰기 직전에
   `r2_layout::is_industrial_complex_gold_profile_key` 로 자기 키가 정본 형태인지 확인한다.
   그 함수는 `industrial_complex_gold_profile_key` 로 왕복시켜 판정하므로, 이 모듈이 스스로
   만들었을 키만 통과한다.
   막는 사고: 커맨드가 임의의 객체 키로 lakehouse 버킷에 쓰는 것 — 그 버킷에는 Bronze 수집
   원본과 Iceberg 표가 함께 있다.

5. **요약의 `output_bucket` 은 남긴다.** 어느 버킷에 썼는지가 산출물에 적힌다. 이 필드는
   실제 사고에서 나왔다: 공유 cargo target 이 `debug/<bin>.exe` 를 갱신하지 않아 수정 이전
   바이너리가 돌았고, 요약의 라벨 하나가 유일한 단서였다.

## Consequences

- Gold 아티팩트의 배치가 하나로 통일된다: parcel marker anchor 도 산업단지 프로필도 lakehouse
  버킷의 `gold/` 아래. 새 Gold 아티팩트도 그 자리를 따른다.
- 버킷 공개 주소나 bucket-bound domain은 prefix 권한을 제공하지 않는다. ADR-0055의 비공개
  버킷 앞 Worker가 `r2_layout` 정본 프로필 키만 허용하며, Bronze·Silver·Iceberg 공간은 계속
  공개 경계 밖에 둔다.
- ADR-0038 은 폐기되지 않고 대체된다. 왜 잘못된 경계를 한 번 그었는지가 기록으로 남는다.
