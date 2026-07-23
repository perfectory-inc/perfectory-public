# 건축물대장 층 정규화 규칙 v1

상태: 초안
소유: Foundation Platform
범위:

- hub.go.kr `hubgokr__building_register_floor_overview` Bronze ZIP 안의 UTF-8 pipe-delimited TXT
- data.go.kr `datagokr__building_register_floor_overview` Bronze JSON의 층 관련 필드

## 목적

건축물대장 층별개요에는 층을 뜻하는 값이 여러 방식으로 들어옵니다. 예를 들어 `지하1층`,
`지1층`, `지층`, `1층`, `옥탑1층`, `각층`처럼 사람이 보면 같은 의미를 알 수 있지만,
기계가 바로 비교하기에는 일관되지 않습니다.

Foundation Platform의 원칙은 다음입니다.

- Bronze 원본은 절대 고치지 않습니다.
- Rust deterministic parser가 먼저 안전하게 정규화합니다.
- 규칙으로 확정할 수 없는 값만 `proposal_required`로 남깁니다.
- AI는 결정자가 아니라 제안자입니다.
- AI 제안은 바로 Silver/Gold/canonical DB에 쓰지 않습니다.
- 승인된 제안만 Foundation Platform 내부 command/pipeline을 통해 반영합니다.

## 입력 필드

현재 층 정규화가 읽는 논리 필드는 아래입니다. data.go.kr JSON은 필드명으로 읽고,
hub.go.kr 벌크 TXT는 같은 의미의 고정 컬럼 위치에서 읽습니다.

| 원본 필드 | 의미 | 예시 |
|---|---|---|
| `mgmBldrgstPk` | 건축물대장 관리 PK | `SYNTHETIC-BUILDING-PK-0001` |
| `flrGbCd` | 층구분코드 | `10`, `20`, `30`, `40` |
| `flrGbCdNm` | 층구분명 | `지하`, `지상`, `옥탑`, `각층` |
| `flrNo` | 층번호 | `1`, `15` |
| `flrNoNm` | 층번호명 | `지하1층`, `지1층`, `1층`, `옥탑1층` |

`flrNo`는 “몇 개 층인지”가 아니라 해당 row가 가리키는 층 번호입니다. 지하/지상 여부는
`flrGbCd`와 `flrGbCdNm`가 1차 기준입니다.

## Silver 출력 형태

Silver handoff는 원본값과 정규화값을 같이 보존합니다.

| 필드 | 의미 |
|---|---|
| `floor_row_id` | row 단위 안정 식별자 |
| `mgm_bldrgst_pk` | 원본 건축물대장 관리 PK |
| `floor_type_code_raw` | 원본 `flrGbCd` |
| `floor_type_name_raw` | 원본 `flrGbCdNm` |
| `floor_number_raw` | 원본 `flrNo` |
| `floor_label_raw` | 원본 `flrNoNm` |
| `floor_kind` | `above_ground`, `basement`, `rooftop`, `all_floors`, `multi_floor_lower`, `multi_floor_upper`, `unknown` |
| `floor_number` | 정규화된 층 번호. 지상/지하 모두 양수 번호로 저장 |
| `floor_index` | 정렬/비교용 인덱스. 지상은 양수, 지하는 음수 |
| `floor_display_ko` | 사람이 보는 표준 표시. 예: `지상 1층`, `지하 1층`, `옥탑 1층` |
| `normalization_status` | `accepted`, `proposal_required`, `rejected` |
| `normalization_reason` | parser가 왜 그렇게 판단했는지 나타내는 reason code |
| `bronze_object_key` | 원본 Bronze 객체 경로 |
| `source_snapshot_id` | 원본 스냅샷 식별자 |
| `row_checksum_sha256` | Silver row 내용 체크섬 |

## 정규화 규칙

### R1. 층구분코드/층구분명이 1차 기준입니다

| 원본 | `floor_kind` |
|---|---|
| `20` 또는 `지상` | `above_ground` |
| `10` 또는 `지하` | `basement` |
| `30` 또는 `옥탑` | `rooftop` |
| `40` 또는 `각층` | `all_floors` |
| 복수층 하층 계열 | `multi_floor_lower` |
| 복수층 상층 계열 | `multi_floor_upper` |
| 비어 있거나 알 수 없음 | `unknown` |

코드와 이름이 서로 충돌하면 추측하지 않고 `proposal_required`로 보냅니다.

### R2. 지상층

`flrGbCdNm=지상`이고 `flrNo=1`, `flrNoNm=1층`이면 다음처럼 정규화합니다.

```text
floor_kind       = above_ground
floor_number     = 1
floor_index      = 1
floor_display_ko = 지상 1층
status           = accepted
```

`flrNo`와 `flrNoNm`의 숫자가 다르면 자동 정규화하지 않습니다.

### R3. 지하층

`flrGbCdNm=지하`이고 `flrNo=1`, `flrNoNm=지하1층` 또는 `지1층`이면 다음처럼 정규화합니다.

```text
floor_kind       = basement
floor_number     = 1
floor_index      = -1
floor_display_ko = 지하 1층
status           = accepted
```

`지층`처럼 번호가 없는 일반 라벨은 `flrNo`가 명확할 때만 그 번호를 사용합니다. 예를 들어
`flrNo=2`, `flrNoNm=지층`이면 `지하 2층`으로 정규화합니다. `flrNo`가 없거나 0이면
`proposal_required`입니다.

### R4. 옥탑

`옥탑`, `옥탑층`은 번호 없는 옥탑으로 보존합니다.

```text
floor_kind       = rooftop
floor_number     = null
floor_index      = null
floor_display_ko = 옥탑
```

`옥탑1층`, `옥탑 1층`은 `옥탑 1층`으로 정규화합니다.

`901`, `9001` 같은 특수 숫자는 지금 단계에서 “9층”이라고 추정하지 않습니다. 공급자 규칙이
증명되기 전까지 `proposal_required`로 남깁니다.

### R5. 각층/복수층

`각층`, 복수층 하층, 복수층 상층은 일반 지상/지하 층으로 강제로 바꾸지 않습니다. 원본 구조의
의미를 별도 `floor_kind`로 보존합니다.

### R6. AI 제안 대상

아래 조건은 자동 정규화하지 않고 `proposal_required`로 보냅니다.

- 층구분코드와 층구분명이 충돌
- 층번호와 층번호명의 숫자가 충돌
- 지하 row인데 층번호명이 지상처럼 보임
- 층번호가 비어 있거나 0인데 라벨만으로 확정할 수 없음
- 공급자 특수 숫자 규칙이 증명되지 않은 값
- parser가 모르는 새 패턴

## AI proposal inbox와 DB

Silver row 자체는 Postgres canonical DB에 바로 들어가지 않습니다. Silver는 lakehouse 계층의
정규화 산출물이며 writer-neutral handoff 계약을 통해 다음 단계로 전달됩니다.

DB에 들어가는 것은 AI가 만든 “정규화 제안”과 그 제안의 심사/적용 기록입니다.

- `catalog.normalization_proposal`: AI 또는 외부 정규화 서비스가 제출한 제안 원문, confidence,
  evidence, raw lineage, trace id, policy/model/prompt 정보
- `catalog.normalization_proposal_review`: staff/admin이 승인/반려/수정요청한 기록
- `catalog.normalization_application`: 승인된 제안이 Foundation Platform command로 canonical 대상에
  적용된 기록과 rollback 추적

즉 DB proposal inbox는 “AI가 바로 쓰는 곳”이 아니라 “AI가 낸 제안을 묶어두는 검수함”입니다.

## 계약 표면

- Rust deterministic parser가 두 Bronze 입력 형식을 동일한
  `silver.building_register_floors` 계약으로 변환해야 합니다.
- `proposal_required` row만 AI proposal 입력으로 내보내야 합니다.
- 승격 전 품질 게이트는 최소한 `proposal_required_count`와
  `invalid_checksum_count`를 검사해야 합니다.
- AI proposal inbox는 `building_register_floor` target을 받을 수 있어야 하지만, 제출만으로
  canonical data를 변경해서는 안 됩니다.

구현 경로와 지원 command의 SSOT는 코드와 `cargo xtask verify foundation`입니다. 이 문서는
배포 상태나 실행 결과의 목록을 복제하지 않습니다.

## 승격 조건

1. proposal handoff는 intelligence-platform의 명시적 ingest 계약이 있을 때만 연결합니다.
2. `building_register_floor` canonical 반영은 staff/admin 검토와 rollback 가능한 application
   기록이 강제된 command를 통해서만 허용합니다.
3. Iceberg/Parquet publish는 Write-Audit-Publish 품질 게이트를 통과한 산출물만 허용합니다.
