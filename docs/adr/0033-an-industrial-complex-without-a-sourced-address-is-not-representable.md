---
status: current
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-08-16
---

# ADR 0033: 주소 출처가 없는 산업단지는 표현할 수 없다

- Status: Accepted
- Date: 2026-08-16
- 관련: [ADR-0027 모든 가드는 자기 위협 모델을 선언한다](./0027-every-guard-declares-its-threat-model.md), [ADR-0020 도형은 사실의 근거가 아니다](./0020-geometry-is-not-evidence-for-a-fact.md), [FP-ADR-0022 handoff와 저장 형식의 경계](../../platforms/foundation-platform/docs/adr/0022-lakehouse-handoff-vs-storage-format-boundary.md), [FP-ADR-0003 산업단지 Catalog SSOT](../../platforms/foundation-platform/docs/adr/0003-industrial-complex-catalog-ssot.md)

## Context

`infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py`는 첫 커밋부터
`bronze.industrial_complexes_raw_jsonl`을 읽었다. 그 이름은 저장소 전체에서 그 파일 한 곳에만
나온다 — **읽는 쪽만 있고 쓰는 쪽이 없었다.** 이 저장소가 반복해 온 결함이고 직전 PR #79가 같은
모양을 고쳤다.

재료는 실물로 확인했다. Bronze 객체 `bronze/source=vworldkr__sandan_profile/30138-6.zip` 안의
`TB_IRSTT_BASS_HIST.xlsx`는 1,442행이고 기준월은 `sta_ym=202506` 하나이며 `krihs_irstt_code`에
중복이 없다. 단지 식별자·분류·상태·관리기관·시행자·지정일·준공일·지정면적은 모두 들어 있다.
**행정구역은 한 컬럼도 들어 있지 않다.** Spark 잡이 요구하는 `sido_code`, `sigungu_code`,
`primary_bjdong_code`, `address_text` 네 값의 출처가 이 객체 안에 없다.

한편 `silver.industrial_complexes` 계약에서 `sido_code`와 `sigungu_code`는 `required: true`이고,
`sido_code`는 파티션 키이며 `gold.complex_catalog`도 두 값을 필수로 요구한다. 그래서 "네 필드
없이 JSONL을 내고 Spark 계약을 고쳐 분리한다"는 방향은 실제로는 둘 중 하나로 귀결한다. 없는
값을 빈 문자열이나 `null`로 채워 넣거나, **정본 계약의 필수 컬럼이자 파티션 키를 한 소스의 결손에
맞춰 완화하거나.** 앞은 없는 사실을 만드는 것이고 뒤는 소비자 전부가 쓰는 계약을 약화시킨다.

[ADR-0027](./0027-every-guard-declares-its-threat-model.md) 결정 4항은 예방 가능한 자리에서
탐지를 만들지 말라고 한다. "주소를 모르는 산업단지가 지도에 올라갔는지" 사후에 찾아내는 검사는
탐지다. 타입으로 그 행이 애초에 만들어지지 못하게 하는 것이 예방이다.

## Decision

1. **생산자는 주소 출처를 주입받고, 주입이 없으면 Bronze 객체를 열기 전에 실패한다.**
   `export-industrial-complex-bronze-raw-jsonl` 커맨드는
   `FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL_ADDRESS_SOURCE_PATH`를 필수로 읽고,
   그 파일을 가장 먼저 읽는다. 위치를 증명하지 못하는 실행은 Bronze 객체도 출력 경로도 건드리지
   않는다.

2. **주소는 타입이 증거다.** `IndustrialComplexAddress`는 비공개 필드와 검증 생성자 하나만
   가진다. `primary_bjdong_code`는 `catalog.industrial_complex`의
   `industrial_complex_primary_bjdong_code_shape` CHECK와 같은 규칙(ASCII 숫자 10자리)으로
   검증하고, `sido_code`와 `sigungu_code`는 **받지 않고 그 코드에서 파생한다.** `address_text`와
   출처 두 필드(`address_source_dataset`, `address_source_record_id`)는 공백일 수 없다.
   `IndustrialComplexBronzeRawRow`는 이 타입을 `Option`이 아니라 값으로 소유한다. 따라서 주소
   없는 행은 **만들어질 수 없고**, 만들 수 없으므로 직렬화될 수 없다.

3. **한 행이라도 주소가 없으면 실행 전체가 실패하고 아무것도 쓰지 않는다.** 부분 출력이 있으면
   그 뒤의 Silver 적재가 전수인 것처럼 보인다. 실패 메시지는 미해결 건수와 처음 몇 개의
   `official_complex_code`를 적는다.

4. **입력 컬럼 목록과 값 도메인은 Rust에서 내보내고 Spark 잡이 읽는다.**
   `bronze.industrial_complexes_raw_jsonl`의 컬럼은 `silver.industrial_complexes` 계약에서 Spark
   잡이 스스로 만드는 네 컬럼(`complex_id`, `complex_name_normalized`, `valid_to_utc`,
   `row_checksum_sha256`)을 뺀 것으로 정의하고, 값 도메인(`complex_kind`, `status`)과 함께
   `industrial_complex_lakehouse_contracts.json`으로 내보낸다. Python 잡의 `INPUT_COLUMNS`,
   `ALLOWED_COMPLEX_KINDS`, `ALLOWED_STATUSES`는 이제 그 산출물을 읽는다. 같은 목록을 두 언어가
   따로 적어 두는 거울을 남기지 않는다.

5. **출력은 append-only다.** 출력 경로에 이미 파일이 있으면 실행은 거부한다. 이전 Silver 적재가
   무엇으로 만들어졌는지를 재실행이 조용히 지우지 못한다.

6. **원본 라벨은 매핑에 없으면 실패한다.** `lrstt_ty`와 `make_sttus_nm`은 표에 있는 라벨만
   wire 값으로 옮기고, 처음 보는 라벨은 `unknown`으로 떨어뜨리지 않고 그 라벨을 적어 실패한다.
   `status`의 `unknown`은 계약이 허용하는 값이지만 **매핑의 기본값이 아니다.**

### 위협 모델 ([ADR-0027](./0027-every-guard-declares-its-threat-model.md) 결정 1항)

- 부류: `honest-mistake detection`이 아니라 **표현 불가능(prevention)** 이다. 권위 경계는 셸
  스캐너가 아니라 Rust 타입과 실행 실패다.
- Prevents: 생산자가 주소 네 필드를 빈 문자열·`null`·`"미상"`으로 채우거나, 주소 출처 배선을
  잊은 채 JSONL을 만들어 내는 일. 그런 행은 생성자를 통과하지 못하므로 존재할 수 없다.
- Does not prevent: 운영자가 **거짓 내용의 주소 해소 파일을 일부러 만들어** 주입하는 일. 그것은
  타입이 아니라 출처의 문제이고, 해소 레코드가 스스로 싣는 `address_source_dataset`과
  `address_source_record_id`가 그 판단의 근거다. 또한 `primary_bjdong_code`가 **실재하는
  법정동인지**는 아직 대조하지 않는다 — 자릿수와 형태만 본다.

## Consequences

- 주소 출처가 붙기 전까지 이 커맨드는 실물 1,442행에 대해 **출력을 만들지 못한다.** 그것이 이
  결정의 요지다. 관을 열어 두고 빈 값을 흘려보내는 것보다, 없는 사실 앞에서 멈추는 편이 낫다.
  산업단지 주소 출처 조사가 끝나면 그 결과가 해소 파일의 생산자가 되어 이 커맨드에 꽂힌다.
- 해소 파일 형식은 JSONL 한 줄에 `official_complex_code`, `primary_bjdong_code`, `address_text`,
  `address_source_dataset`, `address_source_record_id`다. 같은 코드가 두 번 나오면 거부한다.
- `silver.industrial_complexes` 계약은 그대로 둔다. 필수 컬럼도 파티션 키도 바꾸지 않는다.
- 남은 일: (1) `primary_bjdong_code`를 행정구역 원장과 대조하는 검증, (2) `lrstt_ty`·
  `make_sttus_nm` 라벨 표를 실물 1,442행으로 채워 넣기 — 지금 표는 확인한 라벨만 담고 있고
  나머지는 실패로 드러난다.

---

## 개정 각주 — 2026-08-16

결정은 바뀌지 않았다. 위 `남은 일` (2)의 진행만 기록한다.

`202506` Bronze 객체를 실제로 읽어 전수 대조했다. 헤더 10개는 실물 20개 컬럼 안에 모두 있었고
`lrstt_ty`는 4종(`국가` 94 · `일반` 812 · `도시첨단` 51 · `농공` 485)이 표에 모두 있었다.
`make_sttus_nm`은 `준비중`(47)·`보상중`(37)이 표에 없어 1,442건 중 84건이 멈췄다. 두 라벨은
조성공정율 0%·준공인가 0건이라 `planned`로 옮겼고, 근거표는
`industrial_complex_bronze_raw_plan.rs`의 `COMPLEX_STATUS_LABELS_OBSERVED` 문서 주석에 있다.
이제 라벨 표는 측정된 것(`*_OBSERVED`)과 아직 어느 스냅샷도 내지 않은 것(`*_PRESUMED`)으로
나뉘어 있다. 다른 기준월은 여전히 미측정이므로 (2)는 열린 채로 둔다.
