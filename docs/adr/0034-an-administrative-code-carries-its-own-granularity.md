---
status: current
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-08-16
---

# ADR 0034: 행정구역 코드는 자기 정밀도를 싣고 다닌다

- Status: Accepted — 결정 4항의 계약 문장은 [ADR-0035](./0035-a-region-the-pipeline-does-not-use-is-not-required.md)가 대체
- Date: 2026-08-16
- 관련: [ADR-0033 주소 출처가 없는 산업단지는 표현할 수 없다](./0033-an-industrial-complex-without-a-sourced-address-is-not-representable.md), [ADR-0020 도형은 사실의 근거가 아니다](./0020-geometry-is-not-evidence-for-a-fact.md), [ADR-0032 제공기관 신원은 도메인 라벨에서 파생한다](./0032-provider-identity-is-derived-from-domain-label.md), [ADR-0027 모든 가드는 자기 위협 모델을 선언한다](./0027-every-guard-declares-its-threat-model.md)

## Context

[ADR-0033](./0033-an-industrial-complex-without-a-sourced-address-is-not-representable.md)은
산업단지 주소를 **주입 입력**으로 만들고 그 주입기를 다음 작업으로 남겼다. 그 주입기를 실제로
만들면서 출처를 전수로 열어 보니, 남겨 둔 자리의 이름이 실물과 맞지 않았다.

주소 출처는 한국산업단지공단(`industryland.or.kr`, 승인 제공기관 — [ADR-0032](./0032-provider-identity-is-derived-from-domain-label.md))의
단지 목록이다. 실측한 사실:

- `danji_cd` = Bronze 프로필의 `krihs_irstt_code`. **조인은 코드다.** 이름 매칭도 도형 추론도
  아니다([ADR-0020](./0020-geometry-is-not-evidence-for-a-fact.md)).
- `danji_loc`은 공식 주소 텍스트이고 1,442건 전부 비어 있지 않다.
- `addr_cd`는 10자리이고 **끝 5자리가 전부 `00000`이다.** 즉 시군구 코드다. `addr_nm`,
  `addr_si_cd`, `addr_gu_cd`는 목록에서 항상 `null`이고, 상세 엔드포인트에서도 코드 두 개는
  `null`이다(이름만 있다).
- VWorld 산단 도형 6종에는 주소 필드가 아예 없다.

**어느 출처도 읍면동 단위 법정동코드를 주지 않는다.** 그리고 이건 데이터 결함이 아니라 대상의
성질이다. 산업단지는 읍면동 여러 개, 때로는 시도 여러 개에 걸친다. `111010` 한 건의 실제
`danji_loc`이 그 자체로 증거다:

```
서울특별시 구로구 구로동, 금천구 가산동 일원, 인천광역시 부평구 청천동, 서구 가좌동,
남구 주안동, 부평구 십정동 일원
```

이 단지에 "대표 법정동" 하나를 고르는 것은 요약이 아니라 **없는 사실을 만드는 일**이다.

문제는 그 다음이다. 시군구 코드 `1153000000`도 법정동 코드 `1153010200`도 똑같이
`^[0-9]{10}$`를 통과한다. ADR-0033이 만든 `IndustrialComplexAddress`의 검증은 자릿수와
숫자 여부만 본다(그 ADR의 위협 모델이 "실재하는 법정동인지는 아직 대조하지 않는다"고 스스로
적어 두었다). 따라서 시군구 코드를 `primary_bjdong_code`라고 적힌 자리에 그냥 넣으면
**형식 검사를 통과하고, 하위 소비자는 그것을 읍면동으로 읽는다.** 뒤 5자리 `00000`이 "이 시군구의
00000번 동"으로 조용히 해석되는 것이다. 검사로 잡을 수 없다 — 형식은 정말로 맞기 때문이다.

한편 `silver.industrial_complexes` 계약은 이미 옳은 모양을 하고 있었다. `sido_code`와
`sigungu_code`는 `required: true`이고 `primary_bjdong_code`는 **`required: false`다.** Spark 잡도
그 컬럼을 `trim_to_null`로 읽는다. 출처가 시군구까지만 말했다는 사실은 이 계약 안에서 이미
표현 가능하다.

## Decision

1. **주소 값은 코드와 함께 그 코드의 정밀도를 싣는다.**
   `IndustrialComplexAddress`는 `primary_bjdong_code: String`이 아니라
   `administrative_code: String` + `granularity: IndustrialComplexAddressGranularity`를 가진다.
   정밀도는 `sigungu` 또는 `legal_dong` 둘뿐이고 **기본값이 없다.** 정밀도를 말하지 않은 해소는
   그 코드가 무엇을 가리키는지 말하지 않은 것이다.

2. **선언한 정밀도와 코드의 모양이 어긋나면 값이 만들어지지 않는다.**
   `sigungu`는 끝 5자리가 `00000`이어야 하고, `legal_dong`은 `00000`이 **아니어야** 한다.
   시군구 코드를 `legal_dong`이라고 선언하는 것도, 법정동 코드를 `sigungu`라고 선언하는 것도
   생성자가 거부한다. 라벨만으로는 증거가 되지 못하므로 모양이 라벨을 검증한다.

3. **`primary_bjdong_code()`는 `Option`이다.** 정밀도가 `sigungu`면 `None`이다. 따라서
   `primary_bjdong_code` 컬럼에 값을 넣을 방법은 **실제로 읍면동을 가리키는 코드를 쥐고 있는
   것뿐이다.** 시군구 코드가 그 자리에 들어가는 경로가 타입에 존재하지 않는다.

4. **행에는 출처가 말한 것만 적는다.** `sido_code`와 `sigungu_code`는 그대로 채운다 — 출처가
   그것들은 말했다. `primary_bjdong_code`는 `null`이다. 빈 문자열도 `0`도 지어낸 동 코드도
   아니다. `silver.industrial_complexes` 계약은 **바꾸지 않는다.** 이미 이 모양을 허용하고 있다.

5. **해소 파일은 어느 규칙이 그 값을 만들었는지 함께 싣는다.** 한 줄은
   `official_complex_code`, `administrative_code`, `administrative_code_granularity`,
   `address_text`, `address_source_dataset`, `address_source_record_id`, `resolution_tier`다.
   `resolution_tier`는 셋 중 하나이고 셋의 강도가 다르다:

   | tier | 규칙 | 강도 | 실측(1,442건) |
   |---|---|---|---:|
   | `source_code_in_authority` | 단지 자기 코드가 시군구 권위에 그대로 있음 | 정확 | 1,297 |
   | `modal_notice_code` | 그 단지 **자기 고시들**의 유효 코드 최빈값 | **휴리스틱** | 138 |
   | `learned_code_migration` | 위 두 관측에서 **학습한** 구코드→현행코드 표 | 파생 | 6 |
   | — | 미해소 (`446400 지도농공단지`, 고시 0건) | — | 1 |

   리더는 tier가 자기가 지목한 데이터셋과 맞는지도 검사한다. `modal_notice_code`인데 출처가
   고시 데이터셋이 아니라고 적혀 있으면 그것은 라벨 실수가 아니라 값이 반박하는 주장이다.

6. **구코드→현행코드 표를 코드에 상수로 박지 않는다.** 그 표는 관측에서 **학습한다**: 시군구
   권위가 모르는 코드가 어떤 단지의 고시에 나타나고 그 단지의 현행 코드를 알 수 있으면, 그
   쌍이 한 건의 관측이다. 실물에서 59건이 나왔고 충돌은 0이었다. **충돌이 하나라도 있으면
   실행 전체가 멈춘다** — 승자를 조용히 고르면 해당 단지 절반이 틀린 시군구에 놓인다.

7. **시군구 권위는 주입 입력이다.** 없으면 실행이 거부된다. 권위 없이 하는 "검증"은 출처를
   그대로 베끼는 것이고, 그것이 바로 `addr_cd`의 10%가 현행 법정동코드가 아닌 이 데이터에서
   실패하는 방식이다.

### 위협 모델 ([ADR-0027](./0027-every-guard-declares-its-threat-model.md) 결정 1항)

- 부류: **표현 불가능(prevention)**. ADR-0033과 같은 급이다. 셸 스캐너가 아니라 Rust 타입이다.
- Prevents: 시군구 코드가 `primary_bjdong_code` 자리에 앉는 일. 그 자리에 값을 넣으려면
  `legal_dong` 정밀도를 선언해야 하고, 선언하면 생성자가 코드 모양으로 반박한다. 정밀도를
  적지 않은 해소 파일은 파싱 단계에서 거부된다.
- Prevents: 휴리스틱으로 얻은 주소가 정확한 주소와 구분 없이 섞이는 일. tier가 값과 함께
  실리고, 산출물 요약이 tier별 건수를 적는다.
- Does not prevent: 운영자가 **거짓 해소 파일을 일부러 만들어** 주입하는 일. 여전히 출처의
  문제이고, `address_source_dataset`·`address_source_record_id`·`resolution_tier`가 그 판단의
  근거다.
- Does not prevent: `administrative_code`가 **실재하는 시군구인지**의 대조. 그것은 해소 생산자가
  시군구 권위로 하는 일이고, 타입은 모양만 본다. 생산자를 우회해 손으로 쓴 해소 파일은 이
  대조를 받지 않는다.

## Consequences

- **1,442건 중 1,441건이 해소된다.** 남은 `446400 지도농공단지`는 `addr_cd`가 `1287000000`
  (구 광주·전남 통합코드)인데 고시가 0건이고, 18,583건 고시 전수에서 `1287000000`이 단 한 번도
  나오지 않으므로 학습 표에도 없다. 상세 엔드포인트의 `addr_si_cd`·`addr_gu_cd`도 `null`이다.
  남은 경로는 이름 매칭과 도형 추론뿐이고 **둘 다 금지다.** 따라서 이 단지는 주소가 없다.
- 그 결과 `export-industrial-complex-bronze-raw-jsonl`은 실물 1,442행에 대해 **아무것도 쓰지
  않고 실패한다.** 메시지는 `1 of 1442 ... (official_complex_code 446400)`이다. 이것은 결함이
  아니라 ADR-0033 결정 3항이 설계대로 작동한 것이다. 이 단지를 통과시키려면 주소 출처가
  생기거나 ADR-0033이 개정되어야 하며, **둘 다 이 결정의 범위가 아니다.**
- 실물 1,441건 전부가 `sigungu` 정밀도다. 따라서 오늘 이 관을 통과하는 모든 행의
  `primary_bjdong_code`는 `null`이고, 산출물 요약이 그 사실을
  `some_rows_have_no_legal_dong_code_only_a_sigungu_code`로 적는다.
- **`catalog.industrial_complex.primary_bjdong_code`는 `NOT NULL`이다.** 즉 오늘의 산업단지들은
  그 Catalog 투영에 그대로 들어갈 수 없다. 이것을 이 ADR이 해결하지 않는 것은 의도다 — 해결
  방법은 (a) 읍면동 단위 출처를 확보하거나 (b) 그 컬럼을 nullable로 바꾸는 것이고, 둘 다 별도
  결정이다. 시군구 코드를 그 자리에 넣어 제약을 만족시키는 것은 이 ADR이 막으려는 바로 그
  행위다.
- ADR-0033이 적어 둔 해소 파일 형식은 이 결정으로 대체된다. `primary_bjdong_code` 필드는
  `administrative_code` + `administrative_code_granularity`가 되고, `resolution_tier`가 추가된다.
  옛 형식으로 쓴 파일은 **조용히 통과하지 않고 파싱에서 거부된다** — 통과시키면 정밀도를 말하지
  않은 코드를 받는 셈이기 때문이다.
- 남은 일: (1) 읍면동 단위 산업단지 경계 출처가 생기면 `legal_dong` 정밀도가 실제로 쓰인다.
  (2) `446400`의 주소 출처. (3) `modal_notice_code` 138건은 휴리스틱이므로, 시군구 두 곳에
  걸친 단지(`129030 빛그린` 등)에서 어느 쪽이 대표인지는 여전히 열린 질문이다.

---

## 개정 각주 — 2026-08-17

[ADR-0035](./0035-a-region-the-pipeline-does-not-use-is-not-required.md)가 위 Consequences가
남겨 둔 선택지 (b)를 골랐다. 소유자가 지역별 산업단지 기능을 지금 하지 않기로 했으므로
`sido_code`·`sigungu_code`는 두 표에서 필수가 아니고, 파티션 키는 `source_snapshot_id`다.
따라서 결정 4항의 "`silver.industrial_complexes` 계약은 **바꾸지 않는다.** 이미 이 모양을
허용하고 있다"는 더 이상 현행이 아니고, 같은 항의 "`sido_code`와 `sigungu_code`는 그대로
채운다"는 **코드가 있을 때만** 참이다. 해소 파일 한 줄(결정 5항)에는 `address_text_only` tier와,
함께 있거나 함께 없는 두 선택 필드가 더해졌다.

**결정 1·2·3·6·7은 그대로다.** 코드가 있을 때 정밀도를 싣고 모양이 라벨을 검증하는 규칙은 손대지
않았고, 시군구 코드가 `primary_bjdong_code` 자리에 앉는 경로는 여전히 타입에 없다. 위
Consequences의 "이 단지를 통과시키려면 주소 출처가 생기거나 ADR-0033이 개정되어야 한다"는
후자가 일어난 것이다 — `446400`의 주소 출처를 구한 것이 아니라, 지역을 모른다고 적을 수 있게
만들었다. 남은 일 (2)는 열린 채로 둔다.
