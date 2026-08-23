# ADR 0050: 제공자가 이스케이프한 텍스트는 한 곳에서 한 번만 되돌린다

- Status: Accepted
- Date: 2026-08-23

## Context

로컬 `platform_core` 의 `catalog.industrial_complex` 1,443행에서 HTML 이스케이프가 그대로
화면까지 왔다. 전수 계수(2026-08-23):

```
invited_industries_raw     437행
development_purpose_raw    142행
name                         1행
                          ─────
                           580행

토큰별:  &middot; 2093   &amp; 30   &#39; 9   &quot; 8   &ldquo; 2   &rdquo; 2
```

화면에서 이렇게 보인다.

```
유치 업종   전자부품&middot;컴퓨터&middot;영상&middot;음향및통신장비제조업
```

이 DB 의 **모든 기본 테이블의 텍스트 계열 컬럼 225개를 전수 검사**했고, 이스케이프가 남아 있는
것은 위 세 컬럼뿐이었다. 산업단지 경로 하나의 문제다.

다만 이 "225개 중 3개"를 다른 데이터셋이 안전하다는 뜻으로 읽으면 안 된다. **225개 중 값을
한 줄이라도 담은 것은 99개**이고 나머지는 생산자가 아직 없어 비어 있다. 검사가 말할 수 있는
것은 "지금 실린 텍스트 중에서는 산업단지 세 컬럼뿐"까지다.

### 같은 일을 하는 코드가 이미 두 벌 있었다 — 그리고 둘 다 같은 결함을 갖고 있었다

작업 지시가 지목한 세 파일을 실제로 읽어 보니 셋이 세 벌이 아니었다.

| 파일 | 하던 일 |
|---|---|
| `crates/collection/collection-infrastructure/src/building_hub_bulk.rs` | 6개 엔티티 치환 + `trim` |
| `crates/collection/collection-infrastructure/src/vworld_dataset_file.rs` | **같은** 6개 엔티티 치환 + 공백 축약 |
| `services/foundation-outbox-publisher/src/.../profile_workbook_decoder.rs` | **디코딩 없음** |

즉 **한 가지 일의 사본 두 벌**과, 그 일이 빠진 자리 하나였다. 그리고 두 사본은 같은 방식으로
틀려 있었다.

```rust
raw.replace("&amp;", "&")   // ← 먼저 돈다
   .replace("&lt;", "<")    // ← 앞 단계가 만든 "&" 를 다시 읽는다
```

`&amp;lt;` 가 `&lt;` 를 거쳐 `<` 가 된다. 원천이 쓰지 않은 태그를 만들어 내는 것이고, 이것이
사본을 없애야 할 이유이지 사본을 하나 더 만들 이유가 아니다.

### 제공자는 실제로 두 번 이스케이프한다

이 스냅샷에는 `&amp;` 뒤에 다시 엔티티 이름이 오는 값이 12개 있다.

```
&amp;rdquo; 4   &amp;ldquo; 4   &amp;sim; 2   &amp;rsquo; 1   &amp;lsquo; 1
```

한 번 풀면 `&ldquo;` 가 남는다. 두 번 풀면 `“` 가 되지만, **같은 두 번째 패스가 `&amp;lt;` 를
`<` 로 만든다.** 두 값을 구별할 방법은 없다. 그래서 한 번만 푼다.

### 범용 HTML 엔티티 라이브러리를 쓰지 않는 이유

[AGENTS.md](../../AGENTS.md) 4항(오픈소스 우선)에 따라 먼저 후보를 봤다.
[technology-stack](../technology-stack.md) §1.1 명단에는 이 필요가 없고, 워크스페이스에도
HTML 엔티티 라이브러리가 없다. 채택하지 않은 이유는 세 가지다.

1. **WHATWG 명명 문자 참조 표에는 세미콜론 없이 매칭되는 항목이 있다**(`&not`, `&amp`, `&sim`
   등). 규격을 따르는 디코더는 평범한 문장 `R&notation` 을 `R¬ation` 으로 만든다. 이 값들은
   마크업이 아니라 **엑셀 셀의 자유 텍스트**이고, 맨 앰퍼샌드가 실제로 들어 있다 — 같은
   스냅샷에서 `R&D` 를 담은 행이 18개(맨 `R&D` 8회 · `R&amp;D` 17회)이고, `KT&G` 가 단지명에
   있다. 세미콜론을 요구하지 않는 디코더는 이 9개의 맨 앰퍼샌드를, 그리고 `&amp;` 를 푼 뒤
   드러나는 30개를 각각 다른 문자로 바꾼다.
2. **모르는 참조를 이름 대는 기능이 필요하다.** 범용 디코더는 못 푼 것을 그대로 두고 끝나므로,
   제공자가 다음 달에 새 참조를 보내면 아무도 모른 채 화면까지 간다.
3. 40행짜리 표 하나를 위해 서드파티 의존성과 라이선스·고지 산출물을 늘리는 비용이 이득보다 크다.

전체 HTML5 의미가 필요해지면 그때 라이브러리 채택을 별도 결정으로 한다.

### 어느 단계에서 풀 것인가

Bronze 는 원천 그대로다 — 이건 [FP-ADR-0022](../../platforms/foundation-platform/docs/adr/0022-lakehouse-handoff-vs-storage-format-boundary.md)
가 표로 못박은 규칙이다: "Bronze raw evidence → 받은 provider byte를 ZIP·CSV·XML·JSON·SHP 등
그대로 보존한다." 여기서 말하는 Bronze 는 **R2 의 객체**다.

`bronze.industrial_complexes_raw_jsonl` 은 그 Bronze 가 아니다. 같은 ADR 이 JSONL 을
"transient handoff" 로 분류하고, `lakehouse-domain/src/industrial_complex_jsonl_transport.rs` 는
그것을 다시 확인한다 — "a JSONL handoff is writer input, not lakehouse table storage." 실제로
이 핸드오프를 만드는 Rust 생산자는 이미 값을 정규화한다: `국가` → `national`, `19640415` →
`1964-04-15`, 면적 → 소수 두 자리. Spark 잡의 주석이 그 관계를 이미 적어 두었다 —
"the Rust producer normalizes both with one parser."

즉 이 생산자는 Bronze 기록자가 아니라 **Bronze→Silver 정규화의 Rust 쪽 절반**이다. 여기서
푸는 것은 Silver 에서 푸는 것이고, R2 의 원천 바이트는 손대지 않는다.

화면에서 푸는 것은 후보가 아니다. 같은 값의 정의가 두 곳이 되고, API·타일·검색이 각자 다르게
푼다.

## Decision

1. **엔티티 해석은 `foundation-shared-kernel/src/provider_text.rs` 한 곳에만 있다.**
   `decode_provider_html_text` 가 표를 소유하고, 위 두 사본은 지워졌다. 두 수집기의
   `clean_html_text` 는 이 함수를 부르는 두 줄만 남으며, **다른 것은 공백 정책뿐이다** —
   `hub.go.kr` 은 양끝만 `trim`, VWorld 는 줄바꿈으로 접힌 제목 때문에 내부 공백까지 축약한다.
   그 차이는 각 함수의 doc 주석에 적혀 있다. 표는 하나, 공백 정책은 호출자의 것이다.
2. **한 번만 푼다.** 디코더는 왼쪽에서 오른쪽으로 한 번 지나가고, **자기가 쓴 문자를 다시 읽지
   않는다.** `&amp;lt;` 는 `&lt;` 가 되고 `<` 가 되지 않는다.
   `provider_text.rs` 의 `an_escaped_ampersand_is_decoded_exactly_once` 와
   `profile_workbook_decoder.rs` 의
   `an_escaped_ampersand_is_unescaped_exactly_once_and_the_remainder_is_reported` 가 이것을
   못박는다. 실패하면 막는 것: **원천이 쓰지 않은 태그가 값 안에 생기는 것.**
3. **세미콜론이 없으면 참조가 아니다.** 이름은 대소문자를 구별하고, 이름 길이는 32자,
   십진 7자리·십육진 6자리를 넘으면 참조로 보지 않는다. `R&D`·`KT&G`·`a & b` 는 그대로 남는다.
4. **숫자 참조도 푼다.** `&#39;` 와 `&#x27;` 는 `'` 가 된다. 코드포인트가 문자가 아니거나
   제어문자(`\t`·`\n`·`\r` 제외)이면 풀지 않고 그대로 둔다 — `&#0;` 는 Postgres `text` 가 받지
   않는 값이므로, 네 단계 뒤에서 깨지는 대신 여기서 원문으로 남는다.
5. **모르는 참조는 지우지도, 짐작하지도 않는다. 이름을 댄다.** 한 번의 패스가 풀지 못한
   엔티티 모양 토큰은 값 안에 **원문 그대로** 남고, `entity_references` 가 그것을 센다.
   `profile_workbook_decoder` 가 행마다 세어 `DecodedProfileSheet::residual_entity_references`
   로 돌려주고, 내보내기 요약의 `provider_text.residual_entity_reference_counts` 가 토큰별 개수를
   적으며, `evidence_limitations` 에 `some_provider_escapes_did_not_resolve_in_one_pass` 가 붙는다.
   실패하면 막는 것: **아무도 뜻을 정하지 않은 참조가 조용히 화면까지 가는 것.**
6. **못 푼 참조는 내보내기를 실패시키지 않는다.** 지금 스냅샷이 실제로 두 번 이스케이프된 값
   12개를 갖고 있고, 거부하면 1,442행 전부가 못 들어온다. 없는 컬럼을 대하는 방식
   ([ADR-0044](./0044-a-column-named-for-a-fact-must-hold-that-fact.md) §7)과 같다 — 비용은
   전부인데 얻는 것이 없으면 실패가 아니라 기록이다. 여기서 실제로 금지된 것은 두 번 푸는 쪽이다.
7. **푸는 자리는 셀 하나가 Rust `String` 이 되는 지점이다.** `profile_workbook_decoder` 의
   `optional_cell` 하나이며, 컬럼별로 부르지 않는다. 계약에 컬럼이 하나 늘면 그 컬럼도 저절로
   풀린다 — 컬럼마다 부르면 다음에 잊는 자리가 다시 생기고, 그 망각이 이 ADR 이 존재하는 이유다.
   푼 뒤 다시 `trim` 하고 빈 값은 없는 값이 된다: `&nbsp;` 는 U+00A0 를 이름하므로, 그것만 든
   셀은 공백만 든 셀과 같은 말을 한다.
8. **R2 의 Bronze 객체는 손대지 않는다.** `TB_IRSTT_BASS_HIST.xlsx` 는 제공자가 보낸 바이트
   그대로 남는다(FP-ADR-0022). 이 결정은 그 객체를 읽는 쪽의 결정이다.

## Consequences

- `bronze.industrial_complexes_raw_jsonl` → `silver.industrial_complexes` →
  `gold.complex_catalog` → `catalog.industrial_complex` 의 자유 텍스트 컬럼이 앞으로 풀린 값을
  싣는다. `row_checksum_sha256` 도 그만큼 달라진다.
- **되돌린 값은 기존 제약을 깨지 않는다** — 실측(2026-08-23, `catalog.industrial_complex`).
  세 컬럼 모두 길이 제한 없는 `text` 이고, 참조 하나는 항상 문자 하나가 되므로 **길어지는 행이
  0개**다(`name` 최대 42→37, `development_purpose_raw` 497→483). `name` 의 `NOT NULL` 과
  `development_purpose_raw`·`invited_industries_raw` 의 `..._non_blank` CHECK 도 안전하다 —
  되돌린 뒤 빈 값이 되는 행이 **0개**다. 참조만 든 셀은 canonical 에 닿기 전 디코더에서 없는
  값이 되므로 CHECK 자리까지 가지 않는다.
- **이미 적재된 580행은 코드 변경으로 바뀌지 않는다.** Bronze JSONL 재생성 → Silver 재실행 →
  Gold 재실행 → canonical 적재를 다시 태워야 한다. Silver·Gold 는 `overwrite` 로 돈다 — 같은
  `source_snapshot_id` 를 append 하면 유일성 게이트가 그 자리에서 실패한다(ADR-0044). Iceberg
  의 overwrite 는 새 스냅샷을 만들 뿐이므로 이전 스냅샷은 시간여행으로 남는다.
- 두 수집기의 디코딩이 넓어진다. 전에는 6개만 풀었고 이제 표 전체를 풀며, 숫자 참조도 푼다.
  두 수집기의 기존 테스트 28개는 그대로 통과한다.
- 표에 없는 참조를 만나면 값은 그대로 통과하고 요약에 이름이 남는다. 그 이름을 표에 올릴지는
  **다음 사람의 결정**이며, 그때 이 ADR 을 대체할 필요는 없다 — 표에 항목을 더하는 것은 이
  결정의 실행이지 결정의 변경이 아니다. 세미콜론 없는 매칭을 켜거나 두 번 푸는 것은 결정의
  변경이므로 새 root ADR 이 필요하다.
- 남은 것: 두 번 이스케이프된 12개 값의 처리. 지금은 `&ldquo;` 형태로 화면까지 간다. 제공자에게
  물어보거나, 원문이 마크업이 아님을 근거로 별도 결정을 하거나 둘 중 하나이고, 그전까지는
  요약이 개수를 들고 있다.
