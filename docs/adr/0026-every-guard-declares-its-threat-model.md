---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-13
---

# ADR 0026: 모든 가드는 자기 위협 모델을 선언한다

- Status: Accepted
- Date: 2026-08-13
- 관련: [ADR-0001 모노레포 거버넌스와 규칙](./0001-monorepo-governance-and-conventions.md), [ADR-0012 검증 결과는 그 문면대로여야 한다](./0012-verification-results-must-mean-what-they-say.md)
- 발단: `b59374a7`의 `scripts/guard/hand-rolled-error-boundary.sh` 적대적 리뷰

## Context

`b59374a7`은 직접 만든 React error boundary가 되돌아오는 일을 막으려고 tracked JS/TS의 두
lifecycle 식별자를 찾는 셸 가드를 추가했다. 적대적 리뷰는 서로 반대 방향의 결함을 동시에
재현했다. 문자열을 조립한 computed member로 같은 동작을 만들면 가드는 통과했고, 식별자를 설명한
합법적인 JSX text는 실패했다. 가드는 의미 불변식을 보장하지 못하면서 모든 커밋을 막는 거짓
양성을 만들었고, 파일 주석은 실제 보장보다 넓은 경계를 주장했다.

근본 원인은 lexer의 JSX 분기 하나가 빠진 것이 아니다. 임의의 동적 프로그램이 React error
boundary처럼 동작하는지는 프로그램 행동에 관한 자명하지 않은 의미적 성질이다. Rice의 정리에
따라 이를 모든 프로그램에 대해 완전하게 판정하는 정적 분석기는 존재하지 않는다. 정적 검사는
과대 또는 과소 추정하거나, 허용 문법을 제한하거나, 타입·증명 주석·추상 해석 같은 추가 구조를
요구해야 한다. 그러므로 탐지기를 예방 벽으로 부르는 순간 우회는 구현 버그로 오해되고, 우회를
막으려고 사설 parser를 키울수록 다른 문법의 거짓 양성이 생긴다.

Google의 대규모 정적 분석 운영은 코드 리뷰 검사도 effective false positive를 10% 미만으로
유지해야 신뢰를 얻는다고 보고한다. 실제 결함을 가리켜도 개발자가 이해하거나 행동할 수 없으면
그 결과는 effective false positive다. 이 저장소의 가드는 제안이 아니라 모든 커밋을 막는 권위
게이트이므로 그 기준보다 엄격해야 한다.

한편 Error Prone의 `RestrictedApi`와 Bazel visibility는 가능한 자리에서 금지 표현을 뒤쫓기보다
허용된 호출자·경로·의존 package를 한 곳에 열거하고 나머지를 기본 거부하는 선례를 보인다. 이
저장소도 invalid state를 타입·DB capability·FK·불변성 trigger·build visibility로 표현 불가능하게
할 수 있으면 그 경계를 먼저 사용해야 한다.

## Decision

1. **모든 새 가드는 파일의 첫 comment block에 자기 위협 모델을 선언한다.** 선언은
   `honest-mistake detection`과 `deliberate-bypass prevention` 가운데 적용되는 부류를 명시하고,
   `Prevents`와 `Does not prevent`를 각각 한 문장 이상 적는다. 둘을 함께 다루면 보장별로 부류를
   나눈다. 어느 부류인지 적지 않은 가드는 새로 만들 수 없다. `scripts/guard/hand-rolled-error-boundary.sh`는
   전자이며, 직접 철자를 쓴 lifecycle의 실수 재도입만 막는다.

2. **탐지형 가드는 자신이 벽이 아님을 선언하고 알려진 경계를 expected-pass self-test로 고정한다.**
   의미적 판정에는 원리적으로 우회가 있으므로 선언한 범위 밖의 우회는 결함이 아니라 알려진
   경계다. 다만 computed member, 동적 접근처럼 실제로 확인한 경계는 “통과해야 하는” fixture와
   이유를 self-test에 남긴다. 선언한 탐지 범위 안의 누락은 계속 결함이다. 보안·인가·데이터
   무결성처럼 고의 우회를 막아야 하는 불변식은 탐지형 가드 하나를 권위 경계로 삼을 수 없다.

3. **모든 커밋을 막는 가드의 알려진 거짓 양성 예산은 현재 tracked tree에서 `0건`이다.** 실제
   tree 또는 유효한 재현 fixture에서 한 건이라도 나오면 그 가드는 초록이 아니며, merge 전에
   거짓 양성을 고치거나 가드를 권위 게이트에서 내려야 한다. 발견한 사례는 expected-pass
   self-test로 고정한다. Google의 10% 미만 기준은 코드 리뷰 제안의 상한이고, 매 커밋 hard-fail인
   이 저장소에서는 한 건도 반복 비용이 되므로 `0건`을 택한다.

4. **예방 가능한 자리에서는 탐지를 만들지 않는다.** 새 가드를 제안하기 전에 타입, 소유 API,
   DB capability와 권한, FK, append-only·불변성 trigger, module/build visibility로 잘못된 상태나
   의존 경로 자체를 표현 불가능하게 할 수 있는지 먼저 기록한다. 가능하면 그 경계가 권위이고
   scanner는 보조 진단만 할 수 있다. [ADR-0025](./0025-parcel-publication-names-one-sealed-iceberg-evidence.md)의
   필지 증거 봉인자는 출처 네 값을 문자열로 탐지하지 않고 evidence id 하나, FK, append-only
   trigger로 결박한 이 결정의 기준 사례다.

5. **언어 문법과 AST 수준 검사는 그 언어의 기존 분석기가 소유한다.** JS/TS/JSX/TSX는 Biome,
   Rust는 compiler와 Clippy가 parser·AST·type 정보를 소유한다. 셸 가드는 Git index의 path·mode·
   tracked set, 파일명, 구조화 manifest/config, 생성물 drift, 정확히 한정한 token/path 계약까지만
   소유하며 TS parser를 새로 만들거나 의미적 완전성을 주장하지 않는다. 기존
   `hand-rolled-error-boundary.sh`는 범위를 넓히지 않는 legacy lexical detector로 동결한다. 이번
   JSX 변경은 알려진 거짓 양성 제거만 하며, 새 TS 문법 coverage가 필요하면 Biome의 기존 rule이나
   plugin을 평가하고 이 scanner에 문법을 더하지 않는다.

6. **금지 목록보다 허용 목록을 우선한다.** 합법적인 caller, dependency edge, writer, state
   transition을 유한하게 열거할 수 있고 default deny를 권위 경계에서 강제할 수 있으면 한 SSOT의
   allow-list를 쓴다. Error Prone의 허용 annotation·path와 Bazel의 `package_group`이 이 형태다.
   허용 우주가 열려 있거나 권위 seam이 없어서 열거할 수 없을 때만 bounded deny-list 탐지를 쓰고,
   불가능한 이유와 누락 범위를 위협 모델에 적는다. error-boundary detector는 임의의 동적 JS
   생성 경로를 유한하게 열거할 seam이 없어 후자이며, `@suspensive/react`만 허용한다는 주장을
   대신하지 않는다.

## 기각한 대안

### 이 가드에 TS parser를 붙여 완전 탐지를 시도한다

parser는 JSX text와 identifier를 구별할 수 있지만, computed property·prototype mutation·runtime
construction이 같은 동작을 만드는지는 판정하지 못한다. Biome이 이미 소유한 문법을 Python/셸에서
복제하면 TypeScript 문법과 버전이 바뀔 때 두 parser를 유지해야 하고, 이번 JSX 거짓 양성과 같은
새로운 effective false positive를 만든다. 비용을 내고도 Rice의 정리가 가리키는 의미 경계는
닫히지 않으므로 기각한다.

### 이 가드를 지운다

거짓 양성은 즉시 사라지지만, 실제로 한 번 들어왔던 직접 lifecycle 구현의 정직한 재도입을 가장
싼 자리에서 알려 주는 신호도 사라진다. 거짓 양성을 `0건`으로 고치고 보장을 direct identifier
detection으로 좁히면 그 신호는 비용보다 크다. 권위 벽이라는 주장을 지우는 것이지 탐지 자체를
지울 이유는 아니므로 기각한다.

### Biome custom rule로 전부 옮긴다

Biome GritQL plugin은 parser가 만든 syntax pattern에 진단을 붙일 수 있어 새 언어 수준 검사의
우선 후보가 맞다. 그러나 지금 전환하면 Gongzzang의 Biome 실행 범위·plugin 배포·suppression 정책과
전체 JS/TS fixture를 함께 설계해야 하고, 동적 computed/prototype 우회는 여전히 의미 경계 밖이다.
현재 사고의 근본 해결은 더 넓은 탐지를 주장하는 것이 아니라 detector의 보장과 비용을 정직하게
제한하는 것이다. 이 task에서는 전면 이전을 기각하며, 기존 detector에 새 문법 coverage가 필요해질
때는 셸 parser 확장 대신 Biome plugin으로 교체하는 별도 결정을 한다.

## Consequences

- `hand-rolled-error-boundary.sh`의 상단 comment가 honest-mistake detection, 예방 범위, 알려진
  우회를 선언한다. JSX text fixture는 통과하고 computed-member fixture도 선언된 경계로 통과한다.
- 이후 탐지형 가드 리뷰는 “무엇을 잡았는가”와 함께 “무엇을 원리상 보장하지 않는가”를 읽는다.
  expected-pass fixture가 coverage 과장을 막는다.
- 현재 존재하는 다른 가드 91개의 위협 모델을 이 변경에서 소급 작성하지 않는다. 후속 적용은
  먼저 각 가드의 실제 incident와 권위 수준을 조사하고, 이 ADR의 header 형식으로 하나씩 이관한다.
- 신규 가드에는 예방 가능성 검토와 allow-list 불가능 사유가 설계 입력이 된다. 도구 수를 늘리는
  것이 아니라 더 강한 기존 경계에 불변식을 둔다.

## References

- [Rice's theorem](https://en.wikipedia.org/wiki/Rice%27s_theorem)
- [Lessons from Building Static Analysis Tools at Google](https://cacm.acm.org/research/lessons-from-building-static-analysis-tools-at-google/)
- [Error Prone `RestrictedApi`](https://errorprone.info/api/latest/com/google/errorprone/annotations/RestrictedApi.html)
- [Bazel visibility](https://bazel.build/concepts/visibility)
- [Biome linter plugins](https://biomejs.dev/linter/plugins/)
