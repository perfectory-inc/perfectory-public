---
status: current
owner: repository-maintainers
doc_type: reference
last_reviewed: 2026-07-31
---

# 디자인시스템 벤치마킹 레퍼런스

우리 디자인시스템을 직접 설계하기 위해 조사한 외부 시스템의 **공개 계약과 설계 선택** 기록이다.
[AGENTS.md 해결 접근 순서](../../AGENTS.md) 3번(검증된 사례 조사)과 4번(오픈소스 우선 평가)의 근거 자료다.

**이 문서는 채택 결정이 아니다.** 무엇을 취하고 버릴지는 결정 시점에 ADR로 남긴다.
사례의 복잡성을 그대로 복사하지 않고, 우리 규모와 제약에 맞는 보장만 가져오는 것이 원칙이다.

## 조사 대상

| 시스템 | 소속 | 라이선스 | 버전 | 스택 | 조사일 |
|---|---|---|---|---|---|
| [SEED](https://seed-design.io/) | 당근 | Apache-2.0 (1.6.0에서 MIT→Apache 변경) | react 2.x · css 2.x · Rootage 2.3.0 | React + CSS recipe, Lynx | 2026-07-31 |
| [Astryx](https://astryx.atmeta.com/) | Meta | MIT | 0.2.0 (Beta) | React + StyleX | 2026-07-31 |

조사 대상을 추가할 때는 `## <시스템명>` 절을 같은 형식으로 덧붙이고 위 표와 `## 교차 관찰`을 함께 갱신한다.

## 우리 현재 상태와의 대조

조사 시점 기준 우리 자산은 `products/gongzzang/packages/ui` 하나이며, 실 소비자는 `apps/web` 하나다.

| 축 | 우리 현재 | 두 사례의 공통 전제 |
|---|---|---|
| 토큰 계층 | semantic 이름이 raw hex를 직접 물고 있음 (`--color-primary: #cc785c`) | scale 계층을 거쳐 semantic이 참조 |
| 토큰 정의 위치 | `tokens/*.css`와 `tokens.ts` 두 곳에 수동 동기화 | 단일 정의에서 생성 |
| 다크모드 | `--color-surface-dark-*`를 별도 이름으로 보유 | 같은 토큰의 모드 값 |
| 도메인 토큰 분리 | 공용(breakpoint)과 도메인(매물·지도)이 `tokens.ts` 한 파일에 혼재 | 공용/도메인 분리 |

---

## 교차 관찰 — 두 시스템이 독립적으로 같은 결론에 도달한 것

서로 다른 제약(모바일 C2C 대 데스크톱 도구, Korean-first 대 English-first)에서 출발해 같은 답에
도달한 항목이다. 단일 사례보다 근거의 무게가 크다.

1. **cascade layer가 최대 사고 지점.** 둘 다 별도 문서를 할애했다. 레이어 없는 스타일이 모든
   named layer를 이기므로 `@import "reset.css"`에서 `layer()` 하나가 빠지면 전체 컴포넌트 스타일이
   조용히 뒤집힌다. 에러도 경고도 없다.
2. **CSS 변수가 기본, JS 토큰 해석은 예외.** 둘 다 "일반 DOM 스타일링은 CSS 변수, JS 해석 API는
   SVG·canvas·차트·지도처럼 CSS 변수를 받지 못하는 소비자에만"이라고 명시한다. 우리
   `LISTING_TYPE_COLORS`(네이버 지도용 hex)가 정확히 이 예외에 해당한다.
3. **AI 에이전트를 1급 소비자로 취급.** 둘 다 MCP 서버, 에이전트 스킬/AGENTS.md 생성기, 문서의
   기계 판독 경로를 제공한다. 문서 사이트는 사람용 표면일 뿐이다.
4. **컴포넌트 오버라이드는 raw CSS 셀렉터가 아니라 의미 키로.** SEED는 Rootage ComponentSpec,
   Astryx는 `defineTheme({components:{button:{'variant:ghost':…}}})`. 셀렉터 생성은 파이프라인이 한다.
5. **규칙을 문서가 아니라 종료 코드로 강제.** SEED `compat`, Astryx `doctor` 모두 실패 시 exit 1로
   CI 게이트가 된다. Astryx는 하드코딩 문자열 금지를 ESLint 룰로도 강제한다.
6. **타입은 저작 경험용, 검증은 경계 한 곳에서.** Astryx `createIntegration`/`createConfig`, SEED
   `defineTheme` 모두 런타임 검증을 하지 않는 항등 함수이고, 실제 검증은 로더가 단일 스키마로 한다.

---

## SEED (당근)

### 토큰 계층 — Scale → Semantic 2단

Scale Token은 raw 값 하나에 이름을 붙인 것이고, Semantic Token은 Scale의 조합으로 디자인 의도를
표현한 단위다. 실제 디자인·개발의 빌딩블록은 Semantic이다.

> Scale에 적절한 값을 주입하는 것으로 **스키마 변경 없이** 스킴을 재정의할 수 있다.

이 한 문장이 "지금 만든 것을 나중에 디자인시스템으로 교체한다"가 성립하는 조건이다.
Semantic 이름이 고정되면 Scale 값만 갈아끼워도 소비자 코드가 바뀌지 않는다.

### 색상 — 이름이 목록이 아니라 문법

`Property × Role × Variant × State` 로 조합한다.

| 축 | 값 |
|---|---|
| Property | `fg` · `bg` · `stroke` |
| Role | `brand` · `neutral` · `positive` · `warning` · `critical` · `informative` (+ `layer`) |
| Variant | `weak` · `solid` · `contrast` · `muted` · `subtle` · `inverted` |
| State | `pressed` 등 |

예: `$color.bg.brand-solid-pressed`. 이름을 외우는 것이 아니라 조합해서 만든다.

역할 기반 색상의 근거로 세 가지를 든다 — 접근성(모든 fg/bg 조합이 대비 기준 충족), 빈틈없는 업데이트,
테마 적응성. 그중 두 번째가 중요하다.

> 색상을 업데이트할 때 값이 아니라 **의미**를 기준으로 업데이트한다.
> 동일한 값을 가지더라도 의미가 다르다면 따로 업데이트가 가능해야 한다.

같은 hex라도 의미가 다르면 토큰을 합치면 안 된다는 뜻이다. 토큰 중복 제거 유혹에 대한 반박이다.

Palette 색상은 역할로 표현하기 어려운 예외용이며, 화면 모드에 따라 알맞은 명도가 배정되는 적응형
팔레트다. 팔레트 축은 Gray + Chromatic(carrot, blue, green, yellow, red, purple).

### Rootage — 디자인시스템을 데이터로 공개

`https://seed-design.io/rootage/` 아래에 버전(2.3.0)이 붙은 선언적 스펙을 공개한다.

```text
/index.json          리소스 목록
/collections.json    kind: TokenCollections
/color.json          색상 토큰 전량
/components/*.json   kind: ComponentSpec (58개)
```

쿠버네티스 리소스와 같은 `kind` / `metadata` / `data` 형태다. ComponentSpec은
`schema.slots.<slot>.properties`와 `variants`를 갖는다. 문서·Figma·React·Lynx가 모두 이 JSON을
소비한다. 문서가 코드를 설명하는 구조가 아니라, **하나의 데이터가 문서와 코드를 동시에 낳는 구조**다.

`collections.json`이 정의하는 모드 축은 light/dark만이 아니다.

| 컬렉션 | 모드 |
|---|---|
| `global` | default |
| `color` | theme-light · theme-dark |
| `motion` | preferred · **reduced** |
| `viewport-width` | base · sm · md · lg · xl |

반응형과 모션 접근성이 미디어쿼리가 아니라 **토큰 모드 축**이다. 결과적으로 컴포넌트에 breakpoint
분기가 들어가지 않는다.

### Snippet — 라이브러리와 코드 생성의 하이브리드

유연성과 개발자 경험의 트레이드오프를 이렇게 푼다.

1. npm으로는 **compound components**만 배포한다
   (`Checkbox.Root` / `Control` / `Indicator` / `Label` / `HiddenInput`).
2. CLI `add`가 **스니펫을 소비자 저장소에 생성**한다 (`seed-design/ui/checkbox.tsx`).
3. 스니펫은 소비자의 소스가 되고, 프리미티브는 `node_modules`에 남는다.

npm 라이브러리의 "업데이트를 받는다"와 shadcn식 코드 복사의 "내가 소유한다"를 동시에 갖는다.
각 스니펫은 요구 SEED 버전 범위를 갖고 `compat` 명령이 호환을 검사한다(불일치 시 exit 1).

조합 API로 `as`(렌더링 태그 변경)와 `asChild`(Radix Slot 패턴)를 제공한다. `asChild`의 자식은
props를 전개해야 하고 ref를 전달해야 한다는 두 규칙을 명시한다.

### 상호작용 상태 — 토큰 하나, 디바이스별 다른 시점

스펙과 색상 토큰은 `pressed` **하나만** 정의한다. 표시 시점은 React가 디바이스로 가른다.

| 환경 | 트리거 | 표시 |
|---|---|---|
| 마우스 | hover | pressed 스타일 |
| 터치 | active | pressed 스타일 |

```css
@media (hover: hover) and (pointer: fine)      { .x:is(:hover,[data-hover]) { … } }
@media not all and (hover: hover) and (pointer: fine) { .x:is(:active,[data-active]) { … } }
```

터치 환경에서 hover 스타일이 남는 문제가 구조적으로 사라지고 상태 토큰 수가 절반이 된다.

상태는 두 종류로 구분한다 — **상호작용 상태**(pressed, 사용자 조작으로 변함)와
**옵션 상태**(selected·disabled, 적용된 옵션에 따름). 둘은 서로 덮어쓸 수도, 함께 적용될 수도 있다.

### Elevation — 그림자가 아니라 배경 레이어

깊이를 box-shadow가 아니라 **배경 레이어 색 토큰**으로 표현한다(`$color.bg.layer-basement`,
`layer-default`). 모바일 우선 시스템의 선택이다.

레벨을 **Global**(제품 화면 전체 또는 컨테이너 역할의 레이어)과 **Local**(그 안의 콘텐츠 컴포넌트,
항상 Global 위)로 나눈다.

| Global Level | 대상 |
|---|---|
| 0 `layer-basement` | 최하단 배경. 스크롤되는 모든 콘텐츠 뒤 |
| 1 `layer-default` | 페이지 기본 레이아웃. Card·List·TextField·Top Navigation |
| 2 | Bottom Sheet, Menu Sheet — 화면을 덮으며 **독립적 쌓임 맥락을 생성** |
| 3 | Alert Dialog — 최상위 모달. 이미 활성화된 Bottom Sheet 포함 모든 요소 위 |

핵심은 재귀 구조다. Bottom Sheet는 그 자체로 새로운 Global Context가 되고, 그 위에 놓인 List는
다시 Local로 인식된다. "페이지 위 페이지(Page-over-Page)"가 이 규칙으로 자연히 성립한다.

### 타이포그래피

폰트 크기·줄 높이·폰트 두께를 각각 토큰으로 정의하고, 이를 두 가지 텍스트 스타일로 조합한다.

- **스케일 텍스트 스타일** — `t5Regular`, `t1Bold`. 빠른 일반 적용용. t1–t5 본문, t6–t10 제목,
  t11–t14 대형 제목(sm breakpoint 이상 권장)
- **시맨틱 텍스트 스타일** — `screenTitle`, `articleBody`. 역할과 상황에 맞춰 구성되어 의도를 내포

폰트 크기는 rem으로 사용자 폰트 설정을 존중하고, 스케일링에 반응하지 않아야 하는 곳을 위해
`-static` 변형을 별도로 제공한다. 글꼴은 웹폰트가 아니라 시스템 폰트 스택을 쓴다.

### 간격 — 상황 기반 이름

숫자 스케일이 아니라 사용 상황으로 이름 짓는다.

| 토큰 | 뜻 |
|---|---|
| `global-gutter` | 화면 가장자리와 콘텐츠 사이. **모든 서비스에서 동일하게 유지** |
| `nav-to-title` | 네비게이션 바와 그 아래 타이틀 사이 |
| `component-default` | 컴포넌트 기본 간격 |

### 레이아웃

용도로 두 유형을 나눈다 — **Dashboard Layout**(판매자·광고주 센터 등 관리 기능)과
**Contents Layout**(정보 전달 목적의 서비스 페이지).

Dashboard는 밀도 3단계마다 그리드가 명시된다.

| 밀도 | Grid | Column | Gutter | Margin | Max-width |
|---|---|---|---|---|---|
| Low | Centered | 8 | 24px | 32px | 720px |
| Middle (base) | Centered | 12 | 24px | 32px | 1040px |
| High | Fluid | Full-width | – | 32px | 1040px (min) |

Breakpoint는 Mobile First로 base 0 / sm 480 / md 768 / lg 1280 / xl 1440이며, 각 구간의 gutter와
margin이 표로 고정된다. 사이드바는 md 이상에서 노출되고 그 미만에서는 헤더 메뉴로 통합된다.

영역(Region)은 Header(GNB) / Side Navigation / Main Content / Aside로 정의한다.

### 반응형 API

- 반응형 값을 객체로: `padding={{ base: "x3", md: "x4" }}`
- `hideFrom="md"`는 `display={{ md: "none" }}`과 동일
- 훅 `useBreakpoint` / `useBreakpointValue`, SSR용 `BreakpointProvider defaultBreakpoint`
- 규칙: 가능한 경우 항상 CSS 기반 반응형 prop을 쓰고, 훅은 JS 로직이 필요할 때만 쓴다

### 접근성 — APCA 수치로 고정

WCAG 대비비가 아니라 APCA `Lc` 값을 쓴다.

| 대상 | 기준 |
|---|---|
| 가독성 텍스트 (본문 2줄 이상, 화면 제목, 헤드라인, 입력 필드, 툴팁) | **Lc 75 최소 · 90 권장** |
| 그 외 텍스트 | Lc 60 (16px 미만이면 bold 사용) |
| placeholder · disabled | Lc 30 |

- 터치 영역 44×44 이상이 이상적, 제약이 있어도 최소 24×24 보장
- 색상만으로 정보를 전달하지 않는다
- 애니메이션 2초 이상은 지양하고 건너뛰기를 제공, 초당 3회 이상 번쩍이지 않는다
- 오류는 시각 피드백과 `aria-live`를 함께 제공하고, 입력 필드 근처에 구체적 해결 방법을 제시한다

### 컴포넌트 스펙 골격

58개 컴포넌트 문서가 같은 골격을 따른다.

```text
(플랫폼 구현 상태 표)  Figma / React / iOS / Android × Done·InProgress·NotReady·NotPlanned
## Anatomy         해부도
## Properties      Size·Tone·Variant 등 축별 정의
## Guidelines      "…하기" / "…하지 않기" 형태의 판단 규칙 (DontImage로 ❌ 예시)
## Specification   → /rootage/components/<name>.json
```

- 플랫폼 구현 상태가 컴포넌트마다 표로 공개된다. 미구현을 감추지 않는다.
- Guidelines 제목이 서술이 아니라 **행동 규칙**이다: "Brand 컬러는 꼭 필요한 곳에만 사용하기",
  "너무 많은 Badge를 나열하지 않기", "버튼처럼 클릭 가능한 요소로 사용하지 않기".
- 헷갈리는 컴포넌트에는 **선택 가이드 절**을 둔다: `Switch vs. Checkbox`,
  `Select vs. Menu`, `Segmented Control vs. Tabs`, `Radio vs. Checkbox vs. Chip Group`.
- 구현 제약도 문서에 적는다: "max-width가 설정되어 있어 10글자 이상이면 말줄임 처리됩니다.
  전부 표시해야 한다면 엔지니어와 논의해주세요."
- 도메인 고유 컴포넌트가 시스템에 포함된다(`Manner Temp` 매너온도). 디자인시스템이 서비스 개념을
  담는 사례다.

### 패턴 — 소요 시간으로 컴포넌트를 고른다

`Loading` 패턴은 로딩 표시 요소를 **예상 소요 시간** 기준으로 배정한다.

| 요소 | 적합한 로딩 시간 | 목적 |
|---|---|---|
| Progress Circle | 1–4초 | 레이아웃을 미리 보여줄 필요가 없는 짧은 프로세스 |
| Progress Bar | 1–10초 및 그 이상 | 시작·끝이 명확한 설치·업로드·다운로드 |
| Skeleton | 1–10초 | 콘텐츠 레이아웃을 미리 이해시켜 불확실성 감소 |

그리고 상황별(첫 진입 / 페이지 전환 / 추가 로드)로 사용 가능한 요소를 지정한다.
문서 서두에 원칙을 먼저 둔다 — "로딩의 기본 접근 방식은 프로세스 속도를 개선하는 것이다."

### 라이브러리 저자 가이드 — 사내 다중 소비자의 실제 실패 모드

여러 곳에서 쓰이는 공유 패키지를 만들 때의 세 원칙이다. 하나라도 어기면 소비자 번들에 CSS가
두 벌 들어가 스타일이 깨진다.

1. SEED 패키지는 `peerDependencies`로만 선언한다 (`dependencies` 금지)
2. 빌드 산출물에 SEED 코드를 포함하지 않는다 (external 처리)
3. CSS 파일 import는 프로젝트에 위임한다 (라이브러리 코드에서 `*.css` import 금지)

근거가 중요하다. 클래스명(`.seed-action-button`)과 토큰(`--seed-*`)이 **버전이 달라도 같은 이름**이다.
두 벌이 로드되면 승자를 로드 순서가 정하는데, 번들러의 프로덕션 CSS 순서는 개발 환경과 다를 수 있어
**배포 후에야 깨짐을 발견**하는 경우가 많다.

검증 방법까지 문서화되어 있다.

```sh
grep -rhoE '@seed-design/[a-z-]+' dist/ | sort -u   # import가 남아 있어야 정상
grep -rl '\.seed-' dist/                             # 결과가 없어야 정상
```

`peerDependencies` 범위 정책은 버저닝 정책의 분기점을 따른다.

| 지원 대상 | 권장 범위 | 이유 |
|---|---|---|
| 2.0 이상 | `^2.0.0` | strict semver — minor·patch가 하위 호환 |
| 1.x | `~1.2.0` | minor에 breaking이 있어 minor를 가로지르면 안 됨 |
| 둘 다 | `~1.2.0 \|\| ^2.0.0` | 전환기. OR로 넓혀도 **각 구간에 상한 필수** |

major 전환 시 라이브러리의 선택지를 표로 제시한다 — A 범위만 확장 / B 코드 조정 후 dual-compat /
C major 갈라치기 / D 현행 유지. 그리고 "2.0 지원은 라이브러리를 다시 만드는 것이 아니라 받아들이는
범위를 넓히는 선언 변경"이라고 못 박는다.

**공개 표면과 내부 표면을 구분한다.** 디자인 토큰은 SemVer 보장 대상이지만
`@seed-design/css/vars/component/*`는 아니다(Rootage spec이 바뀌면 minor·patch에서도 이름이 바뀐다).

> 버튼 배경색이 필요하다면 버튼의 내부 변수를 꺼내 쓰지 말고, 같은 값을 디자인 토큰에서 찾아 쓰세요.

문서 안에 **AI 자가진단 프롬프트**(6단계 점검 요청문)가 포함되어 있다.

### 버저닝 정책

- **2.0을 분기점**으로 정책이 다르다. 2.0 이상은 strict SemVer, 그 미만은 minor·patch에도 breaking이
  있었다.
- 하위 호환의 범위가 코드에 그치지 않는다.

  > 하위 호환은 코드뿐 아니라 **화면**에도 적용됩니다. 색상 토큰은 이름뿐 아니라 **값도 major에서만**
  > 바뀝니다. 디자인 판단에 따른 색상·스타일 변경은 major에서만 일어납니다.

  예외는 잘못된 값의 교정(대비 기준 미달 등)으로, 이는 patch에 포함될 수 있다.
- 1.x 구간은 peer 선언이 부정확해 **손으로 관리하는 호환 매트릭스**(react × css, stackflow × css)를
  문서로 유지한다. ✅ / ⬇️ / ⬆️ / 🚫 표다. 문서에 "SEED React 2부터는 이 표가 필요 없습니다"라고
  명시되어 있다 — peer 범위를 처음에 정확히 잡지 않으면 호환표를 계속 손으로 관리하게 된다는 실증이다.

### Deprecation 관리

`migration/deprecations.mdx`가 스스로를 **원천 파일**로 선언한다("변경 시 이 문서를 먼저 업데이트합니다").

`항목 | 종류 | Deprecated 버전 | 제거 예정 버전 | 대체안 | 비고` 표와, 별도의 **제거 완료 히스토리** 표를
함께 유지한다. 자기 롤백까지 공개 문서에 남긴다.

> `$color.bg.layer-fill` — 2.0.0에서 제거했으나 대안 부재로 2.1.0에서 deprecated 상태로 부활.
> 추후 동일 값의 새 이름 토큰으로 대체 예정.

### Codemod

`@seed-design/codemod`가 jscodeshift 기반 transform 약 20종을 제공한다. 전부 `replace-*` 이름이고
실제 레거시 스택을 겨냥한다 — `replace-tailwind-color`, `replace-tailwind-typography`,
`replace-stitches-styled-color`, `replace-stitches-theme-color`, `replace-alpha-color`,
`replace-react-icon`, `replace-semantic-stroke-color` 등.
옵션으로 `--parser`, `--extensions`, `--ignore-config`, `--log`(combined.log + warnings.log)를 받는다.

### Cascade Layer 대응

- 기본 CSS는 unlayered로 제공하고, `base.layered.css`라는 레이어 래핑 변형을 별도 배포한다
- 레이어는 둘 — `seed-base`(토큰·글로벌·keyframes), `seed-components`(컴포넌트 스타일)
- 권장 순서: `@layer theme, base, seed-base, components, seed-components, utilities;`
- 번들러 export condition **`seed-layered`** 를 추가하면 컴포넌트가 내부 CSS를 import할 때 자동으로
  layered 변형을 쓴다 (Vite `resolve.conditions`, Rsbuild·Webpack `conditionNames`)
- **chunk splitting 함정**: CSS가 여러 청크로 갈리면 `@layer` 선언 순서가 의도와 달라질 수 있다.
  `@layer` 선언을 HTML `<head>` 맨 앞에 인라인해 모든 `<link>`보다 먼저 로드시킨다

### AI 연동

| 수단 | 내용 |
|---|---|
| Docs MCP (`@seed-design/docs-mcp`) | `discover_tools`(도구 자체를 탐색하는 메타 도구), `list_react_components`, `get_react_component`. 카테고리 discovery·react·breeze·design-guidelines·rootage·icons |
| Figma MCP (`@seed-design/mcp`) | PAT 보유 시 REST API 방식, 미보유 시 Figma 플러그인 + WebSocket으로 선택 레이어 실시간 전달 |
| Figma Codegen 플러그인 | Dev Mode에서 작은 단위 코드 생성. 복잡한 작업은 MCP 권장 |
| Agent Skill | `SKILL.md` + `references/{getting-started,components,foundation,usage,migration,upgrade}.md` |
| `llms.txt` / `llms-full.txt` | 섹션별로 분리 제공 |

Agent Skill의 동작이 라우터 구조다 — ① 프로젝트 상태 파악(설정 파일 존재, 패키지 버전, 번들러 감지,
lock 파일로 패키지 매니저 판별) → ② 상황 분류 → ③ 해당 reference 문서로 분기.
우리 루트 [AGENTS.md](../../AGENTS.md)의 영역 라우터와 같은 발상이다.

### 한국어 UX 규범

**Writing** 가이드가 ✅/❌ 대조표다.

| 항목 | ✅ | ❌ |
|---|---|---|
| 익숙한 말 | 상대방과 대화할 수 없어요 | 상대방과 대화가 불가능합니다 |
| 존칭 최소화 | 최대 2개의 글을 등록할 수 있어요 | 등록하실 수 있어요 |
| 숫자 | 당근 1개를 보냈어요 | 당근 한 개를 보냈어요 |
| 축약어 금지 | 끌어올리기 | 끌올 |
| 기능의 목적 | 댓글을 좋아해요 · 채팅으로 거래하기 | 좋아요를 눌렀어요 · 채팅하기 |
| 긍정문 | 한 달에 한 번만 변경할 수 있어요 | 한 달에 한 번 이상 변경할 수 없어요 |
| 능동문 | 보이는 메시지입니다 | 보여지는 메시지입니다 (이중피동) |
| 사용자 행위 | 거래 후기를 보냈어요 | 후기 작성을 완료했어요 |
| 띄어쓰기 | 예약중 (상태값) · 예약 중일 때 (서술) | 반대로 표기 |

가이드에 자기 예외 조항이 붙어 있다.

> 가이드보다는 좋은 문장이 먼저예요. 능동문보다 피동문이 의미 전달이 더 잘 된다면 당연히 피동문을
> 사용해요.

그리고 문체 전환 규칙이 있다 — 자동 발송 메시지이고 사용자가 조심해야 하는 상황이면 '~요' 대신
'~니다'로 문장에 무게를 싣는다. 마침표는 평서문·명령문 끝에만 쓰고, 메인 타이틀·20pt 이상 큰
글씨·버튼·메뉴·라벨에는 쓰지 않는다.

**International Design**은 번역이 아니라 포맷 명세다.

- ko / en-US / en-GB / en-CA / ja 별 상대시간·날짜·요일+시간·거리·통화·전화번호·인용부호 표
- 한국어 점 축약(`2026. 3. 31.`)에서 마지막 점을 반드시 찍는 이유까지 명시(점이 [연][월][일]을 대신)
- 큰 수 단위 차이 — 한국어는 만 단위, 영어는 천 단위. `12,300`이 ko `1.23만` / en `12.3K`
- **한국어 원문 글자수별 번역 확장 비율** — 10자 이하 150–250%, 11–20자 130–150%, 71자 이상 80%
- 구간 표기는 지역과 무관하게 하이픈(`-`)으로 통일(한국어 물결표를 쓰지 않는다)
- 나라별 **가장 긴 동네명 테스트 데이터** 제공. 주소·지역명을 다루는 제품에 그대로 쓸 수 있는 형태다
- Figma pseudoloc 플러그인으로 길이 변화를 사전 테스트

### 기타

- Motion은 매크로/마이크로를 **0.2초**로 가른다. timing function을 곡선이 아니라 용도로 명명한다 —
  `easing`(마이크로) · `enter` · `exit` · `expressive`
- Breeze는 유틸리티 컴포넌트를 별도 네임스페이스로 분리한다(`add breeze:<name>`). 외부 의존
  (예: motion)을 갖는 것들이다. 디자인시스템 본체와 위젯을 구분한다
- 디자인시스템 자체에 브랜딩을 적용했다("디자인 시스템에도 브랜딩이 필요할까"). 당근에 기반하되
  별도 얼굴을 가진 브랜드로 다룬다
- Patterns 계층은 아직 `Loading` 하나뿐이다. Foundations·Components를 먼저 채우고 Patterns를 나중에
  채우는 순서로 진행 중이다

---

## Astryx (Meta)

Meta 내부에서 8년간 성장해 13,000개 이상 앱을 구동한다고 밝힌다. 공개 버전은 0.2.0 Beta다.

### 원칙

- **Components over primitives** — 컴포넌트가 커버하는 범위에서는 raw HTML을 쓰지 않는다
- **Semantic tokens over hardcoded values**
- **Theme-agnostic code** — 앱 코드가 특정 색·치수를 참조하지 않으므로 테마와 다크모드가 자동 동작한다
- **Open internals** — 모든 프리미티브를 export하고 조합 가능하게 해서, 싸우지 않고 위에 쌓을 수 있게 한다

Anti-Pattern을 규칙 목록으로 명문화한다 — raw 요소에 인라인 스타일 금지, 하드코딩 색상 금지,
하드코딩 간격 금지, margin을 주려고 div로 감싸기 금지, `!important` 금지, **props 지어내기 금지**
("컴포넌트 문서를 먼저 읽어라").

### 토큰

- 색상은 전부 CSS `light-dark()`로 한 줄에 두 모드를 담는다: `--color-accent: light-dark(#262626, #ebebeb)`
- 폰트 크기는 **생성식**이다: `round(base × ratio^step)`, 기본값 14px × 1.2, 4xs(6px)–5xl(42px) 12단계
- Duration은 3밴드(fast·medium·slow) × min/base/max이고, min·max는 **base × ratio로 파생**된다
- **데이터 시각화 전용 토큰 계열**을 UI 색과 분리한다 —
  `--color-data-categorical-*`(10색), `--color-data-<hue>-{1..5}` 순차 스케일, `--color-data-neutral`
- 문법 하이라이팅 토큰(`--color-syntax-*`)까지 시스템에 포함한다
- 컨트롤 높이를 토큰화한다 — `--size-element-{sm,md,lg}` (28/32/36px)
- 표면 계층을 이름으로 규정한다 — body → surface → card → popover

### 간격

4px 기본 단위 스케일이며 작은 쪽(2px·4px·6px)만 반단계를 둔다.

> 스케일에 없는 값이 필요하다면, 디자인을 다시 생각하세요.

### 타이포그래피

두 층이 함께 동작한다 — raw 크기 토큰(`--font-size-*`)이 기하 스케일을 이루고, 시맨틱 타입 스케일
토큰(`--text-heading-1-size` 등)이 `var()`로 그것을 참조한다. `defineTheme`에서 base와 ratio만 바꾸면
모든 시맨틱 토큰이 재계산된다.

**줄 높이가 4px 수직 그리드에 스냅된다.** 크기 구간별 목표 비율(20px 미만 1.5, 20–31px 1.4,
32px 이상 1.25)에서 계산한 뒤 4px 배수로 맞추고, 최소 간격 `fontSize + 4px`를 강제한다.
`expandTypeScale` 유틸리티가 자동 계산하므로 line-height를 손으로 지정할 일이 없다.

접근성 장치로 `accessibilityLevel` prop이 있다. 시각적 위계와 문서 개요가 다를 때
(사이드바·카드 제목 등) 시각 스타일은 유지하면서 스크린리더용 레벨만 분리한다.
`<Heading level={1} type="display-1">`처럼 표시 스타일과 HTML 태그를 분리하는 형태도 제공한다.

> 개별 크기 토큰(`--font-size-lg`)을 덮어써서 제목을 "조정"하지 마세요. base·ratio를 바꿔서 전체
> 스케일의 비례가 유지되게 하세요.

### Radius / Shape

크기가 아니라 **계층으로 명명**한다: `inner → element → container → page` (+ `none`, `full`, `chat`).
테마의 radius multiplier가 전체 스케일을 곱하고 `none`·`full`은 고정 앵커로 남는다.

**concentric radius 공식** — 둥근 컨테이너에 padding이 있으면 내부 요소는
`max(0, 바깥radius − padding)`이어야 동심원으로 보인다. Card가 자동 처리한다.

### Elevation

> 얼마나 그림자를 주고 싶은지가 아니라, 표면이 페이지에서 얼마나 떨어져 있는지로 레벨을 고른다.

Elevation은 **쌓임 순서를 인코딩**한다.

| 레벨 | 언제 |
|---|---|
| `none` | 평면이고 표면에 박혀 있다. ChatComposer를 제외한 모든 표면의 기본값 |
| `low` | 일반 흐름 안이지만 배경과 구별되어야 한다 |
| `med` | 같은 페이지의 다른 콘텐츠 위에 뜬다 |
| `high` | UI 전체 위. 보통 backdrop을 동반하거나 포커스를 가져간다 |

- 두 표면이 겹치면 위에 있는 쪽이 더 높은 레벨을 갖는다. **겹치지 않는 표면은 none 아니면 low이며,
  절대 med나 high가 아니다.**
- 본질적 오버레이(Dialog, Popover, Tooltip, Toast, HoverCard, DropdownMenu)는 elevation을 내장하고
  **prop을 노출하지 않는다.** 틀릴 수 없게 만든다.
- 입력 필드는 elevation이 아니라 inset ring(`--shadow-inset-*`)으로 상태를 표현한다. 별개 개념이다.

### 테마 API

`defineTheme({ color, typography, radius, motion, tokens, components, extends })`

- **scale config가 토큰을 생성한다.** `color: {accent, neutralStyle, contrast}`에서
  `--color-background-*`, `--color-text-*` 등이 파생된다. 명시적 `tokens` 오버라이드가 생성값보다 우선한다.
- 파생을 우회할 때의 위험을 명시한다 — `--color-accent`만 손으로 쓰면 파생 토큰 `--color-on-accent`가
  낡은 기본값(흰색)에 머물러 **대비 보장이 깨진다.**
- `extends`의 필드별 병합 규칙이 다르다: tokens는 덮어쓰기, components는 깊은 병합, icons는 얕은 병합,
  fonts는 이어붙이기, scale config(typography·motion·radius·color)는 **통째로 교체**
- **Runtime과 Built 두 모드.** runtime은 `useInsertionEffect`로 `<style>`을 주입하고(개발용),
  `theme build`는 `.css` + `.js`(`__built: true`) + `.d.ts` + `.variants.d.ts`를 생성한다(프로덕션 SSR용).
  runtime을 SSR에 쓰면 컴포넌트 오버라이드가 hydration 시점에 깜빡인다.
- **커스텀 variant** — `'variant:primary-muted'`처럼 없는 값을 쓰면 새 variant로 취급되고, build가
  TypeScript module augmentation을 생성해 JSX에서 타입 안전해진다. 컴포넌트 소스는 건드리지 않는다.
- 중첩 테마 — 사이드바만 dark로 `<Theme>`를 겹쳐 쓸 수 있다

### 스타일링 표면

- `xstyle` prop은 StyleX 값만 받는다(인라인 객체·클래스 문자열 불가). `:hover`는 반드시
  `@media (hover: hover)` 가드 안에 둔다
- `className`과 `style`도 받는다. `style`은 StyleX 인라인 뒤에 병합되어 소비자 값이 이긴다
- rest props를 루트 DOM에 전개한다(`data-*`, `aria-*`, 이벤트, ref). 단 `contentEditable`,
  raw HTML 주입 prop, `children`은 기본 타입에서 **의도적으로 제외**한다 — slot 기반 컴포넌트가
  children을 조용히 버리지 않게 하기 위해서다
- **선호 셀렉터 표면을 명시한다**: 안정 클래스 + data 속성
  (`.astryx-button[data-variant="primary"][data-size="sm"]`)
- **레거시 표면도 명시한다**: `.primary`, `.sm`, `.level-2` 같은 접두어 없는 prop/state 클래스는 여전히
  emit되지만 deprecated다. 베이스 클래스(`.astryx-button`)는 deprecated가 아니다. 무엇이 공개
  표면인지 선을 긋는다
- 함정을 문서화한다 — 컴포넌트는 사전 컴파일되어 배포되므로 그냥 쓰면 설정이 필요 없지만,
  `swizzle`로 소스를 복사해 오면 StyleX 컴파일러가 필요하고 **없으면 에러 없이 스타일이 통째로 사라진다.**
  Next.js App Router에 Babel 플러그인을 넣으면 SWC가 꺼져 `next/font`가 깨지므로 SWC 기반 transform을
  써야 한다

### Layout — 값이 아니라 판단을 규정한다

**Frame First.** 셸을 고르고 → 영역에 px 예산을 배정하고 → 컨테이너 정책을 정하고 → 반응형 계약을
쓴 다음에 콘텐츠를 채운다.

- 예산 기준값: 사이드 내비 240–280, 아이콘 레일 64–72, 인스펙터 340–420, 필터 레일 220–260

**App Archetypes** — 앱 원형이 프레임과 컨테이너 정책을 결정한다. 취향이 아니다.

| 원형 | 프레임 | 컨테이너 정책 |
|---|---|---|
| Tracker / work tool | AppShell + SideNav, 선택 시 인스펙터 | **행만. 카드 0개** |
| Console / observability | AppShell + SideNav 또는 TopNav + TabList | 대시보드 위젯만 카드, 나머지는 Table |
| Messaging / feed | 레일 + 사이드바 + 스트림 + 패널 | 행과 버블. 스트림에 카드 없음 |
| Media library | AppShell + TopNav, 그리드 | ClickableCard 그리드 |
| Settings / forms | AppShell + SideNav | FormLayout 섹션, 카드는 위험·결제 그룹에만 |

**Cards vs Rows**

> 모든 레코드를 Card로 감싸고 Badge를 다는 것이, 앱을 제네릭 AI 프로토타입처럼 보이게 하는 가장
> 빠른 길이다.

스캔·필터·선택하는 밀집 데이터는 행으로 렌더한다(32–40px, edge-to-edge + divider). Card는 자체
완결적 위젯용이다. Badge는 개수와 열거 가능한 상태에만 쓰고, 상태 표시는 StatusDot이나 Token을 쓴다.

**Responsive Contract를 프레임 루트에 주석으로 박아둔다.**

```text
// > 1024px  nav 256 | content | inspector 380
// <= 1024px inspector가 content 위에 오버레이
// <= 768px  nav가 MobileNav 드로어로 접힘
```

### Migration — Tailwind / shadcn / Radix 앱 대상

> 전역 클래스 치환이 아니라 **제품 셸과 워크플로 마이그레이션**으로 취급하라.

순서: 설치·init → Theme 래핑 → **레이어 순서 명시** → **Foundation Smoke Test** →
셸(AppShell·TopNav·SideNav) → 공유 프리미티브 → 전역 워크플로(커맨드 팔레트·설정·테마 토글·검색·필터) →
레거시 클래스 제거 → 라이트/다크·키보드·반응형·빈/에러/로딩 상태 검증.

shadcn·Radix 프리미티브별 대체 매핑표를 제공한다(button → Button·IconButton, select →
Selector·Typeahead, dialog → Dialog·AlertDialog, card-like row → ListItem 등).

> 낡은 shadcn 컴포넌트를 디자인시스템 스타일로 감싸지 마라. 동작·접근성·상태 클래스·토큰 사용을
> 소유한 컴포넌트로 교체하라.

각 라우트 완료 시 검증 체크리스트를 돌리고, 남은 하드코딩 색상·arbitrary hex·일회성 hover 색상을
검색한다.

#### Foundation Smoke Test

레이어가 깨지면 **에러 없이 모든 페이지에서 동일하게** 망가진다. 그래서 마이그레이션 첫 단계에
버리는 페이지 하나를 만들고 런타임 단언을 건다.

```ts
const button = document.querySelector('[data-foundation-check] button');
if (getComputedStyle(button).paddingInline === '0px') {
  throw new Error('Foundation broken: 레이어 밖 reset 또는 뒤 레이어가 컴포넌트 스타일을 덮고 있음');
}
```

> 조용히 실패하고 모든 페이지에서 똑같이 실패하므로, N개 화면을 옮긴 뒤가 아니라 기능 작업 전에 잡아라.

조용한 실패를 시끄럽게 만드는 장치다. 루트 [AGENTS.md](../../AGENTS.md) 최상위 원칙 3번(재발 불가)의
구체적 구현 형태로 참고할 만하다.

#### Cascade Layer Safety

- 레이어가 없는 스타일시트에서 `* { padding: 0 }` 같은 zero-specificity reset은 클래스 셀렉터에 지므로
  대개 무해하다고 여긴다. **레이어가 이 규칙을 두 번 뒤집는다** — unlayered는 모든 named layer를 이기고,
  나중 레이어가 앞 레이어를 이긴다. 둘 다 specificity와 무관하다
- 사고 경로가 둘이다 — ① 최상위 `@import`에 `layer()`가 없으면 unlayered로 남는다
  ② 레이어 안으로 import된 파일 내부의 `@import`는 그 레이어를 상속한다
- **webpack 계열(Next.js 포함)은 `@import` 내용을 뒤따르는 인라인 CSS 위로 호이스팅**하므로, 레이어
  선언은 별도 파일(`layers.css`)에 두고 가장 먼저 import해야 한다
- Tailwind v4는 `preflight.css`를 `layer(base)`로 import한다. v3는 `@tailwind base`를 named layer로
  감싸고 utilities는 unlayered로 남겨 앱 유틸리티가 계속 이기게 한다

### 국제화

- `<InternationalizationProvider locale messages overrides dir>`
- 로케일 폴백 체인 — `pt-BR` → `pt` → 내장 `en`
- **RTL** — `Intl.Locale.getTextInfo()`로 로케일에서 방향을 자동 도출한다. 단 `<html dir>`은 소비자가
  직접 설정해야 한다(`getLocaleDirection()` 헬퍼 제공). 프로바이더는 컴포넌트를, dir 속성은 나머지
  페이지를 담당한다고 역할을 명확히 나눈다
- 한계를 공개한다 — 부분 RTL 영역 안에서 열린 팝업·다이얼로그는 아직 미러링되지 않는다
- **pseudo locale** — 모든 문자열을 `⟦…⟧`로 감싸고 악센트 유사 문자로 치환한다. 하드코딩된 문자열과
  길이 초과 레이아웃이 눈으로 드러난다
- 앱 i18n 라이브러리와 **두 프로바이더가 병존**한다. 네임스페이스를 분리한다(`@astryx.*` 대 `@myapp.*`).
  단일 카탈로그 통합은 로드맵이며 추적 이슈 번호까지 공개한다
- 기여자 규칙을 ESLint로 강제한다 — `@astryx/no-hardcoded-i18n-string`.
  `useDirection()`은 CSS 논리 속성으로 표현할 수 없을 때만 쓴다(방향성 아이콘 교체, 슬라이더 수학,
  키보드 내비게이션)

### 브라우저 지원 — 결정을 소비자에게 넘긴다

> 디자인시스템은 자기 트래픽을 소유하지 않는다. 그 위에 지어진 제품이 소유한다.

그래서 단일 하한선을 선언하는 대신 **티어**를 정의하고 선택을 넘긴다.

| 티어 | 기준 | 사용자 경험 |
|---|---|---|
| Tier 1 (Full fidelity) | 현재 Baseline (2026) | 전부 동작. anchor positioning 포함 |
| Tier 2 (Functional) | Baseline − 2년 (2024) | 열리고 닫히고 사용 가능. **위치만 맞지 않음** |
| Tier 3 | 그 이전 | best-effort. "크래시하지 않음"만 보장 |

- 하한을 만드는 기능은 셋뿐이다 — **CSS Anchor Positioning**(Baseline 2026, 가장 빡빡),
  **Popover API**(2025), **`light-dark()`**(2024 중반). 나머지(`:has()`, `color-mix()`,
  container query, `<dialog>`)는 2023 이전부터 널리 가능하다
- **티어 경계가 실제 기능 절벽에 놓여 있다.** Baseline − 2가 anchor positioning은 사라지고
  Popover와 `light-dark()`는 남아 있는 지점이다. 추측한 날짜가 아니다
- 영향받는 컴포넌트를 명시한다(Tooltip, HoverCard, Popover, ContextMenu, Selector, Tokenizer,
  Carousel). 이들을 쓰지 않으면 anchor positioning 요구사항이 아예 없다
- 티어는 고정이 아니라 Web Baseline 연도를 따라 굴러간다. UA 스니핑을 금지하고 feature detection
  코드를 제공한다

### CLI — 도구 표면의 계약화

CLI가 문서의 정본 경로다. 사람과 기계가 같은 API를 쓴다.

- 명령: `init` · `component` · `search` · `docs` · `template` · `hook` · `swizzle` · `upgrade` ·
  `theme build` · `discover` · `doctor` · `manifest` · `validate-integration`
- 전역 옵션: `--json` · `--detail <brief|compact|full>` · `--dense`(토큰 절약) · `--lang`

**안정적 에러 코드.** `ERR_UNKNOWN_COMPONENT` 등 40여 개의 기계 판독 코드를 정의한다.

> `code` 필드는 안정적인 기계 판독 식별자다. 여기에 분기하고, 사람이 읽는 에러 문자열에는 절대
> 분기하지 마라 — 문구는 개선하면서 자유롭게 바뀐다.
> 코드는 append-only다. 한 번 배포된 코드의 의미는 절대 바뀌지 않고 제거되지도 않는다.

**Capability manifest.** `astryx manifest --json`이 모든 명령·인자·플래그(타입·선택지·기본값)·
`--json` 지원 여부·응답 타입 식별자를 반환한다. 문서 표현이 "CLI를 위한 OpenAPI 명세"다.

> manifest는 Commander 메타데이터에서 파생되므로 실제 명령 정의와 어긋날 수 없다. Commander가
> 추적하지 않는 두 가지 사실은 선언적 맵으로 덧붙이고, **드리프트 테스트가 이를 지킨다** — 명령을
> 추가하면서 설명하지 않으면 CI가 실패한다.

**프로그래매틱 API와 `--json`이 동일하다.** CLI 핸들러는 API 함수의 얇은 래퍼이고, 인자를 파싱해
API를 호출한 뒤 출력만 포맷한다. 두 표면이 항상 같은 데이터를 반환하도록 구조로 보장한다.

**`astryx doctor`** — 읽기 전용 건강 검사. 검사마다 PASS/WARN/FAIL과 **실행 가능한 fix**를 출력한다.
아무것도 설치·변경하지 않으므로 어디서나 안전하다.

> 종료 코드가 계약이다. 실패가 없으면 0(경고는 무방), 하나라도 실패하면 1.

검사 항목: Node 버전 · core 설치 여부 · core↔cli 버전 정합 · 테마 패키지 · config 로드 ·
AI 에이전트 문서 존재 · peer 의존성 · 패키지 매니저 감지.

**설정은 로드 시 strict 스키마로 검증한다.** 알 수 없는 필드는 조용한 무시가 아니라 **하드 에러**다.

### 통합(plugin) 아키텍처

사내 팀이 자기 컴포넌트를 같은 CLI 표면에 얹는 방법이다.

| 파일 | 작성자 | 역할 |
|---|---|---|
| `astryx.config.{ts,mjs,js}` | 소비자 | 로드할 통합 패키지 목록 |
| `astryx.integration.{ts,mjs,js}` | 저자 | 이 패키지가 기여하는 것(`components`, `templates`, `codemods`, `issuesUrl`) |

- 컴포넌트 문서는 소스 옆 `.doc.ts`, 템플릿은 `.template.ts`로 병치한다. 식별자(name·version)는
  `package.json`에서 오고 manifest에 중복 선언하지 않는다
- **codemod를 동봉해 `astryx upgrade`가 소비자 코드를 마이그레이션한다.** `createCodemod`(소스 변환)와
  `createConfigCodemod`(설정 재작성). 소비자는 `hooks.postCodemod`로 후처리를 건다
- **탄력적 디스커버리** — 깨진 통합은 stderr에 경고 한 줄만 내고 건너뛴다. CLI를 죽이지 않고
  `--json` stdout 봉투도 오염시키지 않는다. 나머지 유효한 기여로 일상 명령이 계속 동작한다
- 검증은 로드 경계 한 곳에서 단일 strict 스키마로 한다. `create*` 헬퍼는 검증하지 않는 항등 함수이며
  값어치가 TypeScript 표면뿐이다
- 진단 명령을 따로 둔다 — `validate-integration <package>`(패키지 하나 상세), `doctor`(전체 건강)

### AI 연동

- `init --features agents [--agent claude|cursor|codex]` → `AGENTS.md` / `.claude/CLAUDE.md` /
  `.cursorrules` 생성. 버전을 올린 뒤 다시 실행하면 제자리에서 갱신된다
- 생성된 컨텍스트가 3단계 워크플로를 가르친다 — `template --list`로 유사 페이지를 찾고,
  `template <name> --skeleton`으로 구조를 보고, `component <Name>`으로 쓸 컴포넌트마다 props와 예제를 읽는다
- **`--dense`** 플래그로 모든 명령이 토큰 절약 형식을 지원한다
- **셋업 자가진단 3문항** — Button import 경로 / Dialog 비닫힘 처리 / Selector의 items prop 이름

  > 이 세 질문은 문서 없이는 통과율 0%다. 모델은 셋 다 자신 있게 틀린다.

  에이전트가 스스로 컨텍스트 부족을 감지하게 만드는 장치다
- **MCP 서버** — `search(query)`와 `get(name)` 두 도구. 컴포넌트 문서의 키워드 인덱스를 그대로 쓰므로
  문서 품질이 오르면 검색 품질도 따라 오른다
- 도구별 함정까지 문서화한다 — Cursor 프로젝트 룰은 관련성 판단으로 누락될 수 있으니 User Rule로
  설치하라, 에이전트가 CLI 바이너리 경로를 자주 틀리니 `package.json` 스크립트 별칭을 두라

### 아이콘

컴포넌트의 `icon` prop은 시맨틱 이름 문자열 또는 SVG 컴포넌트를 받는다. 시맨틱 이름은 전역 아이콘
레지스트리로 해석되고, 테마가 `registerIcons()`로 기본 SVG를 교체할 수 있다 — **컴포넌트 코드를
건드리지 않고 아이콘 세트 전체를 교체**할 수 있다. 시맨틱 이름 추가 절차(타입 · 기본 SVG · 문서 표)를
3단계로 명시한다.

### 스타일링 라이브러리 상호운용

> 다른 라이브러리의 시맨틱 API를 노출하되, 값은 시스템 토큰 변수를 가리키게 한다.

가장 좁은 통합 경로를 고르라고 안내한다.

| 경로 | 언제 | 값 형태 |
|---|---|---|
| CSS 변수 별칭 | 라이브러리가 결국 CSS를 쓰고 문자열 값을 받을 때 | `var(--color-text-primary)` |
| StyleX 타입 import | 앱 코드에서 StyleX를 쓸 때 | `colorVars['--color-text-primary']` |
| Tailwind 브리지 | 유틸리티 클래스를 활성 토큰에 연결할 때 | `tailwind-theme.css` |
| 토큰 resolver API | JS가 차트·canvas·SVG·설정 객체용 값이 필요할 때 | `resolveThemeToken(...)` |

Panda·Chakra는 semanticTokens 잎에, MUI는 palette 슬롯에, Emotion·styled-components는 테마 객체 값에
`var()` 참조를 넣는다. **Sass 변수는 컴파일 타임이라 테마 전환에 반응하지 못한다**고 명시적으로 경고한다.
금지 사항으로 raw 값을 두 번째 테마 객체에 복사하기, **동기화되지 않은 두 번째 다크모드 프로바이더**,
다른 라이브러리 변수를 시스템의 진실 소스로 삼기를 든다. 체크리스트의 첫 줄이 "컬러 모드 소유자를
하나만 정하라"다.

---

## 상충하는 선택 — 같은 문제, 다른 답

"시맨틱 토큰"이라는 같은 말이 두 시스템에서 다른 층위를 가리킨다. 고를 때 이 차이를 알고 골라야 한다.

| 문제 | SEED | Astryx |
|---|---|---|
| 간격 이름 | 상황 기반 (`global-gutter`, `nav-to-title`) | 숫자 스케일 (`--spacing-4`) |
| 타입 스케일 | `t1`–`t14` + 시맨틱(`screenTitle`) 이중 체계 | 생성식 `round(base × ratio^step)` |
| 줄 높이 | 크기 토큰과 짝지어 사용 권장 | 구간별 목표 비율 → **4px 그리드 스냅** |
| 다크모드 | 토큰 컬렉션의 mode 축 | CSS `light-dark()` + `[light, dark]` 튜플 |
| 반응형 | 토큰 mode 축(`viewport-width`) + 반응형 prop | 영역 px 예산 + 반응형 계약 주석 |
| 깊이 표현 | **배경 레이어 색 토큰** + Global/Local 쌓임 맥락 | **그림자 토큰** + none/low/med/high |
| 배포 형태 | npm 프리미티브 + **CLI가 스니펫을 소비자 저장소에 생성** | npm 컴포넌트 (+ `swizzle`로 소스 복사) |
| 접근성 기준 | APCA `Lc` 수치로 명시 | 컴포넌트에 내장, 별도 수치 기준 미공개 |
| 브라우저 지원 | 미공개 | **티어 정의 + 결정 위임** |
| 사내 확산 | 라이브러리 저자 3원칙(peer·external·CSS 위임) | 통합 플러그인 + codemod 동봉 |
| 아이콘 | 자체 아이콘 라이브러리 + 마이그레이션 인덱스 | 시맨틱 이름 레지스트리 + `registerIcons()` 교체 |

---

## 우리 결정이 필요한 지점 (미결)

조사에서 드러난, 우리가 설계 시 반드시 한쪽을 골라야 하는 항목이다. 결정은 ADR로 남긴다.

1. 간격·타이포그래피를 **상황 이름**으로 갈지 **숫자 스케일**로 갈지. 매물·지도 도메인이 있어
   상황 이름이 늘어날 여지가 크다
2. 다크모드를 **토큰 mode 축**으로 설계할지 `light-dark()`로 갈지. 후자가 코드는 짧지만 Baseline 2024를
   요구한다
3. 깊이 표현을 **레이어 색**으로 갈지 **그림자**로 갈지. 우리는 지도 위 오버레이가 많다
4. Rootage식 **선언적 스펙 데이터**를 둘지. 우리 규모에서 값어치가 있는지 판단이 필요하다
5. 배포 형태 — 스니펫 생성 모델이 우리 shadcn 현황과 가장 가깝다
6. 브라우저 지원을 티어로 선언할지. 실사용자 브라우저 분포 확인이 선행되어야 한다
7. 사내 확산 시 peer·external 원칙을 처음부터 강제할지. 강제하지 않으면 호환 매트릭스를 손으로
   관리하게 된다는 SEED의 실증이 있다
8. 접근성 기준을 APCA `Lc`로 수치화할지 WCAG 대비비로 갈지

## 조사 범위

**SEED** — `llms.txt` 인덱스, Get Started, Foundations 20편(color 개요·roles·palette,
design-token 개요·reference, typography, spacing, layout, radius, elevation, gradient, motion, state,
iconography 3편, inclusive-design, international-design, voice-and-tone, writing), 컴포넌트 스펙 56편,
Patterns(loading), Design Guidelines(deprecations, migration-reference), React 문서 103파일 중 아키텍처
전량(concepts 4편, library-authors, styling 3편, migration 2편, codemods 2편, figma codegen, cli 2편,
upgrade 3편), AI Integration 3편, Breeze, Updates, Rootage 원본 JSON(index, collections, color,
typography ComponentSpec), Changelog(구조 확인).
미열람 — 아이콘 라이브러리 이름 목록, Lynx 24파일(우리 플랫폼과 무관), React 컴포넌트별 API 레퍼런스.

**Astryx** — `llms.txt`, docs 22편 중 getting-started, principles, tokens, color, typography, spacing,
shape, elevation, theme, styling, styling-libraries, layout, migration, working-with-ai,
internationalization, browser-support, core, cli, cli-integrations.
미열람 — motion·illustrations 개별 값 표(tokens 문서에 중복).
