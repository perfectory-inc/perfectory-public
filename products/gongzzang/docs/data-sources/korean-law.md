---
status: current
owner: gongzzang-제품
doc_type: reference
last_reviewed: 2026-10-28
---

# 법제처 국가법령정보 — 제품 API 와 에이전트 MCP

법제처 원천은 두 경로로 쓴다. 제품 코드는 공식 API 만, 개발자 에이전트 세션은 MCP 만 쓴다
([GZ-ADR-0010](../adr/0010-scope-information-platform-option-a.md) 옵션 A: 사용자 노출 텍스트를 LLM 이 생성하지 않는다).

## 1. 제품 경로 — 공식 API (아직 구현 없음)

이 저장소에는 법제처 client 도, 법령 원문 표도, `KOREAN_LAW_API_KEY` 를 읽는 코드도 아직 없다. 아래는 도입할 때의
설계 기준이며, 어댑터는 [crates/data-clients](../../crates/data-clients/README.md) 규칙대로 ADR 과 경계 갱신 뒤에
추가한다.

- 운영 기관: 법제처, 공식 사이트 <https://open.law.go.kr>
- 우리 사용: 법령 본문, 조례, 시행령, 별표/서식
- 인증: 회원가입 → API 사용 신청 → 인증키 발급(즉시). 환경변수 `KOREAN_LAW_API_KEY`
- 한도: 일별 한도(정확한 수치는 사이트 확인). 본문 조회가 무거워 캐시가 필수다.

| endpoint | 용도 |
|----------|------|
| `/DRF/lawSearch.do` | 법령 검색 |
| `/DRF/lawService.do` | 법령 본문 조회 |
| `/DRF/lawJoSearch.do` | 조 검색 |
| `/DRF/precSearch.do` | 판례 검색 |
| `/DRF/lawAppendix.do` | 별표/서식 추출 |
| `/DRF/orderSearch.do` | 행정규칙 |

```
GET https://www.law.go.kr/DRF/lawSearch.do?
  OC={KOREAN_LAW_API_KEY}
  &target=law
  &type=XML
  &query=국토의 계획 및 이용에 관한 법률
```

응답은 target 에 따라 XML 또는 JSON 이다.

Circuit Breaker 정책: timeout 15초(큰 본문), retry 1회, fallback 은 캐시 응답(법령은 자주 안 바뀐다).

| 캐시 종류 | TTL |
|------|-----|
| 법령 검색 결과 | 1시간 |
| 법령 본문 | 7일 (개정 시 무효화) |
| 별표/서식 | 30일 |

라이선스: 모든 데이터가 공공저작물이며 재배포 자유, 출처 표기 필수. 매물 분석 화면에 "출처: 법제처"를 표기한다.

사용자 노출 정책:

- 법령 본문은 저장된 원문 *그대로* 표시한다.
- 정식 명칭 + 조·항·호로 인용한다("국토의 계획 및 이용에 관한 법률 제76조제5항").
- LLM 이 *생성*한 법령 인용과 "관련 법령에 따라" 같은 추상적 인용은 금지한다.

원문 보존: 법령 텍스트는 감사와 분쟁 시 인용 검증을 위해 영구 보존한다. 표 설계는 도입 ADR 이 정한다.

임베딩(나중 단계): 법령 본문을 벡터화해 시맨틱 검색에 쓴다([GZ-ADR-0011](../adr/0011-embedding-gemini-pgvector.md)).

## 2. 에이전트 경로 — korean-law-mcp

- 소스: <https://github.com/chrisryugj/korean-law-mcp> (MIT)
- 역할: 법제처 41개 API 를 16개 도구로 묶어 법령·판례·조례·조약 검색과 환각 방지를 제공한다.
- 제품 코드에서 사용 금지. 개발자 Claude 세션의 탐색·검증용이다.
- 실행: 공식 원격 배포 `korean-law-mcp.fly.dev` 또는 로컬 `npx korean-law-mcp`. 인증은 같은 `KOREAN_LAW_API_KEY`
  (발급: <https://open.law.go.kr/LSO/openApi/guideList.do>).
- 캐시: 검색 1시간, 문서 전문 24시간. 문서 포맷: HWPX/HWP, PDF, XLSX, DOCX.

| 묶음 | 도구 |
|------|------|
| Core | `search_laws`(법령 검색), `get_law_text`(전문), `get_annex`(별표/서식) |
| Unified | `search_decisions`(판례·헌재·조세심판·공정위 통합 검색), `get_decision_text`(전문) |
| Chain | `full_legal_analysis`, `law_system_map`, `admin_action_basis`, `dispute_prep`, `amendment_tracking`, `ordinance_comparison`, `document_review` 등 |
| Meta | `discover_tools`(자연어 도구 탐색), `execute`(범용 프록시) |

`verify_citations` 는 LLM 이 쓴 법령 인용을 실시간으로 대조한다. 에이전트가 법령 인용을 담은 산출물을 쓸 때는
먼저 이것으로 인용을 확인한다.

```
입력: "국토계획법 제76조제5항에 따라 우선위임된다."
출력:
  - "국토의 계획 및 이용에 관한 법률 제76조제5항": ✓ (존재)
```

V-World 기반 필지 분석 MCP 와 함께 쓰는 흐름은 [korean-land-mcp](./korean-land-mcp.md)에 있다.
