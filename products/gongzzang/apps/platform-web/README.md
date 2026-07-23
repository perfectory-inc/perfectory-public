# apps/platform-web

> **⚠️ 스텁 (README-only 자리표시자).** 이 디렉토리에 코드는 없다.
> **실제 메인 사용자 사이트는 [`apps/web`](../web/)이다** (Next.js, App Router).
> 아래 의존으로 나열된 `@gongzzang/{api-client,ui-web,map,shared,tsconfig}` 패키지도
> 전부 README 스텁이며 코드 실물이 아니다 (2026-07-20 기준). 지시로 사용하지 말 것 —
> 자리표시자 정리는 문서 대개편 Phase C에서 결정한다.

당초 계획: 메인 사용자 사이트 (매수자/매도자/중개사/시행사/기업).

## 정책 (apps/web 에 동일 적용 중)

- LLM/MCP import **금지** (옵션 A 준수)
- 비즈니스 로직 0줄 (Server Action = 얇은 프록시만)
- 한국어 단일 (i18n 미사용)
