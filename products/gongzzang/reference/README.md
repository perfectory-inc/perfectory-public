---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# reference/

외부 오픈소스 코드 학습 자료. **저장소에 커밋하지 않음** (`.gitignore`가 `reference/*/`를 무시하고 이 README만 추적).

## 권장 clone (개발자가 직접)

```bash
# 도메인 지식 학습용
git clone https://github.com/UrbanWatcherKr/korean-land-mcp reference/korean-land-mcp
git clone https://github.com/chrisryugj/korean-law-mcp reference/korean-law-mcp
```

각 저장소의 라이선스와 용도는 [docs/data-sources/korean-land-mcp.md](../docs/data-sources/korean-land-mcp.md),
[docs/data-sources/korean-law.md](../docs/data-sources/korean-law.md)를 따른다.

## 활용 방식

- 법제처 API 호출 패턴 학습
- 응답 파싱·에러 처리 reference
- V-World 등 Catalog 원천은 Foundation Platform 소유다 — 학습만 하고 Gongzzang에 어댑터를 만들지 않는다
  ([crates/data-clients/README.md](../crates/data-clients/README.md)).

## 금지

- ❌ `packages/`, `apps/`에서 reference/를 import 하지 말 것
- ❌ reference 코드를 그대로 복사하지 말 것 (저작권)
- ✅ 학습 후 우리 도메인 모델로 직접 재작성
- ✅ 번들하는 서드파티 자료의 고지는 루트 [`THIRD_PARTY_NOTICES.md`](../../../THIRD_PARTY_NOTICES.md)에 등록
