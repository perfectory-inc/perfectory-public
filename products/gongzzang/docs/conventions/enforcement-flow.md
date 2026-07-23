---
status: current
---

# 자동 강제 흐름 (Enforcement Flow)

> AGENTS.md §4(자동 강제 흐름)·§9(1500줄 안티패턴 경보)에서 이관 (2026-07-20, docs 대개편 Phase B2).

## 강제 단계

```
1. 에디터        rust-analyzer + Biome 확장        실시간 lint/format
2. pre-commit    lefthook + gitleaks               format + 빠른 lint + 시크릿 스캔 + 파일 크기
3. pre-push      lefthook                          typecheck + cargo check/clippy + 링크 체크
4. CI (PR)       GitHub Actions                    풀스택 (lint/type/test/SAST/SCA/cargo-deny/SBOM)
5. CI (merge)    GitHub Actions                    이미지 빌드 + 서명 + 배포
```

컨벤션별 도구 매핑: [README.md](./README.md)의 "자동 강제 도구 매핑" 표.

## 1500줄 안티패턴 경보

`docs/schema.md` 1349줄, `docs/site-builder.md` 1447줄 같은 거대 SSOT 파일(legacy repo 사례 —
현 트리에는 없음) = **이름만 SSOT**. 폴더 단위 SSOT가 진짜 SSOT.

- 500줄 도달 → 분해 검토
- 1500줄 도달 → CI가 차단
- *처음부터* 폴더로 시작
